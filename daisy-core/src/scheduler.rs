use crate::block::{Block, BlockStatus};
use crate::dependency_graph::DependencyGraph;
use crate::block_tracking::{TaskSummary, TaskTracking};
use crate::error::DaisyError;
use crate::processing_queue::ProcessingQueue;
use crate::ready_surface::ReadySurface;
use crate::task::Task;
use crate::task_state::{RunningTask, TaskState};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, warn};

/// Core scheduler: tracks task states, dispatches blocks to workers, and
/// updates the dependency graph as blocks complete.
pub struct Scheduler {
    /// The task DAG. `Arc` because the closures inside `ready_surface`
    /// also need to call `downstream`/`upstream` on it — sharing one
    /// allocation between the field and the closures, instead of
    /// rebuilding the graph just so we can have an owned copy here.
    dependency_graph: Arc<DependencyGraph>,
    ready_surface: ReadySurface<
        Box<dyn Fn(&Block) -> Vec<Block>>,
        Box<dyn Fn(&Block) -> Vec<Block>>,
    >,
    pub task_map: HashMap<String, Arc<Task>>,
    pub task_states: HashMap<String, TaskState>,
    task_queues: HashMap<String, ProcessingQueue>,
    count_all_orphans: bool,
    /// Per-task persistent block tracking, keyed by task_id. Opened by
    /// `init_tracking` for tasks with a tracking path configured. Also
    /// the single owner of this run's per-task aggregates — there is no
    /// second block counter anywhere.
    task_tracking: HashMap<String, TaskTracking>,
}

impl Scheduler {
    pub fn new(pipeline: &crate::pipeline::Pipeline, count_all_orphans: bool) -> Self {
        let dependency_graph = Arc::new(DependencyGraph::from_pipeline(pipeline));
        let tasks: &[Arc<Task>] = &pipeline.tasks;

        let dg_down = Arc::clone(&dependency_graph);
        let dg_up = Arc::clone(&dependency_graph);
        let downstream_fn: Box<dyn Fn(&Block) -> Vec<Block>> =
            Box::new(move |block| dg_down.downstream(block));
        let upstream_fn: Box<dyn Fn(&Block) -> Vec<Block>> =
            Box::new(move |block| dg_up.upstream(block));
        let ready_surface = ReadySurface::new(downstream_fn, upstream_fn);

        let mut task_map = HashMap::new();
        let mut task_states: HashMap<String, TaskState> = HashMap::new();
        let mut task_queues: HashMap<String, ProcessingQueue> = HashMap::new();

        // Initialize root tasks with their generators. All tasks start
        // as `Running` — terminal variants only appear after the
        // matching transition.
        let roots = dependency_graph.roots();
        for (task_id, (num_roots, root_iter)) in roots {
            let entry = task_states
                .entry(task_id.clone())
                .or_insert_with(|| TaskState::Running(RunningTask::default()));
            if let Some(rt) = entry.as_running_mut() {
                rt.ready_count += num_roots;
            }
            task_queues.insert(task_id, ProcessingQueue::new(num_roots, Some(root_iter)));
        }

        for task in tasks {
            init_task(task, &dependency_graph, &mut task_map, &mut task_states, &mut task_queues);
        }

        Self {
            dependency_graph,
            ready_surface,
            task_map,
            task_states,
            task_queues,
            count_all_orphans,
            task_tracking: HashMap::new(),
        }
    }

    /// Open the block tracking configured on tasks. Must be called after
    /// `new` and before any blocks are processed. On failure (layout
    /// mismatch / IO error) returns the error and leaves the scheduler
    /// without tracking — the caller should treat this as fatal.
    ///
    /// A task asking for `resource_tracking` without a tracking path has
    /// nowhere to write its measurements; that is a configuration error
    /// rather than something to silently ignore.
    pub fn init_tracking(&mut self) -> Result<(), DaisyError> {
        for (task_id, task) in &self.task_map {
            let Some(ref dir) = task.tracking_path else {
                if task.resource_tracking {
                    return Err(DaisyError::InvalidConfig(format!(
                        "task {task_id:?} sets resource_tracking=True but has no tracking \
                         directory to write to. Pass Task(tracking_path=...) or call \
                         daisy.set_tracking_basedir(...) once at pipeline start."
                    )));
                }
                continue;
            };
            match TaskTracking::open_or_create(
                dir,
                &task.total_roi,
                &task.read_roi,
                &task.write_roi,
                &task.fit,
                task.resource_tracking,
            ) {
                Ok(tracking) => {
                    debug!(
                        task_id = %task_id,
                        path = %dir.display(),
                        capacity = tracking.capacity(),
                        already_done = tracking.count_done(),
                        with_stats = tracking.with_stats(),
                        "block tracking opened",
                    );
                    self.task_tracking.insert(task_id.clone(), tracking);
                }
                Err(e) => {
                    warn!(task_id = %task_id, error = %e, "failed to open block tracking");
                    return Err(DaisyError::InvalidConfig(format!("{e}")));
                }
            }
        }
        Ok(())
    }

    /// This run's per-task aggregates, for the end-of-run summary. Tasks
    /// without tracking configured simply don't appear.
    pub fn tracking_summaries(&self) -> HashMap<String, TaskSummary> {
        self.task_tracking
            .iter()
            .map(|(id, t)| (id.clone(), t.summary().clone()))
            .collect()
    }

    /// Whether any task expects blocks to come back carrying resource
    /// stats — used to police workers that return unmeasured blocks.
    pub fn expects_block_stats(&self, task_id: &str) -> bool {
        self.task_tracking
            .get(task_id)
            .map(|t| t.with_stats())
            .unwrap_or(false)
    }

    /// Note one failed attempt for a block (worker-reported failure or a
    /// timeout reclaim). Failure counts are kept whenever tracking is on
    /// at all, independent of resource tracking.
    pub fn note_block_failure(&mut self, block: &Block) {
        if let Some(tracking) = self.task_tracking.get_mut(block.task_id()) {
            tracking.note_failure(block);
        }
    }

    /// Access the dependency graph for upstream/downstream queries.
    pub fn dependency_graph(&self) -> &DependencyGraph {
        &self.dependency_graph
    }

    /// Get tasks that currently have blocks available for scheduling.
    pub fn get_ready_tasks(&self) -> Vec<Arc<Task>> {
        self.task_states
            .iter()
            .filter_map(|(id, state)| match state {
                TaskState::Running(rt) if rt.ready_count > 0 => self.task_map.get(id).cloned(),
                _ => None,
            })
            .collect()
    }

    /// Helper: borrow the inner `RunningTask` mutably, or `None` if
    /// the task has reached a terminal phase. Single gate for every
    /// counter-mutating operation.
    fn running_mut(&mut self, task_id: &str) -> Option<&mut RunningTask> {
        self.task_states.get_mut(task_id)?.as_running_mut()
    }

    /// Get the next ready block for a task. Runs the check function to skip
    /// already-completed blocks. Returns `None` if the task is in a
    /// terminal phase or has no ready work.
    pub fn acquire_block(&mut self, task_id: &str) -> Option<Block> {
        loop {
            // Don't hand out blocks for terminal tasks.
            if !self.task_states.get(task_id)?.is_running() {
                return None;
            }

            let block = self.task_queues.get_mut(task_id)?.get_next()?;

            if let Some(rt) = self.running_mut(task_id) {
                rt.note_acquired();
            }

            // Run the pre-check: skip if block is already done.
            let already_done = self.precheck(task_id, &block);
            if already_done {
                debug!(block_id = %block.block_id, "skipping already-processed block");
                let mut block = block;
                block.status = BlockStatus::Success;
                if let Some(rt) = self.running_mut(task_id) {
                    rt.note_skip_inflight();
                }
                self.task_queues
                    .get_mut(task_id)
                    .unwrap()
                    .processing_blocks
                    .insert(block.block_id.clone());
                self.release_block(block);
                continue;
            }

            self.task_queues
                .get_mut(task_id)
                .unwrap()
                .processing_blocks
                .insert(block.block_id.clone());
            return Some(block);
        }
    }

    /// Update the dependency graph with a completed or failed block.
    /// Returns the set of task ids that just transitioned from
    /// `ready_count == 0` to having ready work — the only signal the
    /// run loop uses for opportunistic rebalancing. The caller still
    /// inspects `task_states` directly when it needs more.
    pub fn release_block(&mut self, block: Block) -> Vec<String> {
        let task_id = block.task_id().to_string();

        // Strip from in-flight tracking unconditionally — even for
        // terminal tasks, the bookkeeper has already marked the block
        // sent and we don't want stale entries in `processing_blocks`.
        if let Some(queue) = self.task_queues.get_mut(&task_id) {
            queue.processing_blocks.remove(&block.block_id);
        }

        // Counter mutations only proceed if the task is Running. This
        // is the single guard that defangs every "late message lands
        // after abandonment" race — see abandon.md.
        if self.running_mut(&task_id).is_none() {
            debug!(
                block_id = %block.block_id,
                "dropping release for non-running task",
            );
            return Vec::new();
        }

        match block.status {
            BlockStatus::Success => {
                let new_blocks = self.ready_surface.mark_success(&block);
                if let Some(rt) = self.running_mut(&task_id) {
                    rt.note_completed();
                }
                if let Some(tracking) = self.task_tracking.get_mut(&task_id) {
                    // Records done + any resource stats the block carried,
                    // and folds both into this run's aggregates.
                    tracking.record(&block);
                }
                let newly_ready = self.update_ready_queue(new_blocks);
                self.maybe_finalize(&task_id);
                newly_ready
            }
            BlockStatus::Failed => {
                // Every failed attempt counts, whether it will be retried
                // or is permanent.
                if let Some(tracking) = self.task_tracking.get_mut(&task_id) {
                    tracking.note_failure(&block);
                }
                let task = self.task_map.get(&task_id).unwrap().clone();
                let max_retries = task.max_retries;
                let queue = self.task_queues.get_mut(&task_id).unwrap();
                let retries = queue
                    .block_retries
                    .entry(block.block_id.clone())
                    .or_insert(0);

                if *retries >= max_retries {
                    debug!(block_id = %block.block_id, "permanently failed");
                    let orphans = self
                        .ready_surface
                        .mark_failure(&block, self.count_all_orphans);
                    if let Some(rt) = self.running_mut(&task_id) {
                        rt.note_failed_permanently();
                    }
                    for orphan in &orphans {
                        let orphan_task = orphan.task_id().to_string();
                        if let Some(rt) = self.running_mut(&orphan_task) {
                            rt.note_orphaned();
                        }
                    }
                    self.maybe_finalize(&task_id);
                    for orphan in &orphans {
                        self.maybe_finalize(orphan.task_id());
                    }
                    Vec::new()
                } else {
                    debug!(block_id = %block.block_id, retries = *retries, "temporarily failed, re-queuing");
                    *retries += 1;
                    let _ = queue;
                    self.push_to_ready_queue(block.clone());
                    if let Some(rt) = self.running_mut(&task_id) {
                        rt.note_failed_for_retry();
                    }
                    // Re-queuing a single retry is a transition from
                    // 0 → 1 ready only if the task had no other
                    // ready work; the run loop re-checks before
                    // spawning so reporting it unconditionally is
                    // fine and slightly cheaper.
                    vec![task_id]
                }
            }
            other => panic!("unexpected status for released block: {other:?}"),
        }
    }

    fn precheck(&self, task_id: &str, block: &Block) -> bool {
        // The done array is the cheapest check (single byte), do it first.
        if let Some(tracking) = self.task_tracking.get(task_id) {
            if tracking.is_done(block) {
                return true;
            }
        }
        if let Some(task) = self.task_map.get(task_id) {
            if let Some(ref check_fn) = task.check_function {
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    check_fn.check(block)
                })) {
                    Ok(result) => result,
                    Err(_) => false,
                }
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Push a block into a task's ready queue and bump its
    /// ready_count. Silently drops the block if the destination task
    /// is no longer Running — covers the race where an upstream
    /// release generates a downstream block for a task that's just
    /// been transitively abandoned.
    ///
    /// Used for *new* ready work (downstream blocks unlocked by
    /// successful upstream releases). For the retry path see
    /// `push_to_ready_queue` — a Failed-block re-queue moves the
    /// block from Processing back to Ready, which is a different
    /// counter mutation handled by `note_failed_for_retry`.
    fn queue_ready_block(&mut self, block: Block) {
        let task_id = block.task_id().to_string();
        if self.running_mut(&task_id).is_none() {
            debug!(
                block_id = %block.block_id,
                task_id = %task_id,
                "dropping new ready block for non-running task",
            );
            return;
        }
        let queue = self.task_queues.get_mut(&task_id).unwrap();
        queue.ready_queue.push_back(block);
        if let Some(rt) = self.running_mut(&task_id) {
            rt.note_ready();
        }
    }

    /// Push a block into the ready queue for an existing retry.
    /// Counter mutation is the caller's responsibility (typically
    /// `note_failed_for_retry`, which moves a block from Processing
    /// to Ready).
    fn push_to_ready_queue(&mut self, block: Block) {
        let task_id = block.task_id().to_string();
        if self.running_mut(&task_id).is_none() {
            return;
        }
        let queue = self.task_queues.get_mut(&task_id).unwrap();
        queue.ready_queue.push_back(block);
    }

    /// Push each new ready block into its task's queue, returning
    /// the deduped set of task ids whose queue actually grew (a
    /// block dropped by the typestate gate doesn't count).
    fn update_ready_queue(&mut self, ready_blocks: Vec<Block>) -> Vec<String> {
        let mut newly_ready = Vec::new();
        for block in ready_blocks {
            let task_id = block.task_id().to_string();
            let pre = self
                .task_states
                .get(&task_id)
                .map(|s| s.counters().ready_count)
                .unwrap_or(0);
            self.queue_ready_block(block);
            let post = self
                .task_states
                .get(&task_id)
                .map(|s| s.counters().ready_count)
                .unwrap_or(0);
            if post > pre && !newly_ready.contains(&task_id) {
                newly_ready.push(task_id);
            }
        }
        newly_ready
    }

    /// Transition the task to `Done` if its counters are balanced.
    /// Called after every counter change.
    fn maybe_finalize(&mut self, task_id: &str) {
        if let Some(state) = self.task_states.get_mut(task_id) {
            state.try_finalize_done();
        }
    }
}

fn init_task(
    task: &Arc<Task>,
    dg: &Arc<DependencyGraph>,
    task_map: &mut HashMap<String, Arc<Task>>,
    task_states: &mut HashMap<String, TaskState>,
    task_queues: &mut HashMap<String, ProcessingQueue>,
) {
    let task_id = &task.task_id;
    if task_map.contains_key(task_id) {
        return;
    }
    task_map.insert(task_id.clone(), task.clone());
    let num_blocks = dg.num_blocks(task_id);
    let entry = task_states
        .entry(task_id.clone())
        .or_insert_with(|| TaskState::Running(RunningTask::default()));
    if let Some(rt) = entry.as_running_mut() {
        rt.total_block_count = num_blocks;
        rt.timeout_secs = task.timeout.map(|d| d.as_secs_f64());
    }

    // Ensure a queue exists (roots are already initialized).
    task_queues
        .entry(task_id.clone())
        .or_insert_with(|| ProcessingQueue::new(0, None));
}
