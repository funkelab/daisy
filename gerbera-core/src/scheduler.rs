use crate::block::{Block, BlockStatus};
use crate::dependency_graph::DependencyGraph;
use crate::processing_queue::ProcessingQueue;
use crate::ready_surface::ReadySurface;
use crate::task::Task;
use crate::task_state::TaskState;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// Core scheduler: tracks task states, dispatches blocks to workers, and
/// updates the dependency graph as blocks complete.
pub struct Scheduler {
    #[allow(dead_code)]
    dependency_graph: DependencyGraph,
    // ReadySurface stores closures that capture an Arc to the DependencyGraph's
    // internals. We use a raw pointer approach instead: the ReadySurface is
    // generic over closures, so we store the functions directly.
    ready_surface: ReadySurface<
        Box<dyn Fn(&Block) -> Vec<Block>>,
        Box<dyn Fn(&Block) -> Vec<Block>>,
    >,
    pub task_map: HashMap<String, Arc<Task>>,
    pub task_states: HashMap<String, TaskState>,
    task_queues: HashMap<String, ProcessingQueue>,
    count_all_orphans: bool,
}

impl Scheduler {
    pub fn new(tasks: &[Arc<Task>], count_all_orphans: bool) -> Self {
        let dependency_graph = DependencyGraph::new(tasks);

        // We need the dependency_graph to live as long as the closures, but
        // the scheduler owns both. Use a shared pointer.
        let dg = Arc::new(dependency_graph);
        let dg_down = Arc::clone(&dg);
        let dg_up = Arc::clone(&dg);

        let downstream_fn: Box<dyn Fn(&Block) -> Vec<Block>> =
            Box::new(move |block| dg_down.downstream(block));
        let upstream_fn: Box<dyn Fn(&Block) -> Vec<Block>> =
            Box::new(move |block| dg_up.upstream(block));

        let ready_surface = ReadySurface::new(downstream_fn, upstream_fn);

        let mut task_map = HashMap::new();
        let mut task_states: HashMap<String, TaskState> = HashMap::new();
        let mut task_queues: HashMap<String, ProcessingQueue> = HashMap::new();

        // Initialize root tasks with their generators.
        let roots = dg.roots();
        for (task_id, (num_roots, root_blocks)) in roots {
            let state = task_states.entry(task_id.clone()).or_default();
            state.ready_count += num_roots;
            let root_iter = Box::new(root_blocks.into_iter()) as Box<dyn Iterator<Item = Block> + Send>;
            task_queues.insert(task_id, ProcessingQueue::new(num_roots, Some(root_iter)));
        }

        for task in tasks {
            init_task(task, &dg, &mut task_map, &mut task_states, &mut task_queues);
        }

        // Extract the inner DependencyGraph back from the Arc. Since we only
        // have the original Arc left (the closures hold clones), we can't
        // unwrap it. Instead, store a fresh DependencyGraph. The closures in
        // ready_surface hold their own Arc clones.
        let dependency_graph = DependencyGraph::new(tasks);

        Self {
            dependency_graph,
            ready_surface,
            task_map,
            task_states,
            task_queues,
            count_all_orphans,
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
            .filter(|(_, state)| state.ready_count > 0)
            .filter_map(|(id, _)| self.task_map.get(id).cloned())
            .collect()
    }

    /// Get the next ready block for a task. Runs the check function to skip
    /// already-completed blocks.
    pub fn acquire_block(&mut self, task_id: &str) -> Option<Block> {
        loop {
            let block = self.task_queues.get_mut(task_id)?.get_next()?;

            {
                let state = self.task_states.get_mut(task_id).unwrap();
                state.ready_count -= 1;
                state.processing_count += 1;
            }

            // Run the pre-check: skip if block is already done.
            let already_done = self.precheck(task_id, &block);
            if already_done {
                debug!(block_id = %block.block_id, "skipping already-processed block");
                let mut block = block;
                block.status = BlockStatus::Success;
                self.task_states.get_mut(task_id).unwrap().skipped_count += 1;
                self.task_queues
                    .get_mut(task_id)
                    .unwrap()
                    .processing_blocks
                    .insert(block.block_id.clone());
                self.release_block(block);
                continue;
            }

            self.task_states.get_mut(task_id).unwrap().started = true;
            self.task_queues
                .get_mut(task_id)
                .unwrap()
                .processing_blocks
                .insert(block.block_id.clone());
            return Some(block);
        }
    }

    /// Update the dependency graph with a completed or failed block.
    /// Returns task_ids whose state changed.
    pub fn release_block(&mut self, block: Block) -> HashMap<String, TaskState> {
        let task_id = block.task_id().to_string();
        self.remove_from_processing(&block);

        match block.status {
            BlockStatus::Success => {
                let new_blocks = self.ready_surface.mark_success(&block);
                self.task_states
                    .get_mut(&task_id)
                    .unwrap()
                    .completed_count += 1;
                self.update_ready_queue(new_blocks)
            }
            BlockStatus::Failed => {
                let task = self.task_map.get(&task_id).unwrap();
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
                    let state = self.task_states.get_mut(&task_id).unwrap();
                    state.failed_count += 1;
                    for orphan in &orphans {
                        let orphan_task = orphan.task_id().to_string();
                        self.task_states
                            .get_mut(&orphan_task)
                            .unwrap()
                            .orphaned_count += 1;
                    }
                    HashMap::new()
                } else {
                    debug!(block_id = %block.block_id, retries = *retries, "temporarily failed, re-queuing");
                    *retries += 1;
                    let _ = queue;
                    self.queue_ready_block(block.clone());
                    let state = self.task_states[&task_id].clone();
                    HashMap::from([(task_id, state)])
                }
            }
            other => panic!("unexpected status for released block: {other:?}"),
        }
    }

    fn precheck(&self, task_id: &str, block: &Block) -> bool {
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

    fn queue_ready_block(&mut self, block: Block) {
        let task_id = block.task_id().to_string();
        let queue = self.task_queues.get_mut(&task_id).unwrap();
        queue.ready_queue.push_back(block);
        self.task_states.get_mut(&task_id).unwrap().ready_count += 1;
    }

    fn remove_from_processing(&mut self, block: &Block) {
        let task_id = block.task_id().to_string();
        let queue = self.task_queues.get_mut(&task_id).unwrap();
        queue.processing_blocks.remove(&block.block_id);
        self.task_states.get_mut(&task_id).unwrap().processing_count -= 1;
    }

    fn update_ready_queue(&mut self, ready_blocks: Vec<Block>) -> HashMap<String, TaskState> {
        let mut updated = HashMap::new();
        for block in ready_blocks {
            let task_id = block.task_id().to_string();
            self.queue_ready_block(block);
            let state = self.task_states[&task_id].clone();
            updated.insert(task_id, state);
        }
        updated
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
    let state = task_states.entry(task_id.clone()).or_default();
    state.total_block_count = num_blocks;

    // Ensure a queue exists (roots are already initialized).
    task_queues
        .entry(task_id.clone())
        .or_insert_with(|| ProcessingQueue::new(0, None));

    for upstream in task.requires() {
        init_task(upstream, dg, task_map, task_states, task_queues);
    }
}
