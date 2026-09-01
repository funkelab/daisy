use crate::block::BlockStatus;
use crate::block_bookkeeper::BlockBookkeeper;
use crate::protocol::{read_message, write_message, Message};
use crate::resource_allocator::{ResourceAllocator, ResourceBudget};
use crate::block_tracking::TaskSummary;
use crate::scheduler::Scheduler;
use crate::task::Task;
use crate::task_state::{AbandonReason, TaskCounters, TaskState};
use crate::worker_context::encode_value;
use crate::worker_pool::WorkerPool;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread::JoinHandle;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Observer hook fired whenever per-task counts change (block release,
/// failure, retry). Implementations should throttle expensive work
/// themselves — this is called once per state-changing event, which can
/// be tens-of-thousands of times per second on large runs.
///
/// Receives a `TaskCounters` snapshot (frozen at the time of the call)
/// rather than the live `TaskState` enum — observers don't transition
/// state, they just display it, so the read-only snapshot is the
/// minimal API.
pub trait ProgressObserver: Send + Sync {
    fn on_progress(&self, states: &HashMap<String, TaskCounters>);
    /// Called once at startup, before any blocks are dispatched, so
    /// observers can size their displays / open progress bars from the
    /// known total_block_count.
    fn on_start(&self, _states: &HashMap<String, TaskCounters>) {}
    /// Called once after the run loop exits. Observers should close
    /// their progress bars / flush state here.
    fn on_finish(&self, _states: &HashMap<String, TaskCounters>) {}
}

fn snapshot_counters(states: &HashMap<String, TaskState>) -> HashMap<String, TaskCounters> {
    states
        .iter()
        .map(|(k, v)| (k.clone(), v.counters()))
        .collect()
}

struct ClientMessage {
    message: Message,
    addr: SocketAddr,
    reply_tx: mpsc::Sender<Message>,
}

/// What a worker thread reports as it exits.
///
/// Deliberately not a statistics type: resource measurements belong to
/// blocks (`Block::stats` → `block_tracking`), and mixing the two is what
/// let a per-worker block counter drift away from the scheduler's. The
/// only payload here is control flow — `last_error` is what abandonment
/// quotes when it explains why a task died.
#[derive(Debug, Default)]
struct WorkerExit {
    task_id: String,
    #[allow(dead_code)]
    worker_id: u64,
    last_error: Option<String>,
}

pub struct Server {
    host: String,
    port: u16,
}

/// Info needed to spawn/respawn a worker thread.
struct WorkerSpec {
    task_id: String,
    task: Arc<Task>,
    worker_id: u64,
    host: String,
    port: u16,
}

enum WorkerThread {
    /// Running. The bool result: true = clean exit, false = should respawn.
    Running(JoinHandle<bool>),
    Finished,
}

/// One spawned worker: its spec, its thread, and whether its allocator
/// slot has already been given back.
struct WorkerEntry {
    spec: WorkerSpec,
    thread: WorkerThread,
    /// Set when a timeout reclaim retires this worker's slot while the
    /// thread is still running (the worker is stuck holding a block and
    /// can no longer be counted alive). The eventual thread exit must
    /// not release the slot a second time.
    slot_retired: bool,
}

/// What the server knows about worker lifecycles beyond the thread
/// handles: which server-assigned worker ids ever spoke to us over TCP,
/// and which spawn threads have exited. Comparing the two catches
/// fire-and-forget spawn functions — a 0-arg spawn function must block
/// for its worker's lifetime (e.g. `sbatch --wait`, `srun`), because
/// every liveness decision (rebalancing, the start budget, abandonment)
/// reads "worker alive" as "the spawn call hasn't returned".
#[derive(Default)]
struct WorkerRegistry {
    /// Worker ids that have sent at least one `AcquireBlock`.
    connected: std::collections::HashSet<u64>,
    /// Worker ids whose spawn thread has been reaped.
    exited: std::collections::HashSet<u64>,
}

impl Server {
    /// Listen for workers, and decide what address to tell them to dial.
    ///
    /// `host` is the caller's explicit choice (`run_blockwise(host=...)`);
    /// `None` means auto-detect. The two addresses are deliberately separate:
    /// the listener binds every interface so workers on other machines can
    /// reach it, while `self.host` — what goes into the worker context — must
    /// be an address those machines can resolve. Using `local_addr()` for
    /// both, as this once did, advertises `0.0.0.0` (unusable) or pins the
    /// whole run to loopback (also unusable off-box). See `advertise`.
    pub async fn bind(host: Option<&str>) -> std::io::Result<(Self, TcpListener)> {
        let advertise = crate::advertise::resolve(host);
        let listener = TcpListener::bind((advertise.bind.as_str(), 0u16)).await?;
        let local_addr = listener.local_addr()?;
        let port = local_addr.port();
        info!(
            listening_on = %local_addr,
            advertised_to_workers = %format!("{}:{}", advertise.host, port),
            "server listening"
        );
        Ok((
            Self {
                host: advertise.host,
                port,
            },
            listener,
        ))
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// Run blockwise with Rust-managed worker threads. Workers call back
    /// into Python via the trait implementations on the task's
    /// process_function / spawn_function.
    ///
    /// `resources` is an optional global budget (e.g. `{"cpu": 32,
    /// "gpu": 8}`). Tasks whose `requires` declares non-empty entries
    /// are gated by this budget — concurrent worker counts are bounded
    /// so the sum across all tasks competing for a resource never
    /// exceeds the corresponding budget. Tasks with empty `requires`
    /// ignore the budget entirely and are bounded only by their own
    /// `max_workers` cap (the legacy behaviour).
    pub async fn run_blockwise(
        &self,
        listener: TcpListener,
        pipeline: &crate::pipeline::Pipeline,
        worker_pools: &mut HashMap<String, WorkerPool>,
        resources: ResourceBudget,
        progress: Option<Arc<dyn ProgressObserver>>,
        abort_check: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
        block_tracking: bool,
    ) -> std::io::Result<(HashMap<String, TaskCounters>, HashMap<String, TaskSummary>)> {
        let mut scheduler = Scheduler::new(pipeline, true);
        if block_tracking {
            if let Err(e) = scheduler.init_tracking() {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()));
            }
        }
        let tasks: &[Arc<Task>] = &pipeline.tasks;
        let mut bookkeeper = BlockBookkeeper::new();

        let (msg_tx, mut msg_rx) = mpsc::channel::<ClientMessage>(256);

        // TCP accept loop.
        let accept_tx = msg_tx.clone();
        let accept_handle = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        debug!(%addr, "new client connection");
                        // Disable Nagle so per-block replies aren't delayed.
                        let _ = stream.set_nodelay(true);
                        let (mut reader, writer) = stream.into_split();
                        let (reply_tx, mut reply_rx) = mpsc::channel::<Message>(32);

                        tokio::spawn(async move {
                            let mut writer: OwnedWriteHalf = writer;
                            while let Some(msg) = reply_rx.recv().await {
                                if let Err(e) = write_message(&mut writer, &msg).await {
                                    debug!(error = %e, "write reply failed");
                                    break;
                                }
                            }
                        });

                        let reader_tx = accept_tx.clone();
                        let reader_reply_tx = reply_tx.clone();
                        tokio::spawn(async move {
                            loop {
                                match read_message(&mut reader).await {
                                    Ok(Some(msg)) => {
                                        let cm = ClientMessage {
                                            message: msg,
                                            addr,
                                            reply_tx: reader_reply_tx.clone(),
                                        };
                                        if reader_tx.send(cm).await.is_err() {
                                            break;
                                        }
                                    }
                                    Ok(None) | Err(_) => {
                                        let _ = reader_tx
                                            .send(ClientMessage {
                                                message: Message::Disconnect,
                                                addr,
                                                reply_tx: reader_reply_tx.clone(),
                                            })
                                            .await;
                                        break;
                                    }
                                }
                            }
                        });
                    }
                    Err(e) => error!(error = %e, "accept failed"),
                }
            }
        });

        let mut pending: VecDeque<ClientMessage> = VecDeque::new();

        // Resource accounting + worker registry.
        let mut allocator = ResourceAllocator::new(resources);
        if let Err(e) = allocator.validate(tasks) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                e.to_string(),
            ));
        }
        let mut workers: Vec<WorkerEntry> = Vec::new();
        let mut registry = WorkerRegistry::default();
        let mut next_worker_id: u64 = 0;
        // Channel each worker thread signals on just before it exits, so
        // the main loop can rebalance immediately rather than waiting for
        // the next 500ms health tick. The payload carries the error that
        // killed a dirty worker — that is control flow, not a statistic:
        // it is what lets abandonment report *why* the task died.
        let (worker_exit_tx, mut worker_exit_rx) = mpsc::unbounded_channel::<WorkerExit>();

        if let Some(ref obs) = progress {
            obs.on_start(&snapshot_counters(&scheduler.task_states));
        }

        // Initial fill — only tasks that already have ready blocks
        // (i.e. roots) get workers up front. Downstream tasks get
        // workers spawned later, when upstream completion makes their
        // first blocks ready. Capped by per-task `max_workers` and the
        // global resource budget.
        Self::rebalance_workers(
            &self.host,
            self.port,
            tasks,
            &mut scheduler,
            &mut allocator,
            &mut workers,
            &mut next_worker_id,
            &worker_exit_tx,
        );

        self.recruit_workers(&scheduler, worker_pools)?;

        let mut all_done = false;
        let mut aborted = false;
        let mut health_interval = tokio::time::interval(std::time::Duration::from_millis(500));
        let mut done_check_interval = tokio::time::interval(std::time::Duration::from_secs(1));
        // Poll the abort callback at 100ms — fast enough that ctrl-C
        // feels responsive, slow enough that the GIL re-acquire cost
        // is negligible. Skip missed ticks so a busy main loop
        // doesn't burn through a queue of stale ticks all at once.
        let mut abort_interval = tokio::time::interval(std::time::Duration::from_millis(100));
        abort_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        while !all_done && !aborted {
            tokio::select! {
                Some(cm) = msg_rx.recv() => {
                    let was_state_change = matches!(
                        cm.message,
                        Message::ReleaseBlock { .. } | Message::BlockFailed { .. }
                    );
                    let updated = self.handle_message(
                        cm, &mut scheduler, &mut bookkeeper, &mut registry,
                        &mut pending, worker_pools,
                    )?;
                    // Rebalance only when this release has actually
                    // unlocked a previously-blocked task — i.e. some
                    // task in the changed-states map has ready work
                    // but no workers spawned yet. Steady-state releases
                    // (a downstream task that's already running picks
                    // up new blocks via its existing workers) don't
                    // trigger rebalance here; the 500ms health tick
                    // covers any drift.
                    // The release just turned `ready_count` from 0 → >0
                    // for these task ids. If any of them currently has
                    // no alive workers, that's a freshly-unblocked task
                    // that needs a worker spawned.
                    let needs_rebalance = updated.iter().any(|tid| allocator.alive(tid) == 0);
                    if needs_rebalance {
                        Self::rebalance_workers(
                            &self.host,
                            self.port,
                            tasks,
                            &mut scheduler,
                            &mut allocator,
                            &mut workers,
                            &mut next_worker_id,
                            &worker_exit_tx,
                        );
                    }
                    // Fire the progress observer on every state-mutating
                    // message — `updated` only carries tasks with newly
                    // ready blocks, but per-task counters change on
                    // every release. The observer is responsible for
                    // throttling its display work.
                    if was_state_change {
                        if let Some(ref obs) = progress {
                            obs.on_progress(&snapshot_counters(&scheduler.task_states));
                        }
                    }
                }

                Some(exit) = worker_exit_rx.recv() => {
                    // A worker thread just signalled it's exiting. Attach
                    // its error (if any) to the task state so abandonment
                    // can report the cause, then reap the thread (frees
                    // its resource slot via the allocator and bumps the
                    // failure count if it exited dirty) and rebalance so
                    // freed budget can grow other tasks.
                    if let Some(err) = &exit.last_error {
                        if let Some(state) = scheduler.task_states.get_mut(&exit.task_id) {
                            if let Some(rt) = state.as_running_mut() {
                                rt.note_worker_error(err.clone());
                            }
                        }
                    }
                    Self::check_thread_health(
                        &mut workers, &mut scheduler, &mut allocator, &mut registry,
                    );
                    Self::rebalance_workers(
                        &self.host,
                        self.port,
                        tasks,
                        &mut scheduler,
                        &mut allocator,
                        &mut workers,
                        &mut next_worker_id,
                        &worker_exit_tx,
                    );
                    Self::abandon_exhausted_tasks(tasks, &mut scheduler, &allocator);
                }

                _ = health_interval.tick() => {
                    let lost = bookkeeper.get_lost_blocks();
                    for (mut block, timed_out, holder) in lost {
                        warn!(block_id = %block.block_id, timed_out, "block lost");
                        if timed_out {
                            if let Some(state) =
                                scheduler.task_states.get_mut(block.task_id())
                            {
                                if let Some(rt) = state.as_running_mut() {
                                    rt.note_timeout_reclaim();
                                }
                            }
                            // The holder blew the deadline: stop counting
                            // it alive so the rebalance below can spawn a
                            // replacement for the retried block instead of
                            // seeing a full roster of workers, one of
                            // which will never acquire again. The worker
                            // itself normally dies moments later (its own
                            // watchdog runs the same deadline), which
                            // makes its spawn call return; retiring here
                            // just means scheduling doesn't wait on that
                            // teardown to travel back through e.g. a
                            // remote job system.
                            if let Some(wid) = holder {
                                Self::retire_worker_slot(
                                    &mut workers, &mut allocator, wid,
                                );
                            }
                        }
                        block.status = BlockStatus::Failed;
                        scheduler.release_block(block);
                    }

                    // Check Rust worker pool health (for external process workers).
                    for (task_id, pool) in worker_pools.iter_mut() {
                        if let Err(e) = pool.check_health(&self.host, self.port) {
                            error!(task_id, error = %e, "worker health check failed");
                        }
                    }

                    // Reap exited threads and free their resources, then
                    // grow any pools that still have work + budget.
                    // Worker-exit notifications usually beat us to this,
                    // but the periodic tick is the safety net for any
                    // worker that died without sending a notification
                    // (e.g. a panic that didn't unwind through `Drop`).
                    Self::check_thread_health(
                        &mut workers, &mut scheduler, &mut allocator, &mut registry,
                    );
                    Self::rebalance_workers(
                        &self.host,
                        self.port,
                        tasks,
                        &mut scheduler,
                        &mut allocator,
                        &mut workers,
                        &mut next_worker_id,
                        &worker_exit_tx,
                    );
                    Self::abandon_exhausted_tasks(tasks, &mut scheduler, &allocator);

                    self.recruit_workers(&scheduler, worker_pools)?;

                    if !pending.is_empty() {
                        self.retry_pending(
                            &mut scheduler, &mut bookkeeper, &mut registry,
                            &mut pending, worker_pools,
                            )?;
                    }
                }

                _ = done_check_interval.tick() => {}

                _ = abort_interval.tick() => {
                    if let Some(ref check) = abort_check {
                        if check() {
                            warn!("abort requested, exiting run loop");
                            aborted = true;
                        }
                    }
                }
            }

            all_done = self.check_all_done(&scheduler);
        }

        if aborted {
            info!("run aborted, shutting down workers");
        } else {
            info!("all tasks completed");
        }

        // Shutdown.
        for cm in pending.drain(..) {
            let _ = cm.reply_tx.try_send(Message::RequestShutdown);
        }
        msg_rx.close();
        while let Ok(cm) = msg_rx.try_recv() {
            if let Message::AcquireBlock { .. } = &cm.message {
                let _ = cm.reply_tx.try_send(Message::RequestShutdown);
            }
        }
        accept_handle.abort();
        tokio::task::yield_now().await;

        for pool in worker_pools.values_mut() {
            pool.stop_all();
        }

        // Wait for worker threads to exit (they'll see TCP close / RequestShutdown).
        for entry in &mut workers {
            if let WorkerThread::Running(handle) =
                std::mem::replace(&mut entry.thread, WorkerThread::Finished)
            {
                let _ = handle.join();
            }
        }
        // Drain any worker-exit notifications still in flight, so a
        // late dirty exit still attaches its cause to the task state.
        while let Ok(exit) = worker_exit_rx.try_recv() {
            if let Some(err) = &exit.last_error {
                if let Some(state) = scheduler.task_states.get_mut(&exit.task_id) {
                    if let Some(rt) = state.as_running_mut() {
                        rt.note_worker_error(err.clone());
                    }
                }
            }
        }

        // The summary is an agglomeration of what the tracking layer
        // actually wrote — no separate accumulator to disagree with it.
        let summaries = scheduler.tracking_summaries();
        if let Some(ref obs) = progress {
            obs.on_finish(&snapshot_counters(&scheduler.task_states));
        }
        if aborted {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "run aborted by abort_check callback",
            ));
        }
        Ok((snapshot_counters(&scheduler.task_states), summaries))
    }

    /// Spawn a worker for a task. Every worker is a spawn function: the
    /// thread created here only *launches* it and waits, so the block
    /// function itself always runs in a dedicated OS process (daisy's
    /// python layer wraps 1-arg block functions into a spawn function that
    /// starts `python -m daisy._subprocess_worker`). There is no
    /// in-process execution path — CPU-bound work therefore scales with
    /// worker count regardless of the GIL, and `Task::timeout` can preempt
    /// a stuck block by killing its process.
    ///
    /// `exit_tx` is signalled (best-effort) right before the thread
    /// returns, so the main loop can rebalance immediately on worker
    /// exit without waiting for the next health-tick poll. A panicking
    /// thread won't notify; the health tick is the safety net for that
    /// case.
    fn spawn_worker(
        spec: &WorkerSpec,
        exit_tx: mpsc::UnboundedSender<WorkerExit>,
    ) -> JoinHandle<bool> {
        let task = spec.task.clone();
        let host = spec.host.clone();
        let port = spec.port;
        let task_id = spec.task_id.clone();
        let worker_id = spec.worker_id;

        std::thread::spawn(move || -> bool {
            // RAII: notify on every return path (including panics that
            // unwind through this thread) so the run loop can rebalance
            // promptly and abandonment can report the cause.
            struct ExitNotifier {
                tx: mpsc::UnboundedSender<WorkerExit>,
                exit: WorkerExit,
            }
            impl Drop for ExitNotifier {
                fn drop(&mut self) {
                    let _ = self.tx.send(std::mem::take(&mut self.exit));
                }
            }
            #[allow(unused_mut)]
            let mut notifier = ExitNotifier {
                tx: exit_tx,
                exit: WorkerExit {
                    task_id: task_id.clone(),
                    worker_id,
                    last_error: None,
                },
            };

            if let Some(ref spawn_fn) = task.spawn_function {
                // resource_tracking rides along so the worker can skip
                // measuring entirely when nobody asked for stats — the
                // server would only discard them.
                // Values are percent-encoded: a task id is user-chosen and
                // may contain the `:` or `=` this framing reserves.
                let env_ctx = format!(
                    "hostname={}:port={}:task_id={}:worker_id={}:resource_tracking={}",
                    encode_value(&host),
                    port,
                    encode_value(&task_id),
                    worker_id,
                    if task.resource_tracking { 1 } else { 0 }
                );
                match spawn_fn.spawn(&env_ctx) {
                    Ok(()) => true,
                    Err(e) => {
                        warn!(worker_id, error = %e, "spawn function failed");
                        notifier.exit.last_error = Some(format!("{e}"));
                        false // should respawn
                    }
                }
            } else if task.process_function.is_some() {
                // A block function reached the distributed runner without
                // being wrapped into a spawn function. daisy's own python
                // layer always wraps (see `_wrap_for_subprocess_workers`),
                // so this only happens when a `daisy_core::Task` is built
                // directly with a `ProcessBlock` and handed to `Server`.
                // There is nowhere to run it: the distributed path executes
                // blocks in worker processes only. Fail loudly rather than
                // silently completing zero blocks.
                let msg = "task has a process_function but no spawn_function: the \
                           distributed runner executes blocks in worker processes \
                           only. Wrap the block function into a spawn function, or \
                           use the serial path (`run_serial`) to run it in-process.";
                error!(worker_id, task_id = %task_id, "{msg}");
                notifier.exit.last_error = Some(msg.to_string());
                false
            } else {
                true
            }
        })
    }

    /// Reap exited worker threads and free their resource slots so the
    /// next `rebalance_workers` call can re-allocate them. Workers
    /// that exit with an error or panic increment the per-task
    /// `worker_failure_count` so the cap (`Task::max_worker_restarts`)
    /// can stop unbounded respawning. We do not respawn here —
    /// `rebalance_workers` decides whether the task still needs (and
    /// is allowed) more workers.
    fn check_thread_health(
        workers: &mut Vec<WorkerEntry>,
        scheduler: &mut Scheduler,
        allocator: &mut ResourceAllocator,
        registry: &mut WorkerRegistry,
    ) {
        for entry in workers.iter_mut() {
            let spec = &entry.spec;
            if let WorkerThread::Running(ref handle) = entry.thread {
                if handle.is_finished() {
                    if let WorkerThread::Running(handle) =
                        std::mem::replace(&mut entry.thread, WorkerThread::Finished)
                    {
                        registry.exited.insert(spec.worker_id);
                        match handle.join() {
                            Ok(true) => {
                                debug!(
                                    worker_id = spec.worker_id,
                                    task_id = %spec.task_id,
                                    "worker exited cleanly",
                                );
                                // A clean spawn-function return whose worker
                                // never once talked to us, while the task
                                // still has work, is the fire-and-forget
                                // signature (e.g. `sbatch` without `--wait`):
                                // the spawn call must block for the worker's
                                // lifetime, or every liveness decision here
                                // is reading tea leaves. The hard error fires
                                // if that worker connects later; this warning
                                // is the early signal (and the only one, if
                                // the worker never comes up at all).
                                let task_running = scheduler
                                    .task_states
                                    .get(&spec.task_id)
                                    .is_some_and(|s| s.is_running());
                                if task_running
                                    && !registry.connected.contains(&spec.worker_id)
                                {
                                    warn!(
                                        worker_id = spec.worker_id,
                                        task_id = %spec.task_id,
                                        "spawn function returned without its worker ever \
                                         connecting, while the task still has work. 0-arg \
                                         spawn functions must block for the worker's \
                                         lifetime (e.g. `sbatch --wait`, `srun`) — a \
                                         fire-and-forget submit makes daisy respawn \
                                         workers it believes dead, burning the worker \
                                         start budget, and can abandon the task while \
                                         real workers still run",
                                    );
                                }
                            }
                            Ok(false) | Err(_) => {
                                warn!(
                                    worker_id = spec.worker_id,
                                    task_id = %spec.task_id,
                                    "worker exited with error",
                                );
                                if let Some(state) =
                                    scheduler.task_states.get_mut(&spec.task_id)
                                {
                                    if let Some(rt) = state.as_running_mut() {
                                        rt.note_worker_died();
                                    }
                                }
                            }
                        }
                        if !entry.slot_retired {
                            allocator.release(&spec.task);
                        }
                    }
                }
            }
        }
        // Drop the now-Finished entries so the workers vec doesn't grow
        // unboundedly across long runs.
        workers.retain(|entry| matches!(entry.thread, WorkerThread::Running(_)));
    }

    /// Stop counting a still-running worker as alive: give its allocator
    /// slot back so rebalancing can spawn a replacement. Used when a
    /// block times out — its holder is stuck, and may be somewhere the
    /// server cannot kill (the far end of an `sbatch`), so scheduling
    /// must not wait for its spawn call to return. Idempotent per
    /// worker; the eventual thread exit skips the second release via
    /// `slot_retired`. If the worker turns out to be merely slow and
    /// comes back, its stale block return is already rejected by the
    /// bookkeeper and it simply rejoins the pool (briefly over-counting
    /// alive by one, which self-corrects at the next clean exit).
    fn retire_worker_slot(
        workers: &mut [WorkerEntry],
        allocator: &mut ResourceAllocator,
        worker_id: u64,
    ) {
        for entry in workers.iter_mut() {
            if entry.spec.worker_id != worker_id {
                continue;
            }
            if entry.slot_retired || !matches!(entry.thread, WorkerThread::Running(_)) {
                return;
            }
            warn!(
                worker_id,
                task_id = %entry.spec.task_id,
                "retiring worker slot: its block exceeded the task timeout; \
                 the worker may still be running but is no longer counted \
                 alive",
            );
            allocator.release(&entry.spec.task);
            entry.slot_retired = true;
            return;
        }
    }

    /// For any task that has exhausted its restart budget AND has no
    /// alive workers AND still has unprocessed blocks, transition it
    /// to `TaskState::Abandoned`. Then BFS through the task DAG and
    /// transition transitive downstream tasks to Abandoned as well —
    /// their input will never arrive. Logs once per abandoned task.
    ///
    /// All counter mutation happens inside the typestate transition
    /// (`TaskState::abandon`), which orphans the remaining blocks
    /// and freezes the snapshot. Any further messages targeting an
    /// Abandoned task are dropped at the `as_running_mut()` gate in
    /// the scheduler.
    fn abandon_exhausted_tasks(
        tasks: &[Arc<Task>],
        scheduler: &mut Scheduler,
        allocator: &ResourceAllocator,
    ) {
        use std::collections::HashSet;

        // 1. Identify directly-exhausted tasks: cap reached, no alive
        // workers, still Running. Already-terminal tasks are skipped.
        let mut directly_abandoned: HashSet<String> = HashSet::new();
        for task in tasks {
            if allocator.alive(&task.task_id) > 0 {
                continue;
            }
            let Some(state) = scheduler.task_states.get(&task.task_id) else {
                continue;
            };
            if !state.is_running() {
                continue;
            }
            if state.worker_restart_count() < task.max_worker_restarts {
                continue;
            }
            let counters = state.counters();
            let remaining = counters.total_block_count
                - counters.completed_count
                - counters.failed_count
                - counters.orphaned_count;
            if remaining > 0 {
                directly_abandoned.insert(task.task_id.clone());
            }
        }

        if directly_abandoned.is_empty() {
            return;
        }

        // 2. BFS the task DAG to collect transitive downstream tasks.
        let dg = scheduler.dependency_graph();
        let mut transitively_abandoned: HashSet<String> = HashSet::new();
        let mut frontier: Vec<String> = directly_abandoned.iter().cloned().collect();
        while let Some(t) = frontier.pop() {
            for d in dg.downstream_task_ids(&t) {
                if !directly_abandoned.contains(d)
                    && transitively_abandoned.insert(d.to_string())
                {
                    frontier.push(d.to_string());
                }
            }
        }

        // 3. Transition each task. The typestate `abandon` consumes
        // the Running variant, accounts remaining blocks as
        // orphaned, and replaces with the Abandoned variant.
        for task in tasks {
            let direct = directly_abandoned.contains(&task.task_id);
            let transitive = transitively_abandoned.contains(&task.task_id);
            if !direct && !transitive {
                continue;
            }
            let Some(state) = scheduler.task_states.get_mut(&task.task_id) else {
                continue;
            };
            let failures = state.worker_failure_count();
            let restarts = state.worker_restart_count();
            let reason = if direct {
                AbandonReason::RestartCapExhausted
            } else {
                AbandonReason::UpstreamAbandoned
            };
            if let Some(orphaned) = state.abandon(reason) {
                if direct {
                    warn!(
                        task_id = %task.task_id,
                        failures,
                        restarts,
                        max_restarts = task.max_worker_restarts,
                        orphaned,
                        "task abandoned: worker restart cap reached, accounting remaining blocks as orphaned",
                    );
                } else {
                    warn!(
                        task_id = %task.task_id,
                        orphaned,
                        "downstream task abandoned: upstream input will never arrive, accounting remaining blocks as orphaned",
                    );
                }
            }
        }
    }

    /// Spawn additional workers for tasks that have ready work, fewer
    /// alive workers than their `max_workers` cap, and whose
    /// per-worker `requires` fits in the remaining resource budget.
    /// Idempotent — safe to call any time.
    ///
    /// Tasks with `ready_count == 0` are skipped here even if they
    /// have pending blocks waiting on upstream completion. Spawning
    /// workers for them now would consume budget that ready tasks
    /// could be using (the new workers would just park at
    /// `acquire_block` until upstream produces). They'll get workers
    /// on the next rebalance after their first block becomes ready.
    fn rebalance_workers(
        host: &str,
        port: u16,
        tasks: &[Arc<Task>],
        scheduler: &mut Scheduler,
        allocator: &mut ResourceAllocator,
        workers: &mut Vec<WorkerEntry>,
        next_id: &mut u64,
        exit_tx: &mpsc::UnboundedSender<WorkerExit>,
    ) {
        // Round-robin grow: at each pass, give *one* more worker to
        // every eligible task. Repeat until no task can grow. This
        // keeps competing tasks sharing a resource roughly fair.
        loop {
            let mut grew_any = false;
            for task in tasks {
                if task.process_function.is_none() && task.spawn_function.is_none() {
                    continue;
                }
                // Skip tasks with no ready work — don't waste budget on
                // workers that would just park immediately.
                // Skip terminal tasks — workers can't help them and
                // would just immediately exit.
                let Some(state) = scheduler.task_states.get(&task.task_id) else {
                    continue;
                };
                if !state.is_running() {
                    continue;
                }
                let counters = state.counters();
                if counters.ready_count <= 0 {
                    continue;
                }
                let alive = allocator.alive(&task.task_id);
                if alive >= task.max_workers {
                    continue;
                }
                // Never keep more workers alive than could possibly
                // have work. A worker finishing its current block
                // picks up a ready one, so workers beyond
                // ready + processing can never receive a block before
                // an existing worker frees up — spawning them only
                // burns start budget and wall-clock (launches
                // serialize, and the run cannot finish until every
                // launched worker has connected and been told to shut
                // down; measured: 2 workers drained a 64-block task
                // while the run waited ~1s on 126 pointless
                // launches). With read_write_conflict or upstream
                // dependencies, ready_count can be temporarily small
                // and grow as neighbors/upstreams complete — that may
                // briefly under-provision, but the rebalance calls on
                // task-state changes and the periodic health tick
                // refill as ready grows, which is the right trade
                // against provably-idle spawns.
                if alive as i64 >= counters.ready_count + counters.processing_count {
                    continue;
                }
                // Hard start budget: a task may ever start at most
                // `max_workers + max_worker_restarts` workers, TOTAL,
                // regardless of how or why previous workers exited —
                // clean, dirty, or productive alike. Workers are
                // expected to be long-running (they may hold large
                // models in memory); recycling workers mid-task is not
                // a supported pattern, and exempting "good" exits
                // would let a spawn function whose worker silently
                // fails to start (e.g. `subprocess.run(...,
                // check=False)` around a broken command) respawn
                // forever. Starts beyond the first `max_workers` are
                // accounted as restarts, which keeps the abandonment
                // condition (`worker_restart_count >=
                // max_worker_restarts`) and user-facing counters
                // meaning what they always did.
                let starts = state.worker_start_count() as u64;
                let budget =
                    task.max_workers as u64 + task.max_worker_restarts as u64;
                if starts >= budget {
                    continue;
                }
                let is_restart = starts >= task.max_workers as u64;
                if !allocator.try_allocate(task) {
                    continue;
                }
                let spec = WorkerSpec {
                    task_id: task.task_id.clone(),
                    task: task.clone(),
                    worker_id: *next_id,
                    host: host.to_string(),
                    port,
                };
                let handle = Self::spawn_worker(&spec, exit_tx.clone());
                workers.push(WorkerEntry {
                    spec,
                    thread: WorkerThread::Running(handle),
                    slot_retired: false,
                });
                *next_id += 1;
                if let Some(state) = scheduler.task_states.get_mut(&task.task_id) {
                    if let Some(rt) = state.as_running_mut() {
                        rt.note_worker_started();
                        if is_restart {
                            rt.note_worker_restarted();
                        }
                    }
                }
                grew_any = true;
            }
            if !grew_any {
                break;
            }
        }
    }

    fn retry_pending(
        &self,
        scheduler: &mut Scheduler,
        bookkeeper: &mut BlockBookkeeper,
        registry: &mut WorkerRegistry,
        pending: &mut VecDeque<ClientMessage>,
        worker_pools: &mut HashMap<String, WorkerPool>,
    ) -> std::io::Result<()> {
        let count = pending.len();
        for _ in 0..count {
            if let Some(cm) = pending.pop_front() {
                let _ = self.handle_message(
                    cm, scheduler, bookkeeper, registry, pending, worker_pools,
                )?;
            }
        }
        Ok(())
    }

    /// Handle one client message. Returns the map of task states that
    /// changed as a side effect (empty for non-state-changing messages
    /// like AcquireBlock / Disconnect). The caller uses this to decide
    /// whether a rebalance is warranted — specifically, whether a
    /// release just made a previously-blocked downstream task eligible
    /// for its first worker.
    fn handle_message(
        &self,
        cm: ClientMessage,
        scheduler: &mut Scheduler,
        bookkeeper: &mut BlockBookkeeper,
        registry: &mut WorkerRegistry,
        pending: &mut VecDeque<ClientMessage>,
        worker_pools: &mut HashMap<String, WorkerPool>,
    ) -> std::io::Result<Vec<String>> {
        let mut updated: Vec<String> = Vec::new();
        match cm.message {
            Message::AcquireBlock { .. } => {
                self.handle_acquire(cm, scheduler, bookkeeper, registry, pending, worker_pools)?;
            }
            Message::ReleaseBlock { block } => {
                if bookkeeper.is_valid_return(&block, cm.addr) {
                    // The bookkeeper's timer still runs the timeout
                    // deadline; per-block *durations* now come from the
                    // worker's own measurement on `block.stats`, which is
                    // the same number in every execution mode.
                    let _ = bookkeeper.notify_block_returned(&block, cm.addr);
                    if scheduler.expects_block_stats(block.task_id()) && block.stats.is_none() {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!(
                                "task {:?} has resource_tracking enabled but worker at {} \
                                 returned block {} without measurements. Wrap the block body \
                                 in `with daisy.profile_block(block):` — daisy's own workers \
                                 do this automatically, so this usually means a custom \
                                 client is bypassing daisy.Client.",
                                block.task_id(),
                                cm.addr,
                                block.block_id,
                            ),
                        ));
                    }
                    updated = scheduler.release_block(block);
                    self.recruit_workers(scheduler, worker_pools)?;
                    if !pending.is_empty() {
                        self.retry_pending(
                            scheduler, bookkeeper, registry, pending, worker_pools,
                                )?;
                    }
                } else {
                    debug!(block_id = %block.block_id, "invalid block return");
                }
            }
            Message::BlockFailed { mut block, error } => {
                warn!(block_id = %block.block_id, %error, "block failed");
                if let Some(state) = scheduler.task_states.get_mut(block.task_id()) {
                    if let Some(rt) = state.as_running_mut() {
                        rt.note_worker_error(error.to_string());
                    }
                }
                if bookkeeper.is_valid_return(&block, cm.addr) {
                    // A failed block's measurement is not folded into the
                    // trend (noisy outlier), but the failure itself is
                    // counted by the scheduler's release path.
                    let _ = bookkeeper.notify_block_returned(&block, cm.addr);
                    block.status = BlockStatus::Failed;
                    updated = scheduler.release_block(block);
                    self.recruit_workers(scheduler, worker_pools)?;
                    if !pending.is_empty() {
                        self.retry_pending(
                            scheduler, bookkeeper, registry, pending, worker_pools,
                                )?;
                    }
                }
            }
            Message::Disconnect => {
                debug!(addr = %cm.addr, "client disconnected");
                bookkeeper.notify_client_disconnected(cm.addr);
            }
            _ => {
                warn!(msg = ?cm.message, "unexpected message");
            }
        }
        Ok(updated)
    }

    fn handle_acquire(
        &self,
        cm: ClientMessage,
        scheduler: &mut Scheduler,
        bookkeeper: &mut BlockBookkeeper,
        registry: &mut WorkerRegistry,
        pending: &mut VecDeque<ClientMessage>,
        worker_pools: &mut HashMap<String, WorkerPool>,
    ) -> std::io::Result<()> {
        let (task_id, worker_id) = match &cm.message {
            Message::AcquireBlock { task_id, worker_id } => (task_id.clone(), *worker_id),
            _ => unreachable!(),
        };

        if let Some(wid) = worker_id {
            // First contact from a worker whose spawn thread has already
            // been reaped is definitive proof of a fire-and-forget spawn
            // function: the worker outlived the spawn call. Every
            // liveness decision (rebalancing, the start budget,
            // abandonment) reads "worker alive" as "the spawn call
            // hasn't returned", so such a run is unsalvageable — fail it
            // loudly rather than respawn workers we believe dead.
            // (A worker that connected *before* its thread exited is the
            // normal shutdown ordering and is left alone; that also
            // keeps a queued message from a just-finished worker from
            // tripping this.)
            if registry.exited.contains(&wid) && !registry.connected.contains(&wid) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "worker {wid} for task {task_id:?} connected after its \
                         spawn function had already returned. 0-arg spawn \
                         functions must block until their worker exits \
                         (e.g. `sbatch --wait`, `srun`, `subprocess.run`): \
                         daisy tracks worker liveness by the spawn call, so \
                         a fire-and-forget submit makes daisy respawn workers \
                         it believes dead and breaks the run's accounting."
                    ),
                ));
            }
            registry.connected.insert(wid);
        }

        match scheduler.acquire_block(&task_id) {
            Some(block) => {
                debug!(block_id = %block.block_id, "sending block");
                let timeout = scheduler
                    .task_map
                    .get(&task_id)
                    .and_then(|t| t.timeout);
                bookkeeper.notify_block_sent(block.clone(), cm.addr, timeout, worker_id);
                let _ = cm.reply_tx.try_send(Message::SendBlock {
                    block,
                    timeout_secs: timeout.map(|d| d.as_secs_f64()),
                });
            }
            None => {
                // No ready block. Release the worker unless blocks are
                // still *pending* (dependency-gated: upstream tasks or
                // intra-task read/write conflicts) — those become ready
                // later and this worker is their taker, so it parks.
                // In-flight blocks deliberately do NOT hold workers: once
                // pending hits 0, the only work that can ever reappear is
                // a retry of an in-flight block, and the rebalance loop
                // spawns a fresh worker for that (the failing worker's
                // own next acquire usually beats it there). Holding the
                // fleet instead meant the last slow blocks of a large run
                // pinned every idle worker until full completion.
                let counters = scheduler.task_states[&task_id].counters();
                let terminal = !scheduler.task_states[&task_id].is_running();
                if terminal || counters.pending_count() <= 0 {
                    debug!(task_id = %task_id, "no more blocks");
                    let _ = cm.reply_tx.try_send(Message::RequestShutdown);
                    self.recruit_workers(scheduler, worker_pools)?;
                } else {
                    debug!(task_id = %task_id, "parking request");
                    pending.push_back(cm);
                }
            }
        }
        Ok(())
    }

    fn recruit_workers(
        &self,
        scheduler: &Scheduler,
        worker_pools: &mut HashMap<String, WorkerPool>,
    ) -> std::io::Result<()> {
        let ready_tasks = scheduler.get_ready_tasks();
        for task in &ready_tasks {
            if let Some(pool) = worker_pools.get_mut(&task.task_id) {
                pool.set_num_workers(task.max_workers, &self.host, self.port)?;
            }
        }
        Ok(())
    }

    fn check_all_done(&self, scheduler: &Scheduler) -> bool {
        scheduler.task_states.values().all(|state| state.is_done())
    }
}
