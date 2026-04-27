use crate::block::BlockStatus;
use crate::block_bookkeeper::BlockBookkeeper;
use crate::client::Client;
use crate::protocol::{read_message, write_message, Message};
use crate::resource_allocator::{ResourceAllocator, ResourceBudget};
use crate::run_stats::{thread_cpu_time, WorkerStats};
use crate::scheduler::Scheduler;
use crate::task::Task;
use crate::task_state::{AbandonReason, TaskCounters, TaskState};
use crate::worker_pool::WorkerPool;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;
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

impl Server {
    pub async fn bind(host: &str) -> std::io::Result<(Self, TcpListener)> {
        let listener = TcpListener::bind((host, 0u16)).await?;
        let local_addr = listener.local_addr()?;
        info!(%local_addr, "server listening");
        Ok((
            Self {
                host: local_addr.ip().to_string(),
                port: local_addr.port(),
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
    /// `num_workers` cap (the legacy behaviour).
    pub async fn run_blockwise(
        &self,
        listener: TcpListener,
        tasks: &[Arc<Task>],
        worker_pools: &mut HashMap<String, WorkerPool>,
        resources: ResourceBudget,
        progress: Option<Arc<dyn ProgressObserver>>,
        abort_check: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    ) -> std::io::Result<(HashMap<String, TaskCounters>, crate::run_stats::RunStats)> {
        let mut scheduler = Scheduler::new(tasks, true);
        if let Err(e) = scheduler.init_done_markers() {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()));
        }
        let mut bookkeeper = BlockBookkeeper::new(None);

        let (msg_tx, mut msg_rx) = mpsc::channel::<ClientMessage>(256);

        // TCP accept loop.
        let accept_tx = msg_tx.clone();
        let accept_handle = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        debug!(%addr, "new client connection");
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
        let mut workers: Vec<(WorkerSpec, WorkerThread)> = Vec::new();
        let mut next_worker_id: u64 = 0;
        // Channel each worker thread signals on just before it exits,
        // so the main loop can rebalance immediately rather than
        // waiting for the next 500ms health tick. The payload also
        // carries that worker's resource-utilisation stats — collected
        // here for the post-run report.
        let (worker_exit_tx, mut worker_exit_rx) =
            mpsc::unbounded_channel::<WorkerStats>();
        let mut collected_worker_stats: Vec<WorkerStats> = Vec::new();
        // Per-task list of `release_index → duration_ms`, so we can fit
        // a linear trend at the end and report whether processing time
        // grew/shrank as the run progressed.
        let mut task_block_durations: HashMap<String, Vec<f64>> = HashMap::new();

        // Process-wide sampler: peak RSS / virt and disk I/O deltas
        // updated every 200ms by a background tokio task.
        let process_stats = std::sync::Arc::new(std::sync::Mutex::new(
            crate::run_stats::ProcessStats::default(),
        ));
        let stats_started = Instant::now();
        let process_stats_clone = process_stats.clone();
        let (sampler_stop_tx, mut sampler_stop_rx) = tokio::sync::oneshot::channel::<()>();
        let sampler_handle = tokio::spawn(async move {
            use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System};
            let pid = Pid::from_u32(std::process::id());
            let mut system = System::new_with_specifics(
                RefreshKind::new().with_processes(ProcessRefreshKind::everything()),
            );
            let mut baseline_disk_read: u64 = 0;
            let mut baseline_disk_write: u64 = 0;
            let mut first = true;
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(200));
            loop {
                tokio::select! {
                    _ = &mut sampler_stop_rx => break,
                    _ = interval.tick() => {
                        system.refresh_processes_specifics(
                            ProcessesToUpdate::Some(&[pid]),
                            true,
                            ProcessRefreshKind::everything(),
                        );
                        if let Some(p) = system.process(pid) {
                            let mut g = process_stats_clone.lock().unwrap();
                            g.peak_rss_bytes = g.peak_rss_bytes.max(p.memory());
                            g.peak_virt_bytes = g.peak_virt_bytes.max(p.virtual_memory());
                            let disk = p.disk_usage();
                            if first {
                                baseline_disk_read = disk.total_read_bytes;
                                baseline_disk_write = disk.total_written_bytes;
                                first = false;
                            }
                            g.disk_read_bytes =
                                disk.total_read_bytes.saturating_sub(baseline_disk_read);
                            g.disk_write_bytes =
                                disk.total_written_bytes.saturating_sub(baseline_disk_write);
                            g.total_cpu_time = std::time::Duration::from_secs_f64(
                                p.run_time() as f64 * (p.cpu_usage() as f64 / 100.0),
                            );
                            g.unavailable = false;
                        } else {
                            process_stats_clone.lock().unwrap().unavailable = true;
                        }
                    }
                }
            }
        });

        if let Some(ref obs) = progress {
            obs.on_start(&snapshot_counters(&scheduler.task_states));
        }

        // Initial fill — only tasks that already have ready blocks
        // (i.e. roots) get workers up front. Downstream tasks get
        // workers spawned later, when upstream completion makes their
        // first blocks ready. Capped by per-task `num_workers` and the
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
                        cm, &mut scheduler, &mut bookkeeper,
                        &mut pending, worker_pools,
                        &mut task_block_durations,
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

                Some(stats) = worker_exit_rx.recv() => {
                    // A worker thread just signalled it's exiting and
                    // handed back its utilisation stats. Stash them
                    // for the report, reap the thread (frees its
                    // resource slot via the allocator and bumps
                    // failure count if it exited dirty), and rebalance
                    // immediately so freed budget can grow other tasks.
                    collected_worker_stats.push(stats);
                    Self::check_thread_health(&mut workers, &mut scheduler, &mut allocator);
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
                    for mut block in lost {
                        warn!(block_id = %block.block_id, "block lost");
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
                    Self::check_thread_health(&mut workers, &mut scheduler, &mut allocator);
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
                            &mut scheduler, &mut bookkeeper,
                            &mut pending, worker_pools,
                            &mut task_block_durations,
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
        for (_, wt) in &mut workers {
            if let WorkerThread::Running(handle) = std::mem::replace(wt, WorkerThread::Finished) {
                let _ = handle.join();
            }
        }
        // Drain any worker-exit notifications still in flight.
        while let Ok(s) = worker_exit_rx.try_recv() {
            collected_worker_stats.push(s);
        }

        // Stop the sysinfo sampler and pull its final snapshot.
        let _ = sampler_stop_tx.send(());
        let _ = sampler_handle.await;
        let process = process_stats.lock().unwrap().clone();

        let run_stats = crate::run_stats::build_run_stats(
            stats_started,
            collected_worker_stats,
            task_block_durations,
            process,
        );
        if let Some(ref obs) = progress {
            obs.on_finish(&snapshot_counters(&scheduler.task_states));
        }
        if aborted {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "run aborted by abort_check callback",
            ));
        }
        Ok((snapshot_counters(&scheduler.task_states), run_stats))
    }

    /// Spawn a worker thread for a task. For 1-arg process functions, the
    /// thread creates a TCP client, loops acquiring blocks, and calls the
    /// function. For 0-arg spawn functions, the thread calls the function
    /// directly (it manages its own Client/subprocess).
    ///
    /// `exit_tx` is signalled (best-effort) right before the thread
    /// returns, so the main loop can rebalance immediately on worker
    /// exit without waiting for the next health-tick poll. A panicking
    /// thread won't notify; the health tick is the safety net for that
    /// case.
    fn spawn_worker(
        spec: &WorkerSpec,
        exit_tx: mpsc::UnboundedSender<WorkerStats>,
    ) -> JoinHandle<bool> {
        let task = spec.task.clone();
        let host = spec.host.clone();
        let port = spec.port;
        let task_id = spec.task_id.clone();
        let worker_id = spec.worker_id;

        std::thread::spawn(move || -> bool {
            // RAII: report stats and notify on every return path
            // (including panics that unwind through this thread).
            struct ExitNotifier {
                tx: mpsc::UnboundedSender<WorkerStats>,
                stats: WorkerStats,
                started: Instant,
                start_cpu: Option<std::time::Duration>,
            }
            impl Drop for ExitNotifier {
                fn drop(&mut self) {
                    self.stats.wall_time = self.started.elapsed();
                    self.stats.cpu_time = match (thread_cpu_time(), self.start_cpu) {
                        (Some(now), Some(then)) => Some(now.saturating_sub(then)),
                        (Some(now), None) => Some(now),
                        _ => None,
                    };
                    let _ = self.tx.send(std::mem::take(&mut self.stats));
                }
            }
            #[allow(unused_mut)]
            let mut notifier = ExitNotifier {
                tx: exit_tx,
                stats: WorkerStats {
                    task_id: task_id.clone(),
                    worker_id,
                    ..Default::default()
                },
                started: Instant::now(),
                start_cpu: thread_cpu_time(),
            };

            if let Some(ref process_fn) = task.process_function {
                // 1-arg block processor: connect via TCP, loop.
                let rt = match tokio::runtime::Runtime::new() {
                    Ok(rt) => rt,
                    Err(e) => {
                        error!(worker_id, error = %e, "failed to create runtime");
                        return false;
                    }
                };
                let mut client = match rt.block_on(Client::connect(&host, port, &task_id)) {
                    Ok(c) => c,
                    Err(e) => {
                        error!(worker_id, error = %e, "failed to connect");
                        return false;
                    }
                };
                // Any block failure kills this worker. The runner
                // then refills (counting toward `worker_restart_count`)
                // until the cap is hit, at which point abandonment
                // fires. This unifies block-function and
                // worker-function semantics: in both modes,
                // `worker_restart_count` counts failed work
                // attempts, and `max_worker_restarts` caps how many
                // failures the runner tolerates before giving up.
                loop {
                    match rt.block_on(client.acquire_block()) {
                        Ok(Some(mut block)) => {
                            block.status = crate::block::BlockStatus::Processing;
                            match process_fn.process(&mut block) {
                                Ok(()) => {
                                    if block.status == crate::block::BlockStatus::Processing {
                                        block.status = crate::block::BlockStatus::Success;
                                    }
                                }
                                Err(e) => {
                                    warn!(worker_id, error = %e, "block processing failed");
                                    block.status = crate::block::BlockStatus::Failed;
                                }
                            }
                            let was_failure =
                                block.status == crate::block::BlockStatus::Failed;
                            if let Err(e) = rt.block_on(client.release_block(block)) {
                                error!(worker_id, error = %e, "failed to release block");
                                return false;
                            }
                            notifier.stats.blocks_processed += 1;
                            if was_failure {
                                debug!(worker_id, "exiting dirty after failed block");
                                let _ = rt.block_on(client.disconnect());
                                return false;
                            }
                        }
                        Ok(None) => {
                            debug!(worker_id, "no more blocks, exiting");
                            break;
                        }
                        Err(e) => {
                            debug!(worker_id, error = %e, "acquire failed");
                            return false;
                        }
                    }
                }
                let _ = rt.block_on(client.disconnect());
                true // clean exit
            } else if let Some(ref spawn_fn) = task.spawn_function {
                let env_ctx = format!(
                    "hostname={}:port={}:task_id={}:worker_id={}",
                    host, port, task_id, worker_id
                );
                match spawn_fn.spawn(&env_ctx) {
                    Ok(()) => true,
                    Err(e) => {
                        warn!(worker_id, error = %e, "spawn function failed");
                        false // should respawn
                    }
                }
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
        workers: &mut Vec<(WorkerSpec, WorkerThread)>,
        scheduler: &mut Scheduler,
        allocator: &mut ResourceAllocator,
    ) {
        for (spec, wt) in workers.iter_mut() {
            if let &mut WorkerThread::Running(ref handle) = wt {
                if handle.is_finished() {
                    if let WorkerThread::Running(handle) =
                        std::mem::replace(wt, WorkerThread::Finished)
                    {
                        match handle.join() {
                            Ok(true) => {
                                debug!(
                                    worker_id = spec.worker_id,
                                    task_id = %spec.task_id,
                                    "worker exited cleanly",
                                );
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
                        allocator.release(&spec.task);
                    }
                }
            }
        }
        // Drop the now-Finished entries so the workers vec doesn't grow
        // unboundedly across long runs.
        workers.retain(|(_, wt)| matches!(wt, WorkerThread::Running(_)));
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
            if let Some(downs) = dg.downstream_tasks.get(&t) {
                for d in downs {
                    if !directly_abandoned.contains(d)
                        && transitively_abandoned.insert(d.clone())
                    {
                        frontier.push(d.clone());
                    }
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
    /// alive workers than their `num_workers` cap, and whose
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
        workers: &mut Vec<(WorkerSpec, WorkerThread)>,
        next_id: &mut u64,
        exit_tx: &mpsc::UnboundedSender<WorkerStats>,
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
                if alive >= task.num_workers {
                    continue;
                }
                // A spawn that *replaces* a previously-dead worker
                // is a "restart"; one that fills an unfilled slot
                // (initial fill, or growth after the resource budget
                // loosens) is not. The differentiator: if the task
                // has more dirty exits on the books than restarts
                // performed, we're refilling a death.
                let failures = counters.worker_failure_count;
                let restarts = counters.worker_restart_count;
                let is_restart = failures > restarts;
                if is_restart && restarts >= task.max_worker_restarts {
                    continue;
                }
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
                workers.push((spec, WorkerThread::Running(handle)));
                *next_id += 1;
                if is_restart {
                    if let Some(state) = scheduler.task_states.get_mut(&task.task_id) {
                        if let Some(rt) = state.as_running_mut() {
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
        pending: &mut VecDeque<ClientMessage>,
        worker_pools: &mut HashMap<String, WorkerPool>,
        task_block_durations: &mut HashMap<String, Vec<f64>>,
    ) -> std::io::Result<()> {
        let count = pending.len();
        for _ in 0..count {
            if let Some(cm) = pending.pop_front() {
                let _ = self.handle_message(
                    cm, scheduler, bookkeeper, pending, worker_pools,
                    task_block_durations,
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
        pending: &mut VecDeque<ClientMessage>,
        worker_pools: &mut HashMap<String, WorkerPool>,
        task_block_durations: &mut HashMap<String, Vec<f64>>,
    ) -> std::io::Result<Vec<String>> {
        let mut updated: Vec<String> = Vec::new();
        match cm.message {
            Message::AcquireBlock { .. } => {
                self.handle_acquire(cm, scheduler, bookkeeper, pending, worker_pools)?;
            }
            Message::ReleaseBlock { block } => {
                if bookkeeper.is_valid_return(&block, cm.addr) {
                    if let Some(elapsed) = bookkeeper.notify_block_returned(&block, cm.addr) {
                        task_block_durations
                            .entry(block.block_id.task_id.clone())
                            .or_default()
                            .push(elapsed.as_secs_f64() * 1000.0);
                    }
                    updated = scheduler.release_block(block);
                    self.recruit_workers(scheduler, worker_pools)?;
                    if !pending.is_empty() {
                        self.retry_pending(
                            scheduler, bookkeeper, pending, worker_pools,
                            task_block_durations,
                        )?;
                    }
                } else {
                    debug!(block_id = %block.block_id, "invalid block return");
                }
            }
            Message::BlockFailed { mut block, error } => {
                warn!(block_id = %block.block_id, %error, "block failed");
                if bookkeeper.is_valid_return(&block, cm.addr) {
                    // Failed blocks intentionally not added to the
                    // duration trend — they're noisy outliers.
                    let _ = bookkeeper.notify_block_returned(&block, cm.addr);
                    block.status = BlockStatus::Failed;
                    updated = scheduler.release_block(block);
                    self.recruit_workers(scheduler, worker_pools)?;
                    if !pending.is_empty() {
                        self.retry_pending(
                            scheduler, bookkeeper, pending, worker_pools,
                            task_block_durations,
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
        pending: &mut VecDeque<ClientMessage>,
        worker_pools: &mut HashMap<String, WorkerPool>,
    ) -> std::io::Result<()> {
        let task_id = match &cm.message {
            Message::AcquireBlock { task_id } => task_id.clone(),
            _ => unreachable!(),
        };

        match scheduler.acquire_block(&task_id) {
            Some(block) => {
                debug!(block_id = %block.block_id, "sending block");
                bookkeeper.notify_block_sent(block.clone(), cm.addr);
                let _ = cm.reply_tx.try_send(Message::SendBlock { block });
            }
            None => {
                let counters = scheduler.task_states[&task_id].counters();
                let terminal = !scheduler.task_states[&task_id].is_running();
                if terminal
                    || (counters.pending_count() <= 0 && counters.processing_count <= 0)
                {
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
                pool.set_num_workers(task.num_workers, &self.host, self.port)?;
            }
        }
        Ok(())
    }

    fn check_all_done(&self, scheduler: &Scheduler) -> bool {
        scheduler.task_states.values().all(|state| state.is_done())
    }
}
