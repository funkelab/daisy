use std::collections::HashMap;
use std::process::{Child, ExitStatus};
use tracing::{debug, info, warn};

/// Typed worker state — the compiler enforces exhaustive handling of every
/// case, eliminating the class of bugs from daisy's Python implementation
/// where nullable `process` fields and ambiguous exit codes led to unbounded
/// worker growth.
#[derive(Debug)]
pub enum WorkerState {
    /// Worker process is running.
    Running(Child),
    /// Worker exited normally (exit code 0). Counted toward the target pool
    /// size so it is NOT replaced.
    Done,
    /// Worker crashed (non-zero exit code or signal). Will be removed from
    /// the pool and replaced.
    Failed(ExitStatus),
    /// We terminated this worker (timeout, shutdown). Not replaced.
    Killed,
}

type WorkerId = u64;

/// Manages a pool of worker processes for a single task.
pub struct WorkerPool {
    workers: HashMap<WorkerId, WorkerState>,
    next_id: u64,
    target_count: usize,
    spawn_fn: Box<dyn Fn(WorkerId, &str, u16) -> std::io::Result<Child> + Send>,
}

impl WorkerPool {
    /// Create a new worker pool. `spawn_fn` takes (worker_id, host, port)
    /// and returns a `Child` process.
    pub fn new(
        spawn_fn: impl Fn(WorkerId, &str, u16) -> std::io::Result<Child> + Send + 'static,
    ) -> Self {
        Self {
            workers: HashMap::new(),
            next_id: 0,
            target_count: 0,
            spawn_fn: Box::new(spawn_fn),
        }
    }

    /// Set the target number of workers. Spawns new workers if needed,
    /// kills excess workers if the target is reduced.
    pub fn set_num_workers(
        &mut self,
        num_workers: usize,
        host: &str,
        port: u16,
    ) -> std::io::Result<()> {
        self.target_count = num_workers;
        let current_live = self.live_count();
        if current_live < num_workers {
            self.start_workers(num_workers - current_live, host, port)?;
        } else if current_live > num_workers {
            self.stop_workers(current_live - num_workers);
        }
        Ok(())
    }

    /// Spawn `n` additional workers.
    pub fn start_workers(&mut self, n: usize, host: &str, port: u16) -> std::io::Result<()> {
        debug!(n, "starting new workers");
        for _ in 0..n {
            let id = self.next_id;
            self.next_id += 1;
            let child = (self.spawn_fn)(id, host, port)?;
            debug!(worker_id = id, pid = child.id(), "spawned worker");
            self.workers.insert(id, WorkerState::Running(child));
        }
        Ok(())
    }

    /// Stop all workers (or just one by id).
    pub fn stop_all(&mut self) {
        let ids: Vec<WorkerId> = self.workers.keys().copied().collect();
        for id in ids {
            self.kill_worker(id);
        }
    }

    /// Stop the `n` most recently spawned live workers.
    fn stop_workers(&mut self, n: usize) {
        let mut live_ids: Vec<WorkerId> = self
            .workers
            .iter()
            .filter(|(_, state)| matches!(state, WorkerState::Running(_)))
            .map(|(id, _)| *id)
            .collect();
        live_ids.sort();
        // Kill the most recently spawned ones (highest IDs).
        for &id in live_ids.iter().rev().take(n) {
            self.kill_worker(id);
        }
    }

    fn kill_worker(&mut self, id: WorkerId) {
        if let Some(state) = self.workers.get_mut(&id) {
            if let WorkerState::Running(child) = state {
                debug!(worker_id = id, pid = child.id(), "terminating worker");
                let _ = child.kill();
                let _ = child.wait();
            }
            self.workers.insert(id, WorkerState::Killed);
        }
    }

    /// Check all running workers for exit. Returns the number of workers
    /// that crashed (non-zero exit) and need replacement. Workers that
    /// exited normally are moved to `Done` state and stay counted toward
    /// the pool target.
    pub fn reap(&mut self) -> usize {
        let mut crashed_ids = Vec::new();

        for (id, state) in &mut self.workers {
            if let WorkerState::Running(child) = state {
                match child.try_wait() {
                    Ok(Some(exit_status)) => {
                        if exit_status.success() {
                            info!(worker_id = id, "worker exited normally");
                            // Will be set to Done below — can't mutate while iterating values.
                            crashed_ids.push((*id, true));
                        } else {
                            warn!(
                                worker_id = id,
                                ?exit_status,
                                "worker crashed"
                            );
                            crashed_ids.push((*id, false));
                        }
                    }
                    Ok(None) => {} // still running
                    Err(e) => {
                        warn!(worker_id = id, error = %e, "failed to check worker status");
                    }
                }
            }
        }

        let mut crash_count = 0;
        for (id, normal) in crashed_ids {
            if normal {
                self.workers.insert(id, WorkerState::Done);
            } else {
                // Remove the crashed worker so it doesn't count toward target.
                self.workers.remove(&id);
                crash_count += 1;
            }
        }

        crash_count
    }

    /// Replace `n` crashed workers with new ones.
    pub fn replace_crashed(
        &mut self,
        n: usize,
        host: &str,
        port: u16,
    ) -> std::io::Result<()> {
        if n > 0 {
            warn!(n, "replacing crashed workers");
            self.start_workers(n, host, port)?;
        }
        Ok(())
    }

    /// Reap dead workers and replace any that crashed.
    pub fn check_health(&mut self, host: &str, port: u16) -> std::io::Result<()> {
        let crashed = self.reap();
        self.replace_crashed(crashed, host, port)
    }

    /// Count of workers that are still in `Running` state.
    pub fn live_count(&self) -> usize {
        self.workers
            .values()
            .filter(|s| matches!(s, WorkerState::Running(_)))
            .count()
    }

    /// Total pool size including Done workers (used to prevent re-spawning
    /// normally exited workers).
    pub fn pool_size(&self) -> usize {
        self.workers
            .values()
            .filter(|s| matches!(s, WorkerState::Running(_) | WorkerState::Done))
            .count()
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.stop_all();
    }
}
