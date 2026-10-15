//! Per-worker lifecycle timing, observed from the server.
//!
//! Every worker-lifecycle event already crosses the server's main loop with
//! the worker's server-assigned id attached — the spawn call, the first
//! `AcquireBlock`, every `SendBlock`, every `ReleaseBlock`/`BlockFailed`,
//! the `RequestShutdown` reply, and the spawn thread's return. Stamping
//! those events is enough to split a worker's life into four phases:
//!
//! ```text
//! spawn ──startup──▶ connect ──idle──▶ block ──compute──▶ return ──idle──▶ block … ──final wait──▶ shutdown ──teardown──▶ exit
//! ```
//!
//! - **startup**: spawn call → first `AcquireBlock`. Process launch,
//!   imports, the user's own setup (model loading), the TCP connect — and,
//!   for a cluster spawn function (`sbatch --wait`, `srun`), the queue wait.
//! - **idle**: returning one block → being handed the next (for the first
//!   block: connect → first block). Waiting on the scheduler, which includes
//!   dependency gating and read/write conflicts.
//! - **compute**: block handed out → block returned. The server round trip,
//!   so it includes transport and the worker's own per-block wrapper; the
//!   block's `wall_seconds` is the worker's narrower measurement of the
//!   user function, and the difference is reported as `overhead`.
//! - **teardown**: `RequestShutdown` sent → spawn call returned. A worker
//!   whose last acquire reached the server after the run had ended never
//!   gets a reply (it sees the socket close instead); for it teardown is
//!   counted from the moment the server began shutting down.
//! - **final wait**: last return → `RequestShutdown`; kept per worker but
//!   not an "idle before a block", so it stays out of the idle aggregate.
//!
//! The server is the only vantage point that can see startup and teardown
//! at all (a worker cannot report time before its interpreter started or
//! after its process exited), and timing here needs no wire change: the
//! protocol is untouched. The cost is one network hop inside idle and
//! compute. Clients that connect without a server-assigned id (hand-rolled
//! workers outside daisy's management) are counted, not timed.
//!
//! Collection is always on: a handful of `Instant`s per worker, two
//! `Welford` accumulators per worker, O(workers) memory in total.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::run_stats::Welford;

/// How a worker's life ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkerExitKind {
    /// Spawn function returned cleanly.
    Clean,
    /// Spawn function returned an error, or its thread panicked.
    Error,
    /// Its block exceeded the task timeout and the slot was retired.
    Timeout,
    /// Still alive when the run was aborted.
    Interrupted,
}

/// One worker's stamped lifecycle. All arithmetic is on `Instant`s; the
/// `now` is passed in so the transitions are unit-testable.
#[derive(Clone, Debug)]
pub struct WorkerTimeline {
    pub worker_id: u64,
    pub task_id: String,
    spawned_at: Instant,
    connected_at: Option<Instant>,
    last_return_at: Option<Instant>,
    in_flight: Option<Instant>,
    shutdown_sent_at: Option<Instant>,
    exited_at: Option<Instant>,
    exit: Option<WorkerExitKind>,
    blocks_ok: u64,
    blocks_failed: u64,
    idle: Welford,
    compute: Welford,
    final_wait: Option<f64>,
}

impl WorkerTimeline {
    pub fn new(worker_id: u64, task_id: &str, now: Instant) -> Self {
        Self {
            worker_id,
            task_id: task_id.to_string(),
            spawned_at: now,
            connected_at: None,
            last_return_at: None,
            in_flight: None,
            shutdown_sent_at: None,
            exited_at: None,
            exit: None,
            blocks_ok: 0,
            blocks_failed: 0,
            idle: Welford::default(),
            compute: Welford::default(),
            final_wait: None,
        }
    }

    /// First (and every) `AcquireBlock`; only the first one is a stamp.
    pub fn acquired(&mut self, now: Instant) {
        if self.connected_at.is_none() {
            self.connected_at = Some(now);
        }
    }

    /// A block was handed to this worker: close the idle gap since the
    /// previous return (or since connecting).
    pub fn block_sent(&mut self, now: Instant) {
        let since = self.last_return_at.or(self.connected_at);
        if let Some(t) = since {
            self.idle.push(now.duration_since(t).as_secs_f64());
        }
        self.in_flight = Some(now);
    }

    /// The block came back (ok or failed); `sent_at` is the bookkeeper's
    /// send stamp for that block. Returns the round-trip seconds.
    pub fn block_returned(&mut self, now: Instant, sent_at: Instant, ok: bool) -> f64 {
        let elapsed = now.duration_since(sent_at).as_secs_f64();
        self.compute.push(elapsed);
        if ok {
            self.blocks_ok += 1;
        } else {
            self.blocks_failed += 1;
        }
        self.in_flight = None;
        self.last_return_at = Some(now);
        elapsed
    }

    pub fn shutdown_sent(&mut self, now: Instant) {
        if self.shutdown_sent_at.is_some() {
            return;
        }
        self.shutdown_sent_at = Some(now);
        if let Some(t) = self.last_return_at.or(self.connected_at) {
            self.final_wait = Some(now.duration_since(t).as_secs_f64());
        }
    }

    /// The first exit wins: a timeout-retired worker that later exits on
    /// its own keeps `Timeout`.
    pub fn exited(&mut self, now: Instant, kind: WorkerExitKind) {
        if self.exit.is_some() {
            return;
        }
        self.exit = Some(kind);
        self.exited_at = Some(now);
    }

    pub fn has_exited(&self) -> bool {
        self.exit.is_some()
    }

    pub fn startup(&self) -> Option<f64> {
        self.connected_at
            .map(|c| c.duration_since(self.spawned_at).as_secs_f64())
    }

    /// `run_shutdown` is when the server began shutting down, the fallback
    /// start of teardown for a clean exit that never received
    /// `RequestShutdown` (its last acquire lost the end-of-run race).
    pub fn teardown(&self, run_shutdown: Option<Instant>) -> Option<f64> {
        if self.exit != Some(WorkerExitKind::Clean) {
            return None;
        }
        let exited = self.exited_at?;
        let from = match self.shutdown_sent_at {
            Some(s) => s,
            None => run_shutdown.filter(|s| *s <= exited)?,
        };
        Some(exited.saturating_duration_since(from).as_secs_f64())
    }

    pub fn record(&self, run_shutdown: Option<Instant>) -> WorkerRecord {
        WorkerRecord {
            worker_id: self.worker_id,
            startup: self.startup(),
            blocks_ok: self.blocks_ok,
            blocks_failed: self.blocks_failed,
            idle: self.idle.clone(),
            compute: self.compute.clone(),
            final_wait: self.final_wait,
            teardown: self.teardown(run_shutdown),
            exit: self.exit,
        }
    }
}

/// One worker's row in the report.
#[derive(Clone, Debug, Serialize)]
pub struct WorkerRecord {
    pub worker_id: u64,
    /// `None` if the worker never connected.
    pub startup: Option<f64>,
    pub blocks_ok: u64,
    pub blocks_failed: u64,
    /// Seconds between blocks, one sample per block handed out.
    pub idle: Welford,
    /// Round-trip seconds per block, one sample per block returned.
    pub compute: Welford,
    /// Last return → `RequestShutdown`.
    pub final_wait: Option<f64>,
    /// `RequestShutdown` → spawn call returned; only for clean exits.
    pub teardown: Option<f64>,
    /// `None` if the worker was still running when the report was built.
    pub exit: Option<WorkerExitKind>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ExitCounts {
    pub clean: u64,
    pub error: u64,
    pub timeout: u64,
    pub interrupted: u64,
    pub running: u64,
}

/// The per-task workers section of the report.
#[derive(Clone, Debug, Default, Serialize)]
pub struct WorkerReport {
    /// Workers daisy spawned for this task.
    pub count: u64,
    /// Distinct clients that took blocks without a server-assigned id.
    pub unmanaged_clients: u64,
    /// One sample per worker that connected.
    pub startup: Welford,
    /// Per-block idle gaps pooled across workers.
    pub idle: Welford,
    /// Per-block round trips pooled across workers.
    pub compute: Welford,
    /// One sample per worker that exited cleanly after a shutdown request.
    pub teardown: Welford,
    pub exits: ExitCounts,
    pub by_id: Vec<WorkerRecord>,
}

#[derive(Debug, Default)]
struct TaskSide {
    /// Server round trip per successfully returned block, managed and
    /// unmanaged clients alike — the block-level time figure that needs
    /// no tracking store.
    round_trip: Welford,
    /// Round trip minus the worker's own `wall_seconds`, per measured block.
    overhead: Welford,
    unmanaged: HashSet<SocketAddr>,
}

/// Everything the server stamps during one run.
#[derive(Debug)]
pub struct RunTimeline {
    origin: Instant,
    origin_epoch: f64,
    /// When the run loop exited and shutdown began.
    shutdown_at: Option<Instant>,
    workers: BTreeMap<u64, WorkerTimeline>,
    tasks: HashMap<String, TaskSide>,
    /// Exit instants reported by the spawn threads themselves, consumed
    /// when the thread is reaped so a busy main loop does not inflate
    /// teardown.
    exit_times: HashMap<u64, Instant>,
}

impl Default for RunTimeline {
    fn default() -> Self {
        Self::new()
    }
}

impl RunTimeline {
    pub fn new() -> Self {
        let origin = Instant::now();
        let origin_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        Self {
            origin,
            origin_epoch,
            shutdown_at: None,
            workers: BTreeMap::new(),
            tasks: HashMap::new(),
            exit_times: HashMap::new(),
        }
    }

    /// Seconds since the UNIX epoch at `at`, on the server's clock.
    pub fn epoch_seconds(&self, at: Instant) -> f64 {
        self.origin_epoch + at.saturating_duration_since(self.origin).as_secs_f64()
    }

    pub fn started_at(&self) -> f64 {
        self.origin_epoch
    }

    pub fn spawned(&mut self, worker_id: u64, task_id: &str) {
        self.workers
            .insert(worker_id, WorkerTimeline::new(worker_id, task_id, Instant::now()));
    }

    pub fn acquired(&mut self, worker_id: u64) {
        if let Some(w) = self.workers.get_mut(&worker_id) {
            w.acquired(Instant::now());
        }
    }

    /// Stamp a dispatch. Returns the send instant for the bookkeeper.
    pub fn block_sent(&mut self, worker_id: Option<u64>, task_id: &str, addr: SocketAddr) -> Instant {
        let now = Instant::now();
        match worker_id.and_then(|id| self.workers.get_mut(&id)) {
            Some(w) => w.block_sent(now),
            None => {
                self.tasks.entry(task_id.to_string()).or_default().unmanaged.insert(addr);
            }
        }
        now
    }

    /// Stamp a return. `wall_seconds` is the worker's own measurement when
    /// the block carried one; the difference to the round trip is the
    /// per-block overhead. Returns the return instant.
    pub fn block_returned(
        &mut self,
        worker_id: Option<u64>,
        task_id: &str,
        sent_at: Instant,
        ok: bool,
        wall_seconds: Option<f64>,
    ) -> Instant {
        let now = Instant::now();
        let elapsed = match worker_id.and_then(|id| self.workers.get_mut(&id)) {
            Some(w) => w.block_returned(now, sent_at, ok),
            None => now.duration_since(sent_at).as_secs_f64(),
        };
        let side = self.tasks.entry(task_id.to_string()).or_default();
        if ok {
            side.round_trip.push(elapsed);
        }
        if let Some(wall) = wall_seconds {
            side.overhead.push((elapsed - wall).max(0.0));
        }
        now
    }

    /// The run loop has exited; every clean exit from here on is a
    /// teardown even if the worker never got an explicit reply.
    pub fn shutdown_started(&mut self) {
        if self.shutdown_at.is_none() {
            self.shutdown_at = Some(Instant::now());
        }
    }

    pub fn shutdown_sent(&mut self, worker_id: u64) {
        if let Some(w) = self.workers.get_mut(&worker_id) {
            w.shutdown_sent(Instant::now());
        }
    }

    /// The spawn thread stamped its own exit; used by the next `exited`.
    pub fn note_exit_time(&mut self, worker_id: u64, at: Instant) {
        self.exit_times.entry(worker_id).or_insert(at);
    }

    pub fn exited(&mut self, worker_id: u64, kind: WorkerExitKind) {
        let at = self.exit_times.remove(&worker_id).unwrap_or_else(Instant::now);
        if let Some(w) = self.workers.get_mut(&worker_id) {
            w.exited(at, kind);
        }
    }

    /// Close every worker that has not exited yet (run aborted).
    pub fn close_remaining(&mut self, kind: WorkerExitKind) {
        let now = Instant::now();
        for w in self.workers.values_mut() {
            if !w.has_exited() {
                w.exited(now, kind);
            }
        }
    }

    pub fn round_trip(&self, task_id: &str) -> Welford {
        self.tasks
            .get(task_id)
            .map(|t| t.round_trip.clone())
            .unwrap_or_default()
    }

    pub fn overhead(&self, task_id: &str) -> Welford {
        self.tasks
            .get(task_id)
            .map(|t| t.overhead.clone())
            .unwrap_or_default()
    }

    /// Fold this task's workers into the report section.
    pub fn report_for(&self, task_id: &str) -> WorkerReport {
        let mut r = WorkerReport {
            unmanaged_clients: self
                .tasks
                .get(task_id)
                .map(|t| t.unmanaged.len() as u64)
                .unwrap_or(0),
            ..WorkerReport::default()
        };
        for w in self.workers.values().filter(|w| w.task_id == task_id) {
            let rec = w.record(self.shutdown_at);
            r.count += 1;
            if let Some(s) = rec.startup {
                r.startup.push(s);
            }
            if let Some(t) = rec.teardown {
                r.teardown.push(t);
            }
            r.idle.merge(&rec.idle);
            r.compute.merge(&rec.compute);
            match rec.exit {
                Some(WorkerExitKind::Clean) => r.exits.clean += 1,
                Some(WorkerExitKind::Error) => r.exits.error += 1,
                Some(WorkerExitKind::Timeout) => r.exits.timeout += 1,
                Some(WorkerExitKind::Interrupted) => r.exits.interrupted += 1,
                None => r.exits.running += 1,
            }
            r.by_id.push(rec);
        }
        r
    }

    pub fn task_ids(&self) -> impl Iterator<Item = &str> {
        self.workers.values().map(|w| w.task_id.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn at(origin: Instant, ms: u64) -> Instant {
        origin + Duration::from_millis(ms)
    }

    #[test]
    fn phases_are_cut_at_the_right_events() {
        let o = Instant::now();
        let mut w = WorkerTimeline::new(0, "t", o);
        w.acquired(at(o, 2000)); // startup 2.0 s
        w.block_sent(at(o, 2100)); // idle 0.1 s (connect → first block)
        w.block_returned(at(o, 3100), at(o, 2100), true); // compute 1.0 s
        w.acquired(at(o, 3100)); // second acquire is not a stamp
        w.block_sent(at(o, 3300)); // idle 0.2 s
        w.block_returned(at(o, 4300), at(o, 3300), false); // compute 1.0 s, failed
        w.shutdown_sent(at(o, 4350)); // final wait 0.05 s
        w.exited(at(o, 4550), WorkerExitKind::Clean); // teardown 0.2 s

        let r = w.record(None);
        assert!((r.startup.unwrap() - 2.0).abs() < 1e-9);
        assert_eq!(r.idle.n(), 2);
        assert!((r.idle.mean() - 0.15).abs() < 1e-9);
        assert_eq!(r.compute.n(), 2);
        assert!((r.compute.mean() - 1.0).abs() < 1e-9);
        assert_eq!((r.blocks_ok, r.blocks_failed), (1, 1));
        assert!((r.final_wait.unwrap() - 0.05).abs() < 1e-9);
        assert!((r.teardown.unwrap() - 0.2).abs() < 1e-9);
        assert_eq!(r.exit, Some(WorkerExitKind::Clean));
    }

    #[test]
    fn a_worker_that_never_connected_has_no_startup_or_teardown() {
        let o = Instant::now();
        let mut w = WorkerTimeline::new(3, "t", o);
        w.exited(at(o, 10), WorkerExitKind::Error);
        let r = w.record(Some(at(o, 5)));
        assert_eq!(r.startup, None);
        assert_eq!(r.teardown, None);
        assert_eq!(r.exit, Some(WorkerExitKind::Error));
        assert!(r.idle.is_empty() && r.compute.is_empty());
    }

    #[test]
    fn first_exit_wins_and_dirty_exits_have_no_teardown() {
        let o = Instant::now();
        let mut w = WorkerTimeline::new(1, "t", o);
        w.acquired(at(o, 1));
        w.shutdown_sent(at(o, 5));
        w.exited(at(o, 7), WorkerExitKind::Timeout);
        w.exited(at(o, 9), WorkerExitKind::Clean);
        let r = w.record(None);
        assert_eq!(r.exit, Some(WorkerExitKind::Timeout));
        assert_eq!(r.teardown, None);
    }

    #[test]
    fn teardown_falls_back_to_the_run_shutdown_for_unanswered_workers() {
        let o = Instant::now();
        let mut w = WorkerTimeline::new(1, "t", o);
        w.acquired(at(o, 1));
        w.exited(at(o, 500), WorkerExitKind::Clean);
        // No RequestShutdown ever went out: without a run shutdown stamp
        // there is nothing to measure from...
        assert_eq!(w.record(None).teardown, None);
        // ...with one, teardown runs from it.
        let r = w.record(Some(at(o, 300)));
        assert!((r.teardown.unwrap() - 0.2).abs() < 1e-9);
        // A shutdown that began after the exit is not a teardown.
        assert_eq!(w.record(Some(at(o, 900))).teardown, None);
    }

    #[test]
    fn run_timeline_groups_by_task_and_pools_samples() {
        let mut t = RunTimeline::new();
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
        t.spawned(0, "a");
        t.spawned(1, "a");
        t.spawned(2, "b");
        for id in 0..3 {
            t.acquired(id);
        }
        let s0 = t.block_sent(Some(0), "a", addr);
        let s1 = t.block_sent(Some(1), "a", addr);
        t.block_sent(None, "a", addr); // unmanaged client
        t.block_returned(Some(0), "a", s0, true, Some(0.0));
        t.block_returned(Some(1), "a", s1, true, None);
        t.shutdown_sent(0);
        t.exited(0, WorkerExitKind::Clean);
        t.exited(1, WorkerExitKind::Error);
        t.close_remaining(WorkerExitKind::Interrupted);

        let a = t.report_for("a");
        assert_eq!(a.count, 2);
        assert_eq!(a.unmanaged_clients, 1);
        assert_eq!(a.startup.n(), 2);
        assert_eq!(a.idle.n(), 2);
        assert_eq!(a.compute.n(), 2);
        assert_eq!(a.teardown.n(), 1);
        assert_eq!((a.exits.clean, a.exits.error), (1, 1));
        assert_eq!(a.by_id.len(), 2);
        assert_eq!(t.overhead("a").n(), 1);
        assert_eq!(t.round_trip("a").n(), 2);
        assert!(t.round_trip("b").is_empty());

        let b = t.report_for("b");
        assert_eq!(b.count, 1);
        assert_eq!(b.exits.interrupted, 1);
        assert_eq!(b.exits.running, 0);
        assert!(t.report_for("c").by_id.is_empty());
    }

    #[test]
    fn epoch_seconds_tracks_the_wall_clock() {
        let t = RunTimeline::new();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let stamped = t.epoch_seconds(Instant::now());
        assert!((stamped - now).abs() < 1.0, "{stamped} vs {now}");
        assert!(t.started_at() <= stamped);
    }
}
