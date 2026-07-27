//! Resource-utilisation tracking for a single `run_blockwise` call.
//!
//! Three layers of measurement:
//!
//! - **Per-worker** — wall-clock alive and OS-thread CPU time are
//!   reported when the worker thread exits (through the exit channel);
//!   blocks processed are counted server-side as block returns arrive,
//!   attributed to workers via the `Register` handshake, and merged in
//!   `build_run_stats`. Server-side counting is what makes the numbers
//!   mode-independent: thread workers, subprocess-shim workers, and
//!   external cluster workers all release blocks over the same TCP
//!   protocol.
//! - **Per-task aggregate** — sum across that task's workers, plus a
//!   linear-regression fit `(release_index, duration_ms)` so the
//!   "first block ran in 2 ms, last block ran in 12 ms" trend is
//!   visible.
//! - **Process-wide** — peak RSS, peak virtual, total CPU, total disk
//!   I/O. Sampled every 200ms by a background task.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Read this thread's user+system CPU time. Returns None on platforms
/// without a per-thread accessor (e.g. some BSDs / Windows MSYS).
pub fn thread_cpu_time() -> Option<Duration> {
    #[cfg(target_os = "linux")]
    {
        unsafe {
            let mut ru: libc::rusage = std::mem::zeroed();
            if libc::getrusage(libc::RUSAGE_THREAD, &mut ru) != 0 {
                return None;
            }
            let user = duration_from_timeval(&ru.ru_utime);
            let sys = duration_from_timeval(&ru.ru_stime);
            Some(user + sys)
        }
    }
    #[cfg(target_os = "macos")]
    {
        // mach_thread_self → thread_info(thread, THREAD_BASIC_INFO, ...)
        // We only need user_time + system_time from thread_basic_info.
        macos_thread_cpu_time()
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

#[cfg(target_os = "linux")]
fn duration_from_timeval(tv: &libc::timeval) -> Duration {
    let secs = tv.tv_sec as u64;
    let micros = tv.tv_usec as u32;
    Duration::new(secs, micros * 1000)
}

#[cfg(target_os = "macos")]
fn macos_thread_cpu_time() -> Option<Duration> {
    // libc on macOS exposes mach_thread_self() and the thread_info()
    // syscall via the `mach2` crate, but bringing that in just for this
    // is overkill. Inline what we need: the Mach syscall numbers and
    // the thread_basic_info layout. The whole thing is two ints +
    // four time_value_t fields.
    use std::os::raw::{c_int, c_uint};

    // thread_t is a mach_port_name_t which is an unsigned int.
    type ThreadT = c_uint;
    type KernReturnT = c_int;
    type ThreadFlavorT = c_int;
    type MachMsgTypeNumberT = c_uint;

    const THREAD_BASIC_INFO: ThreadFlavorT = 3;
    // The size of `thread_basic_info_data_t` in u32-sized words.
    const THREAD_BASIC_INFO_COUNT: MachMsgTypeNumberT = 10;

    #[repr(C)]
    #[derive(Default)]
    struct TimeValueT {
        seconds: c_int,
        microseconds: c_int,
    }

    #[repr(C)]
    #[derive(Default)]
    struct ThreadBasicInfo {
        user_time: TimeValueT,
        system_time: TimeValueT,
        cpu_usage: c_int,
        policy: c_int,
        run_state: c_int,
        flags: c_int,
        suspend_count: c_int,
        sleep_time: c_int,
    }

    unsafe extern "C" {
        fn mach_thread_self() -> ThreadT;
        fn mach_port_deallocate(task: ThreadT, port: ThreadT) -> KernReturnT;
        fn mach_task_self() -> ThreadT;
        fn thread_info(
            target: ThreadT,
            flavor: ThreadFlavorT,
            info: *mut ThreadBasicInfo,
            count: *mut MachMsgTypeNumberT,
        ) -> KernReturnT;
    }

    unsafe {
        let thread = mach_thread_self();
        let mut info = ThreadBasicInfo::default();
        let mut count = THREAD_BASIC_INFO_COUNT;
        let kr = thread_info(thread, THREAD_BASIC_INFO, &mut info, &mut count);
        // Always release the port reference we just took.
        let _ = mach_port_deallocate(mach_task_self(), thread);
        if kr != 0 {
            return None;
        }
        let user_us = info.user_time.seconds as u64 * 1_000_000
            + info.user_time.microseconds as u64;
        let sys_us = info.system_time.seconds as u64 * 1_000_000
            + info.system_time.microseconds as u64;
        Some(Duration::from_micros(user_us + sys_us))
    }
}

/// Per-worker stats reported back when a worker thread exits.
#[derive(Clone, Debug, Default)]
pub struct WorkerStats {
    pub task_id: String,
    pub worker_id: u64,
    pub wall_time: Duration,
    /// `None` if the platform doesn't expose per-thread CPU.
    pub cpu_time: Option<Duration>,
    /// Valid block returns the server observed from this worker. Not
    /// filled in by the worker thread — counted in `RunTally` as
    /// releases arrive and merged in `build_run_stats`.
    pub blocks_processed: u64,
    /// Error that terminated this worker, if it exited dirty. Carried
    /// back so the server can attach the cause to the task state.
    pub last_error: Option<String>,
}

/// Server-side per-run accounting, fed from client messages as the run
/// progresses and folded into `RunStats` by `build_run_stats`.
///
/// Counting block returns at the server (rather than in the worker
/// thread's own loop) is what keeps `blocks_processed` correct across
/// every execution mode: in-process threads, subprocess-shim workers,
/// and external cluster workers all return blocks over the same TCP
/// protocol. It is also more honest — an attempt the server reclaimed
/// (timeout, dead client) was never a valid return and is not counted.
#[derive(Debug, Default)]
pub struct RunTally {
    /// task_id -> per-block server-side durations (ms), in the order
    /// each successful block's `ReleaseBlock` was observed.
    pub block_durations: HashMap<String, Vec<f64>>,
    /// task_id -> valid block returns observed (successes and failures).
    pub task_blocks: HashMap<String, u64>,
    /// (task_id, worker_id) -> valid block returns from that worker,
    /// for connections that announced themselves via `Register`.
    pub worker_blocks: HashMap<(String, u64), u64>,
}

impl RunTally {
    /// Record one valid block return. `worker_id` is the returning
    /// connection's registered identity, when it announced one.
    pub fn count_return(&mut self, task_id: &str, worker_id: Option<u64>) {
        *self.task_blocks.entry(task_id.to_string()).or_default() += 1;
        if let Some(wid) = worker_id {
            *self
                .worker_blocks
                .entry((task_id.to_string(), wid))
                .or_default() += 1;
        }
    }
}

/// Per-task aggregate stats produced at the end of a run.
#[derive(Clone, Debug, Default)]
pub struct TaskStats {
    pub task_id: String,
    pub blocks_processed: u64,
    pub max_concurrent_workers: usize,
    /// Sum of per-block server-side durations (send-block → release-block).
    pub total_block_time: Duration,
    /// Wall time the task had at least one alive worker.
    pub total_wall_time: Duration,
    /// Sum of per-worker CPU time. None if any worker reported None.
    pub total_cpu_time: Option<Duration>,
    /// Linear regression of (release_index, duration_ms).
    pub mean_block_ms: f64,
    pub block_ms_slope: f64,
}

/// Process-wide snapshot. All counters are deltas relative to the
/// process at run start (so disk_read = bytes read *during this run*).
#[derive(Clone, Debug, Default)]
pub struct ProcessStats {
    pub wall_time: Duration,
    pub peak_rss_bytes: u64,
    pub peak_virt_bytes: u64,
    /// Sum of CPU time across all threads in the process at the final
    /// sample. Approximate — the OS only reports cumulative process CPU.
    pub total_cpu_time: Duration,
    pub disk_read_bytes: u64,
    pub disk_write_bytes: u64,
    /// True when sysinfo failed to find this process — stats are zeros.
    pub unavailable: bool,
}

#[derive(Clone, Debug, Default)]
pub struct RunStats {
    pub process: ProcessStats,
    pub per_task: HashMap<String, TaskStats>,
    pub per_worker: Vec<WorkerStats>,
}

/// Least-squares fit of y = m*x + b, where x = 0, 1, ..., n-1.
/// Returns (mean_y, slope_m). Slope is 0 for fewer than 2 points.
pub fn linear_trend(y: &[f64]) -> (f64, f64) {
    let n = y.len();
    if n == 0 {
        return (0.0, 0.0);
    }
    let mean_y: f64 = y.iter().sum::<f64>() / n as f64;
    if n < 2 {
        return (mean_y, 0.0);
    }
    let mean_x = (n - 1) as f64 / 2.0;
    let mut num = 0.0;
    let mut den = 0.0;
    for (i, &yi) in y.iter().enumerate() {
        let xi = i as f64;
        num += (xi - mean_x) * (yi - mean_y);
        den += (xi - mean_x) * (xi - mean_x);
    }
    let slope = if den == 0.0 { 0.0 } else { num / den };
    (mean_y, slope)
}

/// Build a `RunStats` from the raw inputs collected during a run.
///
/// `worker_stats` is the per-worker fold across all retired workers
/// (including any still-alive workers whose stats were captured at
/// shutdown). Their `blocks_processed` is filled in here from the
/// tally's server-side counts; workers the server heard from that have
/// no thread in this process (external cluster workers) get synthetic
/// entries carrying just their block counts.
pub fn build_run_stats(
    started: Instant,
    mut worker_stats: Vec<WorkerStats>,
    tally: RunTally,
    process: ProcessStats,
) -> RunStats {
    let RunTally {
        block_durations,
        task_blocks,
        mut worker_blocks,
    } = tally;

    for ws in &mut worker_stats {
        ws.blocks_processed = worker_blocks
            .remove(&(ws.task_id.clone(), ws.worker_id))
            .unwrap_or(0);
    }
    for ((task_id, worker_id), blocks) in worker_blocks {
        worker_stats.push(WorkerStats {
            task_id,
            worker_id,
            blocks_processed: blocks,
            ..Default::default()
        });
    }

    let mut per_task: HashMap<String, TaskStats> = HashMap::new();

    for ws in &worker_stats {
        let entry = per_task
            .entry(ws.task_id.clone())
            .or_insert_with(|| TaskStats {
                task_id: ws.task_id.clone(),
                ..Default::default()
            });
        entry.total_wall_time += ws.wall_time;
        match (entry.total_cpu_time, ws.cpu_time) {
            (Some(prev), Some(more)) => entry.total_cpu_time = Some(prev + more),
            (Some(_), None) => entry.total_cpu_time = None,
            (None, _) if entry.total_wall_time == ws.wall_time => {
                // First worker; carry through whatever the worker had.
                entry.total_cpu_time = ws.cpu_time;
            }
            _ => {}
        }
    }

    for (task_id, blocks) in task_blocks {
        per_task
            .entry(task_id.clone())
            .or_insert_with(|| TaskStats {
                task_id,
                ..Default::default()
            })
            .blocks_processed = blocks;
    }

    for (task_id, durations) in block_durations {
        let entry = per_task
            .entry(task_id.clone())
            .or_insert_with(|| TaskStats {
                task_id: task_id.clone(),
                ..Default::default()
            });
        let (mean, slope) = linear_trend(&durations);
        entry.mean_block_ms = mean;
        entry.block_ms_slope = slope;
        entry.total_block_time =
            Duration::from_secs_f64(durations.iter().sum::<f64>() / 1000.0);
    }

    // max_concurrent_workers — naive estimate: count of distinct worker
    // ids that ever existed for the task. Real concurrent-peak tracking
    // would require per-event sampling, which the runner does not do
    // for stats; close enough for the report.
    let mut counts: HashMap<String, usize> = HashMap::new();
    for ws in &worker_stats {
        *counts.entry(ws.task_id.clone()).or_insert(0) += 1;
    }
    for (task_id, count) in counts {
        if let Some(entry) = per_task.get_mut(&task_id) {
            entry.max_concurrent_workers = count;
        }
    }

    let mut process_with_wall = process;
    process_with_wall.wall_time = started.elapsed();

    RunStats {
        process: process_with_wall,
        per_task,
        per_worker: worker_stats,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linear_trend_constant_signal_has_zero_slope() {
        let (mean, slope) = linear_trend(&[3.0; 100]);
        assert!((mean - 3.0).abs() < 1e-9);
        assert!(slope.abs() < 1e-9);
    }

    #[test]
    fn linear_trend_recovers_known_slope() {
        // y = 2.0 + 0.5 * x for x in 0..10
        let y: Vec<f64> = (0..10).map(|i| 2.0 + 0.5 * i as f64).collect();
        let (mean, slope) = linear_trend(&y);
        assert!((slope - 0.5).abs() < 1e-9, "slope={slope}");
        // mean of 2.0, 2.5, …, 6.5 is 4.25
        assert!((mean - 4.25).abs() < 1e-9, "mean={mean}");
    }

    #[test]
    fn linear_trend_handles_short_inputs() {
        assert_eq!(linear_trend(&[]), (0.0, 0.0));
        assert_eq!(linear_trend(&[5.0]), (5.0, 0.0));
    }

    #[test]
    fn thread_cpu_time_returns_some_on_linux_and_macos() {
        // Burn a tiny bit of CPU so the result isn't trivially zero.
        let mut s: u64 = 0;
        for i in 0..10_000 {
            s = s.wrapping_add(i);
        }
        std::hint::black_box(s);
        let t = thread_cpu_time();
        if cfg!(any(target_os = "linux", target_os = "macos")) {
            assert!(t.is_some(), "expected per-thread CPU on this platform");
        }
    }
}
