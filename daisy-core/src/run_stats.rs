//! Low-level measurement helpers shared by the block-profiling layer.
//!
//! Statistics themselves live with the blocks: `block_profile` measures a
//! block inside the worker, the payload rides home on `Block::stats`, and
//! `block_tracking` persists and aggregates it. This module holds only the
//! two pieces that are useful on their own:
//!
//! - `thread_cpu_time` — per-OS-thread CPU accounting, so concurrent
//!   thread workers don't contaminate each other's numbers.
//! - `linear_trend` — the least-squares fit behind "blocks started at 2 ms
//!   and ended at 12 ms", applied to the per-block wall times collected by
//!   `block_tracking::TaskSummary`.
//!
//! There is deliberately no per-worker or process-wide stats struct here
//! any more: a second accumulator alongside the tracking layer is exactly
//! what made `blocks_processed` disagree with the scheduler's own counts.

use std::time::Duration;

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
