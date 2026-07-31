//! Per-block resource measurement.
//!
//! Statistics in daisy are an optional layer over normal processing: when a
//! task sets `resource_tracking`, whoever runs a block measures it and the
//! measurements ride home on the block itself (`Block::stats`). The server
//! writes them into the task's mmap'd zarr arrays as releases arrive — the
//! same mechanism, and the same per-block grid index, as the done marker.
//! There is no second block counter anywhere: the tracking layer is the only
//! thing that accumulates.
//!
//! Measurement happens *inside the worker*, which is what makes it
//! mode-independent — in-process threads, subprocess-shim workers, and
//! external cluster workers all measure the same way and return the payload
//! over the existing protocol.
//!
//! ## What the numbers mean, and their honest limits
//!
//! - `wall_seconds` — elapsed time around the block function. Always exact.
//! - `cpu_seconds` — user+system CPU consumed. Per-*thread* where the OS
//!   offers it (Linux `RUSAGE_THREAD`, macOS `thread_info`), so concurrent
//!   thread workers don't contaminate each other.
//! - `io_read_bytes` / `io_write_bytes` — bytes through the syscall layer,
//!   from `/proc/self/task/<tid>/io` (per-thread on Linux) falling back to
//!   `/proc/self/io`. Zero where the platform doesn't expose it.
//! - `peak_rss_bytes` — the process's peak RSS observed at the end of the
//!   block. **Process-wide, not per-block**: `ru_maxrss` is a monotonic
//!   high-water mark, so this reads as "how big had we grown by the time
//!   this block finished". In subprocess mode (the default) each worker is
//!   its own process handling one block at a time, which is the meaningful
//!   case; in thread mode every concurrent block reports the same
//!   process-wide figure.
//! - `gpu_util_pct` / `gpu_mem_bytes` — reserved. Populating them needs
//!   NVML and a sampling loop; the schema slots exist so adding it later
//!   doesn't change the on-disk layout. Written as NaN / 0 for now.

use std::time::{Duration, Instant};

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use crate::run_stats::thread_cpu_time;

/// Resources consumed by one block, measured by whoever ran it.
///
/// Rides on `Block::stats` and therefore over the existing
/// `ReleaseBlock` / `BlockFailed` messages — statistics add no protocol
/// traffic of their own.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize, Encode, Decode)]
pub struct BlockStats {
    pub wall_seconds: f64,
    pub cpu_seconds: f64,
    pub peak_rss_bytes: u64,
    pub io_read_bytes: u64,
    pub io_write_bytes: u64,
    /// Reserved for NVML; NaN until implemented.
    pub gpu_util_pct: f32,
    /// Reserved for NVML; 0 until implemented.
    pub gpu_mem_bytes: u64,
}

impl Default for BlockStats {
    fn default() -> Self {
        Self {
            wall_seconds: 0.0,
            cpu_seconds: 0.0,
            peak_rss_bytes: 0,
            io_read_bytes: 0,
            io_write_bytes: 0,
            gpu_util_pct: f32::NAN,
            gpu_mem_bytes: 0,
        }
    }
}

/// Bytes read/written through the syscall layer by this thread (or the
/// process, where per-thread accounting is unavailable).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct IoCounters {
    read: u64,
    write: u64,
}

/// An in-flight measurement. `start()` snapshots the counters, `finish()`
/// returns the deltas. Cheap enough to wrap every block: three small reads
/// of `/proc` plus a `getrusage`.
#[derive(Debug)]
pub struct BlockProfiler {
    started: Instant,
    cpu_at_start: Option<Duration>,
    io_at_start: Option<IoCounters>,
}

impl BlockProfiler {
    pub fn start() -> Self {
        Self {
            started: Instant::now(),
            cpu_at_start: thread_cpu_time(),
            io_at_start: read_io_counters(),
        }
    }

    /// Stop measuring and report the deltas.
    ///
    /// Counters that went backwards (a thread migrating, `/proc` becoming
    /// unreadable mid-block) saturate at zero rather than wrapping.
    pub fn finish(&self) -> BlockStats {
        let wall = self.started.elapsed();
        let cpu = match (thread_cpu_time(), self.cpu_at_start) {
            (Some(now), Some(then)) => now.saturating_sub(then),
            _ => Duration::ZERO,
        };
        let io = match (read_io_counters(), self.io_at_start) {
            (Some(now), Some(then)) => IoCounters {
                read: now.read.saturating_sub(then.read),
                write: now.write.saturating_sub(then.write),
            },
            _ => IoCounters::default(),
        };
        BlockStats {
            wall_seconds: wall.as_secs_f64(),
            cpu_seconds: cpu.as_secs_f64(),
            peak_rss_bytes: peak_rss_bytes().unwrap_or(0),
            io_read_bytes: io.read,
            io_write_bytes: io.write,
            ..BlockStats::default()
        }
    }
}

/// Peak resident set size of this process, in bytes.
///
/// `ru_maxrss` is a high-water mark, so this is the process's peak so far,
/// not this block's own footprint — see the module docs.
fn peak_rss_bytes() -> Option<u64> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        // SAFETY: getrusage only writes into the rusage we hand it.
        let ru = unsafe {
            let mut ru: libc::rusage = std::mem::zeroed();
            if libc::getrusage(libc::RUSAGE_SELF, &mut ru) != 0 {
                return None;
            }
            ru
        };
        let max_rss = ru.ru_maxrss.max(0) as u64;
        // Linux reports kibibytes; macOS reports bytes.
        #[cfg(target_os = "linux")]
        return Some(max_rss.saturating_mul(1024));
        #[cfg(target_os = "macos")]
        return Some(max_rss);
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

/// Per-thread IO byte counters, falling back to the process totals.
///
/// Linux exposes `rchar`/`wchar` (bytes through read/write syscalls,
/// including page-cache hits) per thread under `/proc/self/task/<tid>/io`.
/// Elsewhere this returns `None` and the stats read zero.
fn read_io_counters() -> Option<IoCounters> {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: gettid takes no arguments and cannot fail.
        let tid = unsafe { libc::gettid() };
        std::fs::read_to_string(format!("/proc/self/task/{tid}/io"))
            .or_else(|_| std::fs::read_to_string("/proc/self/io"))
            .ok()
            .map(|text| parse_proc_io(&text))
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// Pull `rchar` / `wchar` out of the `key: value` lines of a `/proc/*/io`
/// file. Unknown or malformed lines are ignored; missing keys read zero.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_proc_io(text: &str) -> IoCounters {
    let mut io = IoCounters::default();
    for line in text.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let Ok(n) = value.trim().parse::<u64>() else {
            continue;
        };
        match key.trim() {
            "rchar" => io.read = n,
            "wchar" => io.write = n,
            _ => {}
        }
    }
    io
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Burn a known amount of CPU without sleeping.
    fn busy(ms: u64) {
        let until = Instant::now() + Duration::from_millis(ms);
        let mut acc = 0u64;
        while Instant::now() < until {
            acc = acc.wrapping_add(1);
        }
        std::hint::black_box(acc);
    }

    #[test]
    fn measures_wall_and_cpu_of_busy_work() {
        let p = BlockProfiler::start();
        busy(50);
        let s = p.finish();

        assert!(s.wall_seconds >= 0.045, "wall too small: {s:?}");
        assert!(s.wall_seconds < 5.0, "wall implausible: {s:?}");
        // A busy loop spends its wall time on-CPU; allow generous slack for
        // a loaded machine, and accept 0 where the platform has no probe.
        if thread_cpu_time().is_some() {
            assert!(s.cpu_seconds > 0.0, "expected cpu time: {s:?}");
            assert!(
                s.cpu_seconds <= s.wall_seconds * 1.5 + 0.05,
                "cpu exceeds wall: {s:?}"
            );
        }
    }

    #[test]
    fn sleeping_costs_wall_but_not_cpu() {
        let p = BlockProfiler::start();
        std::thread::sleep(Duration::from_millis(30));
        let s = p.finish();

        assert!(s.wall_seconds >= 0.025, "wall too small: {s:?}");
        if thread_cpu_time().is_some() {
            assert!(s.cpu_seconds < 0.020, "sleep should not burn cpu: {s:?}");
        }
    }

    #[test]
    fn counts_bytes_written() {
        let p = BlockProfiler::start();
        let path = std::env::temp_dir().join(format!(
            "daisy-profile-io-{}-{:?}.bin",
            std::process::id(),
            std::thread::current().id()
        ));
        let payload = vec![7u8; 256 * 1024];
        std::fs::write(&path, &payload).unwrap();
        let read_back = std::fs::read(&path).unwrap();
        let s = p.finish();
        let _ = std::fs::remove_file(&path);

        assert_eq!(read_back.len(), payload.len());
        if read_io_counters().is_some() {
            assert!(
                s.io_write_bytes >= payload.len() as u64,
                "expected >= {} written: {s:?}",
                payload.len()
            );
            assert!(
                s.io_read_bytes >= payload.len() as u64,
                "expected >= {} read: {s:?}",
                payload.len()
            );
        }
    }

    #[test]
    fn peak_rss_is_reported_where_supported() {
        let s = BlockProfiler::start().finish();
        if cfg!(any(target_os = "linux", target_os = "macos")) {
            assert!(s.peak_rss_bytes > 0, "expected a peak rss: {s:?}");
        }
    }

    #[test]
    fn gpu_slots_are_reserved_not_fabricated() {
        let s = BlockProfiler::start().finish();
        assert!(s.gpu_util_pct.is_nan(), "gpu util must read NaN until NVML");
        assert_eq!(s.gpu_mem_bytes, 0);
    }

    #[test]
    fn parses_proc_io_and_ignores_noise() {
        let io = parse_proc_io(
            "rchar: 1234\nwchar: 5678\nsyscr: 9\nnot-a-number: abc\nmalformed line\n",
        );
        assert_eq!(io, IoCounters { read: 1234, write: 5678 });
    }

    #[test]
    fn stats_round_trip_through_bincode() {
        let s = BlockStats {
            wall_seconds: 1.5,
            cpu_seconds: 1.25,
            peak_rss_bytes: 4096,
            io_read_bytes: 10,
            io_write_bytes: 20,
            gpu_util_pct: 50.0,
            gpu_mem_bytes: 64,
        };
        let cfg = bincode::config::standard();
        let bytes = bincode::encode_to_vec(s, cfg).unwrap();
        let (back, _): (BlockStats, usize) = bincode::decode_from_slice(&bytes, cfg).unwrap();
        assert_eq!(back, s);
    }
}
