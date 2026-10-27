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
//! Every figure is **process-wide**. Each distributed worker is its own
//! process running one block at a time, so the process is the block's
//! natural accounting unit, and only process-wide counters see work the
//! block hands to other threads (zarr's IO and codec threads, a thread
//! pool, a helper thread). Per-thread counters reported zero IO for zarr
//! writes. In serial mode the process is the driver, and its background
//! threads are counted too.
//!
//! - `wall_seconds` — elapsed time around the block function. Always exact.
//! - `cpu_seconds` — user+system CPU consumed by the process
//!   (`getrusage(RUSAGE_SELF)`).
//! - `io_read_bytes` / `io_write_bytes` — bytes through the syscall layer
//!   (`rchar` / `wchar` from `/proc/self/io`). Zero where the platform
//!   doesn't expose it.
//! - `peak_rss_bytes` — the process's peak RSS **while this block ran**.
//!   On Linux the profiler resets the kernel's high-water mark when the
//!   block starts (writing `5` to `/proc/self/clear_refs`) and reads
//!   `VmHWM` when it ends. That is the worker's resident baseline plus
//!   whatever the block allocated on top. The reset lowers the process's
//!   `ru_maxrss` too, so external tools such as `/usr/bin/time` see the
//!   peak of the last block rather than of the whole process. Where the
//!   reset is unavailable (macOS, kernels before 4.0) this falls back to
//!   `ru_maxrss`, the process's peak so far. Never `ru_maxrss` on Linux
//!   otherwise: it survives fork+exec, so a spawned worker would report
//!   the driver's peak as its own.
//! - `gpu_util_pct` / `gpu_mem_bytes` — reserved. Populating them needs
//!   NVML and a sampling loop; the schema slots exist so adding it later
//!   doesn't change the on-disk layout. Written as NaN / 0 for now.

use std::time::{Duration, Instant};

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use crate::run_stats::process_cpu_time;

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

/// Bytes read/written through the syscall layer by this process.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct IoCounters {
    read: u64,
    write: u64,
}

/// An in-flight measurement. `start()` snapshots the counters, `finish()`
/// returns the deltas. Cheap enough to wrap every block: one small write
/// and three small reads of `/proc` plus two `getrusage` calls.
#[derive(Debug)]
pub struct BlockProfiler {
    started: Instant,
    cpu_at_start: Option<Duration>,
    io_at_start: Option<IoCounters>,
}

impl BlockProfiler {
    pub fn start() -> Self {
        reset_peak_rss();
        Self {
            started: Instant::now(),
            cpu_at_start: process_cpu_time(),
            io_at_start: read_io_counters(),
        }
    }

    /// Stop measuring and report the deltas.
    ///
    /// Counters that went backwards (a thread migrating, `/proc` becoming
    /// unreadable mid-block) saturate at zero rather than wrapping.
    pub fn finish(&self) -> BlockStats {
        let wall = self.started.elapsed();
        let cpu = match (process_cpu_time(), self.cpu_at_start) {
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

/// Reset this process's peak-RSS high-water mark to its current RSS, so
/// the next `peak_rss_bytes` reads the peak since now. Linux 4.0+ only; a
/// failure (older kernel, restricted `/proc`) leaves the mark alone and
/// `peak_rss_bytes` reads the process's peak so far.
fn reset_peak_rss() {
    #[cfg(target_os = "linux")]
    {
        let _ = std::fs::write("/proc/self/clear_refs", "5");
    }
}

/// Peak resident set size of this process since the last `reset_peak_rss`,
/// in bytes. See the module docs.
fn peak_rss_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        // VmHWM is the high-water mark of this process image; ru_maxrss
        // would carry over the parent's peak across fork+exec.
        if let Some(hwm) = std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|text| parse_vm_hwm(&text))
        {
            return Some(hwm);
        }
    }
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

/// The process's IO byte counters.
///
/// Linux exposes `rchar`/`wchar` (bytes through read/write syscalls,
/// including page-cache hits) under `/proc/self/io`, summed over all
/// threads. Elsewhere this returns `None` and the stats read zero.
fn read_io_counters() -> Option<IoCounters> {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/self/io")
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

/// Pull `VmHWM` (reported in kB) out of `/proc/self/status`, in bytes.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_vm_hwm(text: &str) -> Option<u64> {
    let line = text.lines().find(|line| line.starts_with("VmHWM:"))?;
    let kb = line["VmHWM:".len()..].trim().strip_suffix("kB")?.trim();
    kb.parse::<u64>().ok().map(|kb| kb.saturating_mul(1024))
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

    // Every figure is process-wide and cargo runs tests as threads of one
    // process, so other tests' work can only add to a measurement: these
    // tests assert lower bounds, never upper ones.

    #[test]
    fn measures_wall_and_cpu_of_busy_work() {
        let p = BlockProfiler::start();
        busy(50);
        let s = p.finish();

        assert!(s.wall_seconds >= 0.045, "wall too small: {s:?}");
        assert!(s.wall_seconds < 5.0, "wall implausible: {s:?}");
        if process_cpu_time().is_some() {
            assert!(s.cpu_seconds >= 0.04, "expected cpu time: {s:?}");
        }
    }

    #[test]
    fn counts_cpu_burned_on_other_threads() {
        // A block that hands its work to a helper thread (as zarr does with
        // its codec and IO threads) still owns that CPU.
        let p = BlockProfiler::start();
        std::thread::spawn(|| busy(50)).join().unwrap();
        let s = p.finish();

        if process_cpu_time().is_some() {
            assert!(s.cpu_seconds >= 0.04, "helper-thread cpu missing: {s:?}");
        }
    }

    #[test]
    fn counts_bytes_written_on_other_threads() {
        let p = BlockProfiler::start();
        let path = std::env::temp_dir().join(format!(
            "daisy-profile-io-{}-{:?}.bin",
            std::process::id(),
            std::thread::current().id()
        ));
        let payload = vec![7u8; 256 * 1024];
        let read_back = std::thread::spawn({
            let (path, payload) = (path.clone(), payload.clone());
            move || {
                std::fs::write(&path, &payload).unwrap();
                std::fs::read(&path).unwrap()
            }
        })
        .join()
        .unwrap();
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

    /// Allocate and touch `bytes`, so they are resident.
    fn resident(bytes: usize) -> Vec<u8> {
        let mut v = vec![0u8; bytes];
        for i in (0..bytes).step_by(4096) {
            v[i] = 1;
        }
        std::hint::black_box(v)
    }

    #[test]
    fn peak_rss_covers_what_the_block_allocated() {
        let p = BlockProfiler::start();
        let held = resident(64 << 20);
        let s = p.finish(); // still holding it, so no concurrent reset can hide it
        drop(held);

        if cfg!(any(target_os = "linux", target_os = "macos")) {
            assert!(s.peak_rss_bytes >= 64 << 20, "expected a peak rss: {s:?}");
        }
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn peak_rss_starts_over_with_each_block() {
        if std::fs::write("/proc/self/clear_refs", "5").is_err() {
            return; // kernel or sandbox without the reset: ru_maxrss fallback
        }
        let first = BlockProfiler::start();
        let held = resident(512 << 20);
        let big = first.finish();
        drop(held);

        let small = BlockProfiler::start().finish();
        assert!(big.peak_rss_bytes >= 512 << 20, "first block: {big:?}");
        assert!(
            small.peak_rss_bytes + (256 << 20) < big.peak_rss_bytes,
            "second block inherited the first one's peak: {small:?} vs {big:?}"
        );
    }

    #[test]
    fn parses_vm_hwm() {
        let status = "Name:\tpython\nVmPeak:\t 999 kB\nVmHWM:\t   2048 kB\nVmRSS:\t 1 kB\n";
        assert_eq!(parse_vm_hwm(status), Some(2048 * 1024));
        assert_eq!(parse_vm_hwm("VmRSS: 1 kB\n"), None);
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
        assert_eq!(
            io,
            IoCounters {
                read: 1234,
                write: 5678
            }
        );
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
