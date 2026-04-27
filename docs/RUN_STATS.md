# Run statistics

Per-worker, per-task, and process-wide statistics collected during a run. Surfaced after the run as the **Resource Utilization** report. Not in daisy.

## What gets collected

Three nested layers, defined in gerbera-core/src/run_stats.rs:

```rust
pub struct RunStats {
    pub process: ProcessStats,
    pub per_task: HashMap<String, TaskStats>,
    pub per_worker: Vec<WorkerStats>,
}
```

**ProcessStats**: peak RSS, peak virtual memory, total process CPU time, disk read/written bytes, wall time. Sampled every 200ms by a background tokio task using `sysinfo`.

**TaskStats** (per task): total blocks processed, max concurrent workers, total wall time across workers, total block-processing time, total CPU time (where available), mean block duration in ms, **slope** of block duration over time.

**WorkerStats** (per worker thread): task it served, worker id, wall time, CPU time, blocks processed.

## How worker stats arrive

Each worker thread holds a RAII `ExitNotifier` that fires on Drop:

```rust
impl Drop for ExitNotifier {
    fn drop(&mut self) {
        self.stats.wall_time = self.started.elapsed();
        self.stats.cpu_time = match (thread_cpu_time(), self.start_cpu) {
            (Some(now), Some(then)) => Some(now.saturating_sub(then)),
            ...
        };
        let _ = self.tx.send(std::mem::take(&mut self.stats));
    }
}
```

The channel send is best-effort (`let _ = ...`) — if the receiver has already been dropped (run is finalizing) we don't care. The notifier fires on every return path including panics, so even a worker that unwinds with a Rust panic still records its stats.

`thread_cpu_time()` is platform-specific:
- Linux: `getrusage(RUSAGE_THREAD)` syscall.
- macOS: `thread_info(THREAD_BASIC_INFO)` Mach syscall.
- Other: returns `None`, CPU time isn't reported.

## Per-block durations

The bookkeeper records `Instant::now()` when a block is sent to a worker. When the block is returned, `notify_block_returned` returns the elapsed `Duration`. The scheduler pushes this into a per-task `Vec<f64>` of millisecond durations as releases come in.

Failed blocks are deliberately not recorded — they're noisy outliers that would skew the mean and slope.

## The slope

`run_stats::linear_trend(&[f64]) -> (mean, slope)` is a closed-form ordinary least-squares fit of `(release_index, duration_ms)`. `release_index` is 0..N in the order blocks were released.

What it tells you:

- **Slope ≈ 0**: block durations are stable. The workload is steady-state.
- **Slope > 0**: blocks are getting slower as the run progresses. Often a sign of memory pressure, growing data structures, fragmented storage, or thermal throttling.
- **Slope < 0**: blocks are getting faster. Caches warming up, model JIT compilation, dispatch-overhead amortization on small blocks.

The slope is reported as ms-per-block as the run progresses, so a slope of `+0.05` means each successive block takes 0.05ms longer on average than the one before. Over 100k blocks that's a 5-second drift.

## Output format

Printed as part of `gerbera.run_blockwise(...)`:

```
Resource Utilization
--------------------

  Process:
    peak RSS       : 1.2 GB
    total CPU time : 142.3 s   (across all threads)
    wall time      : 38.7 s
    cpu efficiency : 3.7x   (≈ 3.7 cores busy on average)
    disk read      : 2.4 GB
    disk write     : 510.0 MB

  Per-task:
    task          blocks  max conc    mean ms ∠ slope          cpu busy      wall
    ──────────── ─────── ─────────    ──────────────────────  ─────────  ────────
    extract        99997         4    1.42  ∠ +0.0012             89%      35.1s
    predict        99997         2    8.24  ∠ -0.0008             71%      36.4s
    label          99997         8    0.31  ∠ +0.0001             64%      37.2s
```

The `∠` is just decoration to make the trend readable in monospace.

## Why a slope and not a histogram

A histogram tells you the distribution — useful but the user can compute it themselves from the per-block timings if we expose them. The slope is the one summary statistic that surfaces a *trend* from a `Vec<f64>`, and trends are what people actually care about (regressions over time, memory bloat, cache thrashing). Two numbers — mean + slope — fit on a one-line dashboard.

REFACTOR 2.4 has a proposal for emitting per-block timings as a JSON stream, which would make the histogram option easy.

## Where to look

- `gerbera-core/src/run_stats.rs` — types and `linear_trend`.
- `gerbera-core/src/server.rs::run_blockwise` — the `process_stats` sampler and the `task_block_durations` collection.
- `gerbera-py/python/gerbera/_progress.py::_print_resource_utilization` — formatting.
