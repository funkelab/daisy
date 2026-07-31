# Run statistics

Statistics are an **optional layer over normal processing**. Nothing about
scheduling changes when you turn them on: a task with
`resource_tracking=True` simply expects the blocks it hands out to come back
carrying what they cost, and those measurements are written into the same
mmap'd Zarr group that already records which blocks are done.

There is deliberately **one** counter. `blocks` in the summary is the number
of blocks the tracking layer recorded, which is the same event the scheduler
counts as completion — they cannot drift apart, because there is no second
accumulator to drift.

## Why measurement lives in the worker

The measurement is taken by whoever ran the block, not by the server:

- Every **distributed worker** — daisy's own worker processes and any
  hand-written external cluster worker alike — is measured inside
  `Client.acquire_block`, the one seam they all go through.
- **Serial mode** (`multiprocessing=False`) is measured in Rust around the
  call into Python, since there is no client involved. Same measurement
  code, so `resource_tracking` behaves uniformly; the figures are of the
  single process doing the work.

That is the only vantage point that yields comparable numbers. A server-side
timer can measure the round trip (and still does, for the timeout deadline),
but it cannot see CPU time, memory or IO inside another process — let alone
on another node.

The payload rides home on the block itself (`Block.stats`), inside the
existing `ReleaseBlock` / `BlockFailed` messages. **Statistics add no
protocol traffic and no new message types.**

```
worker (any mode)                          server
  profile enter → snapshot counters
  user block fn runs
  profile exit  → block.stats = deltas
  release_block(block) ──ReleaseBlock──▶  TaskTracking::record(&block)
                                            ├─ mmap: done[i]=1, cpu[i], …
                                            └─ running aggregates
                                          end of run: summary = aggregates
```

## What is measured

| field | source | meaning |
|---|---|---|
| `wall_seconds` | `Instant` around the call | elapsed time in the block |
| `cpu_seconds` | `getrusage(RUSAGE_THREAD)` / mach `thread_info` | user+system CPU, **per thread** so concurrent workers don't contaminate each other |
| `io_read_bytes`, `io_write_bytes` | `/proc/self/task/<tid>/io` (`rchar`/`wchar`), falling back to `/proc/self/io` | bytes through the syscall layer, including page-cache hits |
| `peak_rss_bytes` | `ru_maxrss` | see the caveat below |
| `gpu_util_pct`, `gpu_mem_bytes` | — | **reserved**; written NaN / 0 |

Unsupported platforms report zero rather than failing.

### Caveat: peak RSS is process-wide

`ru_maxrss` is a monotonic high-water mark for the whole process, so
`peak_rss_bytes` reads as "how large had this process grown by the time this
block finished" — not "what this block allocated". Every distributed worker
is its own process handling one block at a time, which makes it a useful
per-worker figure; this is one of the things that got simpler when
in-process workers went away, since concurrent blocks sharing a process all
reported the same number. A true per-block figure would still need allocator
interposition.

### GPU is reserved, not faked

The schema slots exist so that adding NVML later cannot change the on-disk
layout, and they are written as NaN / 0 — never as a plausible-looking zero
a reader might mistake for a measurement. Populating them needs a sampling
loop (GPU utilisation is an instantaneous reading, not a counter) plus an
NVML dependency.

## Where it is written

The per-task tracking group, alongside the done array — see
`DONE_MARKERS.md` for the layout. One element per block, indexed by C-order
grid coordinate, so every stat array lines up element-for-element with
`done`:

```python
import zarr

g = zarr.open_group("tracking/segment", mode="r")
g["cpu_seconds"][1, 2]  # what the block at grid (1, 2) cost
g["done"][1, 2]  # …and whether it finished
```

Because the arrays persist across runs, the *summary* is built from this
run's running aggregates (held by `TaskTracking`), not by re-reading whole
arrays — otherwise a resumed run would report the cost of work it skipped.

## Failure counts come for free

`failures` is written whenever tracking is on at all, independent of
`resource_tracking`: every failed attempt increments that block's counter,
whether it will be retried or is permanent, and whether it was reported by a
worker or reclaimed by the timeout. A block with a high failure count and
`done == 1` is one that eventually succeeded after a fight.

## What the user sees

```
Resource Utilization
--------------------

  Totals (summed over measured blocks):
    blocks measured : 12
    CPU time        : 0.09 s
    in-block time   : 0.34 s
    CPU per block-s : 0.27   (≈1.0 CPU-bound, «1.0 IO-bound)
    peak RSS        : 27.0 MB   (largest single worker)
    IO read         : 198.3 KB
    IO write        : 937.5 KB

  Per-task:
    task            blocks  fails    mean ms ∠ slope           cpu s   peak RSS
    ─────────────────────────────    ──────────────────────────────────────────
    segment             12      0     28.27 ∠ -0.1883          0.09s    27.0 MB
```

That run's blocks each slept 20 ms and then burned a little CPU, which is
why `CPU per block-s` reads 0.27 rather than ≈1.0. `blocks measured : 12`
agreeing with the execution summary's `completed 12` is the property this
redesign exists to guarantee — in subprocess mode (the default here) the old
code printed `blocks 0`.

`mean ms ∠ slope` is a least-squares fit over per-block wall times in
completion order (`run_stats::linear_trend`): a positive slope means blocks
got slower as the run progressed. The panel is **omitted entirely** when no
task opted into resource tracking — a table of zeros is worse than no table.

Programmatic access: `daisy.Server().last_tracking_summary`, keyed
`{"per_task": {task_id: {...}}}`.

## Narrowing what gets measured

daisy wraps the whole block body. To exclude your own setup, or to measure a
narrower region, use the context manager directly — the first measurement
attached wins, so an inner scope beats the automatic outer one:

```python
def process(block):
    data = expensive_setup()  # not measured
    with daisy.profile_block(block):  # measured
        compute(data, block)
```

## Enforcement

A task with `resource_tracking=True` and no tracking directory is a
configuration error, raised at run start rather than silently measuring into
the void. And if a block comes back *unmeasured* to a task that expects
measurements, the run fails naming `daisy.profile_block` — reachable only by
a client that bypasses `daisy.Client` entirely (a hand-rolled protocol
implementation), since every daisy-provided path measures automatically.

## What this replaced

Until this redesign, `blocks_processed` was incremented in exactly one
place: the in-process worker thread's loop. Worker processes (the default)
and external cluster workers never touched it, so the per-task panel
reported `blocks 0` for the modes most people run, and the run-stats tests
had to pin in-process workers to see any counts at all. The fix was not
another accumulator but removing the split: measure where the work happens,
persist through the layer that already tracks blocks, and let the summary be
a fold over that. That in-process worker loop has since been deleted
outright, leaving one measurement path.

Deleted along the way: `WorkerStats`, `TaskStats`, `ProcessStats`,
`RunStats`, `build_run_stats`, the 200 ms `sysinfo` process sampler (and the
`sysinfo` dependency), and `Server.last_run_stats`. `thread_cpu_time` and
`linear_trend` survive in `run_stats.rs` because both are still used.

## Implementation pointers

- `daisy-core/src/block_profile.rs` — `BlockStats`, `BlockProfiler`
- `daisy-core/src/block_tracking.rs` — the zarr group, per-block writes, aggregates
- `daisy-py/src/py_profile.rs` — `daisy.profile_block`
- `daisy-py/python/daisy/_task.py` — the `Client.acquire_block` seam
- `daisy-py/python/daisy/_progress.py` — `_print_resource_utilization`
- `tests/test_block_profiling.py` — cross-mode equivalence, persistence, enforcement
