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

  seg   12 blocks, 3 workers, 292.1 ms wall
    blocks   round trip 50.5 ms ± 0.1 ms
             wall 50.2 ms ± 0.0 ms   cpu 60.1 µs ± 13.6 µs   peak RSS 25.5 MB ± 0.0 B
             io read 1.1 KB  write 0.0 B   overhead 372.1 µs ± 43.3 µs   trend ∠ +0.0027 ms/block
    workers  startup 60.8 ms ± 0.1 ms   idle 25.9 µs ± 33.9 µs   teardown 26.4 ms ± 3.7 ms
             exits 3 clean
        id    startup  blocks       idle         idle / block    compute      compute / block   teardown  exit
         0    60.6 ms       4   193.9 µs    48.5 µs ± 54.2 µs   202.2 ms     50.6 ms ± 0.0 ms    23.1 ms  clean
         1    60.8 ms       4    75.3 µs    18.8 µs ± 12.7 µs   202.1 ms     50.5 ms ± 0.0 ms    23.8 ms  clean
         2    60.9 ms       4    41.6 µs    10.4 µs ± 7.1 µs    202.0 ms     50.5 ms ± 0.1 ms    29.0 ms  clean
```

That run's blocks each slept 50 ms, so `wall` is 50 ms with no spread, the
CPU figure is the few microseconds of Python around the sleep, and the three
workers each took four blocks. `12 blocks` agreeing with the execution
summary's `completed 12` is the property this design exists to guarantee —
the old code printed `blocks 0` in subprocess mode.

Every `x ± y` is a mean and sample standard deviation over a `Welford`
accumulator (`run_stats.rs`), so the report costs O(1) memory per metric.
`trend` is the least-squares slope of per-block wall time in completion
order (`run_stats::Trend`, the running form of `linear_trend`): positive
means blocks got slower as the run progressed.

What prints is decided by what it costs. The figures that need **no tracking
store** — the per-block `round trip` the scheduler times itself, and the
per-worker startup / idle / compute / teardown — are printed for every
distributed task, always. The figures measured **inside the worker** and
persisted to the store (`wall`, `cpu`, `peak RSS`, IO, `overhead`, `trend`)
appear only for tasks with `resource_tracking=True`. The panel is omitted
only when there is nothing at all to show (a serial run without resource
tracking) — a table of zeros is worse than no table — and the per-worker
table stops at ten rows, then points at the full table on the report
object.

Programmatic access: `daisy.last_report()` (also `Server.last_report`, and
`daisy_run_report` on the `KeyboardInterrupt` / abandonment error) is a
`RunReport` of `TaskReport`s, and each task that took part carries its own
section as `task.report()`:

```python
task = daisy.Task("seg", ...)
task.run_blockwise()
t = task.report()           # the same object as daisy.last_report()["seg"]
t.blocks.round_trip      # Stat(n, mean, std, min, max, total), seconds; always there
t.blocks.wall            # the worker's own measurement; resource_tracking only
t.blocks.peak_rss        # bytes
t.blocks.overhead        # round trip − wall, per block
t.workers.startup        # Stat over workers
t.workers.idle           # Stat over per-block gaps, pooled across workers
t.worker(1)              # WorkerRecord: startup, idle, compute, teardown, exit
t.block((1, 2))          # BlockRecord from the store: stats, worker_id, sent_at, returned_at
```

## The worker timeline

Per-block figures say what the blocks cost; they say nothing about the time
a run spends *around* them — bringing workers up, waiting for the scheduler,
tearing down. The server stamps those from events it already handles, with
the server-assigned `worker_id` that every message carries:

| phase | from | to | where stamped |
|---|---|---|---|
| **startup** | spawn call | first `AcquireBlock` | `rebalance_workers`, `handle_acquire` |
| **idle** | previous return (or first acquire) | `SendBlock` | `handle_acquire` |
| **compute** | `SendBlock` | `ReleaseBlock` / `BlockFailed` | `handle_acquire`, `handle_message` |
| **final wait** | last return | `RequestShutdown` | `handle_acquire` |
| **teardown** | `RequestShutdown` (or run shutdown) | spawn call returned | spawn thread's exit notifier, `check_thread_health` |

`worker_timeline.rs` holds one `WorkerTimeline` per worker (a few
`Instant`s and two `Welford`s) and folds them per task into the report's
`workers` section; collection is always on and needs no wire change. The
server is also the only vantage point that *can* see startup and teardown:
a worker cannot report the time before its interpreter started or after its
process exited, and for a cluster spawn function (`sbatch --wait`, `srun`)
startup honestly includes the queue wait.

What this vantage point costs, stated plainly:

- **compute is the server round trip**, including transport and the
  worker's per-block wrapper, while the block's `wall_seconds` is the
  worker's narrower measurement of the user function. The difference is
  reported as `overhead` per block — daisy's own cost — and is usually a
  few hundred microseconds.
- **idle includes one network hop**; on a LAN that is noise, on a slow
  interconnect it is real idle time.
- a worker whose last acquire reaches the server after the run has ended
  never gets a reply (it sees the socket close); its teardown is counted
  from the moment the server began shutting down.
- the final wait before shutdown is not an "idle before a block": it is
  kept per worker (`final_wait`) and excluded from the idle aggregate.
- a failed block's round trip counts in its worker's `compute` and
  `blocks_failed`; it stays out of the block-level trend.
- a worker whose block times out is closed as `exit="timeout"` when its
  slot is retired; a crash is `"error"`; a worker alive at an abort is
  `"interrupted"`. Only clean exits have a teardown.
- clients that connect without a server-assigned id (hand-rolled workers
  outside daisy's management) are counted as `unmanaged_clients`, not timed.
- serial mode has no workers: `workers.count == 0`.

### What is persisted

With `resource_tracking=True` the tracking group gains three more arrays,
indexed like every other (see `DONE_MARKERS.md`): `worker_id` (uint32,
`2**32-1` = no managed worker), `sent_at` and `returned_at` (float64
seconds since the UNIX epoch on the scheduler's clock). From those a full
Gantt-style timeline — who ran what, when, with what gaps — can be rebuilt
from the store alone. Worker ids restart at 0 every run, so the value is
"the worker, in the run that completed this block".

The run report itself is appended as one JSON line per run to `runs.jsonl`
in the same directory: append-only, human-readable, `jq`-able, keeps history
across resumes, and ignored by zarr readers. It is the same JSON the Python
`RunReport` is built from, so the object a run returns and the record left
on disk cannot drift apart. Tasks without a store persist nothing; the
report is still built in memory.

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
- `daisy-core/src/run_stats.rs` — `Welford`, `Trend`
- `daisy-core/src/worker_timeline.rs` — per-worker lifecycle stamps and the workers section
- `daisy-core/src/run_report.rs` — `TaskReport`, the JSON handed to Python and appended to `runs.jsonl`
- `daisy-py/python/daisy/_report.py` — `RunReport`, `TaskReport`, `Stat`, the panel renderer
- `daisy-py/python/daisy/_progress.py` — `_print_resource_utilization`
- `tests/test_block_profiling.py` — cross-mode equivalence, persistence, enforcement
- `tests/test_worker_timeline.py` — lifecycle phases, `worker_id` on disk, `runs.jsonl`, block lookup
