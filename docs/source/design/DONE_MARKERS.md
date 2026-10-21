# Block tracking (done markers, failures, resource stats)

Persistent on-disk record of what happened to each block. Lets a partial run
resume from where it left off without re-doing finished work, and — when
`resource_tracking=True` — records what each block cost. This is daisy
v2-specific: daisy 1.x has a `check_function` hook for resumption, but no
built-in persistence layer and no per-block accounting.

Configured with `Task(tracking_path=...)` or globally with
`daisy.set_tracking_basedir(...)`, which resolves to `<basedir>/<task_id>`.
(`done_marker_path` / `set_done_marker_basedir` still work as deprecated
aliases — the directory outgrew the name.)

## What's stored

A **Zarr v3 group** whose children are single-chunk arrays, all sharing one
element-per-block grid index, so they line up element-for-element:

| array | dtype | when | meaning |
|---|---|---|---|
| `done` | uint8 | always | `1` = block completed |
| `failures` | uint32 | always | failed attempts for this block |
| `wall_seconds` | float32 | `resource_tracking` | elapsed time in the block |
| `cpu_seconds` | float32 | `resource_tracking` | user+system CPU |
| `peak_rss_bytes` | uint64 | `resource_tracking` | process peak RSS at block end |
| `io_read_bytes` | uint64 | `resource_tracking` | bytes read via syscalls |
| `io_write_bytes` | uint64 | `resource_tracking` | bytes written via syscalls |
| `gpu_util_pct` | float32 | `resource_tracking` | reserved (NaN) |
| `gpu_mem_bytes` | uint64 | `resource_tracking` | reserved (0) |
| `worker_id` | uint32 | `resource_tracking` | worker that completed the block (`2**32-1` = none) |
| `sent_at` | float64 | `resource_tracking` | dispatch time, seconds since the UNIX epoch (scheduler clock) |
| `returned_at` | float64 | `resource_tracking` | return time, seconds since the UNIX epoch (scheduler clock) |

Next to the arrays, `runs.jsonl` holds one JSON line per run: the task's
run report (block aggregates and the per-worker timeline). It is
append-only and ignored by zarr readers.

See `RUN_STATS.md` for what the resource figures mean and their caveats,
and for the worker timeline the last three arrays and `runs.jsonl` feed.

The index is the **C-order grid coordinate**: `(block.write_roi.offset -
total_roi.offset) / write_roi.shape`, flattened row-major. Boundary blocks
that fall outside the grid (possible under `Fit::Overhang` / `Fit::Shrink`)
are simply not addressable and are skipped.

## On-disk layout

```
<tracking_path>/
├── zarr.json              ← Zarr v3 *group* metadata, holds daisy_task_hash
├── done/
│   ├── zarr.json          ← array metadata (uint8)
│   └── c/0/0/.../0        ← single chunk, one element per block
├── failures/
│   ├── zarr.json          ← array metadata (uint32)
│   └── c/0/0/.../0
├── … one directory per stat array, same shape
└── runs.jsonl             ← one line per run (resource_tracking only)
```

Chunk paths follow Zarr v3 conventions: `c/<idx>/<idx>/...`, all-zero
because there is exactly one chunk per array. Everything is written with
only the `bytes` codec (little-endian, no compression) so each chunk file is
a raw element array daisy can mmap — and so any zarr v3 reader can open the
group while a run is in flight:

```python
import zarr

g = zarr.open_group("tracking/segment", mode="r")
g["done"][:]  # uint8 grid of completion flags
g["failures"][1, 2]  # how many attempts the block at grid (1,2) burned
```

**This layout changed.** Earlier daisy wrote a bare Zarr *array* at this
path rather than a group. Such a directory is refused with the usual
actionable `LayoutMismatch` ("Delete it to start fresh: `rm -rf ...`")
rather than a confusing metadata error — both because the hash domain
separator changed and because the node type is checked explicitly.

## Memory mapping

Reads and writes go through `memmap2::MmapMut`, one mmap per array. `is_done(block)` is a single mmap read; recording a block is a handful of element stores. No syscalls per block.

The mmap is allocated upfront for the full block count. For a 1M-block task, that's 1 MiB of address space (and a 1 MiB sparse file on disk that grows as bytes are touched). For 1B blocks: 1 GiB. The OS handles paging.

## Layout-mismatch detection

The hard problem with persistence is: what if the user re-runs with different task parameters? A different `block_size`, `total_roi`, `read_roi`, `write_roi`, or `fit` would produce different blocks at different IDs — reusing the old marker would mark the wrong blocks as done.

We hash the relevant parameters into the array's metadata as a custom Zarr extension field, `daisy_task_hash`. On open:

1. If the array doesn't exist, create it and write the current task hash.
2. If it exists, hash the *current* task params and compare to `daisy_task_hash` in the metadata.
3. If they match, reuse the existing array. The bytes carry over from the previous run.
4. If they don't match, return `LayoutMismatch` error. The caller decides whether to fail (default) or delete and recreate.

The hash uses SHA-256 over `(total_roi, read_roi, write_roi, fit)` serialized canonically. Block-ID-affecting changes flip the hash; cosmetic changes (task_id, num_workers) don't.

### Known limitation / planned enhancement: resuming a grown volume

The whole-layout hash is deliberately conservative, and today that
conservatism has a sharp corner: **extending `total_roi`** — the canonical
"the microscope wrote more data, process the rest" workflow — invalidates
the entire marker even though every previously-done block is still valid.
The run aborts with `LayoutMismatch` and the only offered remedy is deleting
the marker, i.e. discarding all resume state and reprocessing everything.

The fix is scoped and known, just not implemented yet: store the
*structural* parameters (grid base offset, write stride, read/write context,
`fit`) instead of one opaque hash. On reopen, if stride/offset/context match
but the grid merely **grew**, migrate in place — allocate the new
single-chunk array and copy the old bytes across with a C-order index remap
(one pass, one byte per block; milliseconds even for millions of blocks).
Shrinking or stride/context changes would still hard-fail as they do now.

Until then: if your dataset grows over time, prefer one marker per
acquisition round (e.g. suffix the marker path or task_id with the round),
or size `total_roi` to the final expected extent up front — blocks outside
the currently-written region can be skipped by the process function.

## Skip path in the scheduler

`Scheduler::acquire_block` (daisy-core/src/scheduler.rs:140) calls `precheck` after pulling a block from the ready queue:

```rust
fn precheck(&self, task_id: &str, block: &Block) -> Precheck {
    if let Some(tracking) = self.task_tracking.get(task_id) {
        if tracking.is_done(block) {
            return Precheck::SkipDone;
        }
    }
    if task.is_masked(block) {          // Task(skip_mask=...)
        return Precheck::SkipMasked;
    }
    // fall through to user's check_function if any -> SkipDone / Run
    ...
}
```

The done marker is checked before the skip mask and the user's `check_function` because it's the cheapest (single byte read; the mask is one in-memory byte, the check function a Python callback through PyO3 GIL acquisition). On any skip the scheduler synthesizes a `Success` release immediately without ever dispatching to a worker, increments `skipped_count`, and recurses to fetch the next block. `SkipDone` skips are recorded in the tracking store as before; `SkipMasked` skips are not (see below).

## Skip masks

`Task(skip_mask=mask)` takes an array with one cell per block, nonzero
meaning "skip this block". It is the way to tell daisy ahead of a run which
blocks carry no work (far from a predicted surface, outside the acquired
region, ...). It is held in memory on the task (`daisy-core/src/skip_mask.rs`)
and never written to the tracking store, which gives it per-run semantics:

- masked blocks are reported as skipped and release their downstream
  dependencies, exactly like a done block;
- nothing is recorded for them, so a later run with a narrower mask (or no
  mask) runs them, and a block that was done stays done;
- a store written on a masked run is still a daisy-written store with its
  `daisy_task_hash`, so the layout-mismatch guard keeps working.

The mask is indexed by **block position**, in C order: the cell of a block is
`(block.write_roi.offset - total_write_roi.offset) // write_roi.shape`, where
`total_write_roi` is `total_roi` shrunk by the read/write context. Cell
`(0, 0, ...)` is the first block whatever the context, and the required shape
is `Task.block_grid_shape` (blocks per axis; `fit="valid"` drops a partial
trailing block, `overhang`/`shrink` keep it). This deliberately differs from
the tracking arrays' internal grid, which is anchored at `total_roi` and shifts
by `context // block_size` once the context is at least one block wide. A mask
of the wrong shape is rejected when the `Task` is constructed.

A caller-provided hash-less tracking store (a `done` array written by hand,
see "Layout-mismatch detection") still works but is no longer the recommended
way to seed skips: it reuses the internal grid anchor and forgoes the layout
hash.

## When the marker gets written

Only on `Success`. The marker bookmark only exists for blocks that completed cleanly:

- Failed blocks (after max retries): not marked. A future run will re-attempt them.
- Orphaned blocks (upstream failed): not marked. A future run will re-attempt if upstream is fixed.
- Skipped blocks (the marker said "done"): not re-marked (it's already 1).
- Blocks skipped by a `skip_mask`: not marked. They were not done; the mask
  only applies to the run it was passed to.

Failures and orphans staying unmarked is the natural way to make resume-after-failure work: the user fixes the bug, re-runs, and only the previously-failing blocks are retried.

## Setup from Python

**Done-marker tracking is opt-in.** With no explicit path and no basedir
set, tasks are not tracked and nothing is ever skipped — a rerun executes
every block, exactly like daisy 1.x without a `check_function`.

Two ways to opt in:

```python
# Per-task path (explicit)
task = daisy.Task(..., done_marker_path="/scratch/run_2024_03_15/extract")

# Or globally — every task without an explicit path uses <basedir>/<task_id>.
# This is the recommended pattern: one call at pipeline start, pointing at a
# stable ABSOLUTE location that belongs to the run.
daisy.set_done_marker_basedir("/scratch/run_2024_03_15")
task = daisy.Task(...)  # marker at /scratch/run_2024_03_15/extract
```

Pass `done_marker_path=False` to disable the marker for a specific task even
if the basedir is set. Pass `done_marker_path=None` (the default) to defer to
the basedir (no basedir set → no tracking).

> **History**: daisy 2.0 originally enabled markers by default, falling back
> to `./daisy_logs/<task_id>` relative to the current working directory. That
> default was removed: CWD-relative hidden skip-state meant that rerunning a
> script after *changing its code* silently skipped every block a previous
> (buggy) run had marked done. Markers are keyed by task_id + block geometry
> only — **not by the process function** — so resume-correctness across code
> changes is the user's responsibility: pick distinct task_ids per pipeline
> version, or call `Task.reset()` after changing code.

## What the user sees

In the execution summary, a resumed run prints `Skipped: N` for the count of pre-skipped blocks. A fully-resumed run (everything was already done) takes essentially no time — the runner skips everything, transitions every task to `Done`, and exits.

In addition, every task that skipped at least one block emits an INFO record
on the `daisy._progress` logger, e.g.
`task 'extract': resumed — 40/64 blocks skipped via done markers (Task.reset() or done_marker_path=False reprocesses them)`.
This follows standard Python logging conventions: nothing is forced onto the
terminal, and users who want the notice enable it with
`logging.getLogger("daisy").setLevel(logging.INFO)` (plus a handler).

## Tradeoffs vs daisy 1.x's `check_function`

**Pro daisy v2**: zero-config persistence. The user doesn't have to write a function that knows where its outputs live, or handle "is this output complete and consistent?" themselves.

**Pro daisy**: the check function can validate that output is actually complete and well-formed. The done marker only knows "we said it was done". If a successful block was followed by partial corruption (e.g. user interrupted during a write), the marker will lie about it.

**Mitigation**: nothing prevents using both. `Task` accepts `check_function` and `done_marker_path` simultaneously — the marker is checked first (cheap), then the user's function (authoritative). If the user's function says "no, it's not actually done", the block runs and the marker gets re-written.

## Tests

`tests/test_done_marker.py` covers:

- Fresh creation (file doesn't exist, gets created)
- Reuse with matching layout (skips already-done blocks)
- Layout mismatch (different write_roi → error)
- Concurrent marker writes from multiple workers
- Resume after a partial failure

`tests/test_skip_mask.py` covers `Task(skip_mask=...)`: block-grid shape per
fit mode, the cell anchor under a block-wide context, dtype handling, shape
rejection, composition with the tracking store across runs (masked blocks are
not recorded, the store keeps its hash), downstream release, and the
multiprocessing server path.

## Implementation pointer

`daisy-core/src/done_marker.rs` (~526 lines). The bulk is the Zarr v3 metadata serialization (one struct per piece of layout: `ZarrV3GroupMetadata`, `ZarrV3ArrayMetadata`, codecs, chunk grids). The actual mmap usage is a few dozen lines.
