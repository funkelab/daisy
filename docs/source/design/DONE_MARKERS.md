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

See `RUN_STATS.md` for what the resource figures mean and their caveats.

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
└── … one directory per stat array, same shape
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
fn precheck(&self, task_id: &str, block: &Block) -> bool {
    if let Some(marker) = self.done_markers.get(task_id) {
        if marker.is_done(block) {
            return true;
        }
    }
    // fall through to user's check_function if any
    ...
}
```

The done marker is checked before the user's `check_function` because it's much cheaper (single byte read vs Python callback through PyO3 GIL acquisition). When `is_done` returns true, the scheduler synthesizes a `Success` release immediately without ever dispatching to a worker, increments `skipped_count`, and recurses to fetch the next block.

## When the marker gets written

Only on `Success`. The marker bookmark only exists for blocks that completed cleanly:

- Failed blocks (after max retries): not marked. A future run will re-attempt them.
- Orphaned blocks (upstream failed): not marked. A future run will re-attempt if upstream is fixed.
- Skipped blocks (the marker said "done"): not re-marked (it's already 1).

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

## Implementation pointer

`daisy-core/src/done_marker.rs` (~526 lines). The bulk is the Zarr v3 metadata serialization (one struct per piece of layout: `ZarrV3GroupMetadata`, `ZarrV3ArrayMetadata`, codecs, chunk grids). The actual mmap usage is a few dozen lines.
