# Done markers

Persistent on-disk record of which blocks have completed. Lets a partial run resume from where it left off without re-doing finished work. This is daisy-specific — daisy has a `check_function` hook for the same purpose, but no built-in persistence layer.

## What's stored

For each task with `done_marker_path` set, daisy writes a single-chunk Zarr v3 array. One byte per block:

- `0`: not yet done
- `1`: marked successful

The array is laid out flat in block-id order. Block IDs are the cantor-pyramid numbers the dependency graph computes (matching daisy's funlib block ordering exactly), so block N's status sits at byte offset N.

## On-disk layout

```
<done_marker_path>/
├── zarr.json                      ← Zarr v3 group/array metadata
├── c/0/0/.../0                    ← single chunk, one byte per block
└── (daisy-only) zarr.json
    contains a custom field daisy_task_hash
```

The chunk path follows Zarr v3 conventions: `c/<idx>/<idx>/...` where idxes are 0 because there's one chunk total. The whole thing is a single-chunk Zarr — chosen so users can `zarr.open()` the path in NumPy / Python and read the byte array directly with no daisy dependency.

## Memory mapping

Reads and writes go through `memmap2::MmapMut`. `is_done(block)` is a single mmap read; `mark_success(block)` is a single byte store. No syscalls per block.

The mmap is allocated upfront for the full block count. For a 1M-block task, that's 1 MiB of address space (and a 1 MiB sparse file on disk that grows as bytes are touched). For 1B blocks: 1 GiB. The OS handles paging.

## Layout-mismatch detection

The hard problem with persistence is: what if the user re-runs with different task parameters? A different `block_size`, `total_roi`, `read_roi`, `write_roi`, or `fit` would produce different blocks at different IDs — reusing the old marker would mark the wrong blocks as done.

We hash the relevant parameters into the array's metadata as a custom Zarr extension field, `daisy_task_hash`. On open:

1. If the array doesn't exist, create it and write the current task hash.
2. If it exists, hash the *current* task params and compare to `daisy_task_hash` in the metadata.
3. If they match, reuse the existing array. The bytes carry over from the previous run.
4. If they don't match, return `LayoutMismatch` error. The caller decides whether to fail (default) or delete and recreate.

The hash uses SHA-256 over `(total_roi, read_roi, write_roi, fit)` serialized canonically. Block-ID-affecting changes flip the hash; cosmetic changes (task_id, num_workers) don't.

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

Two ways to enable:

```python
# Per-task path (explicit)
task = daisy.Task(..., done_marker_path="/scratch/run_2024_03_15/extract")

# Or globally — every task without an explicit path uses <basedir>/<task_id>
daisy.set_done_marker_basedir("/scratch/run_2024_03_15")
task = daisy.Task(...)  # marker at /scratch/run_2024_03_15/extract
```

Pass `done_marker_path=False` to disable the marker for a specific task even if the basedir is set. Pass `done_marker_path=None` (the default) to defer to the basedir.

## What the user sees

In the execution summary, a resumed run prints `Skipped: N` for the count of pre-skipped blocks. A fully-resumed run (everything was already done) takes essentially no time — the runner skips everything, transitions every task to `Done`, and exits.

## Tradeoffs vs daisy's `check_function`

**Pro daisy**: zero-config persistence. The user doesn't have to write a function that knows where its outputs live, or handle "is this output complete and consistent?" themselves.

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
