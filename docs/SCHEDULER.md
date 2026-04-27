# Scheduler, dependency graph, and ready surface

How block-level dependencies become the dispatch order. The pieces in this doc are the algorithmic core of daisy; everything else (TCP, worker threads, retries) is plumbing on top of these data structures.

## Three components

```
DependencyGraph         ReadySurface           Scheduler
─────────────────       ──────────────         ─────────
who depends on          which blocks are       glues them together,
whom (static            currently ready or     hands out blocks,
structure)              boundary                receives results
                        (live state)
```

- **DependencyGraph** is a static structure built once at startup from the task definitions. Pure function of `(read_roi, write_roi, total_roi, fit, read_write_conflict)`.
- **ReadySurface** is the live frontier of work. Blocks transition through it as they complete, fail, or get re-queued for retry.
- **Scheduler** owns both, plus per-task ready queues, retry counters, done markers, and the `TaskState` typestate.

## Dependency graph construction

Per task, `BlockwiseDependencyGraph` (daisy-core/src/dependency_graph.rs:11) computes:

- **level_stride** — per-dimension stride between blocks in the same level. When `read_write_conflict = true`, the stride accounts for the read/write context overlap so blocks in the same level can't conflict.
- **level_offsets** — offsets within `level_stride` for the start of each level.
- **level_conflicts** — for each level > 0, the offsets to upstream blocks in the previous level whose write ROI overlaps the read ROI of a level-N block.

A *level* is a set of mutually-independent blocks: every block in level N can run in parallel without touching any other level-N block's read or write ROI. Block N+1 may depend on multiple N blocks (the conflict offsets enumerate which ones).

The construction is identical to daisy's algorithm. The Rust version is just faster — same level-stride formula, same cantor-pairing for block IDs (we use the funlib pyramid-volume generalization to match daisy's IDs exactly).

### Iteration is lazy

`BlockwiseDependencyGraph::level_blocks(level)` returns a borrowing iterator chain; for the hot per-block dispatch path used by `Scheduler::new`, `level_blocks_owned` returns a `LazyBlockIter` that holds clones of the per-block data and is `Send + 'static` — it goes straight into `ProcessingQueue` as a `Box<dyn Iterator>`.

The inner cartesian product over per-dimension ranges is also lazy (`LazyCartesian`, a multi-radix counter) so a 1M-block 1D task allocates ~zero blocks upfront. Output order matches the eager `cartesian_product` helper exactly (rightmost dimension varies fastest), preserving block IDs for done-marker compatibility.

The eager `cartesian_product` function is still used for the small per-call sites (level offsets, sub-graph blocks); only the per-block hot path went lazy.

### Inter-task dependencies

`DependencyGraph` (line 462, the multi-task wrapper) layers task-to-task edges on top: when task B's `upstream_tasks` includes task A, B's blocks whose read ROI overlaps A's write ROI depend on those A blocks.

`downstream(block)` returns blocks downstream of `block` *across* tasks. `upstream(block)` is the inverse. Both are exposed to `ReadySurface` via closures that capture an `Arc<DependencyGraph>`. The scheduler stores the same `Arc` as its `dependency_graph` field — one allocation, shared between the field and the closures.

## Ready surface

`ReadySurface<F, G>` (daisy-core/src/ready_surface.rs) tracks two sets:

- **surface**: blocks that have completed successfully and have unscheduled downstream blocks. They're "exposed to the air" — anything depending on them is now free to run.
- **boundary**: blocks that have failed permanently and have unscheduled downstream blocks. The boundary defines the orphan frontier.

When a block succeeds (`mark_success`):
1. Add to `surface`.
2. For each downstream block: if all its upstreams are in `surface`, the block is newly ready.
3. For each upstream of the success: if all its downstream are now in `surface` or `boundary`, remove it from `surface` (no longer "exposed").

When a block fails permanently (`mark_failure`):
1. Add to `boundary`.
2. BFS through downstream: any block whose upstream is in the boundary becomes orphaned.
3. Cleanup of fully-boundary upstream chains, mirror of the success cleanup.

The data structures are bounded by the *active frontier*, not the total block count. For a pipeline processing 10M blocks linearly, the surface is at most one block deep at any time.

`ReadySurface` is generic over the closure types. The scheduler instantiates it with closures that call `DependencyGraph::downstream` and `::upstream`. Rust monomorphizes — the closure calls are inlined.

## Scheduler

The orchestrator. Holds:

```rust
pub struct Scheduler {
    dependency_graph: DependencyGraph,
    ready_surface: ReadySurface<...>,
    pub task_map: HashMap<String, Arc<Task>>,
    pub task_states: HashMap<String, TaskState>,    // typestate enum
    task_queues: HashMap<String, ProcessingQueue>,  // per-task ready queue + retries
    count_all_orphans: bool,
    done_markers: HashMap<String, DoneMarker>,
}
```

### `acquire_block(task_id)`

Worker requests a block:

1. If task is not `Running` (Done or Abandoned), return `None`.
2. Pop next block from the task's `ProcessingQueue` (root generator first, then the dynamic ready queue).
3. `note_acquired()` on the running task: `R--`, `P++`, `started = true`.
4. **Pre-check**: ask the done marker (cheap, single byte read) and then the user's `check_function` (potentially expensive). If either says "this block is already done", mark it `Success`, increment `skipped_count`, and recursively release it (which triggers downstream readiness). Loop and try again.
5. Register the block in the task's `processing_blocks` set (so the bookkeeper can detect lost blocks) and return.

### `release_block(block)`

Worker returned a block:

1. Strip from in-flight tracking. (Always — even for terminal tasks, the bookkeeper had this block on the books.)
2. **Typestate gate**: if the task isn't `Running`, log and return. This is what defangs the late-message races described in [ABANDONMENT.md](ABANDONMENT.md).
3. Match `block.status`:
   - **Success**: `mark_success` in ready surface → `note_completed` → mark in done marker → `update_ready_queue` for new downstream blocks → `try_finalize_done`.
   - **Failed, retries < max**: increment retry counter, push to ready queue, `note_failed_for_retry`.
   - **Failed, retries == max**: `mark_failure` BFS → `note_failed_permanently` → `note_orphaned` on each orphan task → `try_finalize_done` on this task and each orphan task.

### Update propagation

`update_ready_queue` calls `queue_ready_block` per new ready block and returns the deduped list of task ids whose ready queue actually grew (a block dropped by the typestate gate doesn't count). `queue_ready_block` is the place Race 2 was hiding before the typestate refactor — it now also gates on `running_mut()`, so a downstream task that's been transitively abandoned silently drops the block. The returned `Vec<String>` flows up through `release_block` and into the run loop's rebalance check.

### What `task_queues` does that `ready_surface` doesn't

The ready surface knows *what's* ready in dependency terms. The `ProcessingQueue` per task is the actual FIFO that workers pull from, plus the per-block retry counter map. Two structures, two purposes:

- ReadySurface answers "which blocks have all their dependencies satisfied?" — graph algebra.
- ProcessingQueue answers "which block does this worker get next?" — operational state.

A block can be in the ready surface (its deps are met) but not yet in the queue (e.g. for downstream tasks: the graph knows the downstream block is unblocked, but it's only enqueued when the upstream block is released, via `update_ready_queue`).

## Why this design instead of a simpler "just queue everything"

You could imagine a simpler scheduler that, on startup, computes the full block dispatch order via a topological sort and just hands them out FIFO. That works for static dependency graphs.

It doesn't work for daisy because of:

- **Streaming dependencies**. As soon as upstream block A finishes, downstream block A' becomes available — workers shouldn't wait for *all* of A to finish before starting *any* of A'. The ready surface gives this for free; a static topological sort doesn't.
- **Retries**. A failed block goes back to the ready queue and may be picked up by any worker, in any order relative to other ready blocks. The ProcessingQueue handles this; a static order doesn't.
- **Orphan propagation**. When block A fails permanently, every block downstream of A becomes orphaned and must be removed from any future scheduling. The boundary set is the data structure that makes this efficient.
