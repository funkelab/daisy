# Worker pool coordination across tasks

How concurrent worker counts compose across tasks competing for shared resources. The mechanism is a **per-task `requires`** declaration plus a **global `resources` budget** consumed by the runner.

This replaces daisy 1.x's per-task `num_workers` (which had no notion of cross-task resource sharing — each task got its own static pool, and 5 tasks each declaring `num_workers=10` would happily spawn 50 workers regardless of the host's actual capacity).

## API

Each task declares what one of *its* workers consumes:

```python
extract  = daisy.Task(..., requires={"cpu": 1}, max_workers=8)
predict  = daisy.Task(..., requires={"gpu": 1, "cpu": 4}, max_workers=4)
relabel  = daisy.Task(..., requires={"cpu": 1}, max_workers=8)
```

The runner is given a global budget:

```python
daisy.run_blockwise(
    extract + predict + relabel,
    resources={"cpu": 32, "gpu": 8},
)
```

At any moment, the number of *concurrent* workers for a task is bounded by the smaller of:

- its own `max_workers` cap,
- the global budget after subtracting other tasks' usage.

## How it composes

The allocator (`daisy-core/src/resource_allocator.rs`) maintains:

```rust
pub struct ResourceAllocator {
    budget: ResourceBudget,                  // global cap
    used:   HashMap<String, i64>,            // currently-consumed totals
    alive:  HashMap<String, usize>,          // worker count per task
}
```

`try_allocate(&task)` is the gate every worker spawn goes through. It checks that for every key in `task.requires`, `used[k] + v ≤ budget[k]`; on success, the per-key usage and the per-task `alive` counter both bump. `release(&task)` is the symmetric drop. Spawn paths in `daisy-core/src/server.rs` call `try_allocate` before launching a thread; the worker's RAII `ExitNotifier` calls `release` on Drop.

A task with empty `requires={}` is treated as costing zero resources — it's bounded only by `max_workers`. Useful for IO-bound tasks where you don't want resource accounting at all.

## Disjoint resources run in parallel

Two tasks competing for the *same* key share that key's pool:

```
budget = {"cpu": 4}
A.requires = {"cpu": 1}, A.max_workers = 8
B.requires = {"cpu": 1}, B.max_workers = 8
```

→ combined alive count for A and B never exceeds 4.

Two tasks declaring *disjoint* keys don't compete:

```
budget = {"cpu": 4, "gpu": 2}
A.requires = {"cpu": 1}, A.max_workers = 4
B.requires = {"gpu": 1}, B.max_workers = 2
```

→ A and B both run at full `max_workers` simultaneously.

This is the test matrix in `tests/test_resources.py`:

- `test_max_workers_caps_concurrency_without_requires` — empty requires, only `max_workers` matters
- `test_resource_budget_caps_one_task_below_max_workers` — budget overrides `max_workers` when smaller
- `test_two_tasks_share_a_resource` — combined peak ≤ shared budget
- `test_disjoint_resources_run_in_parallel` — disjoint budgets don't gate each other
- `test_requires_exceeds_budget_hard_errors` — startup validation
- `test_chained_tasks_reassign_workers_when_upstream_drains` — workers shift between dependent tasks as work moves through the DAG

## Validation at startup

`ResourceAllocator::validate(&[Arc<Task>])` runs once before the run loop starts. For each task, for each `requires` key, it checks that the per-worker requirement fits in the global budget (`v ≤ budget[k]`). If not, it returns `DaisyError::InvalidConfig` with a message naming the offending task and key:

```
task "predict" requires {gpu: 8} but the global budget only has {gpu: 1}
```

This is hard-error rather than warn-and-skip: a task whose `requires` doesn't fit will *never* spawn a worker, and silently never running is the worst possible failure mode.

## Why workers are task-bound

A more sophisticated design would have a global pool of fungible workers that the server dispatches across tasks based on where the work is. We don't do that, on purpose:

- Worker-function (0-arg) workers are **stateful** — they're long-running subprocesses that load a model on startup and process blocks in a loop. Reassigning a "predict" worker to handle a "relabel" block would require it to switch models, which defeats the whole point of the lifecycle.
- Process-function (1-arg) workers don't have that constraint, but the protocol and worker threads are uniform across both shapes — making process-function workers fungible while keeping worker-function workers task-bound would split the implementation in two.

The resource-budget approach gets you most of the benefit (no over-subscription, fair sharing under contention) without the per-task statefulness penalty. The downside is that an idle "predict" worker doesn't get reassigned to a busy "extract" task — it just exits when there's no more "predict" work, freeing its resource share for "extract" to grow into.

## Where to look

- `daisy-core/src/resource_allocator.rs` — `ResourceBudget`, `ResourceAllocator::try_allocate`/`release`/`validate`.
- `daisy-core/src/server.rs::rebalance_workers` — calls `try_allocate` before each spawn; per-task spawn caps come from `min(task.max_workers, allocator headroom)`.
- `tests/test_resources.py` — full behavior matrix.
