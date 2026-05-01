# Pipelines

The DAG carrier. `Pipeline` is the canonical input to every runner (`Scheduler::new`, `Server::run_blockwise`, `SerialRunner::run`); tasks are pure data with no fields that describe inter-task structure. This is the single biggest API change from daisy 1.x, where each `Task` carried its own `upstream_tasks=[…]` field and the DAG was reconstructed by walking those edges.

## Why move the DAG off the Task

In daisy 1.x:

```python
a = daisy.Task(task_id="extract", ...)
b = daisy.Task(task_id="predict", upstream_tasks=[a], ...)
c = daisy.Task(task_id="relabel", upstream_tasks=[b], ...)
daisy.run_blockwise([a, b, c])
```

This works for simple chains but has structural problems:

- **A task's identity depends on its position in a DAG.** You can't have a `predict` task that runs in two unrelated pipelines without re-instantiating it with different `upstream_tasks=`.
- **Reusing a sub-pipeline is awkward.** If `extract → predict → relabel` is one logical pipeline, you can't refer to "that pipeline" — only to the leaf task and its transitive parents.
- **Pure data is hard to reason about.** A `Task` that holds references to other Tasks isn't a value in the algebraic-data sense; it's a graph node with stateful relationships.

In daisy v2, `Task` is pure data — no upstream references, no DAG knowledge — and a separate `Pipeline` value type owns the edges:

```python
extract = daisy.Task(task_id="extract", ...)
predict = daisy.Task(task_id="predict", ...)
relabel = daisy.Task(task_id="relabel", ...)

pipeline = extract + predict + relabel
daisy.run_blockwise(pipeline)
```

`extract`, `predict`, and `relabel` are reusable across pipelines — you can build `extract + predict` in one place and `extract + something_else` in another, and `extract` itself is unchanged.

## The data structure

In Rust (`daisy-core/src/pipeline.rs`):

```rust
pub struct Pipeline {
    pub tasks: Vec<Arc<Task>>,
    pub edges: Vec<(String, String)>,   // (upstream_task_id, downstream_task_id)
}
```

Two invariants enforced by `Pipeline::new`:

1. Task ids are unique within the pipeline.
2. Every edge endpoint is one of the listed tasks.

Sources (no incoming edges) and outputs (no outgoing edges) are derived from the edge structure on demand — no separate fields.

In Python (`daisy-py/src/py_pipeline.rs`), `Pipeline` is a `#[pyclass]` that holds `Vec<Py<PyAny>>` (the original Python `Task` references) plus an edge list of index pairs:

```rust
pub struct PyPipeline {
    pub tasks: Vec<Py<PyAny>>,
    pub edges: Vec<(usize, usize)>,
    pub sources: Vec<usize>,
    pub outputs: Vec<usize>,
}
```

`tasks` holds the Python objects directly (not the unwrapped Rust `Task`s) so that subclasses of `_rs.Task` — most importantly the v1.x compat subclass `daisy.v1_compat.Task` — survive composition. The conversion to `daisy_core::Pipeline` happens once at run time via `to_core(py)`, which calls `PyTask::convert_task` on each member.

## Composition operators

`+` (sequential) and `|` (parallel) compose pipelines. Both are implemented in `py_pipeline.rs`; the operators on `Task` delegate to `PyPipeline::from_task` then call `__add__` / `__or__`.

### `a + b` — sequential

Every output of `a` becomes a block-level upstream of every source of `b`. Tasks are deduplicated by Python identity:

```
(extract + predict) + relabel
= Pipeline(
    tasks=[extract, predict, relabel],
    edges=[(extract, predict), (predict, relabel)],
    sources=[extract],
    outputs=[relabel],
  )
```

Fan-out and fan-in fall out naturally:

```python
a + (b | c) + d
# edges = [(a,b), (a,c), (b,d), (c,d)]
# Diamond: a fans out to b and c, both fan in to d.
```

### `a | b` — parallel

Union of two DAGs with no new edges. Sources and outputs are unioned:

```python
(extract_left + predict) | (extract_right + predict)
# Two parallel chains. `predict` appears once if it's the same Python object,
# twice if they're separately constructed (Python identity).
```

### Identity-based deduplication

`merge_tasks` in `py_pipeline.rs` deduplicates by `Py<PyAny>::as_ptr()` — the raw Python object pointer. This means:

- The same Python `Task` instance composed multiple times appears once in the merged pipeline.
- Two `Task`s with the same `task_id` but different Python identities are *two distinct nodes* and will collide at `to_core()` time (the underlying Rust `Pipeline::new` rejects duplicate task ids with `"duplicate task_id in pipeline"`).

The second point is intentional: mistakenly building the same task twice is almost always a bug, and we'd rather hard-error at run time than silently merge.

## Runtime entry points

Every runner accepts `Pipeline | Task` (a singleton task is auto-promoted to a one-task pipeline):

```python
daisy.run_blockwise(pipeline_or_task, ...)
daisy.run_blockwise(pipeline_or_task, multiprocessing=False)   # serial
daisy.Server().run_blockwise(pipeline_or_task)
daisy.Scheduler(pipeline_or_task)                              # debug only
```

The coercion is `_to_pipeline(x)` in `daisy/_task.py`:

- `Pipeline` → as-is.
- `Task` → `Pipeline.from_task(task)` (singleton).
- `list | tuple` of tasks → `_build_pipeline_from_tasks(tasks)`. **This is the v1.x bridge** — see below.

At the FFI boundary, `coerce_pipeline_or_task` in `daisy-py/src/py_server.rs` does the same coercion on the Rust side, then `PyPipeline::to_core` produces the `daisy_core::Pipeline` that `Scheduler::new` consumes.

## v1.x bridge: list-of-tasks calling convention

Existing daisy 1.x code uses `Task(upstream_tasks=[…])` and `run_blockwise([a, b, c])`. We bridge this without breaking compatibility:

1. **`daisy.v1_compat.Task`** is a Python subclass of `_rs.Task`. Its `__new__` accepts `upstream_tasks=[…]` and stores the list as a Python attribute (`_v1_upstream_tasks`) on the subclass instance. PyO3's `#[pyclass(subclass)]` ensures Python subclass instances get their own `__dict__`, so the attribute lives on the subclass — the underlying Rust `Task` is untouched.

2. **`run_blockwise([a, b, c])`** routes through `_to_pipeline` → `_build_pipeline_from_tasks`. That function walks each task's `_v1_upstream_tasks` attribute, transitively collects every reachable task, and builds a Pipeline by parallel-unioning each task and then sequential-edging each `(upstream, downstream)` pair:

```python
seen = transitive_closure(tasks, via=_get_task_upstream)
pipe = Pipeline.from_task(seen[0])
for t in seen[1:]: pipe = pipe | Pipeline.from_task(t)
for (up, down) in edges_from_side_table:
    pipe = pipe | (Pipeline.from_task(up) + Pipeline.from_task(down))
```

The `|` (rather than `+`) for the pair-pipeline is critical — it avoids creating spurious edges from `pipe`'s current outputs to `up`. Each edge contributes exactly the (up, down) edge to the merged DAG without re-introducing any others.

3. **Each `Task(upstream_tasks=…)` call emits a `DeprecationWarning`** pointing at v2 Pipeline composition. The compat path remains supported indefinitely.

The result is that `daisy.v1_compat.run_blockwise([a, b, c])` produces exactly the same `Pipeline` you'd get from `a + b + c` in the v2-native API, but without forcing existing 1.x users to rewrite their scripts.

## Reset and re-run

`Pipeline.reset()` calls `reset()` on every member task. There's no cascade or topological order — each task's `reset()` clears that task's done-marker independently. This matches user expectations: "reset the pipeline" means "clear everything"; users who want partial resets call `task.reset()` per task.

## Why composition is in Rust

Earlier drafts had the `+` / `|` operators in pure Python — Python is the natural place for "DSL ergonomics," and the Rust side just consumed a flat task list with explicit edges. We moved composition into Rust because:

- The edge-merging logic for `(a + b) | (c + d)` and friends gets non-trivial. Doing it in Rust keeps every composition O(tasks + edges) with predictable allocation behavior and lets the type system enforce the invariants (`PyPipeline::new` validates the edge list before construction).
- The Python wrapper around a Rust `Task` previously had to mirror state (its own `tasks` list, edge list, source list, output list), and keeping those in sync across `+` / `|` chains was a class of bugs we saw during the v2 refactor.
- Pipeline construction is on the run-blockwise hot path for very-large-DAG users; doing it in Rust shaves overhead.

The Python operators on `Task` (`__add__`, `__or__`) are PyO3 methods that promote to `PyPipeline::from_task` and then call into Rust. The full composition logic is `merge_tasks` + edge remapping in `py_pipeline.rs`.

## Tests

- `tests/test_pipeline.py` — composition operator semantics: sequential, parallel, fan-in, fan-out, diamond, non-mutation of sub-pipelines.
- `tests/test_topo_order.py` — topological order over composed pipelines (alphabetical tiebreaker on the ready set).
- `tests/daisy_compat/test_scheduler.py` and friends — the v1.x list-of-tasks path with `Task(upstream_tasks=[…])`.

## Where to look

- `daisy-core/src/pipeline.rs` — the Rust value type, validation, `from_task`, `sources`, `outputs`.
- `daisy-py/src/py_pipeline.rs` — `PyPipeline`, the `+` / `|` operators, identity-based dedup, `to_core`.
- `daisy-py/python/daisy/_task.py` — `_to_pipeline`, `_build_pipeline_from_tasks`, the v1.x side-table walk.
- `daisy-py/python/daisy/v1_compat.py` — the `Task(upstream_tasks=…)` subclass and the deprecation warnings.
