# Migration guide: daisy 1.x → daisy v2

daisy v2 is a Rust rewrite. The Python surface is mostly compatible with daisy 1.x; this document describes the deltas that matter to existing users.

## tl;dr

- **Existing 1.x code should keep working.** `pip install -U daisy` upgrades you to v2 and the top-level `import daisy` namespace is the **v1.x backwards-compatibility surface** (`daisy.v1_compat`). Code that uses `Task(num_workers=…)`, `daisy.SerialServer`, etc. continues to run; you'll just see `DeprecationWarning`s pointing at the v2 names.
- **For new code, prefer the v2 interface.** Use `import daisy.v2 as daisy` to get the cleaner v2 API with no compat aliases (and no deprecation warnings).
- **Some 1.x APIs are gone.** Array I/O (`daisy.persistence`, `daisy.Array`, `daisy.open_ds`, `daisy.prepare_ds`) and the `funlib.geometry` Roi/Coordinate types are no longer re-exported. See [Removed APIs](#removed-apis) below.

## Importing the v2 interface

Three import options:

| Import                            | Surface                                                       |
| --------------------------------- | ------------------------------------------------------------- |
| `import daisy`                    | v1.x compat (default — same as `daisy.v1_compat`)             |
| `import daisy.v1_compat as daisy` | v1.x compat (explicit)                                        |
| `import daisy.v2 as daisy`        | v2 native — Rust types exposed directly, no compat aliases    |

The v2 surface drops `Task(num_workers=…)`, `SerialServer`, and a handful of other 1.x-only names. In return you get cleaner kwargs (`max_workers`), no deprecation warnings, and `Roi`/`Coordinate`/`Block`/etc. resolved to the underlying Rust classes (`daisy._daisy.*`) without an extra Python wrapper layer.

If you're starting a fresh project, **use `import daisy.v2 as daisy`**. If you're maintaining a daisy 1.x codebase and want gradual migration, leave bare `import daisy` and address the deprecation warnings file by file.

## API mapping (1.x → v2)

| 1.x                                      | v2                                                            |
| ---------------------------------------- | ------------------------------------------------------------- |
| `Task(num_workers=N)`                    | `Task(max_workers=N)`                                         |
| `daisy.SerialServer().run_blockwise(t)`  | `daisy.run_blockwise(t, multiprocessing=False)`               |
| `daisy.Server().run_blockwise(t)`        | unchanged (or `daisy.run_blockwise(t)`)                       |
| `daisy.run_blockwise(t)` returning bool  | unchanged signature; bool semantics tightened (see below)     |
| `from funlib.geometry import Roi, Coord` | `daisy.Roi`, `daisy.Coordinate` (now native, not funlib)      |
| `daisy.persistence`, `Array`, etc.       | gone — use `funlib.persistence` directly                      |

### `run_blockwise` return value

In daisy 1.x, `run_blockwise` returned `True` whenever the scheduler finished accounting for every block — including blocks that permanently failed. The 1.x docstring claimed "all blocks successfully run" but the implementation was actually "all blocks reached a terminal verdict." daisy v2 tightens this: **`run_blockwise` returns `True` only if every block of every task completed successfully (or was skipped because a previous run already marked it done)**. Permanently failed or orphaned blocks now make the bool return `False`.

If you have 1.x scripts that branched on `if run_blockwise(...):` to detect "did the run finish accounting" rather than "did everything succeed," you'll want to switch to inspecting the `TaskState` counters returned from `Server.run_blockwise(...)` directly.

## New in v2

A few things v2 added that have no 1.x equivalent:

- **Done markers as a built-in** (`Task(done_marker_path=…)` or via `set_done_marker_basedir(…)`). Per-task on-disk record of which blocks completed, used to skip already-done work on resume. `Task.reset()` clears a task's marker for an explicit re-run.
- **`block_tracking` flag on `run_blockwise`** to disable done-marker tracking entirely for a run.
- **`JsonProgressObserver`** — line-delimited JSON progress events, useful for piping into log aggregators or external dashboards.
- **Per-task resource budgets** (`requires` per task + global `resources` dict) — coordinate worker concurrency across tasks competing for shared resources (CPU, GPU, etc.).
- **Worker restart cap** with proper abandonment + transitive downstream orphan propagation.
- **Run statistics** (per-worker, per-task, process-wide) surfaced after each run.

## Removed APIs

These 1.x names are not exposed by daisy v2 (neither in `daisy` nor `daisy.v2`):

- **`daisy.persistence`** — already deprecated in 1.x in favour of [`funlib.persistence`](https://github.com/funkelab/funlib.persistence). v2 drops the re-export. Import directly from `funlib.persistence`.
- **`daisy.Array`, `daisy.open_ds`, `daisy.prepare_ds`** — same story. Use `funlib.persistence`.
- **`daisy.messages`, `daisy.tcp`** — internal protocol modules. v2's wire protocol is bincode over tokio TCP, implemented in Rust; the message types are not exposed at the Python level. If you were writing a worker in another language, see `docs/source/design/PROTOCOL.md`.
- **`funlib.geometry` Roi/Coordinate types** — v2 uses native Rust-backed types. The shape of the API (offset, shape, contains, intersect, grow, translate) is preserved but the types are not interchangeable with `funlib.geometry`'s. Most code that duck-types on these will work; explicit `isinstance(x, funlib.geometry.Roi)` checks won't.

## Wire / on-disk format breaks

v2 introduces format identifiers under the v2 name; existing daisy 1.x stores were never compatible with these because the formats are v2 inventions. Worth knowing:

- **Done markers** are written under attribute `daisy_task_hash` in a Zarr v3 array, with a hash prefix `daisy-done-marker:v1`. These are v2-only.
- **Worker context** is passed via the `DAISY_CONTEXT` environment variable (1.x had no equivalent — workers in 1.x were spawned via `multiprocessing` + `dill`).

## Python version

- daisy 1.x supported Python ≥ 3.10.
- daisy v2 requires Python ≥ 3.11.

## Dependencies

- daisy 1.x: hard runtime deps on `numpy`, `tornado`, `dill`, `funlib.math`, `funlib.geometry`, `tqdm`.
- daisy v2: hard runtime dep on `tqdm` only. The Rust core uses no Python deps; everything else is optional.

## Suppressing deprecation warnings

If you want to silence the v1_compat warnings while you migrate (e.g. in CI), add this to your `pyproject.toml`:

```toml
[tool.pytest.ini_options]
filterwarnings = [
    "ignore::DeprecationWarning:daisy.v1_compat",
]
```

Or in code:

```python
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning, module="daisy.v1_compat")
```

The cleanest long-term fix is to switch the import to `import daisy.v2 as daisy` and address the renamed kwargs in your tasks.
