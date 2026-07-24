# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- `daisy.Block` (compat surface) now accepts `funlib.geometry` ROIs in its
  constructor and presents its ROIs as `funlib.geometry` types — the same
  contract as blocks handed to process functions. v1 code that constructs
  blocks directly (e.g. process-one-ROI helpers) works unchanged; the
  strict native constructor remains at `daisy.v2.Block`.
- Blocks received in process functions ARE that same compat `Block` class
  now (the `_BlockProxy` wrapper is gone): `type()` and
  `isinstance(x, daisy.Block)` agree for every block a user touches,
  whether daisy handed it out or they constructed it. Status mutations
  still propagate to daisy's bookkeeping on the client paths (subprocess
  and custom workers); thread mode continues to determine success by
  exception only, as it always has in v2.

### Documentation

- Documented the blocking-spawn contract: spawn functions must run for the
  worker's lifetime (`sbatch --wait` / `bsub -K` / `subprocess.run`);
  submit-and-return spawns over-submit by up to `max_worker_restarts` jobs
  and can abandon a task whose jobs are still queued. Connection-aware
  worker accounting is planned.

### Changed

- **Blocks always have a timeout.** `Task(timeout=...)` defaults to 600
  seconds and can no longer be disabled (`None` = the default; values
  <= 0 raise ValueError). A hung block can therefore wedge a run — or
  its shutdown — for at most the block deadline: the subprocess worker
  self-kills at the deadline, the block is reclaimed and retried, and
  shutdown joins complete. Failure surfaces attribute timeout reclaims
  (`TaskState.timeout_reclaim_count` / `timeout_secs`, plus run-summary
  and abandonment-error hints pointing at `Task(timeout=...)`).

### Fixed

- Worker-log stream proxies are now uninstalled at the end of each run;
  previously the first run's `sys.stdout`/`sys.stderr` were captured
  forever and every later run's execution summary was written to the
  (possibly closed or replaced) original streams.

### Removed

- The `funlib.persistence.Array` monkey-patching in the v1-compat layer.
  daisy and funlib.persistence are unrelated packages that merely share
  `funlib.geometry`; daisy no longer imports or modifies persistence.
  Compat-surface blocks already carry `funlib.geometry` ROIs (the
  `_BlockProxy` boundary), so v1-style code can index persistence Arrays
  with them directly — no patching required. Native `daisy.v2` ROIs are
  not accepted by persistence; convert explicitly if you mix surfaces.

### Added

- Wheel-building CI (`publish.yaml`): manylinux/musllinux x86_64 + aarch64,
  macOS x86_64 + arm64, Windows x64, plus sdist; a built wheel is
  smoke-tested (install without a Rust toolchain, run a tiny blockwise
  task) before any publish; publishes to PyPI on version tags.

- Spawn functions may declare a keyword-only `context` parameter
  (`def start_worker(*, context):`) to receive their worker's
  `daisy.Context` as an argument — a race-free alternative to reading the
  process-global `DAISY_CONTEXT` environment variable, which concurrent
  slow spawn functions can observe with a later worker's value. The env
  var keeps being set for 0-arg spawn functions and worker children.
  `Context.from_env_string(...)` parses an encoded context without
  touching the environment. The built-in subprocess workers now set the
  child's `DAISY_CONTEXT` deterministically from the argument.

### Changed

- **Done-marker tracking is now opt-in.** `Task(done_marker_path=None)` only
  resolves to a marker when `set_done_marker_basedir(...)` has been called;
  the fallback to the logging basedir (`./daisy_logs/<task_id>`, relative to
  the current working directory) is removed. Previously a rerun of a script
  whose code had changed could silently skip every block a prior run had
  marked done. Explicit `done_marker_path="..."` and `done_marker_path=False`
  behave as before. To restore the old behavior, call
  `daisy.set_done_marker_basedir(...)` once at pipeline start.

### Added

- Resumed runs now emit an INFO record per task on the `daisy._progress`
  logger stating how many blocks were skipped via done markers and how to
  reprocess them.

### Documentation

- Done markers: documented the layout-hash limitation for grown volumes
  (extending `total_roi` currently invalidates the whole marker) and the
  planned in-place migration enhancement, with interim workarounds.
  (adversarial suite case f05)

- Worker starts are now bounded by a hard per-task budget of `max_workers + max_worker_restarts`, regardless of how or why previous workers exited. Previously only dirty exits counted toward the restart cap, so a worker that exited cleanly without processing blocks (e.g. `subprocess.run(..., check=False)` around a command that fails to start) respawned forever and the run never terminated. Workers are expected to be long-running; the recycle-after-N-blocks pattern is not supported — size `max_worker_restarts` for expected worker deaths (preemption, walltime), or resume via done markers. See `docs/source/design/ABANDONMENT.md`.

- **Block functions now run in worker subprocesses by default.** A 1-arg
  `process_function` on the distributed run paths is serialized (with `dill`
  when installed; stdlib `pickle` otherwise) and executed by real OS worker
  processes launched as `python -m daisy._subprocess_worker`, instead of
  GIL-sharing threads inside the server process. Opt back into thread
  workers with `Task(..., worker_processes=False)`; serial execution
  (`run_blockwise(..., multiprocessing=False)`) is unchanged.

  Why: across workload mixes (16 workers, 96 × ~100 ms blocks), thread mode
  wins only when the block function releases the GIL for essentially its
  entire runtime — pure I/O waits or single-threaded C-library calls — and
  then only by 13–17%. With just 10% pure-python glue, threads are 1.7×
  slower; at 30% python, 8× slower; at 100% python, 28× slower. Subprocess
  workers are flat across every mix. Thread mode remains the right choice
  for fully-GIL-releasing block functions and for workers that must share
  large read-only in-process memory.

  **Resource implication**: `max_workers=N` now means N python interpreter
  processes (each importing your function's modules) rather than N threads
  in one process. Budget memory accordingly, or pin `worker_processes=False`
  where the old footprint matters.

  With subprocess workers, `Task(timeout=...)` gains true preemption: a
  block exceeding the deadline kills its worker process (visible as a dirty
  exit, bounded by `max_worker_restarts`) instead of leaving a runaway
  thread behind.

### Added

- `Task(worker_processes=...)` tri-state kwarg (`None` = subprocess default,
  `False` = thread workers, `True` = subprocess, validated eagerly).
- Optional dependency extra `daisy[worker-processes]` installing `dill` for
  lambda/closure support in subprocess workers.

## [2.0.0] — 2026-04-27

### Overview

Complete rewrite of daisy with a Rust scheduling core (`daisy-core`) and PyO3-based Python bindings. The Python API is leaner and more focused; several daisy 1.x conveniences that are now better served by sibling packages have been removed. Performance and correctness are substantially improved on the scheduling and protocol layers.

If you depend on the daisy 1.x API surface, pin `daisy<2`.

### Added

- Rust scheduling core (`daisy-core`) covering dependency graph, ready surface, scheduler, server, worker pool, and resource allocator.
- On-disk done markers (Zarr v3 single-chunk arrays with hash-based layout-change detection) for resumable runs across process restarts. See `docs/DONE_MARKERS.md`.
- Per-task and per-worker run statistics (block durations, CPU time, memory, linear-trend slope) surfaced via the new `RunStats` reporting layer. See `docs/RUN_STATS.md`.
- `JsonProgressObserver` — line-delimited JSON progress events for piping into `jq`, log aggregators, or external dashboards.
- Per-task `requires` resource budget that composes against a global `resources` budget (workers across tasks competing for the same named resource are coordinated). See `docs/WORKER_POOL_COORDINATION.md`.
- Worker restart cap with proper abandonment and transitive downstream orphan propagation.
- 64 MiB frame size cap on the wire protocol; oversized payloads are rejected before allocation.
- `daisy.__version__` available via `importlib.metadata`.
- `py.typed` marker for type-checker discovery.

### Changed

- **Wire protocol**: bincode over TCP (was tornado IOStreams + pickle in 1.x). Workers in other languages need a bincode codec for `Message` and `Block` — see `docs/PROTOCOL.md`. The 1.x `NotifyClientDisconnect` / `AckClientDisconnect` handshake is replaced by a single `Disconnect` message.
- **Roi / Coordinate**: native Rust-backed types instead of `funlib.geometry`. The shape of the API (offset, shape, contains, intersect, grow, translate) is preserved but the types are not interchangeable with `funlib.geometry`'s.
- **Minimum Python version**: 3.11 (was 3.10 in 1.x).

### Removed

- `daisy.persistence` — moved to `funlib.persistence` years ago; not re-exported.
- `daisy.Array`, `daisy.open_ds`, `daisy.prepare_ds` — array I/O now belongs in `funlib.persistence`.
- Hard dependencies on `numpy`, `tornado`, `dill`, `funlib.math`, `funlib.geometry`. The only runtime dependency is `tqdm`.

### Fixed

- Race windows around worker death mid-block are closed by an explicit typestate model on task counters. See `docs/ABANDONMENT.md`.

[Unreleased]: https://github.com/funkelab/daisy/compare/v2.0.0...HEAD
[2.0.0]: https://github.com/funkelab/daisy/releases/tag/v2.0.0
