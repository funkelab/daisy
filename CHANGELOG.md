# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

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
