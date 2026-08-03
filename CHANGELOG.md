# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Removed

- **`Task(worker_processes=...)` is gone, and with it in-process (thread)
  workers.** Every distributed worker now runs in its own OS process, as in
  daisy 1.x. There is nothing to configure: whatever a task's
  `process_function` is, the runner wraps it into a spawn function that
  launches `python -m daisy._subprocess_worker`, which either drives the
  `Client.acquire_block()` loop (1-arg block function) or calls the function
  once and lets it drive its own loop (0-arg worker function, which may in
  turn `srun`/`sbatch` a further process).

  Why: 0-arg worker functions had no choice — they always ran on a server
  thread — so a worker that did its work inline instead of shelling out got
  no parallelism at all. Measured on 8 CPU-bound blocks, 1 worker vs 4:
  **0.59×**, i.e. slower than a single worker. The same workload in worker
  processes measures **3.00×**. volara's default worker has exactly this
  shape. Keeping thread execution to serve the one case where it won (block
  functions that release the GIL for essentially their whole runtime, by
  ~15%) meant two execution models, two measurement paths, and a `timeout=`
  that could not preempt.

  **Breaking changes for callers:**

  - `Task(worker_processes=...)` raises `TypeError`. Delete the argument.
    Code that passed `False` to observe in-process state should use
    `run_blockwise(..., multiprocessing=False)`, which runs the original
    function in-process, single-threaded.
  - **0-arg worker functions must now be picklable**, because they are
    shipped to a subprocess rather than called in place. v2 cannot fork —
    the server runs a tokio runtime per worker thread, and forking a
    multithreaded process is unsafe — so it spawns and serializes. An
    unpicklable function fails before the run starts with guidance.
  - **No cheap shared read-only memory.** Workers can no longer close over a
    large array. `numpy.memmap` a file instead.
  - **`Task(timeout=...)` now always preempts.** There is no longer a mode in
    which a timed-out block keeps running and can double-apply its effects
    concurrently with the retry.

  Also: worker identity is race-free for both spawn signatures. Each child
  gets its own environment, so the `DAISY_CONTEXT` race that the keyword-only
  `context` parameter was introduced to dodge cannot happen; the warning
  about it is gone. And `daisy.logging` settings are carried in the worker
  payload, since a spawned child inherits no process globals.

### Changed

- Per-block measurement is now strictly opt-in end to end. The worker
  context carries the task's `resource_tracking` flag, so a worker skips
  the profiler entirely when stats were not requested — previously every
  worker measured cpu/rss/io per block and the server discarded the
  payload. Thread-mode workers were already gated; this closes the
  `Client.acquire_block` path used by subprocess-shim and external
  cluster workers. An absent key (hand-built `Context`, older server)
  means off.

### Changed

- **Run statistics are now an optional per-block layer.** Set
  `Task(resource_tracking=True)` and every block comes back carrying what it
  cost — wall time, CPU time, peak RSS, IO bytes — measured inside whoever
  ran it, so thread, subprocess and external cluster workers all report the
  same thing. Measurements are persisted into the task's tracking group
  beside the done array, one element per block, and the end-of-run summary
  is a fold over exactly what was written.

  This replaces a parallel accounting layer that disagreed with the
  scheduler: `blocks_processed` was only incremented in the in-process
  worker loop, so the default subprocess mode and external workers reported
  `blocks 0`. There is now one counter, not two.

  New: `daisy.profile_block(block)` context manager (applied automatically;
  use it directly only to narrow what gets measured), `Block.stats`,
  `daisy.BlockStats`.

  **Removed**: `Server.last_run_stats` (replaced by
  `Server.last_tracking_summary`), and internally `WorkerStats`,
  `TaskStats`, `ProcessStats`, `RunStats`, `build_run_stats`, the 200 ms
  `sysinfo` process sampler, and the `sysinfo` dependency. The old
  process-wide panel (peak RSS / cpu efficiency / disk IO) is gone; the same
  ground is covered by agglomerating per-block measurements, and the panel
  is omitted entirely when no task opted in rather than printing zeros.

- **BREAKING (on-disk): the per-task tracking directory is now a Zarr v3
  group**, holding `done` and `failures` — plus the resource arrays when
  enabled — as sibling arrays, instead of a single bare `done` array.
  Directories written by an earlier daisy are refused with the existing
  actionable error ("Delete it to start fresh: `rm -rf ...`"). Delete stale
  tracking directories; completed work will be re-done once.

- **BREAKING (wire): the frame format gained a protocol version byte**, and
  `Block` gained a field, so a driver and its workers must run the same
  daisy build. Previously a mismatch failed deep in the decoder
  (`UnexpectedEnd`, or an `Option` tag parsed out of an unrelated string
  length) — or, in one direction, silently dropped the trailing bytes. A
  mismatch now reports both versions and says to rebuild the workers. The
  realistic exposure is external cluster workers loading daisy from a
  different environment than the driver.

- `Task(tracking_path=...)` and `daisy.set_tracking_basedir(...)` /
  `get_tracking_basedir()` are the canonical names now that the directory
  holds more than done state. `done_marker_path` /
  `set_done_marker_basedir` / `get_done_marker_basedir` keep working and
  emit a `DeprecationWarning`.

### Added

- `failures`: per-block count of failed attempts, written whenever tracking
  is on at all (independent of `resource_tracking`), covering both
  worker-reported failures and timeout reclaims.


### Changed

- Subprocess-worker payloads are serialized with **cloudpickle** instead of
  dill (optional dependency `daisy[worker-processes]` now installs
  cloudpickle). Both ship functions by value; they differ on *modules* a
  block function references as globals. dill pickles any module outside
  site-packages by value — the whole `__dict__` — so one unpicklable member
  anywhere in your project package (a `struct.Struct`, a `threading.local`,
  a live connection) failed the payload even when the block function never
  touched it. cloudpickle pickles importable modules by reference and
  reserves by-value for `__main__`, which is the split daisy wants: workers
  replicate the parent's `sys.path`, and re-importing is what genuinely
  remote cluster workers do — so local subprocess runs behave like the real
  deployment.

  Two consequences worth knowing. Objects that cannot cross a process
  boundary (threading locks/conditions/semaphores, write-mode file handles —
  directly, or via an object the function is bound to) are now **rejected at
  submit time** with guidance, where dill accepted them and shipped a lock
  that synchronized nothing. And module-level state mutated at runtime in
  the driver is no longer visible to workers, because they re-import; pass
  such state into the block function instead. Both changes make local
  subprocess runs behave like cluster runs.

### Added

- Lint/type CI (`lint.yaml`): `ruff check` + `ruff format --check` with a
  pinned rule set (pyflakes, pycodestyle errors, import sorting) and
  `ty check` over the python package, both via `uv run --only-group lint`
  (no Rust build needed in CI). The repo is fully clean under all three;
  the self-comparing upstream `benchmarks/` are excluded from lint pending
  a rewrite.

### Fixed

- **The scheduler no longer binds loopback, so remote workers can connect.**
  `Server::bind` used one value for two different things — the interfaces to
  listen on and the address to advertise to workers — and both defaulted to
  `127.0.0.1`, since `_run_distributed_server`'s `host` parameter existed but
  no caller ever passed it. A worker on another machine dialled its *own*
  loopback and got nothing, which presents as workers that start and die: the
  same symptom as a bad environment, so it is easy to misattribute. Single-node
  runs were unaffected, which is why it survived.

  The two roles are now separate. The listener binds every interface; the
  advertised address is detected — this host's resolved name, else the
  default-route interface, each *verified* with a real TCP round trip before
  use — and falls back to loopback with a warning when nothing is reachable.
  `run_blockwise(host=...)`, `Server.run_blockwise(host=...)` and the
  `DAISY_HOST` environment variable override detection for multi-homed nodes,
  containers, and names only the compute nodes resolve. Asking for loopback
  explicitly binds loopback alone, so a single-machine run is not silently
  exposed to the local network — the wire protocol has no authentication.

- **The worker context carries the log directory again.** `set_log_basedir(...)`
  now reaches *every* worker, including ones daisy never launched. Workers
  daisy starts itself already received the master's logging configuration in
  their payload, but that is the end of the chain: a worker that re-execs
  itself — `srun`, `sbatch`, `docker run`, a plain `subprocess.run` inside a
  spawn function — passes on nothing but the environment. `DAISY_CONTEXT` held
  five keys and no `logdir`, so `daisy.Client()` in such a process fell back
  to *its own* default and reported the relative `daisy_logs`: the
  `client.context["logdir"]` key was present but pointed somewhere the master
  never chose, and per-worker logs scattered beside whatever cwd the job
  happened to start in.

  The directory now travels in the context, and **constructing a
  `daisy.Client()` adopts it** — daisy 1.x semantics (`daisy/client.py`:
  `set_log_basedir(self.context["logdir"])`), restored. Every worker logs where
  the driver asked, whatever launched it, so the 1.x idiom
  `set_log_basedir(client.context["logdir"])` is now redundant rather than
  merely correct. An empty value carries "file logging is off", so
  `set_log_basedir(None)` propagates too; a worker that wants its own location
  sets it after constructing its client.

- **Context values are percent-encoded.** `DAISY_CONTEXT` is
  `key=value:key=value` with no escape mechanism, so a value containing `:` or
  `=` silently corrupted the handoff — reachable today with a task id like
  `stage=1`, and now that paths travel in there, with any log directory
  containing a colon (every Windows path). `%`, `:` and `=` are escaped as
  `%25`, `%3A`, `%3D`; everything else, including non-ASCII, is untouched.
  `Context.to_env()` / `from_env()` handle this transparently, so consumers
  see clean values. Only code that parses the raw environment variable itself
  needs to know — and such code was broken for these values anyway.

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

- **Block functions run in worker subprocesses.** A 1-arg `process_function`
  on the distributed run paths is serialized (with `cloudpickle` when
  installed; stdlib `pickle` otherwise) and executed by real OS worker
  processes launched as `python -m daisy._subprocess_worker`, rather than on
  GIL-sharing threads inside the server process. Serial execution
  (`run_blockwise(..., multiprocessing=False)`) is unchanged.

  Why: across workload mixes (16 workers, 96 × ~100 ms blocks), threads win
  only when the block function releases the GIL for essentially its entire
  runtime — pure I/O waits or single-threaded C-library calls — and then only
  by 13–17%. With just 10% pure-python glue, threads are 1.7× slower; at 30%
  python, 8× slower; at 100% python, 28× slower. Worker processes are flat
  across every mix. (Thread execution was briefly retained as an opt-out via
  `worker_processes=False`; see the Removed section above for why it went.)

  **Resource implication**: `max_workers=N` means N python interpreter
  processes (each importing your function's modules) rather than N threads in
  one process. Budget memory accordingly.

  `Task(timeout=...)` gains true preemption: a block exceeding the deadline
  kills its worker process (visible as a dirty exit, bounded by
  `max_worker_restarts`) instead of leaving a runaway thread behind.

### Added

- Optional dependency extra `daisy[worker-processes]` installing
  `cloudpickle` for lambda/closure support in worker processes.

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
