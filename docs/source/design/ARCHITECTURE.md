# Architecture

A bird's-eye view of how daisy is wired together. Skim this first; the per-component docs go deep on individual subsystems.

## Crates and modules

```
daisy/
├── daisy-core/          Pure Rust, no Python deps
│   └── src/
│       ├── block.rs           Block + BlockId + BlockStatus
│       ├── coordinate.rs      ND integer coordinates and arithmetic
│       ├── roi.rs             Region-of-interest (offset + shape)
│       ├── task.rs            Task definition + builder
│       ├── task_state.rs      Typestate enum: Running / Done / Abandoned
│       ├── dependency_graph.rs  Per-task and inter-task block dep graphs
│       ├── ready_surface.rs   Surface/boundary tracking, orphan BFS
│       ├── processing_queue.rs Per-task ready queue + retry counters
│       ├── scheduler.rs       Coordinates dep graph + ready surface
│       ├── done_marker.rs     Zarr v3 persistent skip markers
│       ├── resource_allocator.rs  Global resource budget
│       ├── block_bookkeeper.rs  In-flight block tracking
│       ├── client.rs          TCP client (workers connect via this)
│       ├── worker_pool.rs     External-process worker pool (subprocess workers)
│       ├── server.rs          The run loop. Owns scheduler, accepts TCP, dispatches
│       ├── serial.rs          Single-threaded runner for debugging
│       ├── run_stats.rs       Per-worker / per-task / process-wide stats
│       ├── protocol.rs        Wire message enum + length-prefixed bincode framing
│       └── error.rs           DaisyError
│
├── daisy-py/            PyO3 bindings + Python adaptation layer
│   ├── src/                   Rust → Python bridge
│   │   ├── lib.rs             Module init, function exports
│   │   ├── py_task.rs         Python Task class wrapping Rust Task
│   │   ├── py_task_state.rs   Python TaskState wrapping TaskCounters snapshot
│   │   ├── py_block.rs        Python Block wrapping Rust Block
│   │   ├── py_roi.rs          Python Roi (transparent passthrough)
│   │   ├── py_callbacks.rs    PyProcessBlock / PySpawnWorker / PyProgressObserver
│   │   ├── py_sync_client.rs  Synchronous TCP client for 0-arg workers
│   │   ├── py_dep_graph.rs    Python wrapper over DependencyGraph
│   │   ├── py_scheduler.rs    Python wrapper over Scheduler (rare; debug only)
│   │   └── py_server.rs       Entry points: _run_serial, _run_distributed_server
│   │
│   └── python/daisy/        Pure Python, user-facing API
│       ├── __init__.py        Re-exports the public API
│       ├── _task.py           Task / Scheduler / Client / Context classes
│       ├── _progress.py       Topo ordering, tqdm observer, summary printers
│       ├── _runner.py         Server class + run_blockwise entry point
│       ├── _compat.py         Thin re-export shim for back-compat imports
│       └── logging.py         Per-thread / per-worker log routing
│
├── examples/              Cell-style scripts (# %%) for VS Code interactive mode
├── tests/                 pytest-driven integration tests (Python entry → Rust runtime)
├── benchmarks/            Throughput comparisons vs daisy 1.x
└── docs/                  This directory
```

## The runtime topology

```
        Python user script
              │
              │  Task(...)  +  run_blockwise([tasks])
              ▼
    daisy-py (PyO3 bindings)
        │
        │  _rs._run_distributed_server(...)
        │  releases the GIL via py.detach
        ▼
    daisy-core::Server::run_blockwise   ◄────── tokio multi-threaded runtime
        │                                          (the main thread blocks here
        │  bind TCP + select! loop                  via rt.block_on)
        │
        ├─ accept loop      ────────►   tokio task per connection
        │                                  ├─ read frames     (TCP → mpsc)
        │                                  └─ write frames    (mpsc → TCP)
        │
        ├─ run loop         ────────►   handles (in order of arrival):
        │                                  ├─ msg_rx           (worker requests)
        │                                  ├─ worker_exit_rx   (thread death)
        │                                  ├─ health_interval  (every 500ms)
        │                                  └─ abort_interval   (every 100ms,
        │                                                       checks SIGINT)
        │
        └─ worker threads   ────────►   std::thread per worker
                                          ├─ for 1-arg fn: tokio Client + loop
                                          └─ for 0-arg fn: GIL acquire + call
                                                            once, user runs loop
```

Three layers, three responsibilities:

- **Python**: API ergonomics. Constructors, defaults, deprecation aliases, the `Client` context manager, the tqdm observer, the per-thread log proxy. No scheduling logic.
- **PyO3 bridge**: type conversion + GIL discipline. Wraps Python callables in `dyn ProcessBlock` / `dyn SpawnWorker`. Releases the GIL across long Rust operations and re-acquires it for callbacks.
- **Rust core**: the actual machinery. Dependency graph, ready surface, scheduler, TCP server, worker threads, resource budget, run statistics, signal handling.

## How a block flows from acquire to release

For a 1-arg `process_function` task:

```
┌─────────────────────────────────────────────────────────────────┐
│ scheduler.acquire_block(task_id)                                │
│   ├─ pull from ready queue                                      │
│   ├─ note_acquired() → R--, P++                                 │
│   ├─ run pre-check (done marker + check_function)               │
│   │   └─ if done: mark Success and recurse                      │
│   └─ register with bookkeeper (sent_blocks)                     │
└────────────────────────────────┬────────────────────────────────┘
                                 │  Message::SendBlock
                                 ▼
            ┌──────────────────────────────────┐
            │ worker thread (Rust)             │
            │   process_fn.process(&mut block) │  ◄── PyO3 GIL acquire
            │     └─ Python user code          │      runs Python
            │   if Failed: exit dirty          │      releases GIL
            └────────────────┬─────────────────┘
                             │  Message::ReleaseBlock
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ scheduler.release_block(block)                                  │
│   ├─ strip from in-flight tracking                              │
│   ├─ if task not Running: drop (typestate gate)                 │
│   ├─ if Success:                                                │
│   │   ├─ ready_surface.mark_success → new ready blocks          │
│   │   ├─ note_completed() → P--, C++                            │
│   │   ├─ done_marker.mark_success                               │
│   │   ├─ queue_ready_block on each new block (typestate gate)   │
│   │   └─ try_finalize_done                                      │
│   └─ if Failed:                                                 │
│       ├─ retries < max → push_to_ready_queue + note_failed_for_retry │
│       └─ retries = max → mark_failure (orphan BFS) +            │
│                          note_failed_permanently +              │
│                          note_orphaned on each orphan task      │
└─────────────────────────────────────────────────────────────────┘
```

## Key design decisions

**Tokio-driven event loop** instead of polling. The run loop wakes only when something happens (message, worker exit, timer). Daisy uses a 100ms polling loop; we use `select!`.

**Typestate task lifecycle** instead of a status enum + runtime checks. The `TaskState` enum's three variants gate counter mutation at compile time: Done and Abandoned variants don't expose `&mut RunningTask`, so a stale message can't accidentally rewind counters. See [ABANDONMENT.md](ABANDONMENT.md).

**Single mutation gate** (`Scheduler::running_mut`). All counter writers funnel through this. No scattered `if state.is_done()` checks.

**Bincode + size-validated framing** for the wire protocol. Type-safe (no pickle code-execution surface), cross-language, hard upper bound on message size before allocation. See [PROTOCOL.md](PROTOCOL.md).

**Raw POSIX SIGINT handler** instead of `Python::check_signals` polling. The latter doesn't fire under tokio's multi-threaded runtime because CPython only processes signals on the main thread. The raw C handler sets an atomic flag from any thread. See `daisy-py/src/py_server.rs`.

**Resource budget composes with `max_workers`**. `requires` per task + `resources` global budget enforce concurrent worker counts across competing tasks. See [WORKER_POOL_COORDINATION.md](WORKER_POOL_COORDINATION.md).

**Persistent done markers in Zarr v3 layout**. Resumable runs without users having to roll their own check function. See [DONE_MARKERS.md](DONE_MARKERS.md).

## Where to look when debugging

| Symptom                                                          | Look at                                          |
| ---------------------------------------------------------------- | ------------------------------------------------ |
| Run hangs at startup                                             | `Scheduler::new` (root iterator materialization) |
| Run hangs after some blocks complete                             | [ABANDONMENT.md](ABANDONMENT.md), race windows   |
| Counts don't add up                                              | `running_mut` gate — late mutations are silently dropped, look for `tracing::debug` lines `dropping release for non-running task` |
| Workers don't restart                                            | `rebalance_workers` — check `worker_restart_count >= max_worker_restarts` |
| `Ctrl+C` doesn't exit                                            | `py_server.rs` SIGINT handler installation       |
| Progress bar order wrong                                         | `_topo_order` in `daisy/_progress.py`          |
| Done marker says "layout mismatch"                               | `done_marker.rs::open_or_create` hash check      |
| Worker can connect but blocks never arrive                       | Task's `requires` doesn't fit in `resources`     |
