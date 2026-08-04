# Internal Differences: daisy 1.x vs daisy v2

A subsystem-by-subsystem comparison of how daisy 1.x and daisy v2 differ, with specific pros and cons for each approach.

---

## 1. Event Loop

**daisy 1.x**: synchronous polling loop with 0.1-second blocking timeout on a tornado message queue.

```python
while not self.stop_event.is_set():
    self._handle_client_messages()  # blocks up to 0.1s on queue.get()
    self._check_for_lost_blocks()
    self.worker_pools.check_worker_health()
    if current_time - last_time > 1:
        self._check_all_tasks_completed()
```

**daisy v2**: async `tokio::select!` loop that wakes only when a message arrives or a timer fires.

```rust
tokio::select! {
    Some(cm) = msg_rx.recv() => { /* handle message */ }
    _ = health_interval.tick() => { /* health check */ }
    _ = done_check_interval.tick() => { /* completion check */ }
}
```

**Pro daisy v2**: zero-cost waiting — the thread sleeps until an event occurs instead of polling. With many idle workers, daisy 1.x wastes CPU spinning through the loop every 0.1s; daisy v2's select is free.

**Pro daisy 1.x**: simpler mental model. The loop is sequential and predictable — you can read it top-to-bottom. daisy v2's select has branch priority semantics (tokio picks randomly among ready branches) that are less obvious.

---

## 2. Message Dispatch: Single Path

Both implementations use a single dispatch path for block requests.

**daisy 1.x**: when no blocks are ready, parks the raw TCP message in a pending queue. On the next loop iteration, `_get_client_message` checks the TCP queue first, then falls back to the pending queue. Either way, the message goes through `_handle_client_message` → `_handle_acquire_block` — the single path that dispatches blocks and registers with the bookkeeper.

**daisy v2**: when no blocks are ready, parks the full `ClientMessage` (including client address and reply channel) in a `VecDeque`. After a block release — the only event that can free downstream dependencies — `retry_pending` pops each parked message and feeds it back through `handle_message` → `handle_acquire`, the same single path fresh messages use.

```rust
Message::ReleaseBlock { block } => {
    bookkeeper.notify_block_returned(&block, cm.addr);
    scheduler.release_block(block);
    if !pending.is_empty() {
        self.retry_pending(scheduler, bookkeeper, pending, worker_pools)?;
    }
}
```

**Pro daisy v2**: retry is only triggered by events that can actually change block availability (release and lost-block recovery). daisy 1.x retries pending on every loop iteration regardless of whether anything changed, which means unfulfillable requests are re-checked every 0.1s.

**Pro daisy 1.x**: the pending queue is checked as part of the message-fetch logic, so it's impossible to forget to check it — it happens automatically on every iteration. daisy v2 must explicitly call `retry_pending` at each release site.

**Shared strength**: both have a single dispatch path. The bookkeeper sees every dispatched block because there's only one place dispatch happens.

---

## 3. Message Serialization

**daisy 1.x**: Python `pickle` over `[4-byte length][payload]` frames on tornado IOStreams.

**daisy v2**: Rust `bincode` over `[4-byte length][payload]` frames on tokio TCP.

**Pro daisy v2**: bincode is type-safe (can't deserialize into an unexpected type), cross-language (any language with a bincode implementation can speak the protocol), and doesn't allow arbitrary code execution (pickle does — a malicious message can run code on deserialization). daisy v2 also validates message size (64 MiB cap) before allocating.

**Pro daisy 1.x**: pickle can serialize arbitrary Python objects, including exception tracebacks and user-defined types. daisy v2's `BlockFailed` message carries only a string error description; daisy 1.x's carries the actual exception object, which the server can re-raise with the original traceback.

**Con daisy v2**: bincode is a binary format with no human-readable form. Debugging protocol issues requires writing a decoder. daisy 1.x's pickled messages can be inspected with `pickle.loads()` in a REPL.

---

## 4. Worker Management

**daisy 1.x**: workers are `multiprocessing.Process` objects spawned by Python. The `process` attribute is `None` when stopped, a `Process` when running. Health monitoring is done by `TaskWorkerPools` which calls `reap_dead_workers()` and spawns replacements. Worker spawning, health checks, and cleanup all live in Python.

```python
self.process = None  # initial
self.process = Process(...)  # after start
self.process = None  # after stop
```

**daisy v2**: worker *supervisors* are `std::thread` instances spawned and managed entirely by the Rust server; the work itself runs in a dedicated OS process per worker, as in v1. Each supervisor thread acquires the GIL just long enough to call the task's spawn function, then waits for it. The Python layer converts whatever the user passed into such a spawn function before the run starts:

- **1-arg block processors** are serialized and run by `python -m daisy._subprocess_worker`, which drives the TCP `Client.acquire_block()` loop in the child.
- **0-arg worker functions** are serialized and called in a child, where they drive their own loop and may `subprocess.run(...)` something further.

Earlier v2 builds ran 1-arg functions on the supervisor thread itself, under the server's GIL, with `worker_processes=False` as an opt-in. That mode is gone: it did not parallelize Python-bound work, and `timeout` could not preempt a thread.

Worker health is monitored by `check_thread_health` which inspects `JoinHandle::is_finished()` and respawns on failure:

```rust
let should_respawn = match handle.join() {
    Ok(true) => false,   // clean exit
    Ok(false) => true,   // function failed
    Err(_) => true,      // thread panicked
};
```

**Pro daisy v2**: no dill serialization, no `multiprocessing.Process`, no Python health-monitor thread. Worker lifecycle is managed by typed Rust code. The `WorkerState` enum makes the worker explosion bug (PR #67/#68) unrepresentable — you can't confuse "exited normally" with "crashed" because they're different enum variants.

**Parity**: both implementations give each worker a separate OS process with its own GIL, so Python-heavy process functions run in true parallel. v1 gets there by forking; v2 cannot fork (its server is multithreaded) and so spawns and serializes instead, which is why v2 requires worker functions to be picklable.

**Con daisy 1.x**: the nullable `process` field caused the worker explosion bug. `reap_dead_workers` couldn't distinguish normal exit from crash because both ended up with `process = None`.

---

## 5. Error Propagation

**daisy 1.x**: workers catch exceptions in `_spawn_wrapper`, serialize them onto a `multiprocessing.Queue` (100-item limit), and the server re-raises them via `check_for_errors()`.

```python
except Exception as e:
    self.error_queue.put(e, timeout=1)  # silently dropped if queue full
```

**daisy v2**: worker threads return `bool` (true = clean exit, false = should respawn). Block-level failures are reported via TCP `BlockFailed` messages with an error string. Thread panics are caught by `JoinHandle::join().is_err()`.

**Pro daisy 1.x**: exception objects preserve tracebacks. When a worker fails, the server can print the exact stack trace from the worker process.

**Pro daisy v2**: no silent drops. The error queue's 100-item limit means daisy 1.x can lose errors under load with only a log message. daisy v2's thread health check always detects failure — there's no queue that can fill up.

---

## 6. Concurrency & Synchronization

**daisy 1.x**: uses `threading.Lock` to protect the `workers` dict. Every method that accesses workers must acquire the lock manually. Several don't — `inc_num_workers` modifies the dict without the lock. The server's `BlockBookkeeper.sent_blocks` dict has no lock at all.

**daisy v2**: the coordinator owns all mutable state on a single async task. There are no locks because there's nothing shared. Worker pools, the scheduler, the bookkeeper, and the pending queue are all `&mut`-accessed from the select loop — the Rust compiler rejects any attempt to share them across tasks. Worker threads communicate with the coordinator only via TCP (the same channel as external workers).

**Pro daisy v2**: compile-time guarantee of no data races. The entire class of "forgot to acquire the lock" bugs is structurally impossible.

**Pro daisy 1.x**: the `threading.Lock` pattern is well-understood by Python developers. daisy v2's ownership model requires understanding Rust's borrow checker.

**Con daisy 1.x**: `task_worker_pools.py` calls `reap_dead_workers()` (acquires lock) then `inc_num_workers()` (doesn't acquire lock) in sequence. Another thread can modify the workers dict between these two calls.

---

## 7. Block Bookkeeping & Lost Block Detection

**daisy 1.x**: tracks blocks by TCP stream object identity (`log.stream`). Lost blocks detected by polling `stream.closed()` on every health check.

**daisy v2**: tracks blocks by `SocketAddr`. Disconnected clients are registered proactively when a Disconnect message arrives, stored in a `HashSet<SocketAddr>`.

**Pro daisy v2**: disconnection tracking is O(1) per block (set lookup) rather than requiring a stream query. Uses `Instant` (monotonic clock) for timeout tracking, immune to system clock adjustments.

**Pro daisy 1.x**: stream identity is unforgeable — if a worker reconnects, it gets a new stream object, so old blocks can't be accidentally claimed by the new connection.

---

## 8. Scheduler & Ready Surface

The algorithms are identical: same dependency graph computation, same level stride formula, same ready surface with surface/boundary sets, same BFS failure propagation, same cantor pairing function for block IDs (daisy v2 uses the funlib pyramid-volume generalization to match daisy 1.x's block ordering exactly).

**Pro daisy v2**: the ready surface is generic over closure types (`ReadySurface<F, G>` where `F: Fn(&Block) -> Vec<Block>`). Rust monomorphizes this at compile time, inlining the closure calls. daisy 1.x uses Python lambdas with function-call overhead per invocation.

**Con daisy v2**: the `Arc<DependencyGraph>` captured by closures means the dependency graph is constructed twice in the scheduler (once for the closures, once stored on the struct). daisy 1.x builds it once.

---

## 9. Shutdown

**daisy 1.x**: uses `try/finally`. Calls `worker_pools.stop()` which SIGTERMs each worker, then `tcp_server.disconnect()` which closes streams. Pending requests are abandoned — workers eventually notice via TCP stream closure.

**daisy v2**: proactively sends `RequestShutdown` to all parked workers before closing the accept loop. Then drains remaining messages, answering late `AcquireBlock`s with `RequestShutdown`. Worker threads exit when they receive `RequestShutdown` or the TCP connection closes. `JoinHandle::join()` waits for each thread during shutdown. `WorkerPool` also implements `Drop` for cleanup.

**Pro daisy v2**: workers get a clean shutdown signal and can exit gracefully instead of discovering a dead TCP connection. The `Drop` impl provides a safety net.

**Pro daisy 1.x**: simpler — three lines (`stop`, `disconnect`, `notify_exit`).

---

## 10. Python Compatibility Layer

daisy v2's Python wrapper (`_compat.py`, ~200 lines) is a thin adapter that maps daisy 1.x's constructor signatures to daisy v2's Rust types. It contains no scheduling logic, no block lifecycle management, and no worker management.

| Class | What it does | Lines |
|---|---|---|
| `Task` | Maps daisy 1.x's positional constructor to Rust keyword args | ~40 |
| `Scheduler` | Delegates all methods to `_rs.Scheduler` | ~15 |
| `Context` | Env var key=value encoding (data class) | ~30 |
| `Client` | Wraps `_rs.SyncClient` + `acquire_block` context manager | ~25 |
| `SerialServer` | One-liner → `_rs._run_serial()` | ~3 |
| `Server` | One-liner → `_rs._run_distributed_server()` | ~5 |
| `run_blockwise` | Routes to Server or SerialServer | ~5 |

All worker spawning, health monitoring, TCP coordination, and block lifecycle management lives in Rust. The Python side only handles constructor signature adaptation.

---

## 11. Performance

Benchmarks on Apple Silicon (M-series), Python 3.14, trivial block processing functions:

### Dependency Graph Block Iteration

| Configuration | daisy 1.x | daisy v2 | Speedup |
|---|---|---|---|
| 1M blocks, no conflict | 15.2s | 1.9s | **7.8x** |
| 970K blocks, with conflict (8 levels) | 46.4s | 7.5s | **6.2x** |
| 125K blocks, small chunks | 1.9s | 0.25s | **7.7x** |
| 970K blocks, small context | 46.3s | 7.7s | **6.0x** |

The 6-8x speedup comes from Rust's compiled code vs Python's interpreter overhead for the per-block iteration (Coordinate arithmetic, ROI construction, cantor number computation). The dependency graph build time is negligible for both.

### Worker Coordination Scaling (10K blocks, noop process function)

| Workers | daisy 1.x | daisy v2 | Speedup |
|---|---|---|---|
| 1 (serial) | 0.17s | 0.07s | **2.5x** |
| 2 | 13.8s | 0.46s | **30x** |
| 4 | 1.8s | 0.33s | **5.5x** |
| 8 | 1.9s | 0.26s | **7.3x** |
| 16 | 2.1s | 0.21s | **10.3x** |
| 32 | 2.7s | 0.22s | **12.3x** |

daisy 1.x's distributed mode has ~2s fixed overhead (tornado IOLoop + multiprocessing.Process startup). daisy v2's thread-based workers have negligible startup cost. The 2-worker daisy 1.x outlier (13.8s) is tornado initialization overhead. As worker count increases, daisy v2's coordination cost stays flat (~0.2s) while daisy 1.x's grows linearly with process count.

### Block Count Scaling (4 workers, noop process function)

| Blocks | daisy 1.x | daisy v2 | Speedup |
|---|---|---|---|
| 100 | 0.22s | 0.005s | **43x** |
| 1,000 | 0.36s | 0.04s | **10x** |
| 10,000 | 1.8s | 0.35s | **5x** |
| 100,000 | 16.0s | 3.3s | **5x** |

The small-block advantage (43x at 100 blocks) reflects the fixed overhead gap: daisy 1.x's process startup costs ~0.2s, daisy v2's thread spawn costs ~0.005s. As block count grows, per-block cost dominates and the ratio stabilizes at ~5x — this is the raw speed difference between Python and Rust for the block dispatch/acquire/release cycle.

---

## Summary

| Aspect | daisy 1.x | daisy v2 |
|--------|-------|---------|
| **Event loop** | Sync 0.1s poll | Async select, zero-cost |
| **Dispatch path** | Single (inherent — pending re-queued as messages) | Single (explicit — `retry_pending` at release sites) |
| **Pending retry** | Every iteration (0.1s) | Only on block release |
| **Serialization** | Pickle (rich errors, code injection risk) | Bincode (type-safe, cross-language, size-validated) |
| **Worker management** | Python multiprocessing.Process + threading.Lock | Rust std::thread + typed health checks |
| **Worker state** | Nullable field (None/Process) | Typed enum (Running/Done/Failed/Killed) |
| **Error propagation** | Exception queue (100-item, can drop) | Thread return value + TCP messages |
| **Concurrency** | threading.Lock (manual, forgettable) | Ownership (compile-time, unforgettable) |
| **Lost block detection** | stream.closed() poll | Proactive HashSet + monotonic clock |
| **Shutdown** | try/finally, stream closure | Proactive RequestShutdown + Drop |
| **Python API** | Native | Thin FFI wrapper (~200 lines, no logic) |
| **Block iteration** | — | **6-8x faster** |
| **Coordination overhead** | ~2s fixed (process startup) | ~0.005s fixed (thread startup) |
| **Per-block throughput** | — | **5x faster** |
