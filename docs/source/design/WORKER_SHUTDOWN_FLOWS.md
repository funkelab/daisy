# Worker Shutdown Flows: daisy 1.x vs daisy v2

Three scenarios, traced through both codebases.

---

## 1. Normal Shutdown: No More Blocks

The happy path. A worker asks for a block, the server has none left, the worker exits.

### Daisy

```
Worker Process                          Server
     │                                     │
     ├─ send(AcquireBlock) ───────────────►│
     │                                     ├─ scheduler.acquire_block() → None
     │                                     ├─ pending_count == 0, processing == 0
     │◄──────────────────── send(RequestShutdown)
     │                                     │
     ├─ Client.acquire_block():            │
     │   receives RequestShutdown          │
     │   calls tcp_client.disconnect()     │
     │     ├─ send(NotifyClientDisconnect) ►│
     │     │◄─── send(AckClientDisconnect)  │
     │     └─ stream.close()               ├─ removes stream from client_streams
     │                                     │
     ├─ acquire_block yields None          │
     │   (caller sees block is None,       │
     │    breaks out of loop)              │
     │                                     │
     ├─ _spawn_wrapper returns             │
     ├─ Process exits (code 0)             │
     │                                     ├─ reap_dead_workers():
     │                                     │   worker.process.is_alive() = False
     │                                     │   exitcode = 0
     │                                     │   (PR #68: kept in pool as Done)
     │                                     │
     ▼                                     ▼
   Clean exit                           Worker counted, not replaced
```

**Key detail**: daisy has a graceful TCP disconnect handshake (`NotifyClientDisconnect` / `AckClientDisconnect`). The worker process then exits normally (code 0).

### Daisy

```
Worker Thread                           Server (tokio event loop)
     │                                     │
     ├─ send(AcquireBlock) ───────────────►│
     │                                     ├─ scheduler.acquire_block() → None
     │                                     ├─ pending_count == 0, processing == 0
     │◄──────────────────── send(RequestShutdown)
     │                                     │
     ├─ client.acquire_block():            │
     │   receives RequestShutdown          │
     │   returns Ok(None)                  │
     │                                     │
     ├─ Worker loop breaks                 │
     ├─ client.disconnect()                │
     │   (sends Disconnect msg, TCP close) │
     │                                     ├─ reader task sees EOF/Disconnect
     ├─ Thread returns true (clean exit)   │   bookkeeper.notify_client_disconnected()
     │                                     │
     ▼                                     │
   Thread done                             │ (on final shutdown:)
                                           ├─ handle.join() → Ok(true)
                                           │   not respawned
                                           ▼
```

**Key detail**: daisy uses a simple `Disconnect` message (no handshake). The worker thread returns `true` to signal clean exit. `check_thread_health` sees `Ok(true)` and does not respawn.

---

## 2. Early Shutdown: Server Gets KeyboardInterrupt

The server needs to stop before all blocks are processed.

### Daisy

```
User hits Ctrl-C
     │
     ▼
convenience.py:
     ├─ KeyboardInterrupt caught in main thread
     ├─ stop_event.set()
     │
     ▼
Server._event_loop():
     ├─ while not self.stop_event.is_set():  ← exits loop
     │
     ▼
Server.run_blockwise() finally block:
     ├─ worker_pools.stop()
     │   └─ for each worker:
     │       ├─ worker.stop()
     │       │   ├─ process.terminate()  ← sends SIGTERM
     │       │   ├─ process.join()       ← waits for exit
     │       │   └─ process = None
     │
     ├─ tcp_server.disconnect()
     │   └─ for each stream:
     │       └─ stream.close()           ← TCP connections dropped
     │
     ├─ notify_server_exit()
     ▼
  Returns task_states (incomplete)


Worker Process (receives SIGTERM):
     ├─ _spawn_wrapper:
     │   except KeyboardInterrupt:        ← SIGTERM raises this in Python
     │       logger.debug("received ^C")
     │   (function returns)
     ├─ Process exits (code 0)
     ▼
  Dead. Any in-flight block is lost.
```

**Key detail**: daisy sends SIGTERM to every worker process and waits for each to exit (`join`). Workers catch `KeyboardInterrupt` (Python translates SIGTERM → KeyboardInterrupt in the main thread of each process). The `finally` block ensures cleanup even if the event loop threw an exception.

**Gap**: blocks that were being processed when SIGTERM hit are lost. The bookkeeper doesn't mark them as failed — the server has already exited the event loop. The task_states returned show them as still "processing."

### Daisy v2

```
User hits Ctrl-C
     │
     ▼
Raw POSIX SIGINT handler (installed by py_server.rs before py.detach)
     ├─ sets SIGINT_FLAG = true (atomic, lock-free)
     │
     ▼
Server tokio event loop:
     ├─ abort_interval (every 100ms):
     │   abort_check() reads SIGINT_FLAG
     │   if true → break out of select! loop with io::Error(Interrupted)
     │
     ▼
Server.run_blockwise() drops scope:
     ├─ TCP listener dropped (accept loop ends)
     ├─ For each worker pool: send RequestShutdown to pending,
     │   join threads (workers exit their loops cleanly)
     ├─ ResourceAllocator drops, ExitNotifier RAII fires per worker
     ├─ Returns Err(io::Error::Interrupted)
     │
     ▼
py_server.rs:
     ├─ Restores the previous SIGINT handler (always — even on early return)
     ├─ Maps Interrupted → raises Python KeyboardInterrupt
     │
     ▼
  Python user code sees a normal KeyboardInterrupt.
```

**Key detail**: a raw `libc::signal(SIGINT, handle_sigint)` C handler is installed before `py.detach`. It runs from any thread regardless of GIL state, sets an atomic flag, and the tokio run loop's 100ms `abort_interval` polls it. `Python::check_signals()` doesn't work here because CPython only processes signals on the main thread and the main thread is blocked inside `py.detach`. The previous handler is always restored before `_run_distributed_server` returns, so subsequent Python code gets normal signal handling back.

**Source**: `daisy-py/src/py_server.rs` lines 307–343 (handler install + restore), `daisy-core/src/server.rs` (run loop integration).

---

## 3. Worker Dies Unexpectedly: OOM, Segfault, SIGKILL

The worker process/thread is killed without getting a chance to send any message.

### Daisy

```
Worker Process                          Server
     │                                     │
     ├─ processing block X                 │
     ├─ SIGKILL / OOM / segfault           │
     ▼                                     │
  Process dead immediately.                │
  No Disconnect sent.                      │
  No ReleaseBlock sent.                    │
  TCP socket closed by OS (FIN sent).      │
                                           │
  ─── time passes (0.1s event loop) ───    │
                                           │
                                           ├─ _event_loop iteration:
                                           │
                                           ├─ _check_for_lost_blocks():
                                           │   bookkeeper.get_lost_blocks():
                                           │     for each sent block:
                                           │       if log.stream.closed(): ← OS closed the socket
                                           │         → block X is lost
                                           │     block X marked FAILED
                                           │     scheduler.release_block(block X)
                                           │       → block X enters retry queue
                                           │
                                           ├─ worker_pools.check_worker_health():
                                           │   reap_dead_workers():
                                           │     worker.process.is_alive() = False
                                           │     exitcode != 0  (or negative for signals)
                                           │     → worker removed from pool
                                           │   inc_num_workers(1):
                                           │     → new worker process spawned
                                           │
                                           ├─ New worker connects, gets block X (retry)
                                           ▼
                                         Recovered via retry
```

**Detection method**: `stream.closed()` returns True after the OS closes the dead process's socket. This happens quickly for SIGKILL (OS cleans up immediately) but can be delayed for network partitions.

**Detection latency**: ~0.1s (next event loop iteration after OS closes the socket).

**Recovery**: the block is marked FAILED and re-queued for retry. A new worker is spawned. If retries are exhausted, the block is permanently failed and downstream blocks are orphaned.

### Daisy

```
Worker Thread (1-arg)                   Server (tokio event loop)
     │                                     │
     ├─ processing block X                 │
     │   (Python::attach → process_fn)     │
     ├─ Thread panics / segfault           │
     ▼                                     │
  Thread dead.                             │
  TCP client in thread is dropped.         │
  tokio runtime in thread is dropped.      │
  TCP connection closed (drop → FIN).      │
                                           │
                                           ├─ Reader task for this client:
                                           │   read_message() → Ok(None) (EOF)
                                           │   sends synthetic Disconnect to msg channel
                                           │
                                           ├─ handle_message(Disconnect):
                                           │   bookkeeper.notify_client_disconnected(addr)
                                           │
  ─── health tick (500ms) ───              │
                                           │
                                           ├─ health_interval fires:
                                           │   bookkeeper.get_lost_blocks():
                                           │     addr in closed_clients → block X is lost
                                           │     block X marked FAILED
                                           │     scheduler.release_block(block X)
                                           │
                                           ├─ check_thread_health():
                                           │   handle.is_finished() = true
                                           │   handle.join() → Err (panic) or Ok(false)
                                           │   → respawn worker thread
                                           │
                                           ├─ New worker connects, gets block X (retry)
                                           ▼
                                         Recovered via retry


Worker Thread (0-arg spawn)             Server (tokio event loop)
     │                                     │
     ├─ spawn_fn.spawn(env_ctx)            │
     │   └─ subprocess.run(script.py)      │
     │       └─ script.py connects via TCP │
     │           ├─ processing block X     │
     │           ├─ SIGKILL / OOM          │
     │           ▼                         │
     │         Subprocess dead.            │
     │         TCP socket closed by OS.    │
     │                                     │
     │                                     ├─ (same as above: EOF → Disconnect
     │                                     │   → bookkeeper → lost block → retry)
     │                                     │
     ├─ subprocess.run returns             │
     │   result.returncode != 0            │
     ├─ spawn_fn returns Err               │
     ├─ Thread returns false               │
     │                                     │
  ─── health tick (500ms) ───              │
                                           │
                                           ├─ check_thread_health():
                                           │   handle.join() → Ok(false)
                                           │   → respawn worker thread
                                           │     → new subprocess connects
                                           ▼
                                         Recovered via retry
```

**Detection method (block)**: TCP reader task sees EOF → sends synthetic `Disconnect` → bookkeeper adds to `closed_clients` → next health tick finds the block in `sent_blocks` belonging to a closed client.

**Detection method (thread)**: `JoinHandle::is_finished()` checked on health tick.

**Detection latency**: up to 500ms (health tick interval) for the block recovery. The Disconnect is registered immediately when the reader task sees EOF, but `get_lost_blocks` only runs on the health tick.

**Gap**: if the worker thread panics but the TCP connection somehow stays open (shouldn't happen in practice since dropping the runtime closes the socket), the block would never be detected as lost unless a processing timeout is configured.

---

## Summary

| Scenario | Daisy | Daisy |
|---|---|---|
| **Normal shutdown** | Graceful TCP handshake (NotifyDisconnect/Ack), process exits 0, reaper keeps in pool | RequestShutdown message, thread returns `true`, not respawned |
| **Ctrl-C** | `stop_event.set()` exits loop, `finally` sends SIGTERM to all workers, joins | Raw `libc::signal` handler sets atomic flag; 100ms `abort_interval` polls it; run loop exits with `Interrupted`, mapped to Python `KeyboardInterrupt`. Previous handler always restored. |
| **Worker crash** | `stream.closed()` detected on next 0.1s tick → block FAILED → retry. `reap_dead_workers` → respawn | TCP EOF → synthetic Disconnect → bookkeeper. Health tick (500ms) → `get_lost_blocks` → block FAILED → retry. `check_thread_health` → respawn |
| **Block recovery** | Block marked FAILED, retried up to `max_retries` | Same |
| **Detection latency** | ~0.1s (event loop poll) | ~500ms (health tick). Disconnect registered immediately but block recovery waits for tick |
| **Timeout backstop** | `BlockBookkeeper.processing_timeout` (available but usually None) | Same field exists, also not wired to Python API |

### Remaining gaps

1. **Detection latency**: daisy v2's 500ms health tick is 5× slower than daisy 1.x's 100ms loop. For most workloads this doesn't matter (blocks take seconds to minutes), but it could be made configurable.

2. **Processing timeout in the Python API**: `Task.timeout` is now wired through (constructor accepts `timeout: float | int | None`) and `BlockBookkeeper::new(Some(duration))` is the backstop for hung workers, network partitions, and edge cases where the TCP socket doesn't close. Default is no timeout, on the assumption that arbitrarily long blocks are legitimate (long imaging volumes, slow ML inference). Set per-task when you have a real upper bound.
