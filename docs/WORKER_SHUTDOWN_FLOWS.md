# Worker Shutdown Flows: daisy vs gerbera

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

### Gerbera

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

**Key detail**: gerbera uses a simple `Disconnect` message (no handshake). The worker thread returns `true` to signal clean exit. `check_thread_health` sees `Ok(true)` and does not respawn.

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

### Gerbera

```
User hits Ctrl-C
     │
     ▼
py.detach() is running the tokio event loop on the main thread.
Python signal handling is deferred until GIL reacquisition.

     ├─ py.detach returns (GIL reacquired)
     │   ⚠ BUT: no KeyboardInterrupt handler installed.
     │   The signal is delivered when Python regains control.
     │   If the event loop is still running, it blocks indefinitely.
     │
     ▼
  ⚠ CURRENTLY: Ctrl-C during py.detach() is NOT handled gracefully.
  The tokio runtime continues running until all tasks complete or
  the process is killed by a second signal.
```

**Gap**: gerbera has no `stop_event` mechanism. The `py.detach` call releases the GIL and runs the tokio event loop synchronously. Python's signal handler for SIGINT can't run until `py.detach` returns, which only happens when all tasks complete. A second Ctrl-C (SIGINT) will kill the process outright.

**What would need to change**: install a tokio signal handler that sets a shutdown flag, causing the event loop to exit early:

```rust
// In run_blockwise, inside the select:
_ = tokio::signal::ctrl_c() => {
    info!("received Ctrl-C, shutting down");
    break;
}
```

Then the existing shutdown code (send RequestShutdown to pending, close accept, join threads) handles cleanup.

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

### Gerbera

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

| Scenario | Daisy | Gerbera |
|---|---|---|
| **Normal shutdown** | Graceful TCP handshake (NotifyDisconnect/Ack), process exits 0, reaper keeps in pool | RequestShutdown message, thread returns `true`, not respawned |
| **Ctrl-C** | `stop_event.set()` exits loop, `finally` sends SIGTERM to all workers, joins | ⚠ **Not handled** — `py.detach` blocks, Python signal handler deferred. Needs `tokio::signal::ctrl_c()` in select loop |
| **Worker crash** | `stream.closed()` detected on next 0.1s tick → block FAILED → retry. `reap_dead_workers` → respawn | TCP EOF → synthetic Disconnect → bookkeeper. Health tick (500ms) → `get_lost_blocks` → block FAILED → retry. `check_thread_health` → respawn |
| **Block recovery** | Block marked FAILED, retried up to `max_retries` | Same |
| **Detection latency** | ~0.1s (event loop poll) | ~500ms (health tick). Disconnect registered immediately but block recovery waits for tick |
| **Timeout backstop** | `BlockBookkeeper.processing_timeout` (available but usually None) | Same field exists, also not wired to Python API |

### Gaps to Address

1. **Gerbera Ctrl-C handling**: add `tokio::signal::ctrl_c()` as a branch in the select loop. On trigger, break out of the event loop and run the existing shutdown code.

2. **Processing timeout**: wire `Task.timeout` through to `BlockBookkeeper::new(Some(duration))` in both daisy and gerbera. This is the only reliable backstop for hung workers, network partitions, and edge cases where the TCP socket doesn't close.

3. **Detection latency**: gerbera's 500ms health tick is 5x slower than daisy's 0.1s loop. For most workloads this doesn't matter (blocks take seconds to minutes), but it could be made configurable.
