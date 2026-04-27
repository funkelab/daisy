# Wire protocol

The TCP protocol workers and the server use to coordinate. Read this if you're writing a worker in another language, debugging a stuck connection, or wondering what gets sent on the wire.

## Transport

- TCP, no TLS by default. The server binds to whatever address is passed to `Server::bind` (default `127.0.0.1` from the Python wrapper).
- Persistent: one connection per worker, reused for the worker's whole lifetime.
- One ordered request/response stream per connection. Workers send a request, wait for the response, send the next.

## Framing

Each message is `[4-byte length, big-endian, u32][bincode payload]`.

- The length is the length of the payload, not including itself.
- Maximum payload size is **64 MiB** (`MAX_MESSAGE_SIZE` in daisy-core/src/protocol.rs). A frame larger than this is rejected before allocation — the server returns an error and closes the connection.
- bincode is the default `bincode::config::standard()` — varint integers, little-endian, no length prefix on the bincode side (we add the length prefix ourselves at the framing layer).

This is identical in shape to daisy's framing (also `[u32 length][payload]`) but the payload is bincode instead of pickle.

## Messages

```rust
pub enum Message {
    AcquireBlock { task_id: String },
    SendBlock { block: Block },
    ReleaseBlock { block: Block },
    BlockFailed { block: Block, error: String },
    Disconnect,
    RequestShutdown,
}
```

(Defined in daisy-core/src/protocol.rs.)

### AcquireBlock

Worker → server. "I'm idle, give me work for `task_id`."

### SendBlock

Server → worker. The block to process. The worker is expected to set `block.status = Processing` while working and `block.status = Success` or `Failed` before sending it back.

### ReleaseBlock

Worker → server. "I'm done with this block; here's the final status." This is the normal happy path for every block, regardless of outcome — both successful and failed blocks come back via this message. The server reads `block.status` to decide which counter to bump.

### BlockFailed

Worker → server. Equivalent to `ReleaseBlock` with status `Failed`, but carries an error string. Used by clients that want to report a structured error message alongside the failure (the Python `Client.report_failure` takes one).

The server treats `ReleaseBlock { block: { status: Failed, ... } }` and `BlockFailed { block, error }` identically with respect to the scheduler — the error string is logged, then the block goes through the failed-block path.

### Disconnect

Worker → server. Best-effort polite close. The server marks this client's address as disconnected so the lost-block detector can recover any in-flight blocks.

The TCP-level FIN packet does the same thing if Disconnect doesn't get sent — the server's read loop sees EOF and synthesizes a Disconnect message internally.

### RequestShutdown

Server → worker. "There's no more work and we're done. Exit cleanly."

The worker is expected to break out of its acquire loop, optionally call disconnect, and exit.

## Flow

A normal block lifetime:

```
Worker                              Server
  │                                    │
  ├─ AcquireBlock(task_id)  ──────────►│
  │                                    │  scheduler.acquire_block(task_id)
  │                                    │    ├─ pull from ready queue
  │                                    │    ├─ pre-check (skip if done)
  │                                    │    └─ register with bookkeeper
  │◄────────────────  SendBlock(block) │
  │                                    │
  │  process(block)                    │
  │   ├─ block.status = Processing     │
  │   ├─ user code runs                │
  │   └─ block.status = Success/Failed │
  │                                    │
  ├─ ReleaseBlock(block)  ────────────►│
  │                                    │  scheduler.release_block(block)
  │                                    │    ├─ note_completed / note_failed_*
  │                                    │    ├─ if Success: enqueue downstream
  │                                    │    ├─ if Failed (retries<max): re-queue
  │                                    │    └─ if Failed (retries=max): orphan downstream
  │                                    │
  ├─ AcquireBlock(task_id)  ──────────►│  loop continues
  │                                    │
  ...                                  ...
  │                                    │  no more blocks for this task
  │◄──────────  RequestShutdown        │
  ├─ Disconnect  ─────────────────────►│
  │  TCP close                         │
```

When the queue is empty but there are still pending dependencies, the server doesn't reply immediately. It parks the request in a `pending: VecDeque<ClientMessage>` and replies later, after a downstream-unlocking release happens. The worker just blocks on the read.

## Lost block recovery

If a worker dies without sending `ReleaseBlock` (network drop, panic, kill -9), the server detects it via:

1. **Disconnect message** — explicit, fast path.
2. **TCP EOF** — the reader task sees an Ok(None) from `read_message`, sends a synthetic Disconnect to the run loop.
3. **Processing timeout** (optional) — if `Task::timeout` is set, the bookkeeper tracks how long each block has been outstanding. Past the timeout, it's considered lost regardless of connection state.

The bookkeeper's `get_lost_blocks()` runs on the 500ms health interval. Each lost block is released as `Failed` so it goes through the normal retry path.

## Wire-compatible workers in other languages

Because the framing is `[u32 length][bincode]`, a worker in another language needs:

1. Length-prefix framing matching the same byte order (big-endian u32).
2. A bincode codec for the `Message` variants and `Block` struct.
3. The `DAISY_CONTEXT` env var encoding, to discover host/port/task_id when launched as a 0-arg `spawn_function` worker.

The `Message` and `Block` types in daisy-core are the source of truth. They're plain Rust structs with `#[derive(Serialize, Deserialize)]` so any language with a bincode crate can speak the protocol. Python workers go through `_rs.SyncClient` which is what `daisy.Client()` wraps — but the underlying TCP protocol is just bincode over TCP.

## Differences from daisy's protocol

- daisy uses pickle; daisy uses bincode. Pickle can carry arbitrary Python objects (including exception tracebacks); bincode carries typed Rust structs (so error reporting goes through a String field).
- daisy doesn't size-validate frames; daisy caps at 64 MiB and rejects oversized payloads before allocating.
- daisy has a separate `NotifyClientDisconnect` / `AckClientDisconnect` handshake; daisy uses a single `Disconnect` message.
- daisy uses tornado's IOStreams; daisy uses tokio TCP. Both reduce to `[u32 length][payload]` on the wire.
