# Refactor and feature recommendations

A review of the daisy codebase as it stands. The library is feature-complete for its core mission (block-wise distributed processing with abandonment, resource budgeting, persistent done markers, and Python compat). This document flags places where the implementation has settled into complexity worth cleaning up, plus features that would slot into the existing architecture for clear user wins.

The recommendations are sized by approximate effort: **S** (one sitting), **M** (a focused PR), **L** (multi-day with design discussion). Effort estimates assume one engineer who already knows the codebase.

## Part 1 — Simplifications

The seven simplifications previously listed (dep-graph dedup, `SerialServer` collapse, `_compat.py` split, `num_workers`/sentinel removal, `TaskCounters::is_done` removal, `update_ready_queue` return-type tightening, `framing.rs` fold) have all been applied. One remains:

### 1.1 Stale `daisy-core/tests/integration_tcp.rs` keeps breaking [S]

These tests have failed-to-compile twice during recent refactors because they call `Server::run_blockwise` with an outdated signature. Each time, fixing them takes a couple of minutes of mechanical edits. Either:

- Wire them into CI so they break loudly when the signature changes, or
- Move what they test into Python integration tests (where they'd run alongside the rest of the suite) and delete the file.

The Python suite already covers most of what they cover.

## Part 2 — Features that fit the current architecture

Ranked by user value × ease of implementation. Each estimates how it slots into existing structure.

### 2.1 Worker init/cleanup hooks for 1-arg mode [M, high value]

**Problem**: in 1-arg `process_function` mode, the worker thread loops over blocks calling the function once per block. There's no place for per-worker setup (loading a model, opening a database connection, allocating a CUDA context). Users currently work around this with module-level globals (which leak across workers in the same process) or by switching to 0-arg mode and writing the loop themselves.

**Proposal**: add `init_function` and `cleanup_function` fields to `Task`, both 0-arg callables. The Rust worker thread calls `init_function()` once after connecting to the server, before the acquire/process/release loop, and `cleanup_function()` after the loop exits (clean or dirty).

This gives 1-arg mode the lifecycle that currently only 0-arg mode has, without forcing users to manage their own TCP loop. It also matches the natural shape of GPU workloads where model load is 90% of startup cost.

Slots into `spawn_worker` (daisy-core/src/server.rs:474) — three new lines around the existing loop.

**Next Steps**: This does not seem particularly urgent or needed. If users want a setup/teardown they can just make a worker function which has a pretty minimal overhead of just connecting a client and handling the block loop themselves.

**Decision**: defer. The 0-arg path is a real precedent. Worth flagging that the 0-arg loop boilerplate (Client setup, `with acquire_block()` lifecycle, `block is None` exit, exception propagation) is non-trivial and the asymmetry between modes is an onboarding cliff — but if the maintainer doesn't feel the pain, no need to pre-empt it.

### 2.2 Per-block timeout [S, medium value]

**Problem**: `Task::timeout` exists in the struct (task.rs:72) but is never read. A block that hangs forever in `process_function` blocks its worker forever; the bookkeeper has a `processing_timeout` parameter (block_bookkeeper.rs:15) that's also never set.

**Proposal**: thread `Task::timeout` through. When a block has been in `processing` longer than `timeout`, the bookkeeper marks it lost and the runner releases it as failed (counts toward `worker_restart_count` since the worker is misbehaving — same path as a dirty exit).

Implementation is small: pass `timeout` into `BlockBookkeeper::new` per task. The lost-block path already exists; this just adds a second condition to `get_lost_blocks`.

**Next Steps**: This is a low effort, medium return effort. We should go ahead and implement this. The default time out should not be set, it should be something a user can set by themselves.

**Decision**: implement. Default `timeout = None` (no timeout). Wrinkle worth being deliberate about: daisy can't actually preempt a running Python function inside a thread — when timeout fires we reclaim the block server-side (release as Failed via the existing lost-block path) and the slow worker keeps running. Its eventual late `release_block` is silently dropped. The worker isn't killed; it'll either finish naturally (and exit clean later) or stay slow on the next block too (which times out again, triggering more reclamation). This is the cleanest semantic threading can give us. If hard preemption matters, users can use 0-arg mode and put their own watchdog.

### 2.3 Resumable runs as a first-class concept [M, high value]

**Problem**: daisy supports persistent done markers (`done_marker_path`), but the resume flow isn't documented as a feature. Users have to know to set the basedir, see that markers are written, and trust that re-running with the same parameters will skip done blocks. There's no "try the run; if it fails halfway, here's how to pick up where it left off" guide.

**Proposal**:

- A `daisy.resume(...)` helper that re-reads the markers, prints a "resuming with N/M blocks already done" summary, and runs the rest.
- A `--from-checkpoint` CLI subcommand on the example scripts.
- Surface "X blocks resumed" in the execution summary when a run starts with non-empty markers.

The plumbing exists. This is a docs + UX layer.

**Next Steps**: I like this idea but I think it needs more thought. Using checkpoints it sounds like there could be multiple resume checkpoints for a given task. IDK how useful that would be. I think we should make it more obvious to users how many blocks are done before they restart, have obvious options for restarting etc. But I'm not sure what the best interface is yet.

**Decision**: defer the broader UX, do the small piece now. Reframing: "checkpoint" was confused terminology — there's no snapshot. The done-marker is per-block "done" persistence. The right tractable next step is a one-liner pre-run log in `Scheduler::init_done_markers` ("extract: 14523/16000 blocks already done; resuming with 1477 remaining"). Doesn't need a new API. ~15 lines. The CLI / `daisy.resume()` helper is a separate design discussion that can wait.

### 2.4 Live progress JSON for piping [M, medium value]

**Problem**: tqdm bars are great for terminals, useless for monitoring dashboards, CI logs, or other tools. There's no way to scrape per-second progress without parsing tqdm's stderr.

**Proposal**: a `JsonProgressObserver` that emits one JSON line per state change to a file path or stdout:

```json
{"t": 1730000000.123, "task": "extract", "completed": 4321, "failed": 2, "ready": 99, "processing": 4, "restarts": 1}
```

Implements the `ProgressObserver` protocol. Users opt in via `progress=JsonProgressObserver(path="...")`. Pipes cleanly into `jq`, `fluentd`, or any log tail.

This is the natural extension of having an observer abstraction — the abstraction is already paid for.

**Next Steps**: This seems like a great feature to implement. It shouldn't really touch anything internally in daisy and should just be its own standalone observer. Seems great.

**Decision**: implement now. Standalone class in `daisy/_progress.py` exposing the same `on_start/on_progress/on_finish` shape as `_TqdmObserver`. ~50 lines.

### 2.5 Multi-machine workers [L, high value if you need it]

**Problem**: daisy binds to `127.0.0.1` by default. Users running on multi-node SLURM/LSF clusters can't farm out workers across nodes; daisy has launchers for this (`daisy.distributed`).

**Proposal**:

- Bind option (`Server(host="0.0.0.0", port=...)` already supported in Rust; expose to Python).
- A worker entry point: `python -m daisy.worker --host=... --port=... --task-id=...` that connects to a remote server and runs the task's `process_function`. The user's process function would need to be importable on each node.
- Optional SLURM helper: `daisy.distributed.launch_workers(server_addr, n_per_node=...)` that submits sbatch jobs.

The protocol already supports remote workers (it's TCP). What's missing is the launcher and the worker entry point. The 0-arg `spawn_function` mode is the precedent — it's how a user could already do this manually with `subprocess.run("ssh node-1 ...")`. Formalizing it would be valuable.

**Next Steps**: This is a high priority. I'm not sure quite how necessary it is at the moment since I haven't started using `daisy` as a replacement for daisy, but this definitely seems like an issue that will have to be resolved before I start using it.

**Decision**: needs a short RFC before coding. Two complications worth resolving up front: (a) the `process_function` and any imports it uses must be installable on every node — that means a packaging/deploy story alongside the launcher; (b) the SIGINT handler we built only catches signals on the coordinator process. If a worker node hangs, killing the coordinator's tab doesn't propagate. Workers need their own watchdog ("coordinator went away → exit"). Park as RFC; don't bundle with anything else.

### 2.6 Block batching for low-latency tasks [M, medium value]

**Problem**: at ~50µs per TCP roundtrip on loopback (much higher across machines), a task with sub-millisecond per-block work is bottlenecked on coordination. The stress test's "1M no-op blocks" hits this.

**Proposal**: let `Task` declare `acquire_batch_size: usize`. The worker requests `N` blocks per `acquire_block` call; the server hands out up to `N` if available. The worker processes them locally, then releases the batch. Reduces TCP messages by a factor of `N`.

Protocol change: `Message::AcquireBlock { task_id, batch_size }` → `Message::SendBlocks { blocks: Vec<Block> }`. Worker loop processes the vec, then sends a `Message::ReleaseBlocks { blocks }`. Bookkeeper tracks per-block as today. Most code is unchanged; the change is at the message-handler level.

**Next Steps**: I very much like this idea. It should be implemented immediately. I have wanted to add support for workers requesting and processing multiple blocks at a time which is along the same lines. My original desire was something along the lines of: We want to maximize the throughput of our gpu. If each block is processed independently, then the gpu has to wait on data reads/writes. If we make the block processing more async, then the gpu node could request e.g. 10 blocks. Each block gets read, and added to a queue. The gpu processes as fast as it can off of the queue and adds the result to a second queue. Finally each block gets written off to disk. Then the gpu can process as much as possible as fast as possible. This seems related to block batch processing. I'm not entirely sure what currently happens if a single worker calls `with acquire_block() as block1, with acquire_block() as block2, ...` and then tries to process them all.

**Decision**: implement; high priority for the GPU pipelining use case. Answer to the stacked-`with` question: today, each `acquire_block()` is a sync TCP roundtrip — the second blocks until the first returns. You'd hold two blocks at once but acquired *serially*. That gives you higher in-flight count without the latency overlap GPUs need. Batching (one `AcquireBlock { batch_size: N }` → one `SendBlocks { Vec<Block> }` round trip) is the actual primitive for the read→GPU→write pipeline. Schedule after the current batch (2.8 + 2.2 + 2.4) lands.

### 2.7 Block prioritization callback [S, niche value]

**Problem**: ready queue is FIFO. For some workloads (large blocks first to balance load, or boundary blocks last to overlap I/O) the user might want a different order.

**Proposal**: optional `Task::priority_function: Option<Fn(&Block) -> i64>`. When set, the ready queue becomes a min-heap keyed by priority. Skipped if not set (FIFO performance unchanged).

`processing_queue.rs` is 45 lines today; this would add maybe 30.

**Next Steps** low priority. This can sit of the list of features to be implemented in the future since IDK if we really need it yet.

**Decision**: defer. Add when there's a concrete user request.

### 2.8 Memory-bounded ready queue [M, low value but a real foot-gun]

**Problem**: with 1M-block tasks, the root iterator materializes all 1M blocks into a `Vec<Block>` upfront (~96 MB of allocations). This was the discovered cost of the stress-test startup hang earlier.

**Proposal**: keep `dg.roots()` lazy — return the iterator instead of `collect()`-ing. The `ProcessingQueue` already supports a `Box<dyn Iterator>` for the root generator; the constructor just doesn't use it that way. Plumb the lazy iterator through and the upfront cost drops to ~zero.

The reason it was eager was a borrow-checker issue with `Arc<DependencyGraph>` and closure lifetimes; that Arc cleanup already landed, so the remaining work is just plumbing the lazy iterator through `ProcessingQueue` and `Scheduler::new`.

**Next Steps**: This should be implemented sooner rather than later. It seems like a simplification refactor that doesn't really change anything about how `daisy` actually works. I think we can go ahead and do this now.

**Decision**: implement now. Make the inner `cartesian_product` lazy as an iterator (multi-radix counter), then plumb the lazy iterator chain through `BlockwiseDependencyGraph::root_iter_owned` (a new owned-iterator method), `DependencyGraph::roots`, and `Scheduler::new`. The downstream `ProcessingQueue` already takes `Box<dyn Iterator<Item = Block> + Send>` — fits naturally.

### 2.9 Better failure visibility in the summary [S, medium value]

**Problem**: when blocks fail, the execution summary tells you the count but not anything about *what* failed. To investigate, you have to dig through per-worker log files in the tempdir.

**Proposal**: collect the first N (configurable, default 5) `BlockFailed` error strings per task during the run. Print them under the execution summary with the failing block id.

```
Execution Summary
-----------------
extract     ✔ 99,997  ✗ 3
  block 47  ValueError: array shape mismatch
  block 88  RuntimeError: cuda OOM
  block 412 KeyError: 'metadata'
```

`BlockFailed` carries the error string already (daisy-core/src/protocol.rs). Just need to keep the first N per task in `RunStats` and render them.

**Next Steps**: This is low priority and can be left to future work. Errors are already logged pretty ergonically so I don't think this is that important.

**Decision**: defer. Logs already cover the use case.

### 2.10 Backoff with jitter for retries [S, low value but easy]

**Problem**: a transient failure (network blip, lock contention) gets retried immediately. With many workers all hitting the same external resource, retry storms cluster.

**Proposal**: `Task::retry_backoff: Duration` (default 0). When set, a failed block isn't returned to the ready queue immediately — it waits `backoff * 2^retries` (capped at some max) before becoming ready again. Implementation: a delayed-ready queue alongside the immediate-ready queue, scanned each tick.

The retry path in `Scheduler::release_block` is the only place that needs to know.

**Next Steps**: I do quite like this. By default the limit could be the same as the block time-out limit. Medium priority. Should probably be implemented before releasing, but its not super urgent for my usecases at the moment since it just handles a somewhat rare error case.

**Decision**: implement before release; not urgent. Mild pushback on tying the default to the block timeout — they're different concerns: backoff paces *retries* (when a block has failed but might succeed later), timeout bounds *individual stuckness* (block has been processing too long). Coupling them mixes two unrelated knobs. Default `retry_backoff = 0` (off, current behavior); users opt in with explicit values.

### 2.11 Dry-run mode [S, low value]

**Problem**: when configuring a complex task graph, users want to know what's about to happen — how many blocks per task, where they live in the ROI — without actually running.

**Proposal**: `daisy.run_blockwise(tasks, dry_run=True)` prints the dependency graph, per-task block count, ROI, and resource budget allocation, then returns. Useful for diagnosing "I expected 80 blocks but I'm seeing 64" (off-by-one in fit/conflict).

The dependency graph already knows everything needed. This is purely a print pass.

**Next Steps**: Leave this for future work. IDK how much use it has. It could just tell a user how many blocks have been processed, at what speed, and how many are left, thus an ETA. That only works on resume though and belongs with the earlier proposal of making resuming a first class citizen. priority is relatively low.

**Decision**: defer; fold into the eventual resume-UX work (2.3). Pre-run "what's planned" and "what's already done" naturally pair.

## Part 3 — Strengths to keep

Things daisy does well that we shouldn't accidentally walk back when refactoring:

- **Typestate task lifecycle** (`TaskState` enum + `RunningTask` mutation methods). The compiler enforces that late events on terminal tasks are dropped — no scattered `if state.is_done() { return; }` checks. See `docs/ABANDONMENT.md`.
- **Resource budget allocator** with `requires`/`resources` — global budget composes with per-task `max_workers`, which daisy doesn't have.
- **Persistent done markers** in Zarr v3 layout — daisy has `check_function` but not a built-in persistence mechanism, so users had to roll their own.
- **Worker restart cap** with proper abandonment + transitive downstream orphan propagation. Daisy's restart-cap is "respawn forever, hope it works".
- **Cross-platform raw SIGINT handler** — Ctrl-C works during `py.detach`d tokio runs without requiring `Python::check_signals` from the main thread (which doesn't fire under tokio's multi-threaded scheduler). Daisy doesn't have to deal with this because it's pure Python.
- **Run-stats with linear regression slopes** for per-block durations — surfaces "is processing getting slower over time" trends that a simple mean would hide. Not in daisy.
- **Topological display ordering** for tqdm bars and reports — Kahn's with alphabetical tiebreaker. Daisy's progress display is dict-order.
- **Bincode + size-validated framing** — kills the pickle code-execution attack surface daisy carries.

When considering changes, prefer ones that strengthen these (unify access to them, document them better, add features that compose with them) over ones that add parallel mechanisms.

## Suggested order of attack

After the discussion documented above:

**This batch (small, clear wins)**:

1. **2.8 lazy roots** — fixes the 1M-block startup hang foot-gun
2. **2.2 per-block timeout** — opt-in, no default, no preemption
3. **2.4 JSON observer** — standalone, ~50 lines

**Soon, before users adopt**:

4. **2.6 block batching** — needed for the GPU pipeline pattern
5. **2.10 retry backoff** — small, opt-in, default off

**Needs design discussion first**:

6. **2.5 multi-machine** — RFC required (deploy + worker watchdog)
7. **2.3 resume UX** — small pre-run "M of N already done" log line is the tractable next step; broader CLI/helper deferred

**Deferred**:

- 2.1 init/cleanup hooks (0-arg path is the precedent)
- 2.7 priority callback (no concrete user yet)
- 2.9 failure summary (logs cover it)
- 2.11 dry-run (folded into 2.3)
