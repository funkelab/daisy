# Task abandonment and the typestate lifecycle

How gerbera ensures the run loop terminates even when workers die in pathological ways, and why the per-task counters are protected by a Rust enum rather than runtime guards.

## The two terminal outcomes

Every task ends in exactly one of:

1. **Done** — every block reached a terminal counter (success / permanent failure / orphan) through normal processing.
2. **Abandoned** — the runner gave up after `max_worker_restarts` failed attempts. Remaining blocks are accounted as orphaned (they were never executed) so `is_done()` can become true and the loop can exit.

The whole abandonment subsystem exists to guarantee that `is_done()` becomes true and *stays true* even when in-flight messages from dying workers race against the abandonment transition.

## Counters

Each `RunningTask` (gerbera-core/src/task_state.rs) has:

| field                  | meaning                                                            |
| ---------------------- | ------------------------------------------------------------------ |
| `total_block_count`    | invariant for the whole run                                        |
| `ready_count`          | in the ready queue, not yet handed out                             |
| `processing_count`     | dispatched to a worker, awaiting `release_block`                   |
| `completed_count`      | terminal: success                                                  |
| `failed_count`         | terminal: hit `max_retries` during real processing                 |
| `orphaned_count`       | terminal: never executed (upstream failure or task abandonment)    |
| `worker_failure_count` | total dirty exits — pure stat, can exceed `max_worker_restarts`    |
| `worker_restart_count` | refill spawns the runner has performed, capped at the configured limit |

**Conservation** (Invariant 1): `total = ready + processing + completed + failed + orphaned + pending`.

**Done condition** (Invariant 2): `is_done() ⟺ ready = 0 ∧ processing = 0 ∧ pending = 0 ⟺ completed + failed + orphaned = total`.

**Stickiness** (Invariant 3): once a task reaches a terminal state, no operation may move it back to non-terminal. This is the property the typestate enforces structurally.

## The typestate

```rust
pub enum TaskState {
    Running(RunningTask),
    Done(DoneTask),
    Abandoned(AbandonedTask),
}
```

Mutation methods (`note_acquired`, `note_completed`, `note_failed_permanently`, `note_failed_for_retry`, `note_ready`, `note_orphaned`, `note_worker_died`, `note_worker_restarted`) live on `RunningTask`. `DoneTask` and `AbandonedTask` are read-only snapshots of `TaskCounters` plus a reason. The compiler enforces that mutating a task requires pattern-matching to extract `&mut RunningTask` first — terminal variants don't expose those methods at all.

`Scheduler::running_mut(&mut self, task_id)` is the single gate every counter writer goes through:

```rust
fn running_mut(&mut self, task_id: &str) -> Option<&mut RunningTask> {
    self.task_states.get_mut(task_id)?.as_running_mut()
}
```

Late events on terminal tasks return `None` here. The writer drops the message — no need to scatter `is_done()` checks.

## Transitions

- `TaskState::try_finalize_done()` — called after every successful release. If counters balance, transitions `Running → Done`. Idempotent for terminal states.
- `TaskState::abandon(reason)` — consumes the `Running` variant, accounts remaining blocks as orphaned, replaces self with `Abandoned`. Returns the count of orphaned blocks for logging. No-op for terminal states.

Both transitions use `mem::replace` with a placeholder. The HashMap key isn't changed; only the value variant.

## Worker death and restart counting

Two worker shapes, both folded into the same death/restart cycle:

**0-arg `spawn_function`**: the user owns the loop. An exception inside their function unwinds out of `PySpawnWorker::spawn`, which returns `Err`. The Rust thread exits dirty.

**1-arg `process_function`**: the Rust thread owns the loop. After every `client.release_block()`, if the block came back with status `Failed`, the thread exits dirty. (This was a deliberate semantic choice — see the discussion in REFACTOR.md.)

In both cases, the dirty exit triggers the same downstream flow:

1. `worker_exit_rx` fires on the server's `select!`.
2. `check_thread_health` joins the thread, decrements `allocator.alive`, increments `worker_failure_count`.
3. `rebalance_workers` decides whether to refill. A spawn is a *restart* iff `worker_failure_count > worker_restart_count` (we owe the task a refill for some past death). Refills bump `worker_restart_count`.
4. If the cap is hit and `alive == 0`, `abandon_exhausted_tasks` transitions the task to `Abandoned` and BFS-propagates the same transition (with reason `UpstreamAbandoned`) to all transitively-downstream tasks.

`max_worker_restarts` caps refill spawns. Because each block-failure is a death, in 1-arg mode the cap also bounds total block failures: a buggy `process_function` will fail at most `max_worker_restarts + 1` blocks before the runner gives up.

## Race windows the typestate closes

The motivating problem: when a worker crashes holding a block, three messages travel over independent transports (TCP `ReleaseBlock`, TCP `Disconnect`, mpsc `WorkerStats`) and arrive at the server in any order. Without the typestate gate, the abandonment transition could run while a stale `release_block` is still in the pipe — the late release would push counters past `total_block_count` and flip `is_done()` back to false, hanging the run loop forever.

Four such races, all closed by the gate:

1. **Late `ReleaseBlock` after direct abandonment** — `release_block` early-returns when `as_running_mut()` is `None`.
2. **Downstream ready-block generation after transitive abandonment** — `queue_ready_block` early-returns for non-running destination tasks. Without this, a healthy upstream's release could repopulate `ready_count` on an already-abandoned downstream.
3. **Retry-path requeue after abandonment** — the retry path goes through `release_block` and hits the same gate.
4. **Lost-block release after abandonment** — bookkeeper-driven lost releases also go through `release_block`.

All four are now structural — *unrepresentable* rather than *runtime-checked*.

## Tests

- `tests/test_worker_restarts.py` covers the cycle end-to-end:
  - `test_restart_cap_terminates_task_and_marks_remaining_failed` — direct abandonment with full restart usage
  - `test_task_abandonment_does_not_block_other_tasks` — abandonment of one task doesn't stall an independent peer
  - `test_abandoned_upstream_unblocks_downstream` — transitive orphan propagation
  - `test_abandonment_handles_in_flight_block_release_race` — Race 1 stress test (5 iterations)
  - `test_block_function_failure_kills_worker_and_drives_abandonment` — 1-arg mode hooks into the same cycle
  - `test_healthy_upstream_does_not_repopulate_abandoned_downstream` — Race 2 regression

- `tests/test_keyboard_interrupt.py` — SIGINT during the run aborts cleanly via the same shutdown path.
