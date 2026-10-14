# Roadmap

Work that is agreed on but not done, in rough priority order. Items come
from the v2 design review and from the 1.x versus 2.0 evaluation; each
says what it fixes and roughly how big it is (S: one sitting, M: a focused
PR, L: multi-day with a design discussion first).

## Scheduling and workers

- **Block batching** [M]. A worker asks for `N` blocks in one round trip
  (`AcquireBlock { batch_size }` → `SendBlocks { blocks }`) and releases them
  as a batch. This is the primitive for a read → GPU → write pipeline inside
  one worker; today each `acquire_block()` is a synchronous round trip, so a
  worker can hold several blocks but never overlaps their latency.
- **Requeue failed blocks at the end of the queue** [S]. A block that fails
  goes straight back to the front of the ready queue, so a block that fails
  deterministically burns its retries, and possibly the worker restart
  budget, before the rest of the task has run. Appending it instead gives
  every other block a turn first and keeps a single bad block from
  abandoning a long task early.
- **Connection-aware worker accounting** [M]. daisy counts a worker as
  alive while its spawn function is running. A submit-and-return launcher
  (`sbatch` without `--wait`, `bsub` without `-K`, a bare `Popen`) therefore
  looks like a dead worker, gets respawned, and can abandon a task whose
  jobs are still queued. Tracking liveness by connection instead would make
  fire-and-forget launchers work and remove the hard requirement that spawn
  functions block.
- **Idle-worker release policy** [S, decision needed]. A worker whose task
  has no ready blocks is released after an idle period and respawned when
  work arrives, which costs a process start each time and, in the
  evaluation, turned a late failure into a restart-budget hit. Keeping
  workers until the task drains, or making the idle timeout per task, are
  the two candidates.
- **Retry backoff** [S]. `Task(retry_backoff=...)`, default off: a failed
  block waits `backoff * 2^retries` before becoming ready again, so transient
  failures against a shared resource do not retry in a storm.

## Multi-machine runs

- **A launcher and worker entry point** [L, RFC first]. Remote workers
  already work: the context reaches them through `DAISY_CONTEXT`, the
  server listens on all interfaces, and `daisy.Client()` connects back. What
  is missing is a packaged way to start them: a `python -m daisy.worker`
  entry point, a SLURM/LSF helper, and the deploy story for the user's
  process function and its imports. Workers also need a watchdog of their
  own, since the driver's Ctrl-C handler cannot reach a worker whose node
  hangs.

## Resume and inspection

- **Resume UX** [M]. The tracking store already makes re-runs skip done
  blocks and the pre-run log says how many remain. Still open: a helper that
  reports what is done and what failed before a re-run, and a dry-run mode
  that prints the planned block counts, ROIs and resource allocation
  without running anything.
- **Hand-launched workers without a spawn function** [M]. A run where the
  user starts every worker themselves and daisy only serves blocks, as 1.x
  allowed with `num_workers=0`. 2.0 refuses `max_workers=0` today.

## Deferred

- Worker init and cleanup hooks for block functions: the 0-arg worker
  function already gives a per-worker lifecycle, so this waits for a
  concrete need.
- A block prioritisation callback: FIFO until someone needs otherwise.
