# default timeout

**Claim.** No timeout passed: 1.x runs an 11-minute block to completion; 2.0 kills it at the 600 s default and reports it failed.

**Table.** different by design

**Run it** (works with daisy 1.x or 2.0 installed):

```
python experiments/different_by_design/default_timeout/run.py
```

## What the script does

One block that sleeps 11 minutes, no timeout passed. 1.x completes it
(Task(timeout=) was never enforced); 2.0 enforces a 600 s default, kills
the worker at 10 minutes, retries once (max_retries=1) and reports the
block failed. Blocks are meant to be small; pass Task(timeout=...) for
genuinely long ones. This script takes 11 min on 1.x and 20 min on 2.0.

## Output on daisy 1.x (1.2.3)

Status: exit 0, 660.3 s.

```text
==================================
11-minute block, no timeout passed
==================================
daisy 1.2.3 (1.x), python 3.12.0


Execution Summary
-----------------

  Task t:

    num blocks : 1
    completed ✔: 1 (skipped 0)
    failed    ✗: 0
    orphaned  ∅: 0

    all blocks processed successfully

RETURNED True  (660.1 s)
EVIDENCE blocks attempted 1/1, succeeded 1/1, total attempts 2
```

stderr: 6 lines, 6 progress-bar refreshes.

## Output on daisy 2.0 (2.0.0)

Status: exit 0, 1201.1 s.

```text
==================================
11-minute block, no timeout passed
==================================
daisy 2.0.0 (2.0), python 3.12.0


Execution Summary
-----------------

    task     blocks   completed  skipped  failed  orphaned
    ──────  ───────  ──────────  ───────  ──────  ────────
    t ✗           1           0        0       1         0

    First failure in task 't':
      block processing failed: Traceback (most recent call last):
        File ".../site-packages/daisy/_task.py", line 607, in wrapped
          return orig(context=context)
                 ^^^^^^^^^^^^^^^^^^^^^
        File ".../site-packages/daisy/_worker_processes.py", line 315, in _spawn_worker_process
          raise exc
      RuntimeError: daisy worker subprocess killed after a block exceeded timeout=600.0s (true preemption; the server retries the block under max_retries)
      worker stderr tail:
      daisy worker 0: block t/0 still running after timeout=600s; killing worker process

    2 block attempt(s) in task 't' exceeded the block timeout (600s — the default; pass Task(timeout=...) to raise it for slow blocks)

    See worker logs for details under daisy_logs/t/

RETURNED False  (1201.0 s)
EVIDENCE blocks attempted 1/1, succeeded 0/1, total attempts 2
```

stderr: 22 lines, 22 other lines. Last lines:

```text
           ^^^^^^^^^^^^^^^^^^^^^
  File ".../site-packages/daisy/_worker_processes.py", line 315, in _spawn_worker_process
    raise exc
RuntimeError: daisy worker subprocess killed after a block exceeded timeout=600.0s (true preemption; the server retries the block under max_retries)
worker stderr tail:
daisy worker 1: block t/0 still running after timeout=600s; killing worker process
 WARN worker exited with error worker_id=1 task_id=t
 WARN block lost block_id=t/0 timed_out=true
```
