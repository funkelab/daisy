# hung block timeout

**Claim.** A block that never returns hangs 1.x forever; 2.0 kills it at Task(timeout=) and reports it failed.

**Table.** v1 hangs v2 finishes

**Run it** (works with daisy 1.x or 2.0 installed):

```
python experiments/v1_hangs_v2_finishes/hung_block_timeout/run.py
```

## What the script does

Block 3 of 8 never returns. Task(timeout=5) is stored but never enforced
on 1.x, so the run waits forever; 2.0 kills the worker at 5 s, retries the
block (which hangs again) and reports it failed after max_retries.

## Output on daisy 1.x (1.2.3)

Status: killed after 45 s (hang), 45.0 s.

```text
==========================
one block hangs, timeout=5
==========================
daisy 1.2.3 (1.x), python 3.12.0
```

stderr: 18 lines, 18 progress-bar refreshes.

## Output on daisy 2.0 (2.0.0)

Status: exit 0, 16.6 s.

```text
==========================
one block hangs, timeout=5
==========================
daisy 2.0.0 (2.0), python 3.12.0


Execution Summary
-----------------

    task     blocks   completed  skipped  failed  orphaned
    ──────  ───────  ──────────  ───────  ──────  ────────
    t ✗           8           7        0       1         0

    First failure in task 't':
      block processing failed: Traceback (most recent call last):
        File ".../site-packages/daisy/_task.py", line 607, in wrapped
          return orig(context=context)
                 ^^^^^^^^^^^^^^^^^^^^^
        File ".../site-packages/daisy/_worker_processes.py", line 315, in _spawn_worker_process
          raise exc
      RuntimeError: daisy worker subprocess killed after a block exceeded timeout=5.0s (true preemption; the server retries the block under max_retries)
      worker stderr tail:
      daisy worker 0: block t/3 still running after timeout=5s; killing worker process

    3 block attempt(s) in task 't' exceeded the block timeout (5s; pass Task(timeout=...) to raise it for slow blocks)

    See worker logs for details under daisy_logs/t/

RETURNED False  (16.5 s)
EVIDENCE blocks attempted 8/8, succeeded 8/8, total attempts 10
```

stderr: 36 lines, 4 progress-bar refreshes, 32 other lines. Last lines:

```text
           ^^^^^^^^^^^^^^^^^^^^^
  File ".../site-packages/daisy/_worker_processes.py", line 315, in _spawn_worker_process
    raise exc
RuntimeError: daisy worker subprocess killed after a block exceeded timeout=5.0s (true preemption; the server retries the block under max_retries)
worker stderr tail:
daisy worker 3: block t/3 still running after timeout=5s; killing worker process
 WARN worker exited with error worker_id=3 task_id=t
 WARN block lost block_id=t/3 timed_out=true
```
