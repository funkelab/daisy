# worker sigkill

**Claim.** A SIGKILLed worker is replaced and its block retried on both lines (1.x master; 1.2.2 hangs).

**Table.** same behavior

**Run it** (works with daisy 1.x or 2.0 installed):

```
python experiments/same_behavior/worker_sigkill/run.py
```

## What the script does

A worker is SIGKILLed while holding a block (block 5 kills its own process
on its first attempt). Both lines reclaim the block, start a replacement
worker and retry it. (daisy 1.2.2 from PyPI does not replace dead workers
and hangs here; master and 2.0 do.)

## Output on daisy 1.x (1.2.3)

Status: exit 0, 0.6 s.

```text
===========================
worker SIGKILLed on block 5
===========================
daisy 1.2.3 (1.x), python 3.12.0


Execution Summary
-----------------

  Task t:

    num blocks : 20
    completed ✔: 20 (skipped 0)
    failed    ✗: 0
    orphaned  ∅: 0

    all blocks processed successfully

RETURNED True  (0.4 s)
EVIDENCE blocks attempted 20/20, succeeded 20/20, total attempts 21
```

stderr: 48 lines, 47 progress-bar refreshes, 1 other lines. Last lines:

```text
Replacing 1 dead workers for task t
```

## Output on daisy 2.0 (2.0.0)

Status: exit 0, 0.7 s.

```text
===========================
worker SIGKILLed on block 5
===========================
daisy 2.0.0 (2.0), python 3.12.0


Execution Summary
-----------------

    task     blocks   completed  skipped  failed  orphaned
    ──────  ───────  ──────────  ───────  ──────  ────────
    t ✔          20          20        0       0         0

RETURNED True  (0.6 s)
EVIDENCE blocks attempted 20/20, succeeded 20/20, total attempts 21
```

stderr: 9 lines, 9 other lines. Last lines:

```text
  File ".../site-packages/daisy/_task.py", line 607, in wrapped
    return orig(context=context)
           ^^^^^^^^^^^^^^^^^^^^^
  File ".../site-packages/daisy/_worker_processes.py", line 315, in _spawn_worker_process
    raise exc
RuntimeError: daisy worker subprocess exited with code -9
 WARN worker exited with error worker_id=0 task_id=t
 WARN block lost block_id=t/5 timed_out=false
```
