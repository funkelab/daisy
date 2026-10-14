# always failing remote worker

**Claim.** An external worker that fails every block: 1.x launches a new job every 4 failures and finishes; 2.0 launches one per failure until its budget is spent, then raises.

**Table.** v1 finishes v2 abandons

**Run it** (works with daisy 1.x or 2.0 installed):

```
python experiments/v1_finishes_v2_abandons/always_failing_remote_worker/run.py
```

## What the script does

Mode C: a blocking launcher starts an external worker that fails every
block. 1.x replaces a worker only after 4 failures, launches 5 jobs for
10 blocks x 2 attempts, and returns True with 10 failed. 2.0 kills the
worker on the first exception, so every attempt costs a job launch; after
the budget of 11 launches it abandons the task and raises.

## Output on daisy 1.x (1.2.3)

Status: exit 0, 1.0 s.

```text
======================================
external worker that fails every block
======================================
daisy 1.2.3 (1.x), python 3.12.0


Execution Summary
-----------------

  Task t:

    num blocks : 10
    completed ✔: 0 (skipped 0)
    failed    ✗: 10
    orphaned  ∅: 0

    Failed Blocks:

      block 0 in worker 0 with exception RuntimeError('always fails')
      block 1 in worker 0 with exception RuntimeError('always fails')
      block 2 in worker 0 with exception RuntimeError('always fails')
      block 3 in worker 0 with exception RuntimeError('always fails')
      block 4 in worker 0 with exception RuntimeError('always fails')
      block 5 in worker 0 with exception RuntimeError('always fails')
      block 6 in worker 0 with exception RuntimeError('always fails')
      block 7 in worker 0 with exception RuntimeError('always fails')
      block 8 in worker 0 with exception RuntimeError('always fails')
      block 9 in worker 1 with exception RuntimeError('always fails')
      block 0 in worker 0 with exception RuntimeError('always fails')
      block 1 in worker 1 with exception RuntimeError('always fails')
      block 2 in worker 0 with exception RuntimeError('always fails')
      block 3 in worker 1 with exception RuntimeError('always fails')
      block 4 in worker 0 with exception RuntimeError('always fails')
      block 5 in worker 1 with exception RuntimeError('always fails')
      block 6 in worker 0 with exception RuntimeError('always fails')
      block 7 in worker 2 with exception RuntimeError('always fails')
      block 8 in worker 1 with exception RuntimeError('always fails')
      block 9 in worker 0 with exception RuntimeError('always fails')

    See worker logs for details:

      daisy_logs/t/worker_0.err / .out
      daisy_logs/t/worker_0.err / .out
      daisy_logs/t/worker_0.err / .out
      daisy_logs/t/worker_0.err / .out
      daisy_logs/t/worker_0.err / .out
      daisy_logs/t/worker_0.err / .out
      daisy_logs/t/worker_0.err / .out
      daisy_logs/t/worker_0.err / .out
      daisy_logs/t/worker_0.err / .out
      daisy_logs/t/worker_1.err / .out
      ...

RETURNED True  (0.8 s)
EVIDENCE blocks attempted 10/10, succeeded 0/10, total attempts 20
EVIDENCE external jobs launched: 5
```

stderr: 47 lines, 47 progress-bar refreshes.

## Output on daisy 2.0 (2.0.0)

Status: exit 0, 1.6 s.

```text
======================================
external worker that fails every block
======================================
daisy 2.0.0 (2.0), python 3.12.0


Execution Summary
-----------------

    task     blocks   completed  skipped  failed  orphaned
    ──────  ───────  ──────────  ───────  ──────  ────────
    t ✗          10           0        0       1         9

    First failure in task 't':
      Traceback (most recent call last):
        File ".../site-packages/daisy/_task.py", line 526, in acquire_block
          yield block
        File "experiments/v1_finishes_v2_abandons/always_failing_remote_worker/run.py", line 38, in external_worker
          body(block)
        File "experiments/v1_finishes_v2_abandons/always_failing_remote_worker/run.py", line 21, in body
          raise RuntimeError("always fails")
      RuntimeError: always fails

    See worker logs for details under daisy_logs/t/

RAISED RuntimeError: task 't' was abandoned (worker restart cap exhausted): workers failed 11 times, 10 restarts performed; 9 of 10 blocks were orphaned. Last worker error:  (1.5 s)
EVIDENCE blocks attempted 10/10, succeeded 0/10, total attempts 11
EVIDENCE external jobs launched: 11
```

stderr: 309 lines, 309 other lines. Last lines:

```text
    external_worker()
  File "experiments/v1_finishes_v2_abandons/always_failing_remote_worker/run.py", line 38, in external_worker
    body(block)
  File "experiments/v1_finishes_v2_abandons/always_failing_remote_worker/run.py", line 21, in body
    raise RuntimeError("always fails")
RuntimeError: always fails
 WARN worker exited with error worker_id=10 task_id=t
 WARN task abandoned: worker restart cap reached, accounting remaining blocks as orphaned task_id=t failures=11 restarts=10 max_restarts=10 orphaned=9
```
