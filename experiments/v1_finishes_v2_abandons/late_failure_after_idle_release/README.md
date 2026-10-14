# late failure after idle release

**Claim.** A failure after the other worker was released as idle: 1.x retries; 2.0 with max_worker_restarts=0 abandons with one orphan.

**Table.** v1 finishes v2 abandons

**Run it** (works with daisy 1.x or 2.0 installed):

```
python experiments/v1_finishes_v2_abandons/late_failure_after_idle_release/run.py
```

## What the script does

Two blocks, two workers, max_worker_restarts=0. Block 0 finishes at once
and its worker is released as idle; block 1 sleeps 2 s and fails on its
first attempt only. 1.x retries it with the worker that failed. On 2.0 the
failing worker died, the idle one is gone, and the retry would need a new
start that the budget (2 + 0) no longer allows: the task is abandoned
with one orphan although max_retries=2 promised a retry.

## Output on daisy 1.x (1.2.3)

Status: exit 0, 4.3 s.

```text
===============================================
late failure after the idle worker was released
===============================================
daisy 1.2.3 (1.x), python 3.12.0

[note] Task(max_worker_restarts=...) is a 2.0 option; ignored on 1.2.3

Execution Summary
-----------------

  Task t:

    num blocks : 2
    completed ✔: 2 (skipped 0)
    failed    ✗: 0
    orphaned  ∅: 0

    Failed Blocks:

      block 1 in worker 1 with exception RuntimeError('fails once')

    See worker logs for details:

      daisy_logs/t/worker_1.err / .out
    all blocks processed successfully

RETURNED True  (4.2 s)
EVIDENCE blocks attempted 2/2, succeeded 2/2, total attempts 3
```

stderr: 11 lines, 11 progress-bar refreshes.

## Output on daisy 2.0 (2.0.0)

Status: exit 0, 2.2 s.

```text
===============================================
late failure after the idle worker was released
===============================================
daisy 2.0.0 (2.0), python 3.12.0


Execution Summary
-----------------

    task     blocks   completed  skipped  failed  orphaned
    ──────  ───────  ──────────  ───────  ──────  ────────
    t ∅           2           1        0       0         1

RAISED RuntimeError: task 't' was abandoned (worker restart cap exhausted): workers failed 1 times, 0 restarts performed; 1 of 2 blocks were orphaned. Last worker error:  (2.1 s)
EVIDENCE blocks attempted 2/2, succeeded 1/2, total attempts 2
```

stderr: 31 lines, 31 other lines. Last lines:

```text
    _run_block_loop(function)
  File ".../site-packages/daisy/_subprocess_worker.py", line 54, in _run_block_loop
    process_function(block)
  File "experiments/v1_finishes_v2_abandons/late_failure_after_idle_release/run.py", line 133, in body
    raise RuntimeError("fails once")
RuntimeError: fails once
 WARN worker exited with error worker_id=0 task_id=t
 WARN task abandoned: worker restart cap reached, accounting remaining blocks as orphaned task_id=t failures=1 restarts=0 max_restarts=0 orphaned=1
```
