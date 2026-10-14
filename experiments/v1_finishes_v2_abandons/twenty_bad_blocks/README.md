# twenty bad blocks

**Claim.** 20 always-failing blocks of 2,000: 1.x finishes with 20 failed and returns True; 2.0 abandons the task after 14 worker deaths and raises.

**Table.** v1 finishes v2 abandons

**Run it** (works with daisy 1.x or 2.0 installed):

```
python experiments/v1_finishes_v2_abandons/twenty_bad_blocks/run.py
```

## What the script does

2,000 blocks, 20 of which raise on every attempt, 4 workers. 1.x marks
the 20 failed and finishes the other 1,980 (and still returns True). On 2.0
an exception kills the worker, so after max_workers + max_worker_restarts
(4 + 10) worker deaths the task is abandoned, the remaining blocks are
orphaned and run_blockwise raises.

## Output on daisy 1.x (1.2.3)

Status: exit 0, 22.2 s.

```text
=====================================
20 always-failing blocks out of 2,000
=====================================
daisy 1.2.3 (1.x), python 3.12.0


Execution Summary
-----------------

  Task t:

    num blocks : 2000
    completed ✔: 1980 (skipped 0)
    failed    ✗: 20
    orphaned  ∅: 0

    Failed Blocks:

      block 0 in worker 0 with exception RuntimeError('bad block 0')
      block 100 in worker 1 with exception RuntimeError('bad block 100')
      block 200 in worker 0 with exception RuntimeError('bad block 200')
      block 300 in worker 0 with exception RuntimeError('bad block 300')
      block 400 in worker 1 with exception RuntimeError('bad block 400')
      block 500 in worker 0 with exception RuntimeError('bad block 500')
      block 600 in worker 3 with exception RuntimeError('bad block 600')
      block 700 in worker 4 with exception RuntimeError('bad block 700')
      block 800 in worker 4 with exception RuntimeError('bad block 800')
      block 900 in worker 4 with exception RuntimeError('bad block 900')
      block 1000 in worker 4 with exception RuntimeError('bad block 1000')
      block 1100 in worker 5 with exception RuntimeError('bad block 1100')
... (27 lines omitted) ...
      block 300 in worker 10 with exception RuntimeError('bad block 300')
      block 500 in worker 9 with exception RuntimeError('bad block 500')
      block 600 in worker 10 with exception RuntimeError('bad block 600')
      block 700 in worker 11 with exception RuntimeError('bad block 700')
      block 800 in worker 9 with exception RuntimeError('bad block 800')
      block 900 in worker 10 with exception RuntimeError('bad block 900')
      block 1000 in worker 11 with exception RuntimeError('bad block 1000')
      block 1200 in worker 12 with exception RuntimeError('bad block 1200')
      block 1400 in worker 11 with exception RuntimeError('bad block 1400')
      block 1500 in worker 12 with exception RuntimeError('bad block 1500')
      block 1600 in worker 11 with exception RuntimeError('bad block 1600')
      block 1900 in worker 12 with exception RuntimeError('bad block 1900')
      block 1800 in worker 13 with exception RuntimeError('bad block 1800')

    See worker logs for details:

      daisy_logs/t/worker_0.err / .out
      daisy_logs/t/worker_1.err / .out
      daisy_logs/t/worker_0.err / .out
      daisy_logs/t/worker_0.err / .out
      daisy_logs/t/worker_1.err / .out
      daisy_logs/t/worker_0.err / .out
      daisy_logs/t/worker_3.err / .out
      daisy_logs/t/worker_4.err / .out
      daisy_logs/t/worker_4.err / .out
      daisy_logs/t/worker_4.err / .out
      ...

RETURNED True  (22.1 s)
EVIDENCE blocks attempted 2000/2000, succeeded 1980/2000, total attempts 2032
```

stderr: 4264 lines, 4264 progress-bar refreshes.

## Output on daisy 2.0 (2.0.0)

Status: exit 0, 0.5 s.

```text
=====================================
20 always-failing blocks out of 2,000
=====================================
daisy 2.0.0 (2.0), python 3.12.0


Execution Summary
-----------------

    task     blocks   completed  skipped  failed  orphaned
    ──────  ───────  ──────────  ───────  ──────  ────────
    t ∅        2000        1287        0       0       713

RAISED RuntimeError: task 't' was abandoned (worker restart cap exhausted): workers failed 14 times, 10 restarts performed; 713 of 2000 blocks were orphaned. Last worker error:  (0.4 s)
EVIDENCE blocks attempted 1301/2000, succeeded 1287/2000, total attempts 1301
```

stderr: 421 lines, 421 other lines. Last lines:

```text
    _run_block_loop(function)
  File ".../site-packages/daisy/_subprocess_worker.py", line 54, in _run_block_loop
    process_function(block)
  File "experiments/v1_finishes_v2_abandons/twenty_bad_blocks/run.py", line 22, in body
    raise RuntimeError(f"bad block {bid}")
RuntimeError: bad block 1300
 WARN worker exited with error worker_id=13 task_id=t
 WARN task abandoned: worker restart cap reached, accounting remaining blocks as orphaned task_id=t failures=14 restarts=10 max_restarts=10 orphaned=713
```
