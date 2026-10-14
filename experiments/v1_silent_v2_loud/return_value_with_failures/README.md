# return value with failures

**Claim.** With every block failed, 1.x returns True; 2.0 returns False.

**Table.** v1 silent v2 loud

**Run it** (works with daisy 1.x or 2.0 installed):

```
python experiments/v1_silent_v2_loud/return_value_with_failures/run.py
```

## What the script does

Four blocks, every one raises, max_retries=1. 1.x prints the failures
and still returns True ("accounting finished"); 2.0 returns False.

## Output on daisy 1.x (1.2.3)

Status: exit 0, 0.6 s.

```text
==================================================
every block fails: what does run_blockwise return?
==================================================
daisy 1.2.3 (1.x), python 3.12.0


Execution Summary
-----------------

  Task t:

    num blocks : 4
    completed ✔: 0 (skipped 0)
    failed    ✗: 4
    orphaned  ∅: 0

    Failed Blocks:

      block 0 in worker 0 with exception RuntimeError('boom 0')
      block 1 in worker 0 with exception RuntimeError('boom 1')
      block 2 in worker 0 with exception RuntimeError('boom 2')
      block 3 in worker 0 with exception RuntimeError('boom 3')
      block 1 in worker 1 with exception RuntimeError('boom 1')
      block 2 in worker 1 with exception RuntimeError('boom 2')
      block 3 in worker 1 with exception RuntimeError('boom 3')

    See worker logs for details:

      daisy_logs/t/worker_0.err / .out
      daisy_logs/t/worker_0.err / .out
      daisy_logs/t/worker_0.err / .out
      daisy_logs/t/worker_0.err / .out
      daisy_logs/t/worker_1.err / .out
      daisy_logs/t/worker_1.err / .out
      daisy_logs/t/worker_1.err / .out

RETURNED True  (0.4 s)
bool(result) = True
EVIDENCE blocks attempted 4/4, succeeded 0/4, total attempts 7
```

stderr: 21 lines, 21 progress-bar refreshes.

## Output on daisy 2.0 (2.0.0)

Status: exit 0, 0.7 s.

```text
==================================================
every block fails: what does run_blockwise return?
==================================================
daisy 2.0.0 (2.0), python 3.12.0


Execution Summary
-----------------

    task     blocks   completed  skipped  failed  orphaned
    ──────  ───────  ──────────  ───────  ──────  ────────
    t ✗           4           0        0       4         0

    First failure in task 't':
      Traceback (most recent call last):
        File ".../site-packages/daisy/_task.py", line 423, in acquire_block
          yield block
        File ".../site-packages/daisy/v1_compat.py", line 155, in acquire_block
          yield compat
        File ".../site-packages/daisy/_subprocess_worker.py", line 54, in _run_block_loop
          process_function(block)
        File ".../site-packages/daisy/v1_compat.py", line 186, in _compat_block_fn
          return fn(compat)
                 ^^^^^^^^^^
        File "experiments/v1_silent_v2_loud/return_value_with_failures/run.py", line 16, in body
          raise RuntimeError(f"boom {block.block_id[1]}")
      RuntimeError: boom 0
      (+ 3 more failed blocks — see worker logs)

    See worker logs for details under daisy_logs/t/

RETURNED False  (0.6 s)
bool(result) = False
EVIDENCE blocks attempted 4/4, succeeded 0/4, total attempts 8
```

stderr: 303 lines, 303 other lines. Last lines:

```text
  File ".../site-packages/daisy/_subprocess_worker.py", line 54, in _run_block_loop
    process_function(block)
  File ".../site-packages/daisy/v1_compat.py", line 186, in _compat_block_fn
    return fn(compat)
           ^^^^^^^^^^
  File "experiments/v1_silent_v2_loud/return_value_with_failures/run.py", line 16, in body
    raise RuntimeError(f"boom {block.block_id[1]}")
RuntimeError: boom 3
```
