# check function raises

**Claim.** A check_function that raises counts as 'not done' on both lines; 1.x logs it per block, 2.0 once per task.

**Table.** same behavior

**Run it** (works with daisy 1.x or 2.0 installed):

```
python experiments/same_behavior/check_function_raises/run.py
```

## What the script does

check_function that raises: both lines treat the block as not done and
run it. 1.x logs the exception for every block, 2.0 once per task.

## Output on daisy 1.x (1.2.3)

Status: exit 0, 1.1 s.

```text
=====================
check_function raises
=====================
daisy 1.2.3 (1.x), python 3.12.0


Execution Summary
-----------------

  Task t:

    num blocks : 20
    completed ✔: 20 (skipped 0)
    failed    ✗: 0
    orphaned  ∅: 0

    all blocks processed successfully

RETURNED True  (0.9 s)
EVIDENCE blocks attempted 20/20, succeeded 20/20, total attempts 20
```

stderr: 190 lines, 49 progress-bar refreshes, 141 other lines. Last lines:

```text
ValueError: check boom
Traceback (most recent call last):
  File ".../site-packages/daisy/scheduler.py", line 195, in __precheck
    return self.task_map[block.task_id].check_function(block)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "experiments/same_behavior/check_function_raises/run.py", line 126, in broken_check
    raise ValueError("check boom")
ValueError: check boom
```

## Output on daisy 2.0 (2.0.0)

Status: exit 0, 0.2 s.

```text
=====================
check_function raises
=====================
daisy 2.0.0 (2.0), python 3.12.0


Execution Summary
-----------------

    task     blocks   completed  skipped  failed  orphaned
    ──────  ───────  ──────────  ───────  ──────  ────────
    t ✔          20          20        0       0         0

RETURNED True  (0.1 s)
EVIDENCE blocks attempted 20/20, succeeded 20/20, total attempts 20
```

stderr: 9 lines, 3 progress-bar refreshes, 6 other lines. Last lines:

```text
.../site-packages/daisy/v1_compat.py:106: UserWarning: Task(check_function=...) runs your callable on the server for EVERY block, and its result persists nowhere — every rerun pays the full check cost again. For resuming interrupted or repeated runs, prefer the built-in done markers (Task(tracking_path=...) or daisy.set_tracking_basedir(...)): one mmap'd byte per block, written on completion, checked in ~microseconds. Keep check_function only when the ground truth genuinely lives in your output data (e.g. verifying non-empty zarr chunks written by an earlier pipeline).
  instance = super().__new__(cls, *args, max_workers=max_workers, **kwargs)
Traceback (most recent call last):
  File "experiments/same_behavior/check_function_raises/run.py", line 126, in broken_check
    raise ValueError("check boom")
ValueError: check boom
```
