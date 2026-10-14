# check function array

**Claim.** A check_function returning an array aborts the whole 1.x run; 2.0 warns once and runs the block.

**Table.** v1 silent v2 loud

**Run it** (works with daisy 1.x or 2.0 installed):

```
python experiments/v1_silent_v2_loud/check_function_array/run.py
```

## What the script does

check_function returning a 3-element numpy array (ambiguous truth value).
1.x evaluates it outside its try block and the whole run aborts with
ValueError; 2.0 warns once, treats the block as not done and runs it.

## Output on daisy 1.x (1.2.3)

Status: exit 0, 0.3 s.

```text
===============================
check_function returns an array
===============================
daisy 1.2.3 (1.x), python 3.12.0


Execution Summary
-----------------

  Task t:

    num blocks : 20
    completed ✔: 0 (skipped 0)
    failed    ✗: 0
    orphaned  ∅: 0


RAISED ValueError: The truth value of an array with more than one element is ambiguous. Use a.any() or a.all()  (0.1 s)
EVIDENCE blocks attempted 0/20, succeeded 0/20, total attempts 0
```

stderr: 0 lines.

## Output on daisy 2.0 (2.0.0)

Status: exit 0, 0.2 s.

```text
===============================
check_function returns an array
===============================
daisy 2.0.0 (2.0), python 3.12.0


Execution Summary
-----------------

    task     blocks   completed  skipped  failed  orphaned
    ──────  ───────  ──────────  ───────  ──────  ────────
    t ✔          20          20        0       0         0

RETURNED True  (0.1 s)
EVIDENCE blocks attempted 20/20, succeeded 20/20, total attempts 20
```

stderr: 4 lines, 4 other lines. Last lines:

```text
.../site-packages/daisy/v1_compat.py:263: UserWarning: Task(check_function=...) runs your callable on the server for EVERY block, and its result persists nowhere — every rerun pays the full check cost again. For resuming interrupted or repeated runs, prefer the built-in done markers (Task(tracking_path=...) or daisy.set_tracking_basedir(...)): one mmap'd byte per block, written on completion, checked in ~microseconds. Keep check_function only when the ground truth genuinely lives in your output data (e.g. verifying non-empty zarr chunks written by an earlier pipeline).
  instance = super().__new__(cls, *args, max_workers=max_workers, **kwargs)
check_function for task "t" raised; treating block t/0 as not done, so it will be (re)processed. Further check failures for this task will not be logged:
ValueError: The truth value of an array with more than one element is ambiguous. Use a.any() or a.all()
```
