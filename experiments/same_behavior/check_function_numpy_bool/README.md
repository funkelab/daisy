# check function numpy bool

**Claim.** A check_function returning numpy.bool_(True) skips every block on both lines.

**Table.** same behavior

**Run it** (works with daisy 1.x or 2.0 installed):

```
python experiments/same_behavior/check_function_numpy_bool/run.py
```

## What the script does

check_function returning numpy.bool_(True): both lines treat it as "done"
and skip every block (nothing runs).

## Output on daisy 1.x (1.2.3)

Status: exit 0, 0.3 s.

```text
========================================
check_function returns numpy.bool_(True)
========================================
daisy 1.2.3 (1.x), python 3.12.0


Execution Summary
-----------------

  Task t:

    num blocks : 20
    completed ✔: 20 (skipped 20)
    failed    ✗: 0
    orphaned  ∅: 0

    all blocks processed successfully

RETURNED True  (0.1 s)
EVIDENCE blocks attempted 0/20, succeeded 0/20, total attempts 0
```

stderr: 0 lines.

## Output on daisy 2.0 (2.0.0)

Status: exit 0, 0.2 s.

```text
========================================
check_function returns numpy.bool_(True)
========================================
daisy 2.0.0 (2.0), python 3.12.0


Execution Summary
-----------------

    task     blocks   completed  skipped  failed  orphaned
    ──────  ───────  ──────────  ───────  ──────  ────────
    t ✔          20          20       20       0         0

RETURNED True  (0.1 s)
EVIDENCE blocks attempted 0/20, succeeded 0/20, total attempts 0
```

stderr: 5 lines, 3 progress-bar refreshes, 2 other lines. Last lines:

```text
.../site-packages/daisy/v1_compat.py:268: UserWarning: Task(check_function=...) runs your callable on the server for EVERY block, and its result persists nowhere — every rerun pays the full check cost again. For resuming interrupted or repeated runs, prefer the built-in done markers (Task(tracking_path=...) or daisy.set_tracking_basedir(...)): one mmap'd byte per block, written on completion, checked in ~microseconds. Keep check_function only when the ground truth genuinely lives in your output data (e.g. verifying non-empty zarr chunks written by an earlier pipeline).
  instance = super().__new__(cls, *args, max_workers=max_workers, **kwargs)
```
