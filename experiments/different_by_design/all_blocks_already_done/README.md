# all blocks already done

**Claim.** A run that skips every block prints an empty summary on 1.x and a table with a skipped column on 2.0.

**Table.** different by design

**Run it** (works with daisy 1.x or 2.0 installed):

```
python experiments/different_by_design/all_blocks_already_done/run.py
```

## What the script does

Run a task twice in serial mode with a check_function that reads marker
files the block body writes. The second run skips every block: 1.x prints
an empty execution summary (#62); 2.0 prints the table with a skipped
column.

## Output on daisy 1.x (1.2.3)

Status: exit 0, 0.2 s.

```text
========================================
second run with every block already done
========================================
daisy 1.2.3 (1.x), python 3.12.0


### run 1

Execution Summary
-----------------

  Task t:

    num blocks : 8
    completed ✔: 8 (skipped 0)
    failed    ✗: 0
    orphaned  ∅: 0

    all blocks processed successfully

RETURNED defaultdict with 1 task state(s)  (0.0 s)

### run 2

Execution Summary
-----------------

RETURNED defaultdict with 1 task state(s)  (0.0 s)
EVIDENCE blocks attempted 8/8, succeeded 8/8, total attempts 8
```

stderr: 19 lines, 19 progress-bar refreshes.

## Output on daisy 2.0 (2.0.0)

Status: exit 0, 0.1 s.

```text
========================================
second run with every block already done
========================================
daisy 2.0.0 (2.0), python 3.12.0


### run 1

Execution Summary
-----------------

    task     blocks   completed  skipped  failed  orphaned
    ──────  ───────  ──────────  ───────  ──────  ────────
    t ✔           8           8        0       0         0

RETURNED True  (0.0 s)

### run 2

Execution Summary
-----------------

    task     blocks   completed  skipped  failed  orphaned
    ──────  ───────  ──────────  ───────  ──────  ────────
    t ✔           8           8        8       0         0

RETURNED True  (0.0 s)
EVIDENCE blocks attempted 8/8, succeeded 8/8, total attempts 8
```

stderr: 2 lines, 2 other lines. Last lines:

```text
.../site-packages/daisy/v1_compat.py:263: UserWarning: Task(check_function=...) runs your callable on the server for EVERY block, and its result persists nowhere — every rerun pays the full check cost again. For resuming interrupted or repeated runs, prefer the built-in done markers (Task(tracking_path=...) or daisy.set_tracking_basedir(...)): one mmap'd byte per block, written on completion, checked in ~microseconds. Keep check_function only when the ground truth genuinely lives in your output data (e.g. verifying non-empty zarr chunks written by an earlier pipeline).
  instance = super().__new__(cls, *args, max_workers=max_workers, **kwargs)
```
