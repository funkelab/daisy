# resume check function vs tracking

**Claim.** Resuming 20,000 blocks: a check_function costs one call per block on the scheduling thread; 2.0's built-in store is one byte per block.

**Table.** different by design

**Run it** (works with daisy 1.x or 2.0 installed):

```
python experiments/different_by_design/resume_check_function_vs_tracking/run.py
```

## What the script does

Resume cost for 20,000 blocks. Run 1 processes everything and leaves a
marker file per block; run 2 skips everything through a check_function
that stats the marker. On 2.0 a third and fourth run use the built-in
tracking store (one byte per block, no check_function) instead.

## Output on daisy 1.x (1.2.3)

Status: exit 0, 215.7 s.

```text
===========================================================
resume: check_function on marker files vs built-in tracking
===========================================================
daisy 1.2.3 (1.x), python 3.12.0


### marker files + check_function, first run

Execution Summary
-----------------

  Task naive:

    num blocks : 20000
    completed ✔: 20000 (skipped 0)
    failed    ✗: 0
    orphaned  ∅: 0

    all blocks processed successfully

RETURNED True  (214.5 s)
TIMING markers first run: 214.49 s

### marker files + check_function, resume (everything skipped)

Execution Summary
-----------------

  Task naive:

    num blocks : 20000
    completed ✔: 20000 (skipped 20000)
    failed    ✗: 0
    orphaned  ∅: 0

    all blocks processed successfully

RETURNED True  (1.0 s)
TIMING markers resume: 1.05 s
```

stderr: 41720 lines, 41720 progress-bar refreshes.

## Output on daisy 2.0 (2.0.0)

Status: exit 0, 2.9 s.

```text
===========================================================
resume: check_function on marker files vs built-in tracking
===========================================================
daisy 2.0.0 (2.0), python 3.12.0


### marker files + check_function, first run

Execution Summary
-----------------

    task      blocks   completed  skipped  failed  orphaned
    ───────  ───────  ──────────  ───────  ──────  ────────
    naive ✔    20000       20000        0       0         0

RETURNED True  (1.5 s)
TIMING markers first run: 1.47 s

### marker files + check_function, resume (everything skipped)

Execution Summary
-----------------

    task      blocks   completed  skipped  failed  orphaned
    ───────  ───────  ──────────  ───────  ──────  ────────
    naive ✔    20000       20000    20000       0         0

RETURNED True  (0.2 s)
TIMING markers resume: 0.23 s

### built-in tracking store, first run

Execution Summary
-----------------

    task        blocks   completed  skipped  failed  orphaned
    ─────────  ───────  ──────────  ───────  ──────  ────────
    builtin ✔    20000       20000        0       0         0

RETURNED True  (0.9 s)
TIMING tracking first run: 0.92 s

### built-in tracking store, resume (everything skipped)

Execution Summary
-----------------

    task        blocks   completed  skipped  failed  orphaned
    ─────────  ───────  ──────────  ───────  ──────  ────────
    builtin ✔    20000       20000    20000       0         0

RETURNED True  (0.2 s)
TIMING tracking resume: 0.15 s
```

stderr: 2 lines, 2 other lines. Last lines:

```text
.../site-packages/daisy/v1_compat.py:263: UserWarning: Task(check_function=...) runs your callable on the server for EVERY block, and its result persists nowhere — every rerun pays the full check cost again. For resuming interrupted or repeated runs, prefer the built-in done markers (Task(tracking_path=...) or daisy.set_tracking_basedir(...)): one mmap'd byte per block, written on completion, checked in ~microseconds. Keep check_function only when the ground truth genuinely lives in your output data (e.g. verifying non-empty zarr chunks written by an earlier pipeline).
  instance = super().__new__(cls, *args, max_workers=max_workers, **kwargs)
```
