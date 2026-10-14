# progress output volume

**Claim.** With stderr redirected to a file, 1.x writes two progress refreshes per block; 2.0 refreshes on a timer.

**Table.** different by design

**Run it** (works with daisy 1.x or 2.0 installed):

```
python experiments/different_by_design/progress_output_volume/run.py
```

## What the script does

20,000 no-op blocks with stderr redirected to a file (what a batch
system's .out looks like). 1.x refreshes the progress bar twice per block;
2.0 refreshes on a timer. The script prints how many bytes and refreshes
landed in the file.

## Output on daisy 1.x (1.2.3)

Status: exit 0, 213.3 s.

```text
=========================================
progress bar output when stderr is a file
=========================================
daisy 1.2.3 (1.x), python 3.12.0


Execution Summary
-----------------

  Task t:

    num blocks : 20000
    completed ✔: 20000 (skipped 0)
    failed    ✗: 0
    orphaned  ∅: 0

    all blocks processed successfully

RETURNED True  (213.1 s)
stderr captured: 4,743,563 bytes, 0 carriage returns, 0 newlines
```

stderr: 0 lines.

## Output on daisy 2.0 (2.0.0)

Status: exit 0, 1.0 s.

```text
=========================================
progress bar output when stderr is a file
=========================================
daisy 2.0.0 (2.0), python 3.12.0


Execution Summary
-----------------

    task     blocks   completed  skipped  failed  orphaned
    ──────  ───────  ──────────  ───────  ──────  ────────
    t ✔       20000       20000        0       0         0

RETURNED True  (0.9 s)
stderr captured: 539 bytes, 0 carriage returns, 0 newlines
```

stderr: 0 lines.
