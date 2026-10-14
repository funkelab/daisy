# funlib geometry types

**Claim.** funlib.geometry Roi/Coordinate work everywhere on the default surface: daisy.Roi is funlib's class, Task/Block accept them positionally and as keywords, blocks and task.total_roi hand them back.

**Table.** same behavior

**Run it** (works with daisy 1.x or 2.0 installed):

```
python experiments/same_behavior/funlib_geometry_types/run.py
```

## What the script does

funlib.geometry Roi and Coordinate used everywhere, on the default
`import daisy` surface: daisy.Roi is funlib's class, Task and Block accept
funlib ROIs positionally and as keywords, blocks handed to the process
function carry funlib ROIs (so funlib arithmetic works inside it), and
task.total_roi comes back as a funlib Roi. 1.x re-exported funlib's
classes; 2.0's compat surface does the same and its Rust entry points
duck-type funlib values.

## Output on daisy 1.x (1.2.3)

Status: exit 0, 0.6 s.

```text
================================
funlib.geometry types everywhere
================================
daisy 1.2.3 (1.x), python 3.12.0

ok   daisy.Roi is funlib.geometry.Roi
ok   daisy.Coordinate is funlib.geometry.Coordinate
ok   Task(id, total_roi, read_roi, write_roi, fn) with funlib ROIs (positional)
ok   Task(total_roi=..., read_roi=..., write_roi=...) with funlib ROIs (keywords)
ok   task.total_roi is a funlib Roi
ok   task.total_roi.intersect(funlib Roi) works
ok   daisy.Block(funlib ROIs).read_roi is a funlib Roi

Execution Summary
-----------------

  Task pos:

    num blocks : 8
    completed ✔: 8 (skipped 0)
    failed    ✗: 0
    orphaned  ∅: 0

    all blocks processed successfully

RETURNED True  (0.4 s)
EVIDENCE blocks attempted 8/8, succeeded 8/8, total attempts 8
ok   process function received funlib ROIs and did funlib arithmetic (no assertion failures)
```

stderr: 21 lines, 21 progress-bar refreshes.

## Output on daisy 2.0 (2.0.0)

Status: exit 0, 0.2 s.

```text
================================
funlib.geometry types everywhere
================================
daisy 2.0.0 (2.0), python 3.12.0

ok   daisy.Roi is funlib.geometry.Roi
ok   daisy.Coordinate is funlib.geometry.Coordinate
ok   Task(id, total_roi, read_roi, write_roi, fn) with funlib ROIs (positional)
ok   Task(total_roi=..., read_roi=..., write_roi=...) with funlib ROIs (keywords)
ok   task.total_roi is a funlib Roi
ok   task.total_roi.intersect(funlib Roi) works
ok   daisy.Block(funlib ROIs).read_roi is a funlib Roi

Execution Summary
-----------------

    task     blocks   completed  skipped  failed  orphaned
    ──────  ───────  ──────────  ───────  ──────  ────────
    pos ✔         8           8        0       0         0

RETURNED True  (0.1 s)
EVIDENCE blocks attempted 8/8, succeeded 8/8, total attempts 8
ok   process function received funlib ROIs and did funlib arithmetic (no assertion failures)
```

stderr: 3 lines, 3 progress-bar refreshes.
