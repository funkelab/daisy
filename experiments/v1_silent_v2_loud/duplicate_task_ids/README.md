# duplicate task ids

**Claim.** Two tasks with one task_id: 1.x silently drops the second; 2.0 raises.

**Table.** v1 silent v2 loud

**Run it** (works with daisy 1.x or 2.0 installed):

```
python experiments/v1_silent_v2_loud/duplicate_task_ids/run.py
```

## What the script does

Two different Task objects with the same task_id. 1.x silently drops the
second and runs the first (4 blocks); 2.0 raises.

## Output on daisy 1.x (1.2.3)

Status: exit 0, 0.1 s.

```text
==========================
two tasks with one task_id
==========================
daisy 1.2.3 (1.x), python 3.12.0


Execution Summary
-----------------

  Task dup:

    num blocks : 4
    completed ✔: 4 (skipped 0)
    failed    ✗: 0
    orphaned  ∅: 0

    all blocks processed successfully

RETURNED defaultdict with 1 task state(s)  (0.0 s)
EVIDENCE blocks attempted 4/8, succeeded 4/8, total attempts 4
```

stderr: 11 lines, 11 progress-bar refreshes.

## Output on daisy 2.0 (2.0.0)

Status: exit 0, 0.1 s.

```text
==========================
two tasks with one task_id
==========================
daisy 2.0.0 (2.0), python 3.12.0


RAISED ValueError: duplicate task_id in pipeline: dup  (0.0 s)
EVIDENCE blocks attempted 0/8, succeeded 0/8, total attempts 0
```

stderr: 0 lines.
