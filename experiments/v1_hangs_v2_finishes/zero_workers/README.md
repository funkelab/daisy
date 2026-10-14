# zero workers

**Claim.** Zero workers waits forever on 1.x; 2.0 raises when the Task is built.

**Table.** v1 hangs v2 finishes

**Run it** (works with daisy 1.x or 2.0 installed):

```
python experiments/v1_hangs_v2_finishes/zero_workers/run.py
```

## What the script does

num_workers=0 / max_workers=0: 1.x starts nobody and waits forever;
2.0 rejects it when the Task is built.

## Output on daisy 1.x (1.2.3)

Status: killed after 30 s (hang), 30.0 s.

```text
============
zero workers
============
daisy 1.2.3 (1.x), python 3.12.0
```

stderr: 0 lines.

## Output on daisy 2.0 (2.0.0)

Status: exit 0, 0.1 s.

```text
============
zero workers
============
daisy 2.0.0 (2.0), python 3.12.0

RAISED ValueError at Task construction: max_workers must be at least 1 (got 0); a task with no workers would wait forever
```

stderr: 0 lines.
