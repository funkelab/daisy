# log basedir none

**Claim.** set_log_basedir(None) reaches external workers as the string 'None' on 1.x and as an empty value on 2.0.

**Table.** different by design

**Run it** (works with daisy 1.x or 2.0 installed):

```
python experiments/different_by_design/log_basedir_none/run.py
```

## What the script does

set_log_basedir(None) with an external worker (mode C) that prints the
DAISY_CONTEXT it received. 1.x ships the string "None" as the log dir;
2.0 ships an empty value meaning off.

## Output on daisy 1.x (1.2.3)

Status: exit 0, 0.6 s.

```text
=====================
set_log_basedir(None)
=====================
daisy 1.2.3 (1.x), python 3.12.0


Execution Summary
-----------------

  Task t:

    num blocks : 4
    completed ✔: 4 (skipped 0)
    failed    ✗: 0
    orphaned  ∅: 0

    all blocks processed successfully

RETURNED True  (0.4 s)
EVIDENCE blocks attempted 4/4, succeeded 4/4, total attempts 4
DAISY_CONTEXT seen by the worker: logdir=None:hostname=10.11.12.171:port=36297:task_id=t:worker_id=0
```

stderr: 12 lines, 12 progress-bar refreshes.

## Output on daisy 2.0 (2.0.0)

Status: exit 0, 0.2 s.

```text
=====================
set_log_basedir(None)
=====================
daisy 2.0.0 (2.0), python 3.12.0


Execution Summary
-----------------

    task     blocks   completed  skipped  failed  orphaned
    ──────  ───────  ──────────  ───────  ──────  ────────
    t ✔           4           4        0       0         0

RETURNED True  (0.1 s)
EVIDENCE blocks attempted 4/4, succeeded 4/4, total attempts 4
DAISY_CONTEXT seen by the worker: logdir=:hostname=10.11.12.171:task_id=t:resource_tracking=0:port=33861:worker_id=0
```

stderr: 0 lines.
