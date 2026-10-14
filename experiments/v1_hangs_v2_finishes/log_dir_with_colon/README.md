# log dir with colon

**Claim.** A log directory containing ':' breaks every 1.x external worker in a respawn storm; 2.0 escapes it and runs.

**Table.** v1 hangs v2 finishes

**Run it** (works with daisy 1.x or 2.0 installed):

```
python experiments/v1_hangs_v2_finishes/log_dir_with_colon/run.py
```

## What the script does

A log directory whose path contains ':' (mode C, external workers). 1.x
ships it unescaped in DAISY_CONTEXT, every worker fails to parse the
context, and the launcher is respawned 10 times a second forever. 2.0
percent-encodes context values and the run completes.

## Output on daisy 1.x (1.2.3)

Status: killed after 20 s (hang), 20.0 s.

```text
==========================
log basedir containing ':'
==========================
daisy 1.2.3 (1.x), python 3.12.0
```

stderr: 194 lines, 194 other lines. Last lines:

```text
Worker worker (logdir=logs:with:colon:hostname=10.11.12.171:port=40605:task_id=t:worker_id=93) (pid 1326006) failed with exit code 1
Replacing 1 dead workers for task t
Worker worker (logdir=logs:with:colon:hostname=10.11.12.171:port=40605:task_id=t:worker_id=94) (pid 1326071) failed with exit code 1
Replacing 1 dead workers for task t
Worker worker (logdir=logs:with:colon:hostname=10.11.12.171:port=40605:task_id=t:worker_id=95) (pid 1326143) failed with exit code 1
Replacing 1 dead workers for task t
Worker worker (logdir=logs:with:colon:hostname=10.11.12.171:port=40605:task_id=t:worker_id=96) (pid 1326208) failed with exit code 1
Replacing 1 dead workers for task t
```

## Output on daisy 2.0 (2.0.0)

Status: exit 0, 0.2 s.

```text
==========================
log basedir containing ':'
==========================
daisy 2.0.0 (2.0), python 3.12.0


Execution Summary
-----------------

    task     blocks   completed  skipped  failed  orphaned
    ──────  ───────  ──────────  ───────  ──────  ────────
    t ✔           4           4        0       0         0

RETURNED True  (0.2 s)
EVIDENCE blocks attempted 4/4, succeeded 4/4, total attempts 4
```

stderr: 4 lines, 4 progress-bar refreshes.
