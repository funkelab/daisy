# fire and forget launcher

**Claim.** A launcher that returns at once, whose worker then dies, hangs 1.x; 2.0 fails the run with an explanation.

**Table.** v1 hangs v2 finishes

**Run it** (works with daisy 1.x or 2.0 installed):

```
python experiments/v1_hangs_v2_finishes/fire_and_forget_launcher/run.py
```

## What the script does

The launcher starts an external worker with Popen and returns at once
(like sbatch without --wait); the worker processes one block and exits.
1.x never respawns a launcher that returned cleanly, so when the worker
dies the run waits forever. 2.0 detects the worker outliving its spawn
call and fails the run with an explanation.

## Output on daisy 1.x (1.2.3)

Status: killed after 45 s (hang), 45.1 s.

```text
======================================================
fire-and-forget launcher, worker exits after one block
======================================================
daisy 1.2.3 (1.x), python 3.12.0

[note] Task(max_worker_restarts=...) is a 2.0 option; ignored on 1.2.3
```

stderr: 6 lines, 6 progress-bar refreshes.

## Output on daisy 2.0 (2.0.0)

Status: exit 0, 0.2 s.

```text
======================================================
fire-and-forget launcher, worker exits after one block
======================================================
daisy 2.0.0 (2.0), python 3.12.0


Execution Summary
-----------------

    task     blocks   completed  skipped  failed  orphaned
    ──────  ───────  ──────────  ───────  ──────  ────────
    t ∅          20           0        0       0        20

RAISED RuntimeError: task 't' was abandoned (worker restart cap exhausted): workers failed 0 times, 0 restarts performed; 20 of 20 blocks were orphaned. No worker error was captured  (0.1 s)
EVIDENCE blocks attempted 0/20, succeeded 0/20, total attempts 0
```

stderr: 5 lines, 3 progress-bar refreshes, 2 other lines. Last lines:

```text
 WARN spawn function returned without its worker ever connecting, while the task still has work. 0-arg spawn functions must block for the worker's lifetime (e.g. `sbatch --wait`, `srun`) — a fire-and-forget submit makes daisy respawn workers it believes dead, burning the worker start budget, and can abandon the task while real workers still run worker_id=1 task_id=t
 WARN task abandoned: worker restart cap reached, accounting remaining blocks as orphaned task_id=t failures=0 restarts=0 max_restarts=0 orphaned=20
```
