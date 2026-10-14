# crashing spawn function

**Claim.** A spawn function that exits at once is respawned forever on 1.x; 2.0 stops after its start budget and raises.

**Table.** v1 hangs v2 finishes

**Run it** (works with daisy 1.x or 2.0 installed):

```
python experiments/v1_hangs_v2_finishes/crashing_spawn_function/run.py
```

## What the script does

The spawn function exits with status 1 at once. 1.x respawns it on every
event-loop iteration without limit (10 per second, forever); 2.0 stops after
max_workers + max_worker_restarts starts and raises.

## Output on daisy 1.x (1.2.3)

Status: killed after 12 s (hang), 12.0 s.

```text
================================
spawn function exits immediately
================================
daisy 1.2.3 (1.x), python 3.12.0
```

stderr: 230 lines, 230 other lines. Last lines:

```text
Worker worker (logdir=daisy_logs:hostname=10.11.12.171:port=43103:task_id=t:worker_id=111) (pid 2034405) failed with exit code 1
Replacing 1 dead workers for task t
Worker worker (logdir=daisy_logs:hostname=10.11.12.171:port=43103:task_id=t:worker_id=112) (pid 2034406) failed with exit code 1
Replacing 1 dead workers for task t
Worker worker (logdir=daisy_logs:hostname=10.11.12.171:port=43103:task_id=t:worker_id=113) (pid 2034407) failed with exit code 1
Replacing 1 dead workers for task t
Worker worker (logdir=daisy_logs:hostname=10.11.12.171:port=43103:task_id=t:worker_id=114) (pid 2034408) failed with exit code 1
Replacing 1 dead workers for task t
```

## Output on daisy 2.0 (2.0.0)

Status: exit 0, 0.8 s.

```text
================================
spawn function exits immediately
================================
daisy 2.0.0 (2.0), python 3.12.0


Execution Summary
-----------------

    task     blocks   completed  skipped  failed  orphaned
    ──────  ───────  ──────────  ───────  ──────  ────────
    t ∅           4           0        0       0         4

RAISED RuntimeError: task 't' was abandoned (worker restart cap exhausted): workers failed 11 times, 10 restarts performed; 4 of 4 blocks were orphaned. Last worker error:  (0.8 s)
EVIDENCE spawn attempts: 11
```

stderr: 91 lines, 3 progress-bar refreshes, 88 other lines. Last lines:

```text
  File ".../site-packages/daisy/_task.py", line 605, in wrapped
    return orig(context=context)
           ^^^^^^^^^^^^^^^^^^^^^
  File ".../site-packages/daisy/_worker_processes.py", line 315, in _spawn_worker_process
    raise exc
RuntimeError: daisy worker subprocess exited with code 1
 WARN worker exited with error worker_id=10 task_id=t
 WARN task abandoned: worker restart cap reached, accounting remaining blocks as orphaned task_id=t failures=11 restarts=10 max_restarts=10 orphaned=4
```
