# log basedir none

**Claim.** set_log_basedir(None) reaches external workers as the string 'None' on 1.x, so a worker that redirects its output writes to ./None/; 2.0 ships an empty value meaning off.

**Table.** different by design

**Run it** (works with daisy 1.x or 2.0 installed):

```
python experiments/different_by_design/log_basedir_none/run.py
```

## What the script does

set_log_basedir(None) with an external worker (mode C) that prints the
DAISY_CONTEXT it received and then redirects its own output with daisy's
worker-log helper, the 1.x idiom for cluster jobs.

1.x serialises the Python None with "%s=%s", so the worker receives the
string "None", Client() turns it into Path("None"), and the redirect
writes the worker's output to ./None/<task>/worker_<n>.out instead of the
terminal the user asked for. 2.0 ships an empty value, Client() maps it
back to None, and the helper returns None, so nothing is written.

## Output on daisy 1.x (1.2.3)

Status: exit 0, 0.5 s.

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
DAISY_CONTEXT seen by the worker: logdir=None:hostname=10.11.12.171:port=41255:task_id=t:worker_id=0
get_log_basedir() inside the worker: PosixPath('None')
directory named 'None' created in the cwd: None/t/worker_0.err, None/t/worker_0.out
  (worker output that should have gone to the terminal:)
    processing ('t', 0) (meant for the terminal)
    processing ('t', 1) (meant for the terminal)
    processing ('t', 2) (meant for the terminal)
    processing ('t', 3) (meant for the terminal)
```

stderr: 12 lines, 12 progress-bar refreshes.

## Output on daisy 2.0 (2.0.0)

Status: exit 0, 0.2 s.

```text
=====================
set_log_basedir(None)
=====================
daisy 2.0.0 (2.0), python 3.12.0

processing ('t', 0) (meant for the terminal)
processing ('t', 1) (meant for the terminal)
processing ('t', 2) (meant for the terminal)
processing ('t', 3) (meant for the terminal)

Execution Summary
-----------------

    task     blocks   completed  skipped  failed  orphaned
    ──────  ───────  ──────────  ───────  ──────  ────────
    t ✔           4           4        0       0         0

RETURNED True  (0.2 s)
EVIDENCE blocks attempted 4/4, succeeded 4/4, total attempts 4
DAISY_CONTEXT seen by the worker: task_id=t:resource_tracking=0:port=44545:worker_id=0:hostname=10.11.12.171:logdir=
get_log_basedir() inside the worker: None
directory named 'None' created in the cwd: no
```

stderr: 0 lines.
