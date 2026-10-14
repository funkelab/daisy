# unpicklable exception

**Claim.** An exception that cannot be pickled leaves 1.x's failure list empty (and returns True); 2.0 carries the traceback as text.

**Table.** v1 silent v2 loud

**Run it** (works with daisy 1.x or 2.0 installed):

```
python experiments/v1_silent_v2_loud/unpicklable_exception/run.py
```

## What the script does

The block raises an exception object that cannot be pickled (it holds a
lock). 1.x cannot send it to the server, so the failure list is empty and
the run returns True; 2.0 carries the traceback as text and returns False.

## Output on daisy 1.x (1.2.3)

Status: exit 0, 0.6 s.

```text
================================
exception that cannot be pickled
================================
daisy 1.2.3 (1.x), python 3.12.0


Execution Summary
-----------------

  Task t:

    num blocks : 4
    completed ✔: 0 (skipped 0)
    failed    ✗: 4
    orphaned  ∅: 0


RETURNED True  (0.4 s)
EVIDENCE blocks attempted 4/4, succeeded 0/4, total attempts 8
--- daisy_logs/t/worker_0.err (160 lines, last 4) ---
  File ".../site-packages/daisy/tcp/tcp_stream.py", line 68, in _send_message
    pickled_data = pickle.dumps(message)
                   ^^^^^^^^^^^^^^^^^^^^^
TypeError: cannot pickle '_thread.lock' object
```

stderr: 21 lines, 21 progress-bar refreshes.

## Output on daisy 2.0 (2.0.0)

Status: exit 0, 0.7 s.

```text
================================
exception that cannot be pickled
================================
daisy 2.0.0 (2.0), python 3.12.0


Execution Summary
-----------------

    task     blocks   completed  skipped  failed  orphaned
    ──────  ───────  ──────────  ───────  ──────  ────────
    t ✗           4           0        0       4         0

    First failure in task 't':
      Traceback (most recent call last):
        File ".../site-packages/daisy/_task.py", line 423, in acquire_block
          yield block
        File ".../site-packages/daisy/v1_compat.py", line 155, in acquire_block
          yield compat
        File ".../site-packages/daisy/_subprocess_worker.py", line 54, in _run_block_loop
          process_function(block)
        File ".../site-packages/daisy/v1_compat.py", line 186, in _compat_block_fn
          return fn(compat)
                 ^^^^^^^^^^
        File "experiments/v1_silent_v2_loud/unpicklable_exception/run.py", line 25, in body
          raise LockedError(f"boom {block.block_id[1]}")
      LockedError: boom 0
      (+ 3 more failed blocks — see worker logs)

    See worker logs for details under daisy_logs/t/

RETURNED False  (0.6 s)
EVIDENCE blocks attempted 4/4, succeeded 0/4, total attempts 8
--- daisy_logs/t/worker_0.err (136 lines, last 4) ---
  File "experiments/v1_silent_v2_loud/unpicklable_exception/run.py", line 25, in body
    raise LockedError(f"boom {block.block_id[1]}")
LockedError: boom 3
worker function t failed: daisy worker subprocess exited with code 1
```

stderr: 303 lines, 303 other lines. Last lines:

```text
  File ".../site-packages/daisy/_subprocess_worker.py", line 54, in _run_block_loop
    process_function(block)
  File ".../site-packages/daisy/v1_compat.py", line 186, in _compat_block_fn
    return fn(compat)
           ^^^^^^^^^^
  File "experiments/v1_silent_v2_loud/unpicklable_exception/run.py", line 25, in body
    raise LockedError(f"boom {block.block_id[1]}")
LockedError: boom 3
```
