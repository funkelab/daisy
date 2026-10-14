# ctrl c

**Claim.** Ctrl-C: 1.x returns False with a summary that hides the in-flight blocks; 2.0 prints what finished and raises KeyboardInterrupt.

**Table.** v1 silent v2 loud

**Run it** (works with daisy 1.x or 2.0 installed):

```
python experiments/v1_silent_v2_loud/ctrl_c/run.py
```

## What the script does

SIGINT (Ctrl-C) 2 s into a run of 2,000 50-ms blocks on 8 workers. 1.x
swallows it and returns False with a summary that shows the blocks in
flight neither failed nor pending; 2.0 prints what finished and raises
KeyboardInterrupt.

## Output on daisy 1.x (1.2.3)

Status: exit 0, 2.2 s.

```text
==============
Ctrl-C mid-run
==============
daisy 1.2.3 (1.x), python 3.12.0


Execution Summary
-----------------

  Task t:

    num blocks : 2000
    completed ✔: 161 (skipped 0)
    failed    ✗: 0
    orphaned  ∅: 0


RETURNED False  (2.0 s)
EVIDENCE blocks attempted 168/2000, succeeded 168/2000, total attempts 168
```

stderr: 344 lines, 344 progress-bar refreshes.

## Output on daisy 2.0 (2.0.0)

Status: exit 0, 2.2 s.

```text
==============
Ctrl-C mid-run
==============
daisy 2.0.0 (2.0), python 3.12.0


Run interrupted; counts at shutdown:

Execution Summary
-----------------

    task     blocks   completed  skipped  failed  orphaned
    ──────  ───────  ──────────  ───────  ──────  ────────
    t …        2000         303        0       0         0

RAISED KeyboardInterrupt: run aborted by SIGINT  (2.1 s)
EVIDENCE blocks attempted 311/2000, succeeded 311/2000, total attempts 311
```

stderr: 49 lines, 49 other lines. Last lines:

```text
    with client.acquire_block() as block:
  File ".../lib/python3/contextlib.py", line 137, in __enter__
    return next(self.gen)
           ^^^^^^^^^^^^^^
  File ".../site-packages/daisy/_task.py", line 511, in acquire_block
    native = self._client.acquire_block()
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
RuntimeError: Broken pipe (os error 32)
```
