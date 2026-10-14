# serial worker function

**Claim.** Serial mode refuses a 0-argument worker function on both lines.

**Table.** same behavior

**Run it** (works with daisy 1.x or 2.0 installed):

```
python experiments/same_behavior/serial_worker_function/run.py
```

## What the script does

Serial mode (multiprocessing=False) with a 0-argument worker function:
there is no server for the worker loop to connect to, so both lines refuse
(1.x with a TypeError at the first block, 2.0 with a configuration error
before anything runs). Earlier 2.0 builds marked every block done without
running anything.

## Output on daisy 1.x (1.2.3)

Status: exit 0, 0.2 s.

```text
==================================
serial mode with a worker function
==================================
daisy 1.2.3 (1.x), python 3.12.0


RAISED TypeError: worker_loop() takes 0 positional arguments but 1 was given  (0.0 s)
EVIDENCE blocks attempted 0/4, succeeded 0/4, total attempts 0
```

stderr: 3 lines, 3 progress-bar refreshes.

## Output on daisy 2.0 (2.0.0)

Status: exit 0, 0.1 s.

```text
==================================
serial mode with a worker function
==================================
daisy 2.0.0 (2.0), python 3.12.0


RAISED RuntimeError: invalid configuration: task "t" has no 1-argument process_function; serial mode (multiprocessing=False) runs block functions in-process and cannot run a worker   (0.0 s)
EVIDENCE blocks attempted 0/4, succeeded 0/4, total attempts 0
```

stderr: 0 lines.
