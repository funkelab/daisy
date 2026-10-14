# A representative task in every worker mode

512 blocks (8 x 8 x 8 of 16^3 voxels with 4 voxels of context, so neighbours conflict and are scheduled in levels), each sleeping 10 ms, 4 workers. The same script runs on daisy 1.x and 2.0; the only difference it hides is the `num_workers` / `max_workers` rename (see `experiments/common.py`).

```
python experiments/representative/modes.py A   # or B, C, S; --workers N
```

## The script

```python
"""A 3-D task (8 x 8 x 8 blocks of 16^3 voxels with 4 voxels of context) whose
block body sleeps 10 ms, run in each worker mode:

  A  a 1-argument block function, workers started by daisy
  B  a 0-argument worker function that runs its own daisy.Client() loop
  C  a launcher that starts an external worker process (how a cluster
     submission looks), the worker connects back over TCP
  S  serial mode, no workers

Usage: python modes.py [A|B|C|S] [--workers N]
"""

import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import common  # noqa: E402

import daisy  # noqa: E402

N = 8


def process_block(block):
    time.sleep(0.01)
    print(f"block {block.block_id[1]} write roi {block.write_roi} done by pid {os.getpid()}", flush=True)
    common.note_attempt(block)


def worker_loop():
    client = daisy.Client()
    while True:
        with client.acquire_block() as block:
            if block is None:
                return
            process_block(block)


def launch_external_worker():
    # Blocks until the worker exits, like `srun` or `sbatch --wait` would.
    subprocess.run([sys.executable, __file__, "--external-worker"], check=False)


def main():
    if "--external-worker" in sys.argv:
        worker_loop()
        return
    mode = next((a for a in sys.argv[1:] if a in "ABCS"), "A")
    workers = int(sys.argv[sys.argv.index("--workers") + 1]) if "--workers" in sys.argv else 4
    common.banner(f"3-D sleep task, mode {mode}, {workers} workers")
    total, read, write = common.grid_3d(N)
    fn = {"A": process_block, "B": worker_loop, "C": launch_external_worker, "S": process_block}[mode]
    t = common.task("sleep3d", total, read, write, fn, workers=workers, read_write_conflict=True, timeout=60)
    common.run(t, multiprocessing=(mode != "S"))
    common.report_attempts(N**3)
    logs = common.worker_logs("sleep3d")
    print(f"\nworker log files: {len(logs)}")
    for path in logs[:2]:
        common.tail(path, 5)


if __name__ == "__main__":
    common.main_guard(main)
```

## Mode A: block function, daisy starts the workers

### daisy 1.x (1.2.3)

Status: exit 0, 7.0 s.

```text
=================================
3-D sleep task, mode A, 4 workers
=================================
daisy 1.2.3 (1.x), python 3.12.0


Execution Summary
-----------------

  Task sleep3d:

    num blocks : 512
    completed ✔: 512 (skipped 0)
    failed    ✗: 0
    orphaned  ∅: 0

    all blocks processed successfully

RETURNED True  (6.8 s)
EVIDENCE blocks attempted 512/512, succeeded 512/512, total attempts 512

worker log files: 8
--- daisy_logs/sleep3d/worker_0.err (0 lines) ---
--- daisy_logs/sleep3d/worker_0.out (128 lines, last 5) ---
block 875 write roi [68:84, 100:116, 100:116] (16, 16, 16) done by pid 1776560
block 391 write roi [100:116, 4:20, 100:116] (16, 16, 16) done by pid 1776560
block 602 write roi [100:116, 36:52, 100:116] (16, 16, 16) done by pid 1776560
block 621 write roi [100:116, 68:84, 68:84] (16, 16, 16) done by pid 1776560
block 1224 write roi [100:116, 100:116, 100:116] (16, 16, 16) done by pid 1776560
```

stderr: 1090 lines, 1090 progress-bar refreshes.

### daisy 2.0 (2.0.0)

Status: exit 0, 1.6 s.

```text
=================================
3-D sleep task, mode A, 4 workers
=================================
daisy 2.0.0 (2.0), python 3.12.0


Execution Summary
-----------------

    task        blocks   completed  skipped  failed  orphaned
    ─────────  ───────  ──────────  ───────  ──────  ────────
    sleep3d ✔      512         512        0       0         0

RETURNED True  (1.5 s)
EVIDENCE blocks attempted 512/512, succeeded 512/512, total attempts 512

worker log files: 8
--- daisy_logs/sleep3d/worker_0.err (0 lines) ---
--- daisy_logs/sleep3d/worker_0.out (129 lines, last 5) ---
block 619 write roi [68:84, 100:116, 68:84] (16, 16, 16) done by pid 1776683
block 247 write roi [100:116, 4:20, 68:84] (16, 16, 16) done by pid 1776683
block 406 write roi [100:116, 36:52, 68:84] (16, 16, 16) done by pid 1776683
block 425 write roi [100:116, 68:84, 36:52] (16, 16, 16) done by pid 1776683
block 900 write roi [100:116, 100:116, 68:84] (16, 16, 16) done by pid 1776683
```

stderr: 6 lines, 6 progress-bar refreshes.

## Mode B: worker function with its own daisy.Client() loop

### daisy 1.x (1.2.3)

Status: exit 0, 7.0 s.

```text
=================================
3-D sleep task, mode B, 4 workers
=================================
daisy 1.2.3 (1.x), python 3.12.0


Execution Summary
-----------------

  Task sleep3d:

    num blocks : 512
    completed ✔: 512 (skipped 0)
    failed    ✗: 0
    orphaned  ∅: 0

    all blocks processed successfully

RETURNED True  (6.9 s)
EVIDENCE blocks attempted 512/512, succeeded 512/512, total attempts 512

worker log files: 8
--- daisy_logs/sleep3d/worker_0.err (0 lines) ---
--- daisy_logs/sleep3d/worker_0.out (128 lines, last 5) ---
block 875 write roi [68:84, 100:116, 100:116] (16, 16, 16) done by pid 1777652
block 391 write roi [100:116, 4:20, 100:116] (16, 16, 16) done by pid 1777652
block 602 write roi [100:116, 36:52, 100:116] (16, 16, 16) done by pid 1777652
block 621 write roi [100:116, 68:84, 68:84] (16, 16, 16) done by pid 1777652
block 1224 write roi [100:116, 100:116, 100:116] (16, 16, 16) done by pid 1777652
```

stderr: 1090 lines, 1090 progress-bar refreshes.

### daisy 2.0 (2.0.0)

Status: exit 0, 1.6 s.

```text
=================================
3-D sleep task, mode B, 4 workers
=================================
daisy 2.0.0 (2.0), python 3.12.0


Execution Summary
-----------------

    task        blocks   completed  skipped  failed  orphaned
    ─────────  ───────  ──────────  ───────  ──────  ────────
    sleep3d ✔      512         512        0       0         0

RETURNED True  (1.5 s)
EVIDENCE blocks attempted 512/512, succeeded 512/512, total attempts 512

worker log files: 8
--- daisy_logs/sleep3d/worker_0.err (0 lines) ---
--- daisy_logs/sleep3d/worker_0.out (128 lines, last 5) ---
block 423 write roi [68:84, 100:116, 36:52] (16, 16, 16) done by pid 1777782
block 147 write roi [100:116, 4:20, 36:52] (16, 16, 16) done by pid 1777782
block 262 write roi [100:116, 36:52, 36:52] (16, 16, 16) done by pid 1777782
block 448 write roi [100:116, 100:116, 4:20] (16, 16, 16) done by pid 1777782
block 644 write roi [100:116, 100:116, 36:52] (16, 16, 16) done by pid 1777782
```

stderr: 6 lines, 6 progress-bar refreshes.

## Mode C: launcher that starts an external worker process

### daisy 1.x (1.2.3)

Status: exit 0, 7.1 s.

```text
=================================
3-D sleep task, mode C, 4 workers
=================================
daisy 1.2.3 (1.x), python 3.12.0


Execution Summary
-----------------

  Task sleep3d:

    num blocks : 512
    completed ✔: 512 (skipped 0)
    failed    ✗: 0
    orphaned  ∅: 0

    all blocks processed successfully

RETURNED True  (7.0 s)
EVIDENCE blocks attempted 512/512, succeeded 512/512, total attempts 512

worker log files: 8
--- daisy_logs/sleep3d/worker_0.err (0 lines) ---
--- daisy_logs/sleep3d/worker_0.out (128 lines, last 5) ---
block 600 write roi [68:84, 68:84, 100:116] (16, 16, 16) done by pid 1778753
block 83 write roi [100:116, 4:20, 4:20] (16, 16, 16) done by pid 1778753
block 162 write roi [100:116, 36:52, 4:20] (16, 16, 16) done by pid 1778753
block 281 write roi [100:116, 68:84, 4:20] (16, 16, 16) done by pid 1778753
block 877 write roi [100:116, 68:84, 100:116] (16, 16, 16) done by pid 1778753
```

stderr: 1090 lines, 1090 progress-bar refreshes.

### daisy 2.0 (2.0.0)

Status: exit 0, 1.6 s.

```text
=================================
3-D sleep task, mode C, 4 workers
=================================
daisy 2.0.0 (2.0), python 3.12.0


Execution Summary
-----------------

    task        blocks   completed  skipped  failed  orphaned
    ─────────  ───────  ──────────  ───────  ──────  ────────
    sleep3d ✔      512         512        0       0         0

RETURNED True  (1.6 s)
EVIDENCE blocks attempted 512/512, succeeded 512/512, total attempts 512

worker log files: 8
--- daisy_logs/sleep3d/worker_0.err (0 lines) ---
--- daisy_logs/sleep3d/worker_0.out (129 lines, last 5) ---
block 875 write roi [68:84, 100:116, 100:116] (16, 16, 16) done by pid 1779130
block 391 write roi [100:116, 4:20, 100:116] (16, 16, 16) done by pid 1779130
block 602 write roi [100:116, 36:52, 100:116] (16, 16, 16) done by pid 1779130
block 621 write roi [100:116, 68:84, 68:84] (16, 16, 16) done by pid 1779130
block 1224 write roi [100:116, 100:116, 100:116] (16, 16, 16) done by pid 1779130
```

stderr: 6 lines, 6 progress-bar refreshes.

## Mode S: serial, no workers

### daisy 1.x (1.2.3)

Status: exit 0, 6.5 s.

```text
=================================
3-D sleep task, mode S, 4 workers
=================================
daisy 1.2.3 (1.x), python 3.12.0

block 14 write roi [20:36, 20:36, 20:36] (16, 16, 16) done by pid 1780032
block 39 write roi [20:36, 20:36, 52:68] (16, 16, 16) done by pid 1780032
block 88 write roi [20:36, 20:36, 84:100] (16, 16, 16) done by pid 1780032
block 169 write roi [20:36, 20:36, 116:132] (16, 16, 16) done by pid 1780032
block 46 write roi [20:36, 52:68, 20:36] (16, 16, 16) done by pid 1780032
block 95 write roi [20:36, 52:68, 52:68] (16, 16, 16) done by pid 1780032
block 176 write roi [20:36, 52:68, 84:100] (16, 16, 16) done by pid 1780032
block 297 write roi [20:36, 52:68, 116:132] (16, 16, 16) done by pid 1780032
block 106 write roi [20:36, 84:100, 20:36] (16, 16, 16) done by pid 1780032
block 187 write roi [20:36, 84:100, 52:68] (16, 16, 16) done by pid 1780032
block 308 write roi [20:36, 84:100, 84:100] (16, 16, 16) done by pid 1780032
block 477 write roi [20:36, 84:100, 116:132] (16, 16, 16) done by pid 1780032
block 202 write roi [20:36, 116:132, 20:36] (16, 16, 16) done by pid 1780032
block 323 write roi [20:36, 116:132, 52:68] (16, 16, 16) done by pid 1780032
block 492 write roi [20:36, 116:132, 84:100] (16, 16, 16) done by pid 1780032
... (494 lines omitted) ...
block 644 write roi [100:116, 100:116, 36:52] (16, 16, 16) done by pid 1780032
block 900 write roi [100:116, 100:116, 68:84] (16, 16, 16) done by pid 1780032
block 1224 write roi [100:116, 100:116, 100:116] (16, 16, 16) done by pid 1780032

Execution Summary
-----------------

  Task sleep3d:

    num blocks : 512
    completed ✔: 512 (skipped 0)
    failed    ✗: 0
    orphaned  ∅: 0

    all blocks processed successfully

RETURNED defaultdict with 1 task state(s)  (6.3 s)
EVIDENCE blocks attempted 512/512, succeeded 512/512, total attempts 512

worker log files: 0
```

stderr: 1085 lines, 1085 progress-bar refreshes.

### daisy 2.0 (2.0.0)

Status: exit 0, 5.4 s.

```text
=================================
3-D sleep task, mode S, 4 workers
=================================
daisy 2.0.0 (2.0), python 3.12.0


Execution Summary
-----------------

    task        blocks   completed  skipped  failed  orphaned
    ─────────  ───────  ──────────  ───────  ──────  ────────
    sleep3d ✔      512         512        0       0         0

RETURNED True  (5.4 s)
EVIDENCE blocks attempted 512/512, succeeded 512/512, total attempts 512

worker log files: 2
--- daisy_logs/sleep3d/worker_0.err (0 lines) ---
--- daisy_logs/sleep3d/worker_0.out (512 lines, last 5) ---
block 621 write roi [100:116, 68:84, 68:84] (16, 16, 16) done by pid 1780120
block 877 write roi [100:116, 68:84, 100:116] (16, 16, 16) done by pid 1780120
block 644 write roi [100:116, 100:116, 36:52] (16, 16, 16) done by pid 1780120
block 900 write roi [100:116, 100:116, 68:84] (16, 16, 16) done by pid 1780120
block 1224 write roi [100:116, 100:116, 100:116] (16, 16, 16) done by pid 1780120
```

stderr: 0 lines.

## Reading the output

Both lines process all 512 blocks exactly once in every mode: `EVIDENCE blocks attempted 512/512, succeeded 512/512` is counted by the script itself from a file every block appends to, not from daisy. The execution summaries agree with it. Worker log files hold the `print` from the block body; on 2.0 they are written while the worker runs, on 1.x as well. Wall time differs because 1.x pays about 43 ms of round trip per block per worker (see `experiments/speed/`).
