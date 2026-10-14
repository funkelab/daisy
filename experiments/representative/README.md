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

import importlib.metadata
import os
import subprocess
import sys
import tempfile
import time
import traceback

import daisy

# ---- helpers shared by every experiment, inlined so this file runs on its own ----

V2 = str(getattr(daisy, "__version__", "1")).startswith("2")
VERSION = importlib.metadata.version("daisy")


def banner(title):
    line = "=" * len(title)
    print(f"{line}\n{title}\n{line}")
    print(f"daisy {VERSION} ({'2.0' if V2 else '1.x'}), python {sys.version.split()[0]}\n")
    # create the evidence file now, so every worker daisy starts inherits
    # its path through the environment
    evidence_file()


def roi(offset, shape):
    return daisy.Roi(tuple(offset), tuple(shape))


def grid_3d(n=8, block=16, context=4):
    """n**3 blocks of block**3 voxels with `context` voxels of context on
    every side, so neighbouring blocks conflict and are scheduled in levels."""
    total = roi((0, 0, 0), (n * block + 2 * context,) * 3)
    read = roi((0, 0, 0), (block + 2 * context,) * 3)
    write = roi((context,) * 3, (block,) * 3)
    return total, read, write


def task(task_id, total, read, write, fn, workers=1, **kw):
    """Build a Task on either line. 2.0-only keywords are dropped on 1.x
    (with a note) so the same script runs everywhere."""
    v2_only = ("tracking_path", "max_worker_restarts", "resource_tracking")
    if not V2:
        for k in v2_only:
            if k in kw:
                print(f"[note] Task({k}=...) is a 2.0 option; ignored on {VERSION}")
                kw.pop(k)
        kw["num_workers"] = workers
    else:
        kw["max_workers"] = workers
    return daisy.Task(task_id, total, read, write, fn, **kw)


def run(tasks, **kw):
    """run_blockwise with the outcome printed in one line: RETURNED <value>
    or RAISED <type>: <message>."""
    if not isinstance(tasks, (list, tuple)):
        tasks = [tasks]
    t0 = time.monotonic()
    try:
        r = daisy.run_blockwise(list(tasks), **kw)
    except BaseException as e:  # noqa: BLE001 - KeyboardInterrupt is a result here
        print(f"\nRAISED {type(e).__name__}: {str(e).splitlines()[0][:160]}  ({time.monotonic() - t0:.1f} s)")
        return None
    shown = r if isinstance(r, bool) else f"{type(r).__name__} with {len(r)} task state(s)"
    print(f"\nRETURNED {shown}  ({time.monotonic() - t0:.1f} s)")
    return r


# ---- ground truth that does not come from daisy --------------------------
# Block bodies append one line per attempt to a file; the driver counts them.
EVIDENCE = os.environ.get("DAISY_EXPERIMENT_EVIDENCE")


def evidence_file():
    global EVIDENCE
    if EVIDENCE is None:
        EVIDENCE = os.path.join(tempfile.mkdtemp(prefix="daisy-exp-"), "attempts.txt")
        os.environ["DAISY_EXPERIMENT_EVIDENCE"] = EVIDENCE
    return EVIDENCE


def note_attempt(block, outcome="ok"):
    with open(evidence_file(), "a") as f:
        f.write(f"{int(block.block_id[1])} {outcome} {os.getpid()}\n")


def report_attempts(expected):
    """Print what actually ran, counted from the evidence file."""
    seen = {}
    ok = set()
    try:
        with open(evidence_file()) as f:
            for line in f:
                bid, outcome, _pid = line.split()
                seen[bid] = seen.get(bid, 0) + 1
                if outcome == "ok":
                    ok.add(bid)
    except FileNotFoundError:
        pass
    print(
        f"EVIDENCE blocks attempted {len(seen)}/{expected}, succeeded {len(ok)}/{expected}, "
        f"total attempts {sum(seen.values())}"
    )
    return seen, ok


def tail(path, n=12):
    """Print the last n lines of a file, with a count of what was skipped."""
    try:
        with open(path, errors="replace") as f:
            lines = f.read().splitlines()
    except OSError:
        print(f"[{path}: not present]")
        return
    print(f"--- {path} ({len(lines)} lines{', last ' + str(n) if len(lines) > n else ''}) ---")
    for line in lines[-n:]:
        print(line)


def worker_logs(task_id):
    """The worker log files daisy wrote for a task, in both lines."""
    base = daisy.logging.get_log_basedir() if hasattr(daisy, "logging") else None
    if base is None:
        return []
    d = os.path.join(str(base), task_id)
    try:
        return sorted(os.path.join(d, f) for f in os.listdir(d))
    except OSError:
        return []


def main_guard(fn):
    try:
        fn()
    except SystemExit:
        raise
    except BaseException:  # noqa: BLE001
        traceback.print_exc()
        sys.exit(1)


# ---- the experiment ----

N = 8


def process_block(block):
    time.sleep(0.01)
    print(f"block {block.block_id[1]} write roi {block.write_roi} done by pid {os.getpid()}", flush=True)
    note_attempt(block)


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
    banner(f"3-D sleep task, mode {mode}, {workers} workers")
    total, read, write = grid_3d(N)
    fn = {"A": process_block, "B": worker_loop, "C": launch_external_worker, "S": process_block}[mode]
    t = task("sleep3d", total, read, write, fn, workers=workers, read_write_conflict=True, timeout=60)
    run(t, multiprocessing=(mode != "S"))
    report_attempts(N**3)
    logs = worker_logs("sleep3d")
    print(f"\nworker log files: {len(logs)}")
    for path in logs[:2]:
        tail(path, 5)


if __name__ == "__main__":
    main_guard(main)
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
block 600 write roi [68:84, 68:84, 100:116] (16, 16, 16) done by pid 3664109
block 83 write roi [100:116, 4:20, 4:20] (16, 16, 16) done by pid 3664109
block 162 write roi [100:116, 36:52, 4:20] (16, 16, 16) done by pid 3664109
block 281 write roi [100:116, 68:84, 4:20] (16, 16, 16) done by pid 3664109
block 877 write roi [100:116, 68:84, 100:116] (16, 16, 16) done by pid 3664109
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
block 619 write roi [68:84, 100:116, 68:84] (16, 16, 16) done by pid 3664224
block 391 write roi [100:116, 4:20, 100:116] (16, 16, 16) done by pid 3664224
block 602 write roi [100:116, 36:52, 100:116] (16, 16, 16) done by pid 3664224
block 621 write roi [100:116, 68:84, 68:84] (16, 16, 16) done by pid 3664224
block 1224 write roi [100:116, 100:116, 100:116] (16, 16, 16) done by pid 3664224
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

RETURNED True  (6.8 s)
EVIDENCE blocks attempted 512/512, succeeded 512/512, total attempts 512

worker log files: 8
--- daisy_logs/sleep3d/worker_0.err (0 lines) ---
--- daisy_logs/sleep3d/worker_0.out (128 lines, last 5) ---
block 423 write roi [68:84, 100:116, 36:52] (16, 16, 16) done by pid 3665200
block 147 write roi [100:116, 4:20, 36:52] (16, 16, 16) done by pid 3665200
block 262 write roi [100:116, 36:52, 36:52] (16, 16, 16) done by pid 3665200
block 448 write roi [100:116, 100:116, 4:20] (16, 16, 16) done by pid 3665200
block 644 write roi [100:116, 100:116, 36:52] (16, 16, 16) done by pid 3665200
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
block 875 write roi [68:84, 100:116, 100:116] (16, 16, 16) done by pid 3665330
block 391 write roi [100:116, 4:20, 100:116] (16, 16, 16) done by pid 3665330
block 602 write roi [100:116, 36:52, 100:116] (16, 16, 16) done by pid 3665330
block 621 write roi [100:116, 68:84, 68:84] (16, 16, 16) done by pid 3665330
block 1224 write roi [100:116, 100:116, 100:116] (16, 16, 16) done by pid 3665330
```

stderr: 6 lines, 6 progress-bar refreshes.

## Mode C: launcher that starts an external worker process

### daisy 1.x (1.2.3)

Status: exit 0, 7.2 s.

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
block 423 write roi [68:84, 100:116, 36:52] (16, 16, 16) done by pid 3666302
block 147 write roi [100:116, 4:20, 36:52] (16, 16, 16) done by pid 3666302
block 262 write roi [100:116, 36:52, 36:52] (16, 16, 16) done by pid 3666302
block 448 write roi [100:116, 100:116, 4:20] (16, 16, 16) done by pid 3666302
block 644 write roi [100:116, 100:116, 36:52] (16, 16, 16) done by pid 3666302
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

RETURNED True  (1.5 s)
EVIDENCE blocks attempted 512/512, succeeded 512/512, total attempts 512

worker log files: 8
--- daisy_logs/sleep3d/worker_0.err (0 lines) ---
--- daisy_logs/sleep3d/worker_0.out (128 lines, last 5) ---
block 600 write roi [68:84, 68:84, 100:116] (16, 16, 16) done by pid 3666682
block 83 write roi [100:116, 4:20, 4:20] (16, 16, 16) done by pid 3666682
block 391 write roi [100:116, 4:20, 100:116] (16, 16, 16) done by pid 3666682
block 281 write roi [100:116, 68:84, 4:20] (16, 16, 16) done by pid 3666682
block 877 write roi [100:116, 68:84, 100:116] (16, 16, 16) done by pid 3666682
```

stderr: 6 lines, 6 progress-bar refreshes.

## Mode S: serial, no workers

### daisy 1.x (1.2.3)

Status: exit 0, 7.1 s.

```text
=================================
3-D sleep task, mode S, 4 workers
=================================
daisy 1.2.3 (1.x), python 3.12.0

block 14 write roi [20:36, 20:36, 20:36] (16, 16, 16) done by pid 3667582
block 39 write roi [20:36, 20:36, 52:68] (16, 16, 16) done by pid 3667582
block 88 write roi [20:36, 20:36, 84:100] (16, 16, 16) done by pid 3667582
block 169 write roi [20:36, 20:36, 116:132] (16, 16, 16) done by pid 3667582
block 46 write roi [20:36, 52:68, 20:36] (16, 16, 16) done by pid 3667582
block 95 write roi [20:36, 52:68, 52:68] (16, 16, 16) done by pid 3667582
block 176 write roi [20:36, 52:68, 84:100] (16, 16, 16) done by pid 3667582
block 297 write roi [20:36, 52:68, 116:132] (16, 16, 16) done by pid 3667582
block 106 write roi [20:36, 84:100, 20:36] (16, 16, 16) done by pid 3667582
block 187 write roi [20:36, 84:100, 52:68] (16, 16, 16) done by pid 3667582
block 308 write roi [20:36, 84:100, 84:100] (16, 16, 16) done by pid 3667582
block 477 write roi [20:36, 84:100, 116:132] (16, 16, 16) done by pid 3667582
block 202 write roi [20:36, 116:132, 20:36] (16, 16, 16) done by pid 3667582
block 323 write roi [20:36, 116:132, 52:68] (16, 16, 16) done by pid 3667582
block 492 write roi [20:36, 116:132, 84:100] (16, 16, 16) done by pid 3667582
... (494 lines omitted) ...
block 644 write roi [100:116, 100:116, 36:52] (16, 16, 16) done by pid 3667582
block 900 write roi [100:116, 100:116, 68:84] (16, 16, 16) done by pid 3667582
block 1224 write roi [100:116, 100:116, 100:116] (16, 16, 16) done by pid 3667582

Execution Summary
-----------------

  Task sleep3d:

    num blocks : 512
    completed ✔: 512 (skipped 0)
    failed    ✗: 0
    orphaned  ∅: 0

    all blocks processed successfully

RETURNED defaultdict with 1 task state(s)  (6.9 s)
EVIDENCE blocks attempted 512/512, succeeded 512/512, total attempts 512

worker log files: 0
```

stderr: 1090 lines, 1090 progress-bar refreshes.

### daisy 2.0 (2.0.0)

Status: exit 0, 5.3 s.

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

RETURNED True  (5.3 s)
EVIDENCE blocks attempted 512/512, succeeded 512/512, total attempts 512

worker log files: 2
--- daisy_logs/sleep3d/worker_0.err (0 lines) ---
--- daisy_logs/sleep3d/worker_0.out (512 lines, last 5) ---
block 621 write roi [100:116, 68:84, 68:84] (16, 16, 16) done by pid 3667648
block 877 write roi [100:116, 68:84, 100:116] (16, 16, 16) done by pid 3667648
block 644 write roi [100:116, 100:116, 36:52] (16, 16, 16) done by pid 3667648
block 900 write roi [100:116, 100:116, 68:84] (16, 16, 16) done by pid 3667648
block 1224 write roi [100:116, 100:116, 100:116] (16, 16, 16) done by pid 3667648
```

stderr: 0 lines.

## Reading the output

Both lines process all 512 blocks exactly once in every mode: `EVIDENCE blocks attempted 512/512, succeeded 512/512` is counted by the script itself from a file every block appends to, not from daisy. The execution summaries agree with it. Worker log files hold the `print` from the block body; on 2.0 they are written while the worker runs, on 1.x as well. Wall time differs because 1.x pays about 43 ms of round trip per block per worker (see `experiments/speed/`).
