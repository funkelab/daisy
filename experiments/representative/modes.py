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
