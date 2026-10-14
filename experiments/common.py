"""Shared helpers for the migration experiments.

Every script under ``experiments/`` imports this and runs unchanged on
daisy 1.x and daisy 2.0: the only API differences hidden here are the
``num_workers``/``max_workers`` rename and the 2.0-only Task keywords.
Everything else that differs between the lines is the point of the
experiment and is left visible.
"""

import importlib.metadata
import os
import sys
import tempfile
import time
import traceback

import daisy

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


def grid_1d(n_blocks, block=10):
    """n blocks of `block` voxels, no context, no conflicts."""
    return roi((0,), (n_blocks * block,)), roi((0,), (block,)), roi((0,), (block,))


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
