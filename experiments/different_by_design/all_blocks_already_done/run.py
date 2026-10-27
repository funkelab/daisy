"""Run a task twice in serial mode with a check_function that reads marker
files the block body writes. The second run skips every block: 1.x prints
an empty execution summary (#62); 2.0 prints the table with a skipped
column."""

import importlib.metadata
import os
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
    print(
        f"daisy {VERSION} ({'2.0' if V2 else '1.x'}), python {sys.version.split()[0]}\n"
    )
    # create the evidence file now, so every worker daisy starts inherits
    # its path through the environment
    evidence_file()


def roi(offset, shape):
    return daisy.Roi(tuple(offset), tuple(shape))


def grid_1d(n_blocks, block=10):
    """n blocks of `block` voxels, no context, no conflicts."""
    return roi((0,), (n_blocks * block,)), roi((0,), (block,)), roi((0,), (block,))


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
        print(
            f"\nRAISED {type(e).__name__}: {str(e).splitlines()[0][:160]}  ({time.monotonic() - t0:.1f} s)"
        )
        return None
    shown = (
        r if isinstance(r, bool) else f"{type(r).__name__} with {len(r)} task state(s)"
    )
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
MARKERS = tempfile.mkdtemp(prefix="daisy-markers-")


def body(block):
    note_attempt(block)
    open(os.path.join(MARKERS, str(block.block_id[1])), "w").close()


def is_done(block):
    return os.path.exists(os.path.join(MARKERS, str(block.block_id[1])))


def main():
    banner("second run with every block already done")
    total, read, write = grid_1d(N)
    for i in (1, 2):
        print(f"\n### run {i}")
        t = task(
            "t",
            total,
            read,
            write,
            body,
            check_function=is_done,
            read_write_conflict=False,
            timeout=30,
        )
        run(t, multiprocessing=False)
    report_attempts(N)


if __name__ == "__main__":
    main_guard(main)
