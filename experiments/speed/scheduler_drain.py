"""Pure scheduling cost: one thread acquires and releases every block from
the Scheduler, no workers, no network. Prints CSV lines:
version,grid,blocks,seconds,blocks_per_second."""

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


# ---- ground truth that does not come from daisy --------------------------
# Block bodies append one line per attempt to a file; the driver counts them.
EVIDENCE = os.environ.get("DAISY_EXPERIMENT_EVIDENCE")


def evidence_file():
    global EVIDENCE
    if EVIDENCE is None:
        EVIDENCE = os.path.join(tempfile.mkdtemp(prefix="daisy-exp-"), "attempts.txt")
        os.environ["DAISY_EXPERIMENT_EVIDENCE"] = EVIDENCE
    return EVIDENCE


def main_guard(fn):
    try:
        fn()
    except SystemExit:
        raise
    except BaseException:  # noqa: BLE001
        traceback.print_exc()
        sys.exit(1)


# ---- the experiment ----


def body(block):
    return None


def main():
    banner("scheduler drain")
    label = "2.0" if V2 else "1.x"
    print("version,grid,blocks,seconds,blocks_per_second")
    for name, (total, read, write), conflict in (
        ("1d-100k", grid_1d(100000), False),
        ("3d-8000-conflicts", grid_3d(20), True),
    ):
        t = task(
            "t", total, read, write, body, read_write_conflict=conflict, timeout=60
        )
        sched = daisy.Scheduler([t])
        n = 0
        t0 = time.monotonic()
        while True:
            b = sched.acquire_block("t")
            if b is None:
                break
            b.status = daisy.BlockStatus.SUCCESS
            sched.release_block(b)
            n += 1
        secs = time.monotonic() - t0
        print(f"{label},{name},{n},{secs:.2f},{n / secs:.0f}", flush=True)


if __name__ == "__main__":
    main_guard(main)
