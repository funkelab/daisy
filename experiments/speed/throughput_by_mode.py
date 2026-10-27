"""Throughput of no-op blocks by worker mode and worker count. Prints one
CSV line per cell: version,mode,workers,blocks,seconds,blocks_per_second.

Usage: python throughput_by_mode.py [--modes A,B,C] [--workers 1,4,16,64]
                                     [--blocks 10000] [--csv out.csv]
"""

import argparse
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


def worker_loop():
    client = daisy.Client()
    while True:
        with client.acquire_block() as block:
            if block is None:
                return
            body(block)


def launcher():
    subprocess.run([sys.executable, __file__, "--external-worker"], check=False)


def main():
    if "--external-worker" in sys.argv:
        worker_loop()
        return
    ap = argparse.ArgumentParser()
    ap.add_argument("--modes", default="A,B,C")
    ap.add_argument("--workers", default="1,4,16,64")
    ap.add_argument("--blocks", type=int, default=10000)
    ap.add_argument("--csv", default=None)
    a = ap.parse_args()
    banner(f"throughput, {a.blocks} no-op blocks")
    label = "2.0" if V2 else "1.x"
    out = open(a.csv, "a") if a.csv else None
    total, read, write = grid_1d(a.blocks)
    fns = {"A": body, "B": worker_loop, "C": launcher}
    print("version,mode,workers,blocks,seconds,blocks_per_second")
    for mode in a.modes.split(","):
        for w in (int(x) for x in a.workers.split(",")):
            t = task(
                f"bench_{mode}{w}",
                total,
                read,
                write,
                fns[mode],
                workers=w,
                read_write_conflict=False,
                timeout=600,
            )
            t0 = time.monotonic()
            if V2:
                daisy.run_blockwise([t], progress=False, summary=False)
            else:
                import contextlib  # noqa: E401
                import io

                with contextlib.redirect_stdout(io.StringIO()):
                    daisy.run_blockwise([t])
            secs = time.monotonic() - t0
            line = f"{label},{mode},{w},{a.blocks},{secs:.2f},{a.blocks / secs:.0f}"
            print(line, flush=True)
            if out:
                out.write(line + "\n")
                out.flush()


if __name__ == "__main__":
    main_guard(main)
