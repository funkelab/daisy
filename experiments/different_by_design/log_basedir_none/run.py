"""set_log_basedir(None) with an external worker (mode C) that prints the
DAISY_CONTEXT it received and then redirects its own output with daisy's
worker-log helper, the 1.x idiom for cluster jobs.

1.x serialises the Python None with "%s=%s", so the worker receives the
string "None", Client() turns it into Path("None"), and the redirect
writes the worker's output to ./None/<task>/worker_<n>.out instead of the
terminal the user asked for. 2.0 ships an empty value, Client() maps it
back to None, and the helper returns None, so nothing is written."""

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

N = 4


def body(block):
    note_attempt(block)
    print("processing", block.block_id, "(meant for the terminal)", flush=True)


def launcher():
    subprocess.run([sys.executable, __file__, "--external-worker"], check=False)


def external_worker():
    with open(os.environ["DAISY_EXPERIMENT_EVIDENCE"] + ".context", "w") as f:
        f.write(os.environ.get("DAISY_CONTEXT", "") + "\n")
    client = daisy.Client()
    with open(os.environ["DAISY_EXPERIMENT_EVIDENCE"] + ".basedir", "w") as f:
        f.write(repr(daisy.logging.get_log_basedir()) + "\n")
    # The 1.x idiom for a hand-written worker that wants per-worker log
    # files; 2.0 keeps get_worker_log_basename but not the redirect helper.
    basename = daisy.get_worker_log_basename(client.worker_id, client.task_id)
    redirect = getattr(daisy, "redirect_stdouterr", None) or getattr(
        daisy.logging, "redirect_stdouterr", None
    )
    if redirect is not None and basename is not None:
        redirect(basename)
    while True:
        with client.acquire_block() as block:
            if block is None:
                return
            body(block)


def main():
    if "--external-worker" in sys.argv:
        external_worker()
        return
    banner("set_log_basedir(None)")
    daisy.logging.set_log_basedir(None)
    total, read, write = grid_1d(N)
    t = task("t", total, read, write, launcher, read_write_conflict=False, timeout=30)
    run(t, progress=False) if V2 else run(t)
    report_attempts(N)
    print(
        "DAISY_CONTEXT seen by the worker:",
        open(evidence_file() + ".context").read().strip(),
    )
    print(
        "get_log_basedir() inside the worker:",
        open(evidence_file() + ".basedir").read().strip(),
    )
    stray = [os.path.join(d, f) for d, _, fs in os.walk("None") for f in fs]
    if stray:
        print("directory named 'None' created in the cwd:", ", ".join(sorted(stray)))
        print("  (worker output that should have gone to the terminal:)")
        for line in open(sorted(stray)[-1]):
            print("   ", line.rstrip())
    else:
        print("directory named 'None' created in the cwd: no")


if __name__ == "__main__":
    main_guard(main)
