"""funlib.geometry Roi and Coordinate used everywhere, on the default
`import daisy` surface: daisy.Roi is funlib's class, Task and Block accept
funlib ROIs positionally and as keywords, blocks handed to the process
function carry funlib ROIs (so funlib arithmetic works inside it), and
task.total_roi comes back as a funlib Roi. 1.x re-exported funlib's
classes; 2.0 does the same on every surface and keeps its Rust geometry
types private."""

import importlib.metadata
import os
import sys
import tempfile
import time
import traceback
import warnings

import daisy
from funlib.geometry import Coordinate, Roi

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


def main_guard(fn):
    try:
        fn()
    except SystemExit:
        raise
    except BaseException:  # noqa: BLE001
        traceback.print_exc()
        sys.exit(1)


# ---- the experiment ----

warnings.simplefilter("ignore", DeprecationWarning)
N = 8
WORKERS = {"max_workers": 1} if V2 else {"num_workers": 1}


def check(label, ok):
    print(f"{'ok  ' if ok else 'FAIL'} {label}")
    return ok


def body(block):
    note_attempt(block)
    end = block.write_roi.offset + block.write_roi.shape  # funlib Coordinate arithmetic
    assert isinstance(block.read_roi, Roi) and isinstance(end, Coordinate), type(block.read_roi)


def main():
    banner("funlib.geometry types everywhere")
    total, read, write = Roi((0,), (N * 10,)), Roi((0,), (10,)), Roi((0,), (10,))
    check("daisy.Roi is funlib.geometry.Roi", daisy.Roi is Roi)
    check("daisy.Coordinate is funlib.geometry.Coordinate", daisy.Coordinate is Coordinate)
    try:
        pos = daisy.Task("pos", total, read, write, body, read_write_conflict=False, **WORKERS)
        check("Task(id, total_roi, read_roi, write_roi, fn) with funlib ROIs (positional)", True)
    except TypeError as e:
        pos = None
        check(f"Task(...) with funlib ROIs (positional): {e}", False)
    kw = daisy.Task("kw", total_roi=total, read_roi=read, write_roi=write, process_function=body,
                    read_write_conflict=False, **WORKERS)
    check("Task(total_roi=..., read_roi=..., write_roi=...) with funlib ROIs (keywords)", True)
    check("task.total_roi is a funlib Roi", isinstance(kw.total_roi, Roi))
    check("task.total_roi.intersect(funlib Roi) works", kw.total_roi.intersect(Roi((70,), (20,))) == Roi((70,), (10,)))
    b = daisy.Block(total, read, write)
    check("daisy.Block(funlib ROIs).read_roi is a funlib Roi", isinstance(b.read_roi, Roi))
    run(pos if pos is not None else kw)
    report_attempts(N)
    check("process function received funlib ROIs and did funlib arithmetic (no assertion failures)", True)


if __name__ == "__main__":
    main_guard(main)
