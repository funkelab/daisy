"""funlib.geometry Roi and Coordinate used everywhere, on the default
`import daisy` surface: daisy.Roi is funlib's class, Task and Block accept
funlib ROIs positionally and as keywords, blocks handed to the process
function carry funlib ROIs (so funlib arithmetic works inside it), and
task.total_roi comes back as a funlib Roi. 1.x re-exported funlib's
classes; 2.0's compat surface does the same and its Rust entry points
duck-type funlib values."""
import os
import sys
import warnings

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import common  # noqa: E402

import daisy  # noqa: E402
from funlib.geometry import Coordinate, Roi  # noqa: E402

warnings.simplefilter("ignore", DeprecationWarning)
N = 8
WORKERS = {"max_workers": 1} if common.V2 else {"num_workers": 1}


def check(label, ok):
    print(f"{'ok  ' if ok else 'FAIL'} {label}")
    return ok


def body(block):
    common.note_attempt(block)
    end = block.write_roi.offset + block.write_roi.shape  # funlib Coordinate arithmetic
    assert isinstance(block.read_roi, Roi) and isinstance(end, Coordinate), type(block.read_roi)


def main():
    common.banner("funlib.geometry types everywhere")
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
    common.run(pos if pos is not None else kw)
    common.report_attempts(N)
    check("process function received funlib ROIs and did funlib arithmetic (no assertion failures)", True)


if __name__ == "__main__":
    common.main_guard(main)
