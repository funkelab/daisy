"""Resume cost for 20,000 blocks. Run 1 processes everything and leaves a
marker file per block; run 2 skips everything through a check_function
that stats the marker. On 2.0 a third and fourth run use the built-in
tracking store (one byte per block, no check_function) instead."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import common  # noqa: E402

import daisy  # noqa: E402

import tempfile  # noqa: E402
import time  # noqa: E402

N = 20000
MARKERS = tempfile.mkdtemp(prefix="daisy-markers-")
os.environ["DAISY_EXP_MARKERS"] = MARKERS


def body(block):
    open(os.path.join(os.environ["DAISY_EXP_MARKERS"], str(block.block_id[1])), "w").close()


def body_plain(block):
    return None


def is_done(block):
    return os.path.exists(os.path.join(os.environ["DAISY_EXP_MARKERS"], str(block.block_id[1])))


def timed(label, t, **kw):
    t0 = time.monotonic()
    common.run(t, progress=False, **kw) if common.V2 else common.run(t, **kw)
    print(f"TIMING {label}: {time.monotonic() - t0:.2f} s")


def main():
    common.banner("resume: check_function on marker files vs built-in tracking")
    total, read, write = common.grid_1d(N)
    print("\n### marker files + check_function, first run")
    timed("markers first run", common.task("naive", total, read, write, body, workers=4, check_function=is_done, read_write_conflict=False, timeout=60))
    print("\n### marker files + check_function, resume (everything skipped)")
    timed("markers resume", common.task("naive", total, read, write, body, workers=4, check_function=is_done, read_write_conflict=False, timeout=60))
    if common.V2:
        store = tempfile.mkdtemp(prefix="daisy-tracking-")
        print("\n### built-in tracking store, first run")
        timed("tracking first run", common.task("builtin", total, read, write, body_plain, workers=4, read_write_conflict=False, timeout=60, tracking_path=store))
        print("\n### built-in tracking store, resume (everything skipped)")
        timed("tracking resume", common.task("builtin", total, read, write, body_plain, workers=4, read_write_conflict=False, timeout=60, tracking_path=store))


if __name__ == "__main__":
    common.main_guard(main)
