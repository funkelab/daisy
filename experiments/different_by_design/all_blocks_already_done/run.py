"""Run a task twice in serial mode with a check_function that reads marker
files the block body writes. The second run skips every block: 1.x prints
an empty execution summary (#62); 2.0 prints the table with a skipped
column."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import common  # noqa: E402

import daisy  # noqa: E402

import tempfile  # noqa: E402

N = 8
MARKERS = tempfile.mkdtemp(prefix="daisy-markers-")


def body(block):
    common.note_attempt(block)
    open(os.path.join(MARKERS, str(block.block_id[1])), "w").close()


def is_done(block):
    return os.path.exists(os.path.join(MARKERS, str(block.block_id[1])))


def main():
    common.banner("second run with every block already done")
    total, read, write = common.grid_1d(N)
    for i in (1, 2):
        print(f"\n### run {i}")
        t = common.task("t", total, read, write, body, check_function=is_done, read_write_conflict=False, timeout=30)
        common.run(t, multiprocessing=False)
    common.report_attempts(N)


if __name__ == "__main__":
    common.main_guard(main)
