"""Block 3 of 8 never returns. Task(timeout=5) is stored but never enforced
on 1.x, so the run waits forever; 2.0 kills the worker at 5 s, retries the
block (which hangs again) and reports it failed after max_retries."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import common  # noqa: E402

import daisy  # noqa: E402

import time  # noqa: E402

N = 8


def body(block):
    common.note_attempt(block)
    if int(block.block_id[1]) == 3:
        time.sleep(10**6)


def main():
    common.banner("one block hangs, timeout=5")
    total, read, write = common.grid_1d(N)
    t = common.task("t", total, read, write, body, workers=2, read_write_conflict=False, timeout=5)
    common.run(t)
    common.report_attempts(N)


if __name__ == "__main__":
    common.main_guard(main)
