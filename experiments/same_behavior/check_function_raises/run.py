"""check_function that raises: both lines treat the block as not done and
run it. 1.x logs the exception for every block, 2.0 once per task."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import common  # noqa: E402

import daisy  # noqa: E402

N = 20


def body(block):
    common.note_attempt(block)


def broken_check(block):
    raise ValueError("check boom")


def main():
    common.banner("check_function raises")
    total, read, write = common.grid_1d(N)
    t = common.task("t", total, read, write, body, check_function=broken_check, read_write_conflict=False, timeout=30)
    common.run(t)
    common.report_attempts(N)


if __name__ == "__main__":
    common.main_guard(main)
