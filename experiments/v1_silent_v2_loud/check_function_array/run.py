"""check_function returning a 3-element numpy array (ambiguous truth value).
1.x evaluates it outside its try block and the whole run aborts with
ValueError; 2.0 warns once, treats the block as not done and runs it."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import common  # noqa: E402

import daisy  # noqa: E402

import numpy as np  # noqa: E402

N = 20


def body(block):
    common.note_attempt(block)


def bad_check(block):
    return np.ones(3)


def main():
    common.banner("check_function returns an array")
    total, read, write = common.grid_1d(N)
    t = common.task("t", total, read, write, body, check_function=bad_check, read_write_conflict=False, timeout=30)
    common.run(t, progress=False) if common.V2 else common.run(t)
    common.report_attempts(N)


if __name__ == "__main__":
    common.main_guard(main)
