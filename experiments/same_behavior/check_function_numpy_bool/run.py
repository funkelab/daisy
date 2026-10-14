"""check_function returning numpy.bool_(True): both lines treat it as "done"
and skip every block (nothing runs)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import common  # noqa: E402

import daisy  # noqa: E402

import numpy as np  # noqa: E402

N = 20


def body(block):
    common.note_attempt(block)


def already_done(block):
    return np.bool_(True)


def main():
    common.banner("check_function returns numpy.bool_(True)")
    total, read, write = common.grid_1d(N)
    t = common.task("t", total, read, write, body, check_function=already_done, read_write_conflict=False, timeout=30)
    common.run(t)
    common.report_attempts(N)


if __name__ == "__main__":
    common.main_guard(main)
