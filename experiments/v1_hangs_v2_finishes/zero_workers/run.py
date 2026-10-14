"""num_workers=0 / max_workers=0: 1.x starts nobody and waits forever;
2.0 rejects it when the Task is built."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import common  # noqa: E402

import daisy  # noqa: E402

N = 4


def body(block):
    common.note_attempt(block)


def main():
    common.banner("zero workers")
    total, read, write = common.grid_1d(N)
    try:
        t = common.task("t", total, read, write, body, workers=0, read_write_conflict=False, timeout=30)
    except ValueError as e:
        print(f"RAISED ValueError at Task construction: {e}")
        return
    common.run(t)
    common.report_attempts(N)


if __name__ == "__main__":
    common.main_guard(main)
