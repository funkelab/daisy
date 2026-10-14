"""Two different Task objects with the same task_id. 1.x silently drops the
second and runs the first (4 blocks); 2.0 raises."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import common  # noqa: E402

import daisy  # noqa: E402

def body(block):
    common.note_attempt(block)


def main():
    common.banner("two tasks with one task_id")
    t1 = common.task("dup", *common.grid_1d(4), body, read_write_conflict=False, timeout=30)
    t2 = common.task("dup", *common.grid_1d(8), body, read_write_conflict=False, timeout=30)
    common.run([t1, t2], multiprocessing=False)
    common.report_attempts(8)


if __name__ == "__main__":
    common.main_guard(main)
