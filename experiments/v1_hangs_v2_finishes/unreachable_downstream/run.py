"""A downstream task whose total_roi extends 10 blocks beyond the upstream
task's write region. Those 10 blocks have no upstream block to wait on, so
1.x waits forever after the rest completes; 2.0 refuses the pipeline before
it starts."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import common  # noqa: E402

import daisy  # noqa: E402

N = 20


def body(block):
    common.note_attempt(block)


def main():
    common.banner("downstream task reaches beyond its upstream")
    total, read, write = common.grid_1d(N)
    up = common.task("up", total, read, write, body, workers=2, read_write_conflict=False, timeout=30)
    down_total = common.roi((0,), (N * 10 + 100,))
    down = common.task("down", down_total, read, write, body, workers=2, read_write_conflict=False, timeout=30, upstream_tasks=[up])
    common.run([down, up] if common.V2 else [down])
    common.report_attempts(N + N + 10)


if __name__ == "__main__":
    common.main_guard(main)
