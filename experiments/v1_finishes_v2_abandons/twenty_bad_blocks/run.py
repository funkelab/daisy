"""2,000 blocks, 20 of which raise on every attempt, 4 workers. 1.x marks
the 20 failed and finishes the other 1,980 (and still returns True). On 2.0
an exception kills the worker, so after max_workers + max_worker_restarts
(4 + 10) worker deaths the task is abandoned, the remaining blocks are
orphaned and run_blockwise raises."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import common  # noqa: E402

import daisy  # noqa: E402

N = 2000
BAD = {i * 100 for i in range(20)}


def body(block):
    bid = int(block.block_id[1])
    if bid in BAD:
        common.note_attempt(block, "raised")
        raise RuntimeError(f"bad block {bid}")
    common.note_attempt(block)


def main():
    common.banner("20 always-failing blocks out of 2,000")
    total, read, write = common.grid_1d(N)
    t = common.task("t", total, read, write, body, workers=4, read_write_conflict=False, timeout=30, max_retries=2)
    common.run(t, progress=False) if common.V2 else common.run(t)
    common.report_attempts(N)


if __name__ == "__main__":
    common.main_guard(main)
