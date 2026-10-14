"""Two blocks, two workers, max_worker_restarts=0. Block 0 finishes at once
and its worker is released as idle; block 1 sleeps 2 s and fails on its
first attempt only. 1.x retries it with the worker that failed. On 2.0 the
failing worker died, the idle one is gone, and the retry would need a new
start that the budget (2 + 0) no longer allows: the task is abandoned
with one orphan although max_retries=2 promised a retry."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import common  # noqa: E402

import daisy  # noqa: E402

import time  # noqa: E402

N = 2


def body(block):
    bid = int(block.block_id[1])
    if bid == 1:
        time.sleep(2)
        marker = os.environ["DAISY_EXPERIMENT_EVIDENCE"] + ".failed-once"
        if not os.path.exists(marker):
            open(marker, "w").close()
            common.note_attempt(block, "raised")
            raise RuntimeError("fails once")
    common.note_attempt(block)


def main():
    common.banner("late failure after the idle worker was released")
    total, read, write = common.grid_1d(N)
    t = common.task("t", total, read, write, body, workers=2, read_write_conflict=False, timeout=30, max_retries=2, max_worker_restarts=0)
    common.run(t, progress=False) if common.V2 else common.run(t)
    common.report_attempts(N)


if __name__ == "__main__":
    common.main_guard(main)
