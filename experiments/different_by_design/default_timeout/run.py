"""One block that sleeps 11 minutes, no timeout passed. 1.x completes it
(Task(timeout=) was never enforced); 2.0 enforces a 600 s default, kills
the worker at 10 minutes, retries once (max_retries=1) and reports the
block failed. Blocks are meant to be small; pass Task(timeout=...) for
genuinely long ones. This script takes 11 min on 1.x and 20 min on 2.0."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import common  # noqa: E402

import daisy  # noqa: E402

import time  # noqa: E402


def body(block):
    common.note_attempt(block, "started")
    time.sleep(660)
    common.note_attempt(block)


def main():
    common.banner("11-minute block, no timeout passed")
    total, read, write = common.grid_1d(1)
    t = common.task("t", total, read, write, body, read_write_conflict=False, max_retries=1)
    common.run(t, progress=False) if common.V2 else common.run(t)
    common.report_attempts(1)


if __name__ == "__main__":
    common.main_guard(main)
