"""The block raises an exception object that cannot be pickled (it holds a
lock). 1.x cannot send it to the server, so the failure list is empty and
the run returns True; 2.0 carries the traceback as text and returns False."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import common  # noqa: E402

import daisy  # noqa: E402

import threading  # noqa: E402

N = 4


class LockedError(RuntimeError):
    def __init__(self, msg):
        super().__init__(msg)
        self.lock = threading.Lock()


def body(block):
    common.note_attempt(block, "raised")
    raise LockedError(f"boom {block.block_id[1]}")


def main():
    common.banner("exception that cannot be pickled")
    total, read, write = common.grid_1d(N)
    t = common.task("t", total, read, write, body, read_write_conflict=False, timeout=30, max_retries=1)
    common.run(t, progress=False) if common.V2 else common.run(t)
    common.report_attempts(N)
    for path in common.worker_logs("t"):
        if path.endswith(".err"):
            common.tail(path, 4)


if __name__ == "__main__":
    common.main_guard(main)
