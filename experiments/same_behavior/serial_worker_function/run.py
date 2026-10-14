"""Serial mode (multiprocessing=False) with a 0-argument worker function:
there is no server for the worker loop to connect to, so both lines refuse
(1.x with a TypeError at the first block, 2.0 with a configuration error
before anything runs). Earlier 2.0 builds marked every block done without
running anything."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import common  # noqa: E402

import daisy  # noqa: E402

N = 4


def worker_loop():
    client = daisy.Client()
    while True:
        with client.acquire_block() as block:
            if block is None:
                return
            common.note_attempt(block)


def main():
    common.banner("serial mode with a worker function")
    total, read, write = common.grid_1d(N)
    t = common.task("t", total, read, write, worker_loop, read_write_conflict=False, timeout=30)
    common.run(t, multiprocessing=False)
    common.report_attempts(N)


if __name__ == "__main__":
    common.main_guard(main)
