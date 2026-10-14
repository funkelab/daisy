"""SIGINT (Ctrl-C) 2 s into a run of 2,000 50-ms blocks on 8 workers. 1.x
swallows it and returns False with a summary that shows the blocks in
flight neither failed nor pending; 2.0 prints what finished and raises
KeyboardInterrupt."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import common  # noqa: E402

import daisy  # noqa: E402

import signal  # noqa: E402
import threading  # noqa: E402
import time  # noqa: E402

N = 2000


def body(block):
    time.sleep(0.05)
    common.note_attempt(block)


def main():
    common.banner("Ctrl-C mid-run")
    total, read, write = common.grid_1d(N)
    t = common.task("t", total, read, write, body, workers=8, read_write_conflict=False, timeout=30)
    threading.Thread(target=lambda: (time.sleep(2), os.kill(os.getpid(), signal.SIGINT)), daemon=True).start()
    common.run(t, progress=False) if common.V2 else common.run(t)
    common.report_attempts(N)


if __name__ == "__main__":
    common.main_guard(main)
