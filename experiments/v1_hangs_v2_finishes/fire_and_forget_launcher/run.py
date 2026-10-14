"""The launcher starts an external worker with Popen and returns at once
(like sbatch without --wait); the worker processes one block and exits.
1.x never respawns a launcher that returned cleanly, so when the worker
dies the run waits forever. 2.0 detects a worker connecting after its spawn
call returned and fails the run with an explanation (with
max_worker_restarts=0 it would instead abandon the task before any worker
connects, because the launchers all "exited" at once)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import common  # noqa: E402

import daisy  # noqa: E402

import subprocess  # noqa: E402

N = 20


def body(block):
    common.note_attempt(block)


def launcher():
    subprocess.Popen([sys.executable, __file__, "--external-worker"])


def external_worker():
    client = daisy.Client()
    with client.acquire_block() as block:
        if block is not None:
            body(block)
    os._exit(1)


def main():
    if "--external-worker" in sys.argv:
        external_worker()
        return
    common.banner("fire-and-forget launcher, worker exits after one block")
    total, read, write = common.grid_1d(N)
    t = common.task("t", total, read, write, launcher, workers=2, read_write_conflict=False, timeout=30)
    common.run(t)
    common.report_attempts(N)


if __name__ == "__main__":
    common.main_guard(main)
