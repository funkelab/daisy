"""Mode C: a blocking launcher starts an external worker that fails every
block. 1.x replaces a worker only after 4 failures, launches 5 jobs for
10 blocks x 2 attempts, and returns True with 10 failed. 2.0 kills the
worker on the first exception, so every attempt costs a job launch; after
the budget of 11 launches it abandons the task and raises."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import common  # noqa: E402

import daisy  # noqa: E402

import subprocess  # noqa: E402

N = 10


def body(block):
    common.note_attempt(block, "raised")
    raise RuntimeError("always fails")


def launcher():
    with open(os.environ["DAISY_EXPERIMENT_EVIDENCE"] + ".launches", "a") as f:
        f.write("launch\n")
    rc = subprocess.run([sys.executable, __file__, "--external-worker"]).returncode
    if rc != 0:
        raise SystemExit(rc)


def external_worker():
    client = daisy.Client()
    while True:
        with client.acquire_block() as block:
            if block is None:
                return
            body(block)


def main():
    if "--external-worker" in sys.argv:
        external_worker()
        return
    common.banner("external worker that fails every block")
    total, read, write = common.grid_1d(N)
    t = common.task("t", total, read, write, launcher, read_write_conflict=False, timeout=30, max_retries=1)
    common.run(t, progress=False) if common.V2 else common.run(t)
    common.report_attempts(N)
    with open(common.evidence_file() + ".launches") as f:
        print(f"EVIDENCE external jobs launched: {sum(1 for _ in f)}")


if __name__ == "__main__":
    common.main_guard(main)
