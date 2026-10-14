"""A worker is SIGKILLed while holding a block (block 5 kills its own process
on its first attempt). Both lines reclaim the block, start a replacement
worker and retry it. (daisy 1.2.2 from PyPI does not replace dead workers
and hangs here; master and 2.0 do.)"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import common  # noqa: E402

import daisy  # noqa: E402

import signal  # noqa: E402

N = 20
KILLED = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".killed-once")


def body(block):
    if int(block.block_id[1]) == 5 and not os.path.exists(os.environ["DAISY_EXP_KILLED"]):
        open(os.environ["DAISY_EXP_KILLED"], "w").close()
        common.note_attempt(block, "killed")
        os.kill(os.getpid(), signal.SIGKILL)
    common.note_attempt(block)


def main():
    import tempfile

    os.environ["DAISY_EXP_KILLED"] = os.path.join(tempfile.mkdtemp(), "killed")
    common.banner("worker SIGKILLed on block 5")
    total, read, write = common.grid_1d(N)
    t = common.task("t", total, read, write, body, workers=3, read_write_conflict=False, timeout=30)
    common.run(t, progress=False) if common.V2 else common.run(t)
    common.report_attempts(N)


if __name__ == "__main__":
    common.main_guard(main)
