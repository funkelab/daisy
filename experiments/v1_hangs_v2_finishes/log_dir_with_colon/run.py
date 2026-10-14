"""A log directory whose path contains ':' (mode C, external workers). 1.x
ships it unescaped in DAISY_CONTEXT, every worker fails to parse the
context, and the launcher is respawned 10 times a second forever. 2.0
percent-encodes context values and the run completes."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import common  # noqa: E402

import daisy  # noqa: E402

import subprocess  # noqa: E402

N = 4


def body(block):
    common.note_attempt(block)


def launcher():
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
    common.banner("log basedir containing ':'")
    daisy.logging.set_log_basedir("logs:with:colon")
    total, read, write = common.grid_1d(N)
    t = common.task("t", total, read, write, launcher, read_write_conflict=False, timeout=30)
    common.run(t)
    common.report_attempts(N)


if __name__ == "__main__":
    common.main_guard(main)
