"""set_log_basedir(None) with an external worker (mode C) that prints the
DAISY_CONTEXT it received. 1.x ships the string "None" as the log dir;
2.0 ships an empty value meaning off."""
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
    subprocess.run([sys.executable, __file__, "--external-worker"], check=False)


def external_worker():
    with open(os.environ["DAISY_EXPERIMENT_EVIDENCE"] + ".context", "w") as f:
        f.write(os.environ.get("DAISY_CONTEXT", "") + "\n")
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
    common.banner("set_log_basedir(None)")
    daisy.logging.set_log_basedir(None)
    total, read, write = common.grid_1d(N)
    t = common.task("t", total, read, write, launcher, read_write_conflict=False, timeout=30)
    common.run(t, progress=False) if common.V2 else common.run(t)
    common.report_attempts(N)
    print("DAISY_CONTEXT seen by the worker:", open(common.evidence_file() + ".context").read().strip())


if __name__ == "__main__":
    common.main_guard(main)
