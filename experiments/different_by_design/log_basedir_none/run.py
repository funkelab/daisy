"""set_log_basedir(None) with an external worker (mode C) that prints the
DAISY_CONTEXT it received and then redirects its own output with daisy's
worker-log helper, the 1.x idiom for cluster jobs.

1.x serialises the Python None with "%s=%s", so the worker receives the
string "None", Client() turns it into Path("None"), and the redirect
writes the worker's output to ./None/<task>/worker_<n>.out instead of the
terminal the user asked for. 2.0 ships an empty value, Client() maps it
back to None, and the helper returns None, so nothing is written."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import common  # noqa: E402

import daisy  # noqa: E402

import subprocess  # noqa: E402

N = 4


def body(block):
    common.note_attempt(block)
    print("processing", block.block_id, "(meant for the terminal)", flush=True)


def launcher():
    subprocess.run([sys.executable, __file__, "--external-worker"], check=False)


def external_worker():
    with open(os.environ["DAISY_EXPERIMENT_EVIDENCE"] + ".context", "w") as f:
        f.write(os.environ.get("DAISY_CONTEXT", "") + "\n")
    client = daisy.Client()
    with open(os.environ["DAISY_EXPERIMENT_EVIDENCE"] + ".basedir", "w") as f:
        f.write(repr(daisy.logging.get_log_basedir()) + "\n")
    # The 1.x idiom for a hand-written worker that wants per-worker log
    # files; 2.0 keeps get_worker_log_basename but not the redirect helper.
    basename = daisy.get_worker_log_basename(client.worker_id, client.task_id)
    redirect = getattr(daisy, "redirect_stdouterr", None) or getattr(daisy.logging, "redirect_stdouterr", None)
    if redirect is not None and basename is not None:
        redirect(basename)
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
    print("get_log_basedir() inside the worker:", open(common.evidence_file() + ".basedir").read().strip())
    stray = [os.path.join(d, f) for d, _, fs in os.walk("None") for f in fs]
    if stray:
        print("directory named 'None' created in the cwd:", ", ".join(sorted(stray)))
        print("  (worker output that should have gone to the terminal:)")
        for line in open(sorted(stray)[-1]):
            print("   ", line.rstrip())
    else:
        print("directory named 'None' created in the cwd: no")


if __name__ == "__main__":
    common.main_guard(main)
