"""A 3-D task (8 x 8 x 8 blocks of 16^3 voxels with 4 voxels of context) whose
block body sleeps 10 ms, run in each worker mode:

  A  a 1-argument block function, workers started by daisy
  B  a 0-argument worker function that runs its own daisy.Client() loop
  C  a launcher that starts an external worker process (how a cluster
     submission looks), the worker connects back over TCP
  S  serial mode, no workers

Usage: python modes.py [A|B|C|S] [--workers N]
"""

import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import common  # noqa: E402

import daisy  # noqa: E402

N = 8


def process_block(block):
    time.sleep(0.01)
    print(f"block {block.block_id[1]} write roi {block.write_roi} done by pid {os.getpid()}", flush=True)
    common.note_attempt(block)


def worker_loop():
    client = daisy.Client()
    while True:
        with client.acquire_block() as block:
            if block is None:
                return
            process_block(block)


def launch_external_worker():
    # Blocks until the worker exits, like `srun` or `sbatch --wait` would.
    subprocess.run([sys.executable, __file__, "--external-worker"], check=False)


def main():
    if "--external-worker" in sys.argv:
        worker_loop()
        return
    mode = next((a for a in sys.argv[1:] if a in "ABCS"), "A")
    workers = int(sys.argv[sys.argv.index("--workers") + 1]) if "--workers" in sys.argv else 4
    common.banner(f"3-D sleep task, mode {mode}, {workers} workers")
    total, read, write = common.grid_3d(N)
    fn = {"A": process_block, "B": worker_loop, "C": launch_external_worker, "S": process_block}[mode]
    t = common.task("sleep3d", total, read, write, fn, workers=workers, read_write_conflict=True, timeout=60)
    common.run(t, multiprocessing=(mode != "S"))
    common.report_attempts(N**3)
    logs = common.worker_logs("sleep3d")
    print(f"\nworker log files: {len(logs)}")
    for path in logs[:2]:
        common.tail(path, 5)


if __name__ == "__main__":
    common.main_guard(main)
