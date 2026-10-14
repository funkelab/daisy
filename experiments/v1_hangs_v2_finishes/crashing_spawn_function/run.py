"""The spawn function exits with status 1 at once. 1.x respawns it on every
event-loop iteration without limit (10 per second, forever); 2.0 stops after
max_workers + max_worker_restarts starts and raises."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import common  # noqa: E402

import daisy  # noqa: E402

N = 4


def spawn_and_die():
    with open(os.environ["DAISY_EXPERIMENT_EVIDENCE"], "a") as f:
        f.write(f"spawn crash {os.getpid()}\n")
    raise SystemExit(1)


def main():
    common.banner("spawn function exits immediately")
    total, read, write = common.grid_1d(N)
    t = common.task("t", total, read, write, spawn_and_die, read_write_conflict=False, timeout=30)
    common.run(t)
    with open(common.evidence_file()) as f:
        print(f"EVIDENCE spawn attempts: {sum(1 for _ in f)}")


if __name__ == "__main__":
    common.main_guard(main)
