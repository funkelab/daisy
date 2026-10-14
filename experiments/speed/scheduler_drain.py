"""Pure scheduling cost: one thread acquires and releases every block from
the Scheduler, no workers, no network. Prints CSV lines:
version,grid,blocks,seconds,blocks_per_second."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import common  # noqa: E402

import daisy  # noqa: E402

import time  # noqa: E402


def body(block):
    return None


def main():
    common.banner("scheduler drain")
    label = "2.0" if common.V2 else "1.x"
    print("version,grid,blocks,seconds,blocks_per_second")
    for name, (total, read, write), conflict in (
        ("1d-100k", common.grid_1d(100000), False),
        ("3d-8000-conflicts", common.grid_3d(20), True),
    ):
        t = common.task("t", total, read, write, body, read_write_conflict=conflict, timeout=60)
        sched = daisy.Scheduler([t])
        n = 0
        t0 = time.monotonic()
        while True:
            b = sched.acquire_block("t")
            if b is None:
                break
            b.status = daisy.BlockStatus.SUCCESS
            sched.release_block(b)
            n += 1
        secs = time.monotonic() - t0
        print(f"{label},{name},{n},{secs:.2f},{n / secs:.0f}", flush=True)


if __name__ == "__main__":
    common.main_guard(main)
