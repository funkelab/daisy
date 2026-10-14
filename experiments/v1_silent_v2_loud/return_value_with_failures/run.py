"""Four blocks, every one raises, max_retries=1. 1.x prints the failures
and still returns True ("accounting finished"); 2.0 returns False."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import common  # noqa: E402

import daisy  # noqa: E402

N = 4


def body(block):
    common.note_attempt(block, "raised")
    raise RuntimeError(f"boom {block.block_id[1]}")


def main():
    common.banner("every block fails: what does run_blockwise return?")
    total, read, write = common.grid_1d(N)
    t = common.task("t", total, read, write, body, read_write_conflict=False, timeout=30, max_retries=1)
    r = common.run(t, progress=False) if common.V2 else common.run(t)
    print(f"bool(result) = {bool(r)}")
    common.report_attempts(N)


if __name__ == "__main__":
    common.main_guard(main)
