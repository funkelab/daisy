"""20,000 no-op blocks with stderr redirected to a file (what a batch
system's .out looks like). 1.x refreshes the progress bar twice per block;
2.0 refreshes on a timer. The script prints how many bytes and refreshes
landed in the file."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import common  # noqa: E402

import daisy  # noqa: E402

import tempfile  # noqa: E402

N = 20000


def body(block):
    return None


def main():
    common.banner("progress bar output when stderr is a file")
    total, read, write = common.grid_1d(N)
    t = common.task("t", total, read, write, body, workers=4, read_write_conflict=False, timeout=60)
    capture = tempfile.NamedTemporaryFile(delete=False, suffix=".stderr")
    saved = os.dup(2)
    os.dup2(capture.fileno(), 2)
    try:
        common.run(t)
    finally:
        os.dup2(saved, 2)
    data = open(capture.name, "rb").read()
    print(f"stderr captured: {len(data):,} bytes, {data.count(b'\r'):,} carriage returns, {data.count(b'\n')} newlines")


if __name__ == "__main__":
    common.main_guard(main)
