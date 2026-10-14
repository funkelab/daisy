"""Throughput of no-op blocks by worker mode and worker count. Prints one
CSV line per cell: version,mode,workers,blocks,seconds,blocks_per_second.

Usage: python throughput_by_mode.py [--modes A,B,C] [--workers 1,4,16,64]
                                     [--blocks 10000] [--csv out.csv]
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import common  # noqa: E402

import daisy  # noqa: E402

import argparse  # noqa: E402
import subprocess  # noqa: E402
import time  # noqa: E402


def body(block):
    return None


def worker_loop():
    client = daisy.Client()
    while True:
        with client.acquire_block() as block:
            if block is None:
                return
            body(block)


def launcher():
    subprocess.run([sys.executable, __file__, "--external-worker"], check=False)


def main():
    if "--external-worker" in sys.argv:
        worker_loop()
        return
    ap = argparse.ArgumentParser()
    ap.add_argument("--modes", default="A,B,C")
    ap.add_argument("--workers", default="1,4,16,64")
    ap.add_argument("--blocks", type=int, default=10000)
    ap.add_argument("--csv", default=None)
    a = ap.parse_args()
    common.banner(f"throughput, {a.blocks} no-op blocks")
    label = "2.0" if common.V2 else "1.x"
    out = open(a.csv, "a") if a.csv else None
    total, read, write = common.grid_1d(a.blocks)
    fns = {"A": body, "B": worker_loop, "C": launcher}
    print("version,mode,workers,blocks,seconds,blocks_per_second")
    for mode in a.modes.split(","):
        for w in (int(x) for x in a.workers.split(",")):
            t = common.task(f"bench_{mode}{w}", total, read, write, fns[mode], workers=w, read_write_conflict=False, timeout=600)
            t0 = time.monotonic()
            if common.V2:
                daisy.run_blockwise([t], progress=False, summary=False)
            else:
                import contextlib, io  # noqa: E401

                with contextlib.redirect_stdout(io.StringIO()):
                    daisy.run_blockwise([t])
            secs = time.monotonic() - t0
            line = f"{label},{mode},{w},{a.blocks},{secs:.2f},{a.blocks / secs:.0f}"
            print(line, flush=True)
            if out:
                out.write(line + "\n")
                out.flush()


if __name__ == "__main__":
    common.main_guard(main)
