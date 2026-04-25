"""Worker script that crashes on first invocation, works on subsequent ones.

Uses a marker file to track whether a crash has already occurred. The first
worker to run creates the marker and exits via SystemExit (which bypasses
the normal exception handling in daisy's Worker._spawn_wrapper). Subsequent
workers see the marker and process blocks normally.
"""

import gerbera as daisy

import os
import sys

tmp_path = sys.argv[1]
marker = os.path.join(tmp_path, "worker_crashed")

if not os.path.exists(marker):
    # First worker: create marker and crash
    with open(marker, "w") as f:
        f.write("crashed")
    raise SystemExit(1)

# Subsequent workers: process blocks normally
client = daisy.Client()

while True:
    with client.acquire_block() as block:
        if block is None:
            break
