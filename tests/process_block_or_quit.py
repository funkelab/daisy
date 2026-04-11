"""Worker that exits early on first invocation, loops normally on subsequent.

The first worker to start creates a marker file and processes only one block
before exiting normally (code 0). Subsequent workers see the marker and
process all blocks in a loop. This forces a normal-exit worker to leave
while blocks are still pending, triggering the reap-replace path.
"""

import daisy

import os
import sys
import time

from filelock import FileLock

tmp_path = sys.argv[1]
marker = os.path.join(tmp_path, "first_worker_done")
counter = os.path.join(tmp_path, "worker_count")
lock = os.path.join(tmp_path, "count.lock")

# Atomically increment spawn counter and check if first worker
with FileLock(lock):
    count = int(open(counter).read())
    open(counter, "w").write(str(count + 1))
    is_first = not os.path.exists(marker)
    if is_first:
        open(marker, "w").write("done")

client = daisy.Client()

if is_first:
    # Process exactly one block then exit normally
    with client.acquire_block() as block:
        pass
else:
    # Process all blocks, but slowly — give the reap cycle time to
    # notice the first worker exited while blocks are still pending
    while True:
        with client.acquire_block() as block:
            if block is None:
                break
            time.sleep(0.5)
