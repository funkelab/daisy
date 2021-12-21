import daisy

import time
import random
import sys
import logging

from filelock import FileLock

logging.basicConfig(level=logging.DEBUG)

tmp_path = sys.argv[1]

client = daisy.Client()

with FileLock(f"{tmp_path}/worker_{client.worker_id}.lock"):
    while True:
        with client.acquire_block() as block:
            if block is None:
                break
            else:
                time.sleep(random.random())
