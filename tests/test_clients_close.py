import daisy

import logging
import subprocess

logging.basicConfig(level=logging.DEBUG)


def start_worker():
    subprocess.run(["python", "tests/process_block.py"])


def test_basic():

    task = daisy.Task(
        "test_server_task",
        total_roi=daisy.Roi((0,), (42,)),
        read_roi=daisy.Roi((0,), (10,)),
        write_roi=daisy.Roi((1,), (8,)),
        process_function=start_worker,
        check_function=None,
        read_write_conflict=True,
        fit="valid",
        num_workers=5,
        max_retries=2,
        timeout=None,
    )

    server = daisy.Server()
    server.run_blockwise([task])
