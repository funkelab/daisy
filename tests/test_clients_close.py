import daisy

import pytest

import logging
import subprocess
import sys

logging.basicConfig(level=logging.DEBUG)


def start_worker():
    subprocess.run([sys.executable, "tests/process_block.py"])


@pytest.mark.skip()
def test_workers_close():

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
