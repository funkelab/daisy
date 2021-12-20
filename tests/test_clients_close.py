import daisy
from daisy.logging import set_log_basedir

import pytest

import logging
import subprocess
import sys
from filelock import FileLock

logging.basicConfig(level=logging.DEBUG)


# @pytest.mark.skip()
def test_workers_close(tmp_path):
    set_log_basedir(tmp_path)
    num_workers = 5

    def start_worker():
        subprocess.run([sys.executable, "tests/process_block.py", f"{tmp_path}"])

    task = daisy.Task(
        "test_server_task",
        total_roi=daisy.Roi((0,), (42,)),
        read_roi=daisy.Roi((0,), (10,)),
        write_roi=daisy.Roi((1,), (8,)),
        process_function=start_worker,
        check_function=None,
        read_write_conflict=True,
        fit="valid",
        num_workers=num_workers,
        max_retries=2,
        timeout=None,
    )

    server = daisy.Server()
    server.run_blockwise([task])

    for i in range(num_workers):
        with FileLock(f"{tmp_path}/worker_{i}.lock", timeout=0.1):
            pass
