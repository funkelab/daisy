import logging
import subprocess
import sys

import pytest
from filelock import FileLock

import daisy
from daisy.logging import set_log_basedir

logging.basicConfig(level=logging.DEBUG)


# Originally xfailed because v2 dropped `num_workers`; now passes
# natively against the top-level `daisy` namespace because that
# resolves to `daisy.v1_compat`, which accepts both `num_workers`
# and `max_workers`. The v2-native equivalent lives in
# `test_workers_close_migrated` below.
def test_workers_close(tmp_path):
    set_log_basedir(tmp_path)
    num_workers = 5

    def start_worker():
        subprocess.run([sys.executable, "tests/daisy_compat/process_block.py", f"{tmp_path}"])

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
        with FileLock(f"{tmp_path}/worker_{i}.lock", timeout=1.0):
            pass


def test_workers_close_migrated(tmp_path):
    """v2 equivalent: rename `num_workers` to `max_workers`."""
    set_log_basedir(tmp_path)
    num_workers = 5

    def start_worker():
        subprocess.run([sys.executable, "tests/daisy_compat/process_block.py", f"{tmp_path}"])

    task = daisy.Task(
        "test_server_task",
        total_roi=daisy.Roi((0,), (42,)),
        read_roi=daisy.Roi((0,), (10,)),
        write_roi=daisy.Roi((1,), (8,)),
        process_function=start_worker,
        check_function=None,
        read_write_conflict=True,
        fit="valid",
        max_workers=num_workers,
        max_retries=2,
        timeout=None,
    )

    server = daisy.Server()
    server.run_blockwise([task])

    for i in range(num_workers):
        with FileLock(f"{tmp_path}/worker_{i}.lock", timeout=1.0):
            pass
