import logging
import subprocess
import sys

import daisy
import pytest
from daisy.logging import set_log_basedir
from filelock import FileLock

logging.basicConfig(level=logging.DEBUG)


# `num_workers` is the daisy 1.x spelling, accepted by the top-level `daisy`
# namespace (`daisy.v1_compat`); `max_workers` is the v2 spelling.
@pytest.mark.parametrize("workers_keyword", ["num_workers", "max_workers"])
def test_workers_close(tmp_path, workers_keyword):
    set_log_basedir(tmp_path)
    num_workers = 5

    def start_worker():
        subprocess.run(
            [sys.executable, "tests/daisy_compat/process_block.py", f"{tmp_path}"]
        )

    task = daisy.Task(
        "test_server_task",
        total_roi=daisy.Roi((0,), (42,)),
        read_roi=daisy.Roi((0,), (10,)),
        write_roi=daisy.Roi((1,), (8,)),
        process_function=start_worker,
        check_function=None,
        read_write_conflict=True,
        fit="valid",
        max_retries=2,
        timeout=None,
        **{workers_keyword: num_workers},
    )

    server = daisy.Server()
    server.run_blockwise([task])

    # every worker held its lock while it ran and has released it
    for i in range(num_workers):
        lock = FileLock(f"{tmp_path}/worker_{i}.lock", timeout=1.0)
        with lock:
            assert lock.is_locked
