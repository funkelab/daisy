"""Test that the server detects and replaces dead worker processes.

Workers can die silently (e.g., SIGKILL/OOM, SystemExit) without queuing an
error. Without dead worker detection, the server would hang forever waiting
for messages from workers that no longer exist.
"""

import logging
import os
import subprocess
import sys

import pytest

import daisy
from daisy.logging import set_log_basedir

logging.basicConfig(level=logging.DEBUG)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Task() renamed `num_workers` to `max_workers` in v2. "
        "See test_dead_worker_replacement_migrated."
    ),
)
def test_dead_worker_replacement(tmp_path):
    """Workers that exit via SystemExit are detected and replaced.

    The first batch of workers raises SystemExit (simulating an OOM kill or
    similar unrecoverable crash that bypasses normal exception handling).
    The dead worker detection logic replaces them, and the replacement
    workers complete the task successfully.
    """
    set_log_basedir(tmp_path)

    def start_worker():
        result = subprocess.run(
            [sys.executable, "tests/daisy_compat/process_block_or_die.py", str(tmp_path)]
        )
        if result.returncode != 0:
            raise SystemExit(result.returncode)

    task = daisy.Task(
        "test_dead_worker_task",
        total_roi=daisy.Roi((0,), (10,)),
        read_roi=daisy.Roi((0,), (10,)),
        write_roi=daisy.Roi((0,), (10,)),
        process_function=start_worker,
        check_function=None,
        read_write_conflict=False,
        fit="valid",
        num_workers=1,
        max_retries=2,
        timeout=None,
    )

    server = daisy.Server()
    task_states = server.run_blockwise([task])
    assert task_states[task.task_id].is_done(), task_states[task.task_id]

    assert os.path.exists(tmp_path / "worker_crashed"), (
        "Expected first worker to crash and leave a marker file"
    )


def test_dead_worker_replacement_migrated(tmp_path):
    """v2 equivalent: rename `num_workers` to `max_workers`."""
    set_log_basedir(tmp_path)

    def start_worker():
        result = subprocess.run(
            [sys.executable, "tests/daisy_compat/process_block_or_die.py", str(tmp_path)]
        )
        if result.returncode != 0:
            raise SystemExit(result.returncode)

    task = daisy.Task(
        "test_dead_worker_task",
        total_roi=daisy.Roi((0,), (10,)),
        read_roi=daisy.Roi((0,), (10,)),
        write_roi=daisy.Roi((0,), (10,)),
        process_function=start_worker,
        check_function=None,
        read_write_conflict=False,
        fit="valid",
        max_workers=1,
        max_retries=2,
        timeout=None,
    )

    server = daisy.Server()
    task_states = server.run_blockwise([task])
    assert task_states[task.task_id].is_done(), task_states[task.task_id]

    assert os.path.exists(tmp_path / "worker_crashed"), (
        "Expected first worker to crash and leave a marker file"
    )
