"""Test that the server detects and replaces dead worker processes.

Workers can die silently (e.g., SIGKILL/OOM, SystemExit) without queuing an
error. Without dead worker detection, the server would hang forever waiting
for messages from workers that no longer exist.
"""

import daisy
from daisy.logging import set_log_basedir
from daisy.worker_pool import WorkerPool
from unittest.mock import MagicMock

import logging
import os
import subprocess
import sys

logging.basicConfig(level=logging.DEBUG)


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
            [sys.executable, "tests/process_block_or_die.py", str(tmp_path)]
        )
        # Propagate subprocess exit code so the daisy worker process also
        # exits non-zero on crash (SystemExit bypasses _spawn_wrapper's
        # except Exception, producing a non-zero exitcode for reaping).
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

    # Verify the crash marker exists (first worker did crash)
    assert os.path.exists(tmp_path / "worker_crashed"), (
        "Expected first worker to crash and leave a marker file"
    )


def test_reap_distinguishes_normal_from_crash():
    """reap_dead_workers only removes crashed workers from the pool.

    Workers that exit normally (exitcode 0) stay in the dict with
    process=None so len(workers) stays at the target count. Only
    crashed workers (exitcode != 0) are removed and counted.
    """
    pool = WorkerPool(lambda: None)

    normal_worker = MagicMock()
    normal_worker.process.is_alive.return_value = False
    normal_worker.process.exitcode = 0
    normal_worker.process.pid = 1000

    crashed_worker = MagicMock()
    crashed_worker.process.is_alive.return_value = False
    crashed_worker.process.exitcode = 1
    crashed_worker.process.pid = 1001

    pool.workers = {0: normal_worker, 1: crashed_worker}

    reaped = pool.reap_dead_workers()

    # Only the crashed worker counts as reaped
    assert reaped == 1
    # Normal worker stays in dict with process=None
    assert 0 in pool.workers
    assert pool.workers[0].process is None
    # Crashed worker is removed
    assert 1 not in pool.workers
