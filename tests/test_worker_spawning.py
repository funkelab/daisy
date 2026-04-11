"""Test that workers exiting normally are not endlessly respawned.

When workers finish processing their blocks and exit with code 0, the server
should not treat them as crashed and replace them. Without this fix, the
reap-replace cycle causes unbounded worker growth.
"""

import daisy
from daisy.logging import set_log_basedir

import logging
import subprocess
import sys

logging.basicConfig(level=logging.DEBUG)


def test_normal_exit_no_respawn(tmp_path):
    """Workers that exit normally are not replaced.

    Uses 4 blocks with 2 workers. The first worker to start processes
    exactly one block then exits normally (code 0) via
    process_block_or_quit.py, while blocks are still pending. The second
    worker loops normally.

    Without the fix, the exited worker is reaped and replaced, spawning
    a third worker. With the fix, the exited worker stays counted in the
    pool and no replacement is spawned.
    """
    set_log_basedir(tmp_path)

    counter = tmp_path / "worker_count"
    counter.write_text("0")

    def start_worker():
        subprocess.run([
            sys.executable, "tests/process_block_or_quit.py", str(tmp_path)
        ])

    task = daisy.Task(
        "test_no_respawn_task",
        total_roi=daisy.Roi((0,), (40,)),
        read_roi=daisy.Roi((0,), (10,)),
        write_roi=daisy.Roi((0,), (10,)),
        process_function=start_worker,
        check_function=None,
        read_write_conflict=False,
        fit="valid",
        num_workers=2,
        max_retries=2,
        timeout=None,
    )

    server = daisy.Server()
    task_states = server.run_blockwise([task])
    assert task_states[task.task_id].is_done(), task_states[task.task_id]

    total_workers = int(counter.read_text())
    assert total_workers <= 2, (
        f"Expected at most 2 workers, but {total_workers} were spawned. "
        "Normal worker exits are being treated as dead and replaced."
    )
