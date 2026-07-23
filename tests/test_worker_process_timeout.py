"""True timeout preemption for `Task(worker_processes=True, timeout=...)`.

In default (thread) mode, `timeout` reclaims a slow block but cannot stop
the thread running it: the stale attempt runs to completion, can double-
apply its effects concurrently with the retry, and the run waits on it at
shutdown (see tests/test_block_timeout.py for the documented limitation).

With worker processes, the child self-terminates at the deadline
(EXIT_BLOCK_TIMEOUT), so the stuck attempt is *gone*: no late writes, no
double-apply, no waiting for a sleeper.
"""

import time

import pytest

import daisy


@pytest.mark.timeout(30)
def test_stuck_block_is_preempted_not_awaited(tmp_path):
    first_attempt = tmp_path / "first_attempt_started"
    late_write = tmp_path / "late_write"  # only a SURVIVING sleeper writes it

    def slow_first(block):
        if not first_attempt.exists():
            first_attempt.touch()
            time.sleep(5)  # stuck attempt; deadline is 0.5s
            late_write.touch()  # thread mode reaches this; process mode dies

    task = daisy.Task(
        task_id="preempt",
        total_roi=daisy.Roi([0], [10]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=slow_first,
        read_write_conflict=False,
        max_workers=2,
        max_retries=2,
        timeout=0.5,
        worker_processes=True,
    )

    t0 = time.perf_counter()
    server = daisy.Server()
    states = server.run_blockwise([task], progress=False)
    wall = time.perf_counter() - t0

    state = states["preempt"]
    assert state.is_done()
    assert state.completed_count == 1, "retry should have completed the block"
    assert first_attempt.exists(), "first (stuck) attempt never ran"
    # the whole point: the sleeper was killed, not awaited
    assert wall < 4.0, f"run waited on the stuck attempt ({wall:.1f}s)"
    time.sleep(0.3)  # would-be late write lands within sleep+epsilon if alive
    assert not late_write.exists(), (
        "stuck attempt survived its deadline and wrote output after the "
        "block had been retried"
    )
    # the preempted worker is accounted as a dirty exit (visible, capped)
    assert state.worker_failure_count >= 1


@pytest.mark.timeout(30)
def test_fast_blocks_unaffected_by_timeout(tmp_path):
    """Watchdog arms/cancels around every block without disturbing normal
    completion."""

    def quick(block):
        (tmp_path / str(block.block_id[1])).touch()

    task = daisy.Task(
        task_id="preempt-fast",
        total_roi=daisy.Roi([0], [80]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=quick,
        read_write_conflict=False,
        max_workers=2,
        timeout=5.0,
        worker_processes=True,
    )
    assert daisy.run_blockwise([task], progress=False)
    assert len([f for f in tmp_path.iterdir() if f.name.isdigit()]) == 8
