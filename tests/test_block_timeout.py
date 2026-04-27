"""Per-block processing timeout.

`Task(timeout=...)` sets the maximum wall-clock time the bookkeeper
allows a single block to spend in `processing` before reclaiming it.
The reclaimed block goes through the normal lost-block path: released
as Failed, eligible for retry under `max_retries`.

Timeout doesn't preempt the worker — daisy can't kill a thread
that's busy in Python code. The slow worker keeps running; its
eventual late `release_block` is silently dropped (the bookkeeper
no longer recognizes the block as in-flight). This is the cleanest
semantic threading allows; users who need hard preemption should
use 0-arg worker mode and put their own watchdog inside the loop.
"""

import time

import daisy


def test_slow_block_is_reclaimed_and_succeeds_on_retry():
    """A block whose first attempt exceeds the timeout is reclaimed and
    retried. On retry, it completes normally."""
    attempts = {}

    def maybe_slow(block):
        bid = block.block_id
        attempts[bid] = attempts.get(bid, 0) + 1
        # First attempt: take longer than the timeout. Retry: fast.
        if attempts[bid] == 1:
            time.sleep(1.5)

    task = daisy.Task(
        task_id="slow_first",
        total_roi=daisy.Roi([0], [10]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=maybe_slow,
        read_write_conflict=False,
        max_workers=1,
        max_retries=2,
        timeout=0.5,  # block deadline well under the slow path's sleep
    )

    server = daisy.Server()
    states = server.run_blockwise([task], progress=False)
    state = states["slow_first"]

    # The block was attempted at least twice (timeout reclaim + retry).
    # Eventually it succeeded — completed_count == 1, no permanent failure.
    assert state.is_done()
    assert state.completed_count == 1
    assert state.failed_count == 0


def test_persistently_slow_blocks_eventually_fail():
    """If every attempt times out, the block is permanently failed
    after `max_retries` reclaim cycles. The run still terminates."""
    def always_slow(block):
        time.sleep(1.0)

    task = daisy.Task(
        task_id="always_slow",
        total_roi=daisy.Roi([0], [10]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=always_slow,
        read_write_conflict=False,
        max_workers=1,
        max_retries=1,
        timeout=0.2,
        # Generous worker-restart cap so we exit via permanent failure
        # rather than abandonment (the block is what's broken, not
        # the worker — for this test we want the failure path, not
        # the abandon path).
        max_worker_restarts=20,
    )

    server = daisy.Server()
    states = server.run_blockwise([task], progress=False)
    state = states["always_slow"]
    assert state.is_done()
    # The single block hit max_retries via timeouts → permanent fail.
    assert state.failed_count == 1
    assert state.completed_count == 0


def test_no_timeout_lets_block_run_indefinitely():
    """With `timeout=None` (default), a slow block isn't reclaimed —
    the bookkeeper waits for the worker to release it normally."""
    def slow(block):
        time.sleep(0.3)

    task = daisy.Task(
        task_id="patient",
        total_roi=daisy.Roi([0], [10]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=slow,
        read_write_conflict=False,
        max_workers=1,
        max_retries=0,
        # no timeout
    )

    server = daisy.Server()
    states = server.run_blockwise([task], progress=False)
    state = states["patient"]
    assert state.is_done()
    assert state.completed_count == 1
    assert state.failed_count == 0
