"""Port of daisy's test_tcp.py and test_client.py.

Daisy's originals test raw TCP message passing and a mock server/client
handshake. Gerbera's TCP layer is Rust-internal and tested via Rust
integration tests (test_framing_roundtrip, test_server_client_*). These
Python tests verify the equivalent user-visible behavior: that blocks
flow correctly through the scheduler/server path and that the protocol
handles all message types.
"""

from gerbera import Task, Roi, Block, BlockStatus, Scheduler, run_blockwise
import pytest


def test_client_acquire_release():
    """Port of daisy's test_client.test_basic.

    Daisy's test spins up a mock TCP server in a subprocess, connects a
    Client, acquires a block, marks it SUCCESS, and releases it. We test
    the same flow through the Scheduler (which is what the server delegates to).
    """
    task = Task(
        "test_client",
        total_roi=Roi([0, 0, 0], [10, 10, 10]),
        read_roi=Roi([0, 0, 0], [10, 10, 10]),
        write_roi=Roi([0, 0, 0], [10, 10, 10]),
        process_function=lambda b: None,
        read_write_conflict=False,
    )

    scheduler = Scheduler([task])

    # Acquire
    block = scheduler.acquire_block("test_client")
    assert block is not None
    assert block.status == BlockStatus.CREATED

    # Process
    block.status = BlockStatus.SUCCESS

    # Release
    scheduler.release_block(block)

    # Task should be done (single block)
    assert scheduler.task_states["test_client"].is_done()
    assert scheduler.task_states["test_client"].completed_count == 1


def test_multiple_workers_complete():
    """Port of daisy's test_clients_close.test_workers_close.

    Daisy's test spawns 5 subprocess workers that each acquire blocks via
    TCP and verifies all workers ran. We verify the equivalent: all blocks
    are processed when simulating multiple workers acquiring blocks.
    """
    processed_blocks = []

    def process(block):
        processed_blocks.append(block.block_id)

    task = Task(
        "test_workers",
        total_roi=Roi([0], [42]),
        read_roi=Roi([0], [10]),
        write_roi=Roi([1], [8]),
        process_function=process,
        read_write_conflict=True,
        fit="valid",
        max_workers=5,
        max_retries=2,
    )

    result = run_blockwise([task], multiprocessing=False)
    assert result is True

    # All blocks should have been processed
    ts = len(processed_blocks)
    assert ts == 5, f"expected 5 blocks, got {ts}"


def test_block_failure_recovery():
    """Port of daisy's test_dead_workers.test_dead_worker_replacement.

    Daisy's test has a worker crash via SystemExit, then a replacement
    worker completes the blocks. We test the same retry logic through
    the Scheduler directly.
    """
    attempt_count = {}

    def flaky_process(block):
        bid = block.block_id
        attempt_count[bid] = attempt_count.get(bid, 0) + 1
        if attempt_count[bid] == 1 and bid[1] == 0:
            # Simulate crash on first attempt of block 0
            raise RuntimeError("simulated crash")

    task = Task(
        "test_recovery",
        total_roi=Roi([0], [20]),
        read_roi=Roi([0], [10]),
        write_roi=Roi([0], [10]),
        process_function=flaky_process,
        read_write_conflict=False,
        max_retries=3,
    )

    result = run_blockwise([task], multiprocessing=False)
    assert result is True

    # Block 0 should have been retried
    assert attempt_count[("test_recovery", 0)] >= 2


def test_no_message_after_shutdown():
    """Port of daisy's test_tcp.TestTCPConnections.test_single_connection.

    Daisy's test verifies: send message, receive reply, no extra messages
    in queue, disconnect works. We verify the analogous scheduler behavior:
    after all blocks are done, acquire returns None (no spurious blocks).
    """
    task = Task(
        "test_shutdown",
        total_roi=Roi([0], [10]),
        read_roi=Roi([0], [10]),
        write_roi=Roi([0], [10]),
        process_function=lambda b: None,
        read_write_conflict=False,
    )

    scheduler = Scheduler([task])

    block = scheduler.acquire_block("test_shutdown")
    block.status = BlockStatus.SUCCESS
    scheduler.release_block(block)

    # No more blocks — equivalent to "no more messages"
    assert scheduler.acquire_block("test_shutdown") is None
    assert scheduler.task_states["test_shutdown"].is_done()


def test_worker_normal_exit_no_respawn():
    """Port of daisy's test_worker_spawning.test_normal_exit_no_respawn.

    Daisy's test verifies that workers exiting normally (code 0) are NOT
    replaced, preventing unbounded worker growth. We verify the equivalent:
    once a task's blocks are exhausted, no extra blocks appear.
    """
    block_count = [0]

    def counting_process(block):
        block_count[0] += 1

    task = Task(
        "test_no_respawn",
        total_roi=Roi([0], [40]),
        read_roi=Roi([0], [10]),
        write_roi=Roi([0], [10]),
        process_function=counting_process,
        read_write_conflict=False,
        max_workers=2,
        max_retries=2,
    )

    result = run_blockwise([task], multiprocessing=False)
    assert result is True
    # Exactly 4 blocks, no extra processing from respawn cycles
    assert block_count[0] == 4
