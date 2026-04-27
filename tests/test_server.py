"""Port of daisy's test_server.py — serial server execution test."""

from daisy import Task, Roi, run_blockwise


def process_block(block):
    pass


def test_serial_server():
    """Equivalent of daisy's test_basic with SerialServer."""
    task = Task(
        "test_server_task",
        total_roi=Roi([0], [100]),
        read_roi=Roi([0], [10]),
        write_roi=Roi([1], [8]),
        process_function=process_block,
        read_write_conflict=True,
        fit="valid",
        max_workers=1,
        max_retries=2,
    )

    result = run_blockwise([task], multiprocessing=False)
    assert result is True


def test_serial_server_2d():
    """2D task to exercise multi-dimensional scheduling."""
    task = Task(
        "test_2d",
        total_roi=Roi([0, 0], [30, 30]),
        read_roi=Roi([0, 0], [10, 10]),
        write_roi=Roi([0, 0], [10, 10]),
        process_function=process_block,
        read_write_conflict=False,
    )

    result = run_blockwise([task], multiprocessing=False)
    assert result is True


def test_serial_server_with_check():
    """Test that check_function skips already-done blocks."""
    processed = []

    def process(block):
        processed.append(block.block_id)

    def check(block):
        # Pretend block 0 is already done
        return block.block_id[1] == 0

    task = Task(
        "test_check",
        total_roi=Roi([0], [40]),
        read_roi=Roi([0], [10]),
        write_roi=Roi([0], [10]),
        process_function=process,
        check_function=check,
        read_write_conflict=False,
    )

    result = run_blockwise([task], multiprocessing=False)
    assert result is True
    # Block 0 should have been skipped
    assert all(bid[1] != 0 for bid in processed)
    assert len(processed) == 3


def test_serial_chained_tasks():
    """Test run_blockwise with chained/dependent tasks."""
    first = Task(
        "first",
        total_roi=Roi([0], [40]),
        read_roi=Roi([0], [10]),
        write_roi=Roi([0], [10]),
        process_function=process_block,
        read_write_conflict=False,
    )
    second = Task(
        "second",
        total_roi=Roi([0], [40]),
        read_roi=Roi([0], [10]),
        write_roi=Roi([0], [10]),
        process_function=process_block,
        read_write_conflict=False,
        upstream_tasks=[first],
    )

    result = run_blockwise([second], multiprocessing=False)
    assert result is True
