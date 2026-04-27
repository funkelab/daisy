"""Port of daisy's test_scheduler.py — all 13 scheduler tests.

Block IDs and ordering match daisy exactly, using the same funlib-compatible
generalized Cantor pairing function.
"""

from gerbera import Scheduler, Task, Block, BlockStatus, Roi
import pytest


def process_block(block):
    pass


@pytest.fixture
def task_zero_levels():
    """Minimum stride for independent write blocks is larger than total_read_roi."""
    return Task(
        task_id="test_zero_levels",
        total_roi=Roi([0, 0], [10, 10]),
        read_roi=Roi([0, 0], [9, 9]),
        write_roi=Roi([4, 4], [1, 1]),
        process_function=process_block,
        fit="overhang",
        max_retries=0,
    )


@pytest.fixture
def task_1d():
    # block ids: 1, 2
    # 2  ->  Level 1
    # 1  ->  Level 2
    return Task(
        task_id="test_1d",
        total_roi=Roi([0], [4]),
        read_roi=Roi([0], [3]),
        write_roi=Roi([1], [1]),
        process_function=process_block,
    )


@pytest.fixture
def task_no_conflicts():
    return Task(
        task_id="test_1d",
        total_roi=Roi([0], [4]),
        read_roi=Roi([0], [3]),
        write_roi=Roi([1], [1]),
        read_write_conflict=False,
        process_function=process_block,
    )


@pytest.fixture
def task_2d():
    # block ids (grid):
    # 4,  7,  11, 16
    # 8,  12, 17, 23
    # 13, 18, 24, 31
    # 19, 25, 32, 40
    #
    # Level 1: 12, 23, 25, 40
    # Level 2: 8, 17, 19, 32
    # Level 3: 7, 16, 18, 31
    # Level 4: 4, 11, 13, 24
    return Task(
        task_id="test_2d",
        total_roi=Roi([0, 0], [6, 6]),
        read_roi=Roi([0, 0], [3, 3]),
        write_roi=Roi([1, 1], [1, 1]),
        process_function=process_block,
    )


@pytest.fixture
def chained_task():
    first_task = Task(
        task_id="first",
        total_roi=Roi([0, 0], [6, 6]),
        read_roi=Roi([0, 0], [3, 3]),
        write_roi=Roi([1, 1], [1, 1]),
        process_function=process_block,
    )
    second_task = Task(
        task_id="second",
        total_roi=Roi([1, 1], [4, 4]),
        read_roi=Roi([0, 0], [3, 3]),
        write_roi=Roi([1, 1], [1, 1]),
        process_function=process_block,
        upstream_tasks=[first_task],
    )
    return first_task, second_task


@pytest.fixture
def overlapping_tasks():
    task_1 = Task(
        "task1",
        total_roi=Roi([0], [100]),
        read_roi=Roi([0], [10]),
        write_roi=Roi([1], [8]),
        process_function=process_block,
        read_write_conflict=True,
        fit="valid",
        max_workers=1,
        max_retries=2,
    )
    task_2 = Task(
        "task2",
        total_roi=Roi([0], [100]),
        read_roi=Roi([0], [10]),
        write_roi=Roi([1], [8]),
        process_function=process_block,
        read_write_conflict=True,
        fit="valid",
        max_workers=1,
        max_retries=2,
        upstream_tasks=[task_1],
    )
    return task_1, task_2


def test_simple_no_conflicts(task_no_conflicts):
    scheduler = Scheduler([task_no_conflicts])

    block = scheduler.acquire_block(task_no_conflicts.task_id)
    expected = Block(Roi([0], [3]), Roi([0], [3]), Roi([1], [1]), task_id="test_1d", block_id=1)
    assert block.read_roi == expected.read_roi
    assert block.write_roi == expected.write_roi
    assert block.block_id == expected.block_id

    block = scheduler.acquire_block(task_no_conflicts.task_id)
    expected = Block(Roi([1], [3]), Roi([1], [3]), Roi([2], [1]), task_id="test_1d", block_id=2)
    assert block.read_roi == expected.read_roi
    assert block.write_roi == expected.write_roi
    assert block.block_id == expected.block_id


def test_simple_acquire_block(task_1d):
    scheduler = Scheduler([task_1d])
    block = scheduler.acquire_block(task_1d.task_id)

    expected = Block(Roi([1], [3]), Roi([1], [3]), Roi([2], [1]), task_id="test_1d", block_id=2)
    assert block.read_roi == expected.read_roi
    assert block.write_roi == expected.write_roi
    assert block.block_id == expected.block_id


def test_retries(task_1d):
    scheduler = Scheduler([task_1d])

    block_1 = scheduler.acquire_block(task_1d.task_id)
    block_1.status = BlockStatus.FAILED
    scheduler.release_block(block_1)

    block_1_retry = scheduler.acquire_block(task_1d.task_id)
    assert block_1.block_id == block_1_retry.block_id
    block_1_retry.status = BlockStatus.SUCCESS
    scheduler.release_block(block_1_retry)

    for _ in range(task_1d.max_retries + 1):
        block_2 = scheduler.acquire_block(task_1d.task_id)
        assert block_1.block_id != block_2.block_id
        block_2.status = BlockStatus.FAILED
        scheduler.release_block(block_2)

    assert scheduler.acquire_block(task_1d.task_id) is None
    assert scheduler.task_states[task_1d.task_id].completed_count == 1
    assert scheduler.task_states[task_1d.task_id].failed_count == 1


def test_orphan_count(task_1d):
    scheduler = Scheduler([task_1d])

    for _ in range(task_1d.max_retries + 1):
        block = scheduler.acquire_block(task_1d.task_id)
        block.status = BlockStatus.FAILED
        scheduler.release_block(block)

    assert scheduler.acquire_block(task_1d.task_id) is None
    assert scheduler.task_states[task_1d.task_id].completed_count == 0
    assert scheduler.task_states[task_1d.task_id].failed_count == 1
    assert scheduler.task_states[task_1d.task_id].orphaned_count == 1


def test_simple_release_block(task_1d):
    scheduler = Scheduler([task_1d])
    block = scheduler.acquire_block(task_1d.task_id)
    block.status = BlockStatus.SUCCESS
    scheduler.release_block(block)
    block = scheduler.acquire_block(task_1d.task_id)

    expected = Block(Roi([0], [3]), Roi([0], [3]), Roi([1], [1]), task_id="test_1d", block_id=1)
    assert block.read_roi == expected.read_roi
    assert block.write_roi == expected.write_roi
    assert block.block_id == expected.block_id


def test_complete_task(task_2d):
    scheduler = Scheduler([task_2d])

    for level_ids in [(12, 23, 25, 40), (8, 17, 19, 32), (7, 16, 18, 31), (4, 11, 13, 24)]:
        blocks = []
        for b in level_ids:
            block = scheduler.acquire_block(task_2d.task_id)
            block.status = BlockStatus.SUCCESS
            assert block.block_id == ("test_2d", b), (
                f"expected ('test_2d', {b}), got {block.block_id}"
            )
            blocks.append(block)

        assert scheduler.acquire_block(task_2d.task_id) is None

        for block in blocks:
            scheduler.release_block(block)

    assert scheduler.task_states[task_2d.task_id].is_done()


def test_downstream(chained_task):
    first_task, second_task = chained_task
    scheduler = Scheduler([second_task])

    test_block = Block(
        Roi([1, 1], [4, 4]),
        Roi([2, 2], [3, 3]),
        Roi([3, 3], [1, 1]),
        task_id=second_task.task_id,
    )

    upstream_blocks = scheduler.dependency_graph.upstream(test_block)
    upstream_ids = set(block.block_id for block in upstream_blocks)
    assert upstream_ids == {
        ("first", 12),
        ("first", 17),
        ("first", 23),
        ("first", 18),
        ("first", 24),
        ("first", 31),
        ("first", 25),
        ("first", 32),
        ("first", 40),
    }


def test_upstream(chained_task):
    first_task, second_task = chained_task
    scheduler = Scheduler([second_task])

    test_block = Block(
        Roi([0, 0], [6, 6]),
        Roi([2, 2], [3, 3]),
        Roi([3, 3], [1, 1]),
        task_id=first_task.task_id,
    )

    downstream_blocks = scheduler.dependency_graph.downstream(test_block)
    downstream_ids = set(block.block_id for block in downstream_blocks)
    assert downstream_ids == {
        ("second", 4),
        ("second", 7),
        ("second", 8),
        ("second", 12),
    }


def test_chained_tasks(chained_task):
    first_task, second_task = chained_task
    scheduler = Scheduler([second_task])

    # Level 1
    blocks = []
    for b in (12, 23, 25, 40):
        block = scheduler.acquire_block(first_task.task_id)
        block.status = BlockStatus.SUCCESS
        blocks.append(block)
    for block in blocks:
        scheduler.release_block(block)
    assert scheduler.acquire_block(second_task.task_id) is None

    # Level 2
    blocks = []
    for b in (8, 17, 19, 32):
        block = scheduler.acquire_block(first_task.task_id)
        block.status = BlockStatus.SUCCESS
        blocks.append(block)
    for block in blocks:
        scheduler.release_block(block)
    assert scheduler.acquire_block(second_task.task_id) is None

    # Level 3
    blocks = []
    for b in (7, 16, 18, 31):
        block = scheduler.acquire_block(first_task.task_id)
        block.status = BlockStatus.SUCCESS
        blocks.append(block)
    for block in blocks:
        scheduler.release_block(block)
    assert scheduler.acquire_block(second_task.task_id) is None

    # Level 4
    blocks = []
    for b in (4, 11, 13, 24):
        block = scheduler.acquire_block(first_task.task_id)
        block.status = BlockStatus.SUCCESS
        blocks.append(block)

    # releasing (first, 24) should free (second, 12)
    scheduler.release_block(blocks[-1])
    block = scheduler.acquire_block(second_task.task_id)
    block.status = BlockStatus.SUCCESS
    assert block.block_id == ("second", 12)
    scheduler.release_block(block)

    # releasing (first, 13) should free (second, 8)
    scheduler.release_block(blocks[-2])
    block = scheduler.acquire_block(second_task.task_id)
    block.status = BlockStatus.SUCCESS
    scheduler.release_block(block)

    # releasing (first, 11) should free (second, 7)
    scheduler.release_block(blocks[-3])
    block = scheduler.acquire_block(second_task.task_id)
    block.status = BlockStatus.SUCCESS
    assert block.block_id == ("second", 7)
    scheduler.release_block(block)

    # releasing (first, 4) should free (second, 4)
    scheduler.release_block(blocks[-4])
    assert scheduler.task_states[first_task.task_id].is_done()
    block = scheduler.acquire_block(second_task.task_id)
    block.status = BlockStatus.SUCCESS
    assert block.block_id == ("second", 4)
    scheduler.release_block(block)
    assert scheduler.task_states[second_task.task_id].is_done()


def test_overlapping_tasks(overlapping_tasks):
    first, second = overlapping_tasks
    scheduler = Scheduler([second])

    for _ in range(24):
        block = scheduler.acquire_block(first.task_id)
        if block is None:
            block = scheduler.acquire_block(second.task_id)
        assert block is not None, (
            f"{scheduler.task_states[first.task_id]}, "
            f"{scheduler.task_states[second.task_id]}"
        )
        block.status = BlockStatus.SUCCESS
        scheduler.release_block(block)

    assert scheduler.task_states[first.task_id].completed_count == 12
    assert scheduler.task_states[second.task_id].completed_count == 12


def test_zero_levels(task_zero_levels):
    scheduler = Scheduler([task_zero_levels])

    for _ in range(4):
        task_state = scheduler.task_states[task_zero_levels.task_id]
        assert task_state.ready_count == 1
        block = scheduler.acquire_block(task_zero_levels.task_id)
        assert block is not None
        block.status = BlockStatus.SUCCESS
        scheduler.release_block(block)

    block = scheduler.acquire_block(task_zero_levels.task_id)
    assert block is None


def test_zero_levels_failure(task_zero_levels):
    scheduler = Scheduler([task_zero_levels])

    task_state = scheduler.task_states[task_zero_levels.task_id]
    assert task_state.ready_count == 1
    block = scheduler.acquire_block(task_zero_levels.task_id)
    assert block is not None
    block.status = BlockStatus.FAILED
    scheduler.release_block(block)

    block = scheduler.acquire_block(task_zero_levels.task_id)
    assert block is None

    task_state = scheduler.task_states[task_zero_levels.task_id]
    assert (
        task_state.failed_count + task_state.orphaned_count
        == task_state.total_block_count
    ), str(task_state)


def test_orphan_double_counting():
    task = Task(
        task_id="test_orphans",
        total_roi=Roi([0, 0], [25, 25]),
        read_roi=Roi([0, 0], [7, 7]),
        write_roi=Roi([3, 3], [1, 1]),
        process_function=process_block,
        read_write_conflict=True,
    )
    scheduler = Scheduler([task])

    while True:
        block = scheduler.acquire_block(task.task_id)
        if block is None:
            break
        block.status = BlockStatus.FAILED
        scheduler.release_block(block)

    task_state = scheduler.task_states[task.task_id]
    assert (
        task_state.failed_count + task_state.orphaned_count
        == task_state.total_block_count
    ), str(task_state)
