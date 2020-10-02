from daisy import Scheduler
from daisy import Task
from daisy import Block, BlockStatus
from daisy import Roi

import pytest

import logging

logger = logging.getLogger(__name__)


@pytest.fixture
def task_1d(tmpdir):
    # block ids:
    # 1, 2

    # 2  ->  Level 1
    # 1  ->  Level 2
    return Task(
        task_id="test_1d",
        total_roi=Roi((0,), (4,)),
        read_roi=Roi((0,), (3,)),
        write_roi=Roi((1,), (1,)),
        process_function=None,
        check_function=None,
    )


@pytest.fixture
def task_2d(tmpdir):
    # block ids:
    # 4,  7,  11, 16
    # 8,  12, 17, 23
    # 13, 18, 24, 31
    # 19, 25, 32, 40

    # if every block depends on its neighbors then there are 4 sets
    # of 4 blocks that are safe to run in parallel in the order (1,1), (0,1), (1,0), (0,0)
    # 12, 23, 25, 40  ->  Level 1
    # 8,  17, 19, 32  ->  Level 2
    # 7,  16, 18, 31  ->  Level 3
    # 4,  11, 13, 24  ->  Level 4
    return Task(
        task_id="test_2d",
        total_roi=Roi((0, 0), (6, 6)),
        read_roi=Roi((0, 0), (3, 3)),
        write_roi=Roi((1, 1), (1, 1)),
        process_function=None,
        check_function=None,
    )


def test_simple_acquire_block(task_1d):
    scheduler = Scheduler([task_1d])
    block = scheduler.acquire_block(task_1d.task_id)

    expected_block = Block(
        Roi((1,), (3,)), Roi((1,), (3,)), Roi((2,), (1,)), task_id="test_1d", block_id=2
    )
    assert block.read_roi == expected_block.read_roi
    assert block.write_roi == expected_block.write_roi
    assert block.block_id == expected_block.block_id


def test_simple_release_block(task_1d):
    scheduler = Scheduler([task_1d])
    block = scheduler.acquire_block(task_1d.task_id)
    scheduler.release_block(block)
    block = scheduler.acquire_block(task_1d.task_id)

    expected_block = Block(
        Roi((0,), (3,)), Roi((0,), (3,)), Roi((1,), (1,)), task_id="test_1d", block_id=1
    )
    assert block.read_roi == expected_block.read_roi
    assert block.write_roi == expected_block.write_roi
    assert block.block_id == expected_block.block_id


def test_complete_task(task_2d):
    scheduler = Scheduler([task_2d])

    # test Level 1
    blocks = []
    for b in (12, 23, 25, 40):
        block = scheduler.acquire_block(task_2d.task_id)
        block.status = BlockStatus.SUCCESS
        assert block.block_id == ("test_2d", b)
        blocks.append(block)

    for block in blocks:
        scheduler.release_block(block)

    # test Level 2
    blocks = []
    for b in (8, 17, 19, 32):
        block = scheduler.acquire_block(task_2d.task_id)
        block.status = BlockStatus.SUCCESS
        assert block.block_id == ("test_2d", b)
        blocks.append(block)

    for block in blocks:
        scheduler.release_block(block)

    # test Level 3
    blocks = []
    for b in (7, 16, 18, 31):
        block = scheduler.acquire_block(task_2d.task_id)
        block.status = BlockStatus.SUCCESS
        assert block.block_id == ("test_2d", b)
        blocks.append(block)

    for block in blocks:
        scheduler.release_block(block)

    # test Level 4
    blocks = []
    for b in (4, 11, 13, 24):
        block = scheduler.acquire_block(task_2d.task_id)
        block.status = BlockStatus.SUCCESS
        assert block.block_id == ("test_2d", b)
        blocks.append(block)

    for block in blocks:
        scheduler.release_block(block)
