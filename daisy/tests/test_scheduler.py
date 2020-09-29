from daisy import Scheduler
from daisy import Task
from daisy import Block, BlockStatus
from daisy import Roi, Parameter

import pytest


@pytest.fixture
def task_1d(tmpdir):
    return Task1D(outdir=tmpdir)


class Task1D(Task):
    outdir = Parameter()

    def __init__(self, **kwargs):
        super().__init__(
            0,
            total_roi=Roi((0,), (4,)),
            read_roi=Roi((0,), (3,)),
            write_roi=Roi((1,), (1,)),
            process_function=None,
            check_function=None,
            **kwargs,
        )

    def requires(self):
        return []

    def prepare(self):
        pass


@pytest.fixture
def task_2d(tmpdir):
    return Task2D(outdir=tmpdir)


class Task2D(Task):
    outdir = Parameter()

    def __init__(self, **kwargs):
        super().__init__(
            0,
            total_roi=Roi((0, 0), (6, 6)),
            read_roi=Roi((0, 0), (3, 3)),
            write_roi=Roi((1, 1), (1, 1)),
            process_function=None,
            check_function=None,
            **kwargs,
        )

    def requires(self):
        return []

    def prepare(self):
        pass


def test_simple_acquire_block(task_1d):
    scheduler = Scheduler([task_1d], {})
    block = scheduler.acquire_block(task_1d)
    # NOTE: block id is based on the cantor number of a blocks index if
    # the total roi were tiled with cubes of size write_roi. Since we use
    # the total roi and not the total write roi, the first block may not
    # have index 0. In this case the total roi is (0, 4), but our first
    # write block has roi (1, 1) and thus index 1, wheras roi (0, 1) would
    # have index 0 but is never used.
    expected_block_id = 2
    expected_block = Block(
        Roi((1,), (3,)), Roi((1,), (3,)), Roi((2,), (1,)), block_id=expected_block_id
    )
    assert block.read_roi == expected_block.read_roi
    assert block.write_roi == expected_block.write_roi
    assert block.block_id == expected_block.block_id


def test_simple_release_block(task_2d):
    scheduler = Scheduler([task_2d], {})
    # expected block ordering:
    # a, b, c, d
    # e, f, g, h
    # i, j, k, l
    # m, n, o, p
    # 4, 7, 11, 16
    # 8, 12, 17, 23
    # 13, 18, 24, 31
    # 19, 25, 32, 40

    # if every block depends on its neighbors then there are 4 sets
    # of 4 blocks that are safe to run in parallel in the order (1,1), (0,1), (1,0), (0,0)
    # f, h, n, p
    # e, g, m, o
    # b, d, j, l
    # a, c, i, k

    # test first set: f, h, n, p
    blocks = []
    for b in (12, 23, 25, 40):
        block = scheduler.acquire_block(task_2d)
        assert block.block_id == b
        blocks.append(block)

    for block in blocks:
        scheduler.release_block(task_2d, block, BlockStatus.SUCCESS)

    # test second set: e, g, m, o
    blocks = []
    for b in (8, 17, 19, 32):
        block = scheduler.acquire_block(task_2d)
        assert block.block_id == b
        blocks.append(block)

    for block in blocks:
        scheduler.release_block(task_2d, block, BlockStatus.SUCCESS)

    # check first element of third set
    expected_block_id = 7
    block = scheduler.acquire_block(task_2d)
    assert block.block_id == expected_block_id
    scheduler.release_block(task_2d, block, BlockStatus.SUCCESS)

    # next should be the first element of fourth set
    expected_block_id = 4
    block = scheduler.acquire_block(task_2d)
    assert block.block_id == expected_block_id
    scheduler.release_block(task_2d, block, BlockStatus.SUCCESS)