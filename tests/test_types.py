"""Tests for core types: Roi, Coordinate, Block — covering the API surface
used by daisy's test_tcp.py, test_client.py, and general type operations."""

from gerbera import Roi, Coordinate, Block, BlockStatus


def test_roi_basic():
    roi = Roi([10, 20], [30, 40])
    assert roi.begin == Coordinate([10, 20])
    assert roi.end == Coordinate([40, 60])
    assert roi.shape == Coordinate([30, 40])
    assert roi.dims == 2


def test_roi_equality():
    assert Roi([0, 0], [10, 10]) == Roi([0, 0], [10, 10])
    assert Roi([0, 0], [10, 10]) != Roi([1, 0], [10, 10])


def test_roi_contains_roi():
    outer = Roi([0, 0], [100, 100])
    inner = Roi([10, 10], [20, 20])
    assert outer.contains(inner)
    assert not inner.contains(outer)


def test_roi_contains_point():
    roi = Roi([10, 20], [30, 40])
    assert roi.contains(Coordinate([10, 20]))
    assert roi.contains(Coordinate([39, 59]))
    assert not roi.contains(Coordinate([40, 60]))  # exclusive end


def test_roi_grow():
    roi = Roi([10, 10], [20, 20])
    grown = roi.grow([5, 5], [5, 5])
    assert grown == Roi([5, 5], [30, 30])


def test_roi_intersect():
    a = Roi([0, 0], [20, 20])
    b = Roi([10, 10], [20, 20])
    i = a.intersect(b)
    assert i == Roi([10, 10], [10, 10])


def test_coordinate_basic():
    c = Coordinate([1, 2, 3])
    assert c.dims == 3
    assert len(c) == 3
    assert c[0] == 1
    assert c[1] == 2
    assert c[2] == 3
    assert c.to_list() == [1, 2, 3]


def test_coordinate_equality():
    assert Coordinate([1, 2]) == Coordinate([1, 2])
    assert Coordinate([1, 2]) != Coordinate([1, 3])


def test_block_construction():
    total = Roi([0, 0], [100, 100])
    read_roi = Roi([0, 0], [20, 20])
    write_roi = Roi([0, 0], [10, 10])
    block = Block(total, read_roi, write_roi, task_id="test")
    assert block.task_id == "test"
    assert block.read_roi == read_roi
    assert block.write_roi == write_roi
    assert isinstance(block.block_id, tuple)
    assert block.block_id[0] == "test"


def test_block_status():
    total = Roi([0], [10])
    block = Block(total, Roi([0], [5]), Roi([0], [5]), task_id="t")
    assert block.status == BlockStatus.CREATED
    block.status = BlockStatus.SUCCESS
    assert block.status == BlockStatus.SUCCESS
    block.status = BlockStatus.FAILED
    assert block.status == BlockStatus.FAILED


def test_block_equality():
    total = Roi([0, 0], [100, 100])
    b1 = Block(total, Roi([0, 0], [20, 20]), Roi([0, 0], [10, 10]), task_id="t")
    b2 = Block(total, Roi([0, 0], [20, 20]), Roi([0, 0], [10, 10]), task_id="t")
    assert b1 == b2
    assert hash(b1) == hash(b2)

    b3 = Block(total, Roi([10, 0], [20, 20]), Roi([10, 0], [10, 10]), task_id="t")
    assert b1 != b3


def test_block_set_operations():
    """Blocks should work in sets, matching daisy's usage."""
    total = Roi([0, 0], [100, 100])
    b1 = Block(total, Roi([0, 0], [20, 20]), Roi([0, 0], [10, 10]), task_id="t")
    b2 = Block(total, Roi([0, 0], [20, 20]), Roi([0, 0], [10, 10]), task_id="t")
    b3 = Block(total, Roi([10, 0], [20, 20]), Roi([10, 0], [10, 10]), task_id="t")

    s = {b1.block_id, b2.block_id, b3.block_id}
    assert len(s) == 2  # b1 and b2 are the same block
