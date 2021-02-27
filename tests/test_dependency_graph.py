from daisy import DependencyGraph, BlockwiseDependencyGraph
from daisy import Task
from daisy import Block, BlockStatus
from daisy import Roi

import pytest

import logging

logger = logging.getLogger(__name__)

total_roi = Roi((0,), (12,))
read_roi = Roi((0,), (5,))
write_roi_small = Roi((2,), (1,))
write_roi_medium = Roi((1,), (3,))

names = [
    "small_valid",
    "medium_valid",
    "small_overhang",
    "medium_overhang",
    "small_shrink",
    "medium_shrink",
]
write_rois = [
    write_roi_small,
    write_roi_medium,
    write_roi_small,
    write_roi_medium,
    write_roi_small,
    write_roi_medium,
]
fits = ["valid", "valid", "overhang", "overhang", "shrink", "shrink"]
conflicts = [True, False]

tasks = [
    Task(
        name,
        total_roi,
        read_roi,
        write_roi,
        process_function=lambda b: None,
        read_write_conflict=True,
        fit=fit,
    )
    for name, write_roi, fit in zip(names, write_rois, fits)
]

num_blocks = [
    8,  # small_valid       write roi should cover [2:10)
    3,  # medium_valid      write roi should cover [1:10)
    8,  # small_overhang    write roi should cover [2:10)
    4,  # medium_overhang   write roi should cover [1:12)
    8,  # small_shrink      write roi should cover [2:10)
    4,  # medium_shrink     write roi should cover [1:11)
]


@pytest.mark.parametrize("task_blocks", list(zip(tasks, num_blocks)))
def test_downstream(task_blocks):
    task, num_blocks = task_blocks
    graph = BlockwiseDependencyGraph(
        task.task_id,
        task.read_roi,
        task.write_roi,
        task.read_write_conflict,
        task.fit,
        total_read_roi=task.total_roi,
    )
    num_enumerated = len(list(graph.enumerate_all_dependencies()))
    num_calculated = graph.num_blocks
    assert num_blocks == num_enumerated, (
        f"number of blocks enumerated ({num_enumerated}) does not match "
        f"expected number of blocks ({num_blocks})"
    )
    assert num_blocks == num_calculated, (
        f"number of blocks calculated ({num_calculated}) does not match "
        f"expected number of blocks ({num_blocks})"
    )


def process_block(block):
    pass


def test_get_subgraph_blocks():
    total_read_roi = Roi((0, 0), (16, 16))
    block_read_roi = Roi((0, 0), (4, 4))
    block_write_roi = Roi((1, 1), (2, 2))

    sub_roi = Roi((6, 6), (3, 3))

    graph = BlockwiseDependencyGraph(
        "test",
        block_read_roi,
        block_write_roi,
        True,
        "valid",
        total_read_roi=total_read_roi,
    )

    blocks = graph.get_subgraph_blocks(sub_roi)
    expected_blocks = [
        Block(
            total_read_roi,
            Roi((4, 4), (4, 4)),
            Roi((5, 5), (2, 2)),
            task_id="test",
        ),
        Block(
            total_read_roi,
            Roi((6, 4), (4, 4)),
            Roi((7, 5), (2, 2)),
            task_id="test",
        ),
        Block(
            total_read_roi,
            Roi((4, 6), (4, 4)),
            Roi((5, 7), (2, 2)),
            task_id="test",
        ),
        Block(
            total_read_roi,
            Roi((6, 6), (4, 4)),
            Roi((7, 7), (2, 2)),
            task_id="test",
        ),
    ]

    assert set(blocks) == set(expected_blocks)


def test_shrink_downstream_upstream_equivalence():

    total_roi = Roi((0,), (100,))
    read_roi = Roi((0,), (7,))
    write_roi = Roi((1,), (5,))

    graph = BlockwiseDependencyGraph(
        "test",
        read_roi,
        write_roi,
        True,
        "shrink",
        total_read_roi=total_roi,
    )

    # iterate through blocks and check downstream == upstream
    remaining_blocks = set(graph.root_gen())
    while len(remaining_blocks) > 0:
        b = remaining_blocks.pop()
        down_blocks = graph.downstream(b)
        for down_b in down_blocks:
            assert b in set(
                graph.upstream(down_b)
            ), f"{b in set(graph.upstream(down_b))}, {down_b in set(graph.downstream(b))}"
