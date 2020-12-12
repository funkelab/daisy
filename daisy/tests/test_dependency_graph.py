from daisy import DependencyGraph, BlockwiseDependencyGraph
from daisy import Task
from daisy import Block, BlockStatus
from daisy import Roi

import pytest

import logging

logger = logging.getLogger(__name__)

total_roi = Roi((0,), (11,))
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
    7,  # write roi should cover [2:9)
    4,  # wrote roi should cover [1:9)
    11, # write roi should cover [0:11)
    5,  # write roi should cover [0:10)
    7,  # write roi should cover [2:9) 
    5,  # write roi should cover []
]


@pytest.mark.parametrize("task_blocks", list(zip(tasks, num_blocks))[:1])
def test_downstream(task_blocks):
    task, num_blocks = task_blocks
    graph = BlockwiseDependencyGraph(
        task.task_id,
        task.total_roi,
        task.read_roi,
        task.write_roi,
        task.read_write_conflict,
        task.fit,
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
