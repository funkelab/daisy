"""Port of daisy's test_dependency_graph.py — 3 tests (6 parameterized variants)."""

from daisy import BlockwiseDependencyGraph, Task, Block, Roi
import pytest

total_roi = Roi([0], [12])
read_roi = Roi([0], [5])
write_roi_small = Roi([2], [1])
write_roi_medium = Roi([1], [3])

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


@pytest.mark.parametrize("task_blocks", list(zip(tasks, num_blocks)),
                         ids=names)
def test_block_enumeration(task_blocks):
    task, expected_num_blocks = task_blocks
    graph = BlockwiseDependencyGraph(
        task.task_id,
        task.read_roi,
        task.write_roi,
        True,  # read_write_conflict
        task.fit,
        total_read_roi=task.total_roi,
    )
    num_enumerated = len(graph.enumerate_all_dependencies())
    num_calculated = graph.num_blocks
    assert expected_num_blocks == num_enumerated, (
        f"enumerated ({num_enumerated}) != expected ({expected_num_blocks})"
    )
    assert expected_num_blocks == num_calculated, (
        f"calculated ({num_calculated}) != expected ({expected_num_blocks})"
    )


def test_get_subgraph_blocks():
    total_read_roi = Roi([0, 0], [16, 16])
    block_read_roi = Roi([0, 0], [4, 4])
    block_write_roi = Roi([1, 1], [2, 2])

    sub_roi = Roi([6, 6], [3, 3])

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
        Block(total_read_roi, Roi([4, 4], [4, 4]), Roi([5, 5], [2, 2]), task_id="test"),
        Block(total_read_roi, Roi([6, 4], [4, 4]), Roi([7, 5], [2, 2]), task_id="test"),
        Block(total_read_roi, Roi([4, 6], [4, 4]), Roi([5, 7], [2, 2]), task_id="test"),
        Block(total_read_roi, Roi([6, 6], [4, 4]), Roi([7, 7], [2, 2]), task_id="test"),
    ]

    assert set(b.block_id for b in blocks) == set(b.block_id for b in expected_blocks)


def test_shrink_downstream_upstream_equivalence():
    graph = BlockwiseDependencyGraph(
        "test",
        Roi([0], [7]),
        Roi([1], [5]),
        True,
        "shrink",
        total_read_roi=Roi([0], [100]),
    )

    remaining_blocks = set(b.block_id for b in graph.root_gen())
    root_blocks = {b.block_id: b for b in graph.root_gen()}

    visited = set()
    to_visit = list(root_blocks.values())
    while to_visit:
        b = to_visit.pop()
        if b.block_id in visited:
            continue
        visited.add(b.block_id)
        down_blocks = graph.downstream(b)
        for down_b in down_blocks:
            up_ids = set(u.block_id for u in graph.upstream(down_b))
            assert b.block_id in up_ids, (
                f"block {b.block_id} should be in upstream of {down_b.block_id}"
            )
            to_visit.append(down_b)
