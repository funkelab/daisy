from daisy import Roi, Task, Scheduler, BlockStatus

import pytest

read_roi = Roi((0, 0, 0), (4, 4, 4))
write_roi = Roi((1, 1, 1), (2, 2, 2))
total_rois = [
    Roi((10, 10, 10), (10, 10, 10)),  # small roi (4x4x4) 64 write rois
    Roi((5, 5, 5), (20, 20, 20)),  # medium roi (9x9x9) 729 write rois
    Roi((0, 0, 0), (30, 30, 30)),  # large roi (14x14x14) 2,744 write rois
]

small_task = Task(
    task_id="small",
    total_roi=total_rois[0],
    read_roi=read_roi,
    write_roi=write_roi,
    process_function=lambda: None,
    check_function=None,
)

medium_task = Task(
    task_id="medium",
    total_roi=total_rois[1],
    read_roi=read_roi,
    write_roi=write_roi,
    process_function=lambda: None,
    check_function=None,
)

large_task = Task(
    task_id="large",
    total_roi=total_rois[2],
    read_roi=read_roi,
    write_roi=write_roi,
    process_function=lambda: None,
    check_function=None,
)

tasks = {
    "small": (small_task, 64),
    "medium": (medium_task, 729),
    "large": (large_task, 2_744),
}


@pytest.mark.parametrize("test", ["iterate", "init"])
@pytest.mark.parametrize("size", ["small", "medium", "large"])
@pytest.mark.parametrize("block_gen", ["lazy", "enumerated"])
def benchmark_dep_graph(benchmark, test, size, block_gen):
    task = tasks[size]
    lazy = block_gen == "lazy"
    iterate = test == "iterate"

    def benchmark_scheduler(task, block_count):
        scheduler = Scheduler([task], lazy=lazy)
        for i in range(block_count):
            block = scheduler.acquire_block(task.task_id)
            assert block is not None, f"Failed to get the {i}'th block!"
            block.status = BlockStatus.SUCCESS
            scheduler.release_block(block)
            if not iterate:
                return

        extra_block = scheduler.acquire_block(task.task_id)
        assert extra_block is None, f"{i+1}'th block is not None!"

    task, block_count = task
    benchmark(benchmark_scheduler, task, block_count)
