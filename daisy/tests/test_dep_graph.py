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
    process_function=None,
    check_function=None,
)

medium_task = Task(
    task_id="medium",
    total_roi=total_rois[1],
    read_roi=read_roi,
    write_roi=write_roi,
    process_function=None,
    check_function=None,
)

large_task = Task(
    task_id="large",
    total_roi=total_rois[2],
    read_roi=read_roi,
    write_roi=write_roi,
    process_function=None,
    check_function=None,
)


@pytest.mark.parametrize("task", [small_task, medium_task, large_task])
def test_startup_time(benchmark, task):
    def init_and_request_first_block(task):
        scheduler = Scheduler([task])
        return scheduler.acquire_block(task.task_id)

    benchmark(init_and_request_first_block, task)


tasks = [(small_task, 64), (medium_task, 729), (large_task, 2_744)]


@pytest.mark.parametrize("task", tasks)
def test_iterate_time(benchmark, task):
    def init_and_iterate_all_blocks(task, block_count):
        scheduler = Scheduler([task])
        for i in range(block_count):
            block = scheduler.acquire_block(task.task_id)
            assert block is not None, f"Failed to get the {i}'th block!"
            block.status = BlockStatus.SUCCESS
            scheduler.release_block(block)

        extra_block = scheduler.acquire_block(task.task_id)
        assert extra_block is None, f"{i+1}'th block is not None!"

    task, block_count = task
    benchmark(init_and_iterate_all_blocks, task, block_count)
