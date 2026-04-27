import logging

import pytest

import daisy

logging.basicConfig(level=logging.DEBUG)


def process_block(block):
    print("Processing block %s" % block)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "daisy v2 has no SerialServer class. Serial mode is selected via "
        "`daisy.run_blockwise(..., multiprocessing=False)`. Task() also "
        "renamed `num_workers` to `max_workers`. See test_basic_migrated."
    ),
)
# Class names are looked up via getattr at test time so that the missing
# `SerialServer` symbol triggers the xfail-tracked failure during execution
# rather than at collection.
@pytest.mark.parametrize("server_class_name", ["Server", "SerialServer"])
def test_basic(server_class_name):
    server = getattr(daisy, server_class_name)()
    task = daisy.Task(
        "test_server_task",
        total_roi=daisy.Roi((0,), (100,)),
        read_roi=daisy.Roi((0,), (10,)),
        write_roi=daisy.Roi((1,), (8,)),
        process_function=process_block,
        check_function=None,
        read_write_conflict=True,
        fit="valid",
        num_workers=1,
        max_retries=2,
        timeout=None,
    )

    task_state = server.run_blockwise([task])
    assert task_state[task.task_id].is_done(), task_state[task.task_id]


@pytest.mark.parametrize("multiprocessing", [True, False])
def test_basic_migrated(multiprocessing):
    """v2 equivalent: parametrize over `multiprocessing` rather than two
    server classes, and use `max_workers` instead of `num_workers`."""
    task = daisy.Task(
        "test_server_task",
        total_roi=daisy.Roi((0,), (100,)),
        read_roi=daisy.Roi((0,), (10,)),
        write_roi=daisy.Roi((1,), (8,)),
        process_function=process_block,
        check_function=None,
        read_write_conflict=True,
        fit="valid",
        max_workers=1,
        max_retries=2,
        timeout=None,
    )
    assert daisy.run_blockwise(
        [task], multiprocessing=multiprocessing, progress=False
    )
