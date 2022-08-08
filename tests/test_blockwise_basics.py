from __future__ import absolute_import

import daisy
import glob
import os
import logging

logger = logging.getLogger(__name__)
# daisy.scheduler._NO_SPAWN_STATUS_THREAD = True


def process_block(temp_dir, block, fail=None):

    logger.debug("Processing block %s", block)

    if block.block_id[1] == fail:
        raise RuntimeError("intended failure")

    path = os.path.join(temp_dir, "%d.block" % block.block_id[1])
    with open(path, "w") as f:
        f.write(str(block.block_id[1]))


def worker(temp_dir, fail=None):

    client = daisy.Client()

    while True:

        with client.acquire_block() as block:
            if block is None:
                break

            process_block(temp_dir, block, fail)


def test_callback(tmpdir):

    total_roi = daisy.Roi((0,), (100,))
    read_roi = daisy.Roi((0,), (5,))
    write_roi = daisy.Roi((0,), (3,))

    task = daisy.Task(
        "test_task",
        total_roi=total_roi,
        read_roi=read_roi,
        write_roi=write_roi,
        process_function=lambda b: process_block(tmpdir, b),
        num_workers=10,
    )
    ret = daisy.run_blockwise([task])

    outfiles = glob.glob(os.path.join(tmpdir, "*.block"))
    block_ids = sorted([int(path.split("/")[-1].split(".")[0]) for path in outfiles])

    assert ret
    assert block_ids == list(range(32))


def test_callback_failure(tmpdir):

    total_roi = daisy.Roi((0,), (100,))
    read_roi = daisy.Roi((0,), (5,))
    write_roi = daisy.Roi((0,), (3,))

    task = daisy.Task(
        "test_task",
        total_roi=total_roi,
        read_roi=read_roi,
        write_roi=write_roi,
        process_function=lambda b: process_block(tmpdir, b, fail=16),
        num_workers=10,
    )
    ret = daisy.run_blockwise([task])

    outfiles = glob.glob(os.path.join(tmpdir, "*.block"))
    block_ids = sorted([int(path.split("/")[-1].split(".")[0]) for path in outfiles])

    assert not ret
    expected_block_ids = list(range(32))
    expected_block_ids.remove(16)
    assert block_ids == expected_block_ids


def test_worker(tmpdir):

    total_roi = daisy.Roi((0,), (100,))
    read_roi = daisy.Roi((0,), (5,))
    write_roi = daisy.Roi((0,), (3,))

    task = daisy.Task(
        "test_task",
        total_roi=total_roi,
        read_roi=read_roi,
        write_roi=write_roi,
        process_function=lambda: worker(tmpdir),
        num_workers=10,
    )
    ret = daisy.run_blockwise([task])

    outfiles = glob.glob(os.path.join(tmpdir, "*.block"))
    block_ids = sorted([int(path.split("/")[-1].split(".")[0]) for path in outfiles])

    assert ret
    assert block_ids == list(range(32))


def test_worker_failure(tmpdir):

    total_roi = daisy.Roi((0,), (100,))
    read_roi = daisy.Roi((0,), (5,))
    write_roi = daisy.Roi((0,), (3,))

    task = daisy.Task(
        "test_task",
        total_roi=total_roi,
        read_roi=read_roi,
        write_roi=write_roi,
        process_function=lambda: worker(tmpdir, fail=16),
        num_workers=10,
    )
    ret = daisy.run_blockwise([task])

    outfiles = glob.glob(str(tmpdir / "*.block"))
    block_ids = sorted([int(path.split("/")[-1].split(".")[0]) for path in outfiles])

    assert not ret
    expected_block_ids = list(range(32))
    expected_block_ids.remove(16)
    assert block_ids == expected_block_ids


def test_negative_offset(tmpdir):

    logger.warning("A warning")

    total_roi = daisy.Roi((-100,), (2369,))
    block_write_roi = daisy.Roi((0,), (500,))
    block_read_roi = block_write_roi.grow((100,), (100,))

    task = daisy.Task(
        "test_task",
        total_roi,
        block_read_roi,
        block_write_roi,
        process_function=lambda b: process_block(tmpdir, b),
        num_workers=1,
        fit="shrink",
    )
    ret = daisy.run_blockwise([task])

    outfiles = glob.glob(os.path.join(tmpdir, "*.block"))
    block_ids = sorted([int(path.split("/")[-1].split(".")[0]) for path in outfiles])

    assert ret
    assert len(block_ids) == 5


def test_multidim(tmpdir):

    total_roi = daisy.Roi((199, -100, -100, -100), (12, 5140, 2248, 2369))
    block_write_roi = daisy.Roi((0, 0, 0, 0), (5, 500, 500, 500))
    block_read_roi = block_write_roi.grow((1, 100, 100, 100), (1, 100, 100, 100))

    task = daisy.Task(
        "test_task",
        total_roi,
        block_read_roi,
        block_write_roi,
        process_function=lambda b: process_block(tmpdir, b),
        num_workers=8,
        fit="shrink",
    )
    ret = daisy.run_blockwise([task])

    outfiles = glob.glob(os.path.join(tmpdir, "*.block"))
    block_ids = sorted([int(path.split("/")[-1].split(".")[0]) for path in outfiles])

    assert ret
    assert len(block_ids) == 500
