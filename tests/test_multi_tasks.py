import daisy

import pytest

import glob
import os
import logging

logger = logging.getLogger(__name__)
# daisy.scheduler._NO_SPAWN_STATUS_THREAD = True



def process_block(tmpdir, block, fail=None):

    logger.debug("Processing block %s", block)

    if block.block_id == fail:
        raise RuntimeError("intended failure")

    path = os.path.join(tmpdir, "%d.block" % block.block_id)
    with open(path, "w") as f:
        f.write(str(block.block_id))

    # print(block.read_roi.get_begin())
    # print(block.write_roi.get_begin()[0])
    path = os.path.join(tmpdir, "%d.write_roi" % block.write_roi.get_begin()[0])
    with open(path, "w") as f:
        f.write(str(block.write_roi))


def process_block_null(block):
    return 0


def worker(tmpdir, fail=None):

    client = daisy.Client()

    while True:

        with client.acquire_block() as block:
            if block is None:
                break

            process_block(tmpdir, block, fail)

        client.release_block(block, 0)

@pytest.fixture()
def leaf_task(tmpdir):
    yield daisy.Task(
        task_id="leaf_task",
        total_roi=daisy.Roi((0,), (10,)),
        read_roi=daisy.Roi((0,), (1,)),
        write_roi=daisy.Roi((0,), (1,)),
        # process_function=TestMultipleTasks.process_block,
        process_function=lambda: worker(tmpdir),
        max_retries=0,
        fit="shrink",
    )

@pytest.fixture()
def leaf_task_another(tmpdir):
    yield daisy.Task(
        task_id="leaf_task_another",
        total_roi=daisy.Roi((20,), (10,)),
        read_roi=daisy.Roi((0,), (1,)),
        write_roi=daisy.Roi((0,), (1,)),
        # process_function=TestMultipleTasks.process_block,
        process_function=lambda: worker(tmpdir),
        max_retries=0,
        fit="shrink",
    )

@pytest.fixture()
def parent_task(tmpdir, leaf_task):
    yield daisy.Task(
        task_id="parent_task",
        total_roi=daisy.Roi((0,), (10,)),
        read_roi=daisy.Roi((0,), (1,)),
        write_roi=daisy.Roi((0,), (1,)),
        # process_function=TestMultipleTasks.process_block,
        process_function=lambda: worker(tmpdir),
        max_retries=0,
        fit="shrink",
        upstream_tasks=[leaf_task],
    )

@pytest.fixture()
def parent_task_another(tmpdir, leaf_task):
    yield daisy.Task(
        task_id="parent_task_another",
        total_roi=daisy.Roi((0,), (10,)),
        read_roi=daisy.Roi((0,), (1,)),
        write_roi=daisy.Roi((0,), (1,)),
        # process_function=TestMultipleTasks.process_block,
        process_function=lambda: worker(tmpdir),
        max_retries=0,
        fit="shrink",
        upstream_tasks=[leaf_task],
    )

@pytest.fixture()
def task_write_roi_22(tmpdir):
    yield daisy.Task(
        task_id="task_write_roi_22",
        total_roi=daisy.Roi((1,), (10,)),
        read_roi=daisy.Roi((0,), (2,)),
        write_roi=daisy.Roi((0,), (2,)),
        # process_function=TestMultipleTasks.process_block,
        process_function=lambda: worker(tmpdir),
        max_retries=0,
        fit="shrink",
    )

@pytest.fixture()
def task_write_roi_2(tmpdir):
    yield daisy.Task(
        task_id="task_write_roi_2",
        total_roi=daisy.Roi((0,), (10,)),
        read_roi=daisy.Roi((0,), (2,)),
        write_roi=daisy.Roi((0,), (2,)),
        # process_function=TestMultipleTasks.process_block,
        process_function=lambda: worker(tmpdir),
        max_retries=0,
        fit="shrink",
    )


def test_single(leaf_task, tmpdir):
    """Tests a vanilla task"""

    # this task generates 0-10

    expected_block_ids = list(range(10))

    ret = daisy.distribute([leaf_task])

    outfiles = glob.glob(os.path.join(tmpdir, "*.block"))
    block_ids = sorted([int(path.split("/")[-1].split(".")[0]) for path in outfiles])

    assert ret
    assert block_ids == expected_block_ids


def test_single_with_request(leaf_task, tmpdir):
    """Tests a task with request for a subset ROI"""

    ret = daisy.distribute([leaf_task])

    outfiles = glob.glob(os.path.join(tmpdir, "*.block"))
    block_ids = sorted([int(path.split("/")[-1].split(".")[0]) for path in outfiles])

    assert ret
    expected_block_ids = list(range(3, 5))
    assert block_ids == expected_block_ids


def test_null_request(leaf_task, tmpdir):
    """Tests a task with request for null ROI"""

    ret = daisy.distribute([leaf_task])

    outfiles = glob.glob(os.path.join(tmpdir, "*.block"))
    block_ids = sorted([int(path.split("/")[-1].split(".")[0]) for path in outfiles])

    assert ret
    expected_block_ids = []
    assert block_ids == expected_block_ids


def test_multi(parent_task, tmpdir):
    """Tests multiple different task targets"""

    ret = daisy.distribute([parent_task])
    expected_block_ids += list(range(20, 30))

    outfiles = glob.glob(os.path.join(tmpdir, "*.block"))
    block_ids = sorted([int(path.split("/")[-1].split(".")[0]) for path in outfiles])

    assert ret
    assert block_ids == expected_block_ids


def test_multi_with_request(leaf_task, leaf_task_another, tmpdir):
    """Tests multiple different task targets with requests"""

    task0_spec = {"task": leaf_task, "request": [daisy.Roi((3,), (2,))]}
    expected_block_ids = list(range(3, 5))
    task1_spec = {"task": leaf_task_another, "request": [daisy.Roi((27,), (2,))]}
    expected_block_ids += list(range(27, 29))

    ret = daisy.distribute([task0_spec, task1_spec])

    outfiles = glob.glob(os.path.join(tmpdir, "*.block"))
    block_ids = sorted([int(path.split("/")[-1].split(".")[0]) for path in outfiles])

    assert ret
    assert block_ids == expected_block_ids


def test_multi_with_request_same(leaf_task, tmpdir):
    """Tests multiple same task targets with requests"""

    task0_spec = {"task": leaf_task, "request": [daisy.Roi((3,), (2,))]}
    expected_block_ids = list(range(3, 5))
    task1_spec = {"task": leaf_task, "request": [daisy.Roi((7,), (1,))]}
    expected_block_ids += list(range(7, 8))

    ret = daisy.distribute([task0_spec, task1_spec])

    outfiles = glob.glob(os.path.join(tmpdir, "*.block"))
    block_ids = sorted([int(path.split("/")[-1].split(".")[0]) for path in outfiles])

    assert ret
    assert block_ids == expected_block_ids


def test_multi_with_request_same_overlapping(leaf_task, tmpdir):
    """Tests multiple same task targets with overlapping requests"""

    task0_spec = {"task": leaf_task, "request": [daisy.Roi((3,), (7,))]}
    task1_spec = {"task": leaf_task, "request": [daisy.Roi((5,), (5,))]}
    expected_block_ids = list(range(3, 10))

    ret = daisy.distribute([task0_spec, task1_spec])

    outfiles = glob.glob(os.path.join(tmpdir, "*.block"))
    block_ids = sorted([int(path.split("/")[-1].split(".")[0]) for path in outfiles])

    assert ret
    assert block_ids == expected_block_ids


def test_request_outside_error(leaf_task):
    """Tests request that lies outside of total_roi"""

    task0_spec = {"task": leaf_task, "request": [daisy.Roi((3,), (17,))]}

    with pytest.raises(RuntimeError):
        daisy.distribute([task0_spec])


def test_task_chain(parent_task, tmpdir):
    """Tests vanilla task chain"""

    # this task generates 0-10
    task0_spec = {"task": parent_task}
    expected_block_ids = list(range(0, 10))

    ret = daisy.distribute([task0_spec])

    outfiles = glob.glob(os.path.join(tmpdir, "*.block"))
    block_ids = sorted([int(path.split("/")[-1].split(".")[0]) for path in outfiles])

    assert ret
    assert block_ids == expected_block_ids


def test_task_chain_with_request(parent_task, tmpdir):
    """Tests task chain with request"""

    # this task generates 0-10
    task0_spec = {"task": parent_task, "request": [daisy.Roi((3,), (2,))]}
    expected_block_ids = list(range(3, 5))

    ret = daisy.distribute([task0_spec])

    outfiles = glob.glob(os.path.join(tmpdir, "*.block"))
    block_ids = sorted([int(path.split("/")[-1].split(".")[0]) for path in outfiles])

    assert ret
    assert block_ids == expected_block_ids


def test_task_chain_multi(parent_task, parent_task_another, tmpdir):
    """Tests multiple tasks with the same dependency"""

    # this task generates 0-10
    task0_spec = {"task": parent_task}
    # this task also generates 0-10
    task1_spec = {"task": parent_task_another}
    # their deps are merged
    expected_block_ids = list(range(0, 10))

    ret = daisy.distribute([task0_spec, task1_spec])

    outfiles = glob.glob(os.path.join(tmpdir, "*.block"))
    block_ids = sorted([int(path.split("/")[-1].split(".")[0]) for path in outfiles])

    assert ret
    assert block_ids == expected_block_ids


def test_task_chain_multi_with_request(parent_task, parent_task_another, tmpdir):
    """Tests multiple tasks with the same dependency and with request"""

    # this task generates 0-10
    task0_spec = {"task": parent_task, "request": [daisy.Roi((1,), (2,))]}
    # this task also generates 0-10
    task1_spec = {"task": parent_task_another, "request": [daisy.Roi((7,), (2,))]}
    # their deps are merged
    expected_block_ids = list(range(1, 3))
    expected_block_ids += list(range(7, 9))

    ret = daisy.distribute([task0_spec, task1_spec])

    outfiles = glob.glob(os.path.join(tmpdir, "*.block"))
    block_ids = sorted([int(path.split("/")[-1].split(".")[0]) for path in outfiles])

    assert ret
    assert block_ids == expected_block_ids


def test_task_chain_multi_with_overlapping_request(parent_task, parent_task_another, tmpdir):
    """Tests multiple tasks with the same dependency and with request"""

    # this task generates 0-10
    task0_spec = {"task": [parent_task], "request": [daisy.Roi((1,), (5,))]}
    # this task also generates 0-10
    task1_spec = {"task": parent_task_another, "request": [daisy.Roi((2,), (5,))]}
    # their deps are merged
    expected_block_ids = list(range(1, 7))

    ret = daisy.distribute([task0_spec, task1_spec])

    outfiles = glob.glob(os.path.join(tmpdir, "*.block"))
    block_ids = sorted([int(path.split("/")[-1].split(".")[0]) for path in outfiles])

    assert ret
    assert block_ids == expected_block_ids


def test_task_chain_multi_with_mixed_request(parent_task, parent_task_another, tmpdir):
    """Tests multiple tasks with the same dependency and with request"""

    # this task generates 0-10
    task0_spec = {"task": parent_task, "request": [daisy.Roi((1,), (2,))]}
    # this task also generates 0-10
    task1_spec = {"task": parent_task_another}
    # their deps are merged
    expected_block_ids = list(range(0, 10))

    ret = daisy.distribute([task0_spec, task1_spec])

    outfiles = glob.glob(os.path.join(tmpdir, "*.block"))
    block_ids = sorted([int(path.split("/")[-1].split(".")[0]) for path in outfiles])

    assert ret
    assert block_ids == expected_block_ids


def test_task_request_alignment(task_write_roi_2, tmpdir):
    """Tests multiple tasks with the same dependency and with request"""

    # this task generates 0-5
    task0_spec = {"task": task_write_roi_2}
    expected_block_ids = list(range(0, 5))

    ret = daisy.distribute([task0_spec])

    outfiles = glob.glob(os.path.join(tmpdir, "*.block"))
    block_ids = sorted([int(path.split("/")[-1].split(".")[0]) for path in outfiles])

    assert ret
    assert block_ids == expected_block_ids


def test_task_request_alignment_with_req(task_write_roi_2, tmpdir):
    """Tests multiple tasks with the same dependency and with request"""

    # this task generates 0-10
    task0_spec = {"task": task_write_roi_2, "request": [daisy.Roi((2,), (2,))]}
    # request lies in block 1
    expected_block_ids = list(range(1, 2))

    ret = daisy.distribute([task0_spec])

    outfiles = glob.glob(os.path.join(tmpdir, "*.block"))
    block_ids = sorted([int(path.split("/")[-1].split(".")[0]) for path in outfiles])

    assert ret
    assert block_ids == expected_block_ids


def test_task_request_alignment_with_req2(task_write_roi_2, tmpdir):
    """Tests multiple tasks with the same dependency and with request"""

    # this task generates 0-10
    task0_spec = {"task": task_write_roi_2, "request": [daisy.Roi((1,), (2,))]}
    # request lies between block 0 and block 1
    expected_block_ids = list(range(0, 2))

    ret = daisy.distribute([task0_spec])

    outfiles = glob.glob(os.path.join(tmpdir, "*.block"))
    block_ids = sorted([int(path.split("/")[-1].split(".")[0]) for path in outfiles])

    assert ret
    assert block_ids == expected_block_ids


def test_task_request_alignment_with_req3(task_write_roi_22, tmpdir):
    """Tests multiple tasks with the same dependency and with request"""

    # this task generates 0-10
    task0_spec = {"task": task_write_roi_22}
    # request lies between block 0 and block 1
    expected_write_begins = list(range(1, 11, 2))

    ret = daisy.distribute([task0_spec])

    outfiles = glob.glob(os.path.join(tmpdir, "*.write_roi"))
    block_ids = sorted([int(path.split("/")[-1].split(".")[0]) for path in outfiles])

    assert ret
    assert block_ids == expected_write_begins


def test_task_request_alignment_with_req4(task_write_roi_22, tmpdir):
    """Tests multiple tasks with the same dependency and with request"""

    # this task generates 0-10
    task0_spec = {"task": task_write_roi_22, "request": [daisy.Roi((1,), (2,))]}
    expected_write_begins = [1]

    ret = daisy.distribute([task0_spec])

    outfiles = glob.glob(os.path.join(tmpdir, "*.write_roi"))
    block_ids = sorted([int(path.split("/")[-1].split(".")[0]) for path in outfiles])

    assert ret
    assert block_ids == expected_write_begins


def test_task_request_alignment_with_req5(task_write_roi_22, tmpdir):
    """Tests multiple tasks with the same dependency and with request"""

    # this task generates 0-10
    task0_spec = {"task": task_write_roi_22, "request": [daisy.Roi((1,), (6,))]}
    # requesting 1, 2, 3, 4, 5, 6
    # is satisfied with 3 write blocks
    expected_write_begins = [1, 3, 5]

    ret = daisy.distribute([task0_spec])

    outfiles = glob.glob(os.path.join(tmpdir, "*.write_roi"))
    block_ids = sorted([int(path.split("/")[-1].split(".")[0]) for path in outfiles])

    assert ret
    assert block_ids == expected_write_begins

