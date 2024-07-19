import daisy
import pytest
import logging

logging.basicConfig(level=logging.DEBUG)


def process_block(block):
    print("Processing block %s" % block)


@pytest.mark.parametrize("server", [daisy.Server(), daisy.SerialServer()])
def test_basic(server):

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

    server.run_blockwise([task])
