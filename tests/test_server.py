import daisy
import unittest
import logging

logging.basicConfig(level=logging.DEBUG)


def process_block(block):
    print("Processing block %s" % block)


def test_basic():

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

    server = daisy.Server()
    server.run_blockwise([task])
