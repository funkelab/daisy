import daisy
import unittest
import logging

logging.basicConfig(level=logging.DEBUG)


class TestServer(unittest.TestCase):

    def test_basic(self):

        task = daisy.Task(
            "test_server_task",
            total_roi=daisy.Roi((0,), (100,)),
            read_roi=daisy.Roi((0,), (10,)),
            write_roi=daisy.Roi((1,), (8,)),
            process_function=lambda b: self.process_block(b),
            check_function=None,
            read_write_conflict=True,
            fit="valid",
            num_workers=1,
            max_retries=2,
            timeout=None,
        )

        server = daisy.Server()
        server.run_blockwise([task])

    def process_block(self, block):
        print("Processing block %s" % block)
