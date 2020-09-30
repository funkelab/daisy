import daisy
import unittest
import logging

logging.basicConfig(level=logging.DEBUG)


class DummyTaskState:

    def __init__(self, p, r):
        self.num_pending_blocks = p
        self.num_ready_blocks = r


class DummyScheduler:

    def __init__(self, tasks, num_blocks):
        self.tasks = tasks
        self.num_blocks = num_blocks
        print("DummyScheduler got some tasks!")

    def get_ready_tasks(self):
        return self.tasks

    def acquire_block(self, task_id):

        if self.num_blocks == 0:
            return None, DummyTaskState(0, 0)

        self.num_blocks -= 1

        block = daisy.Block(
            total_roi=self.tasks[0].total_roi,
            read_roi=self.tasks[0].read_roi + (self.num_blocks,),
            write_roi=self.tasks[0].write_roi + (self.num_blocks,),
            status=daisy.BlockStatus.PROCESSING)
        task_state = DummyTaskState(self.num_blocks, 1)

        return block, task_state

    def release_block(self, block):

        return {self.tasks[0].task_id: DummyTaskState(self.num_blocks, 1)}

class TestServer(unittest.TestCase):

    def test_basic(self):

        task = daisy.Task(
                'test_server_task',
                total_roi=daisy.Roi((0,), (100,)),
                read_roi=daisy.Roi((0,), (10,)),
                write_roi=daisy.Roi((1,), (8,)),
                process_function=lambda b: self.process_block(b),
                check_function=None,
                read_write_conflict=True,
                fit='valid',
                num_workers=2,
                max_retries=2,
                timeout=None)

        scheduler = DummyScheduler([task], 3)

        server = daisy.Server()
        server.run_blockwise([task], scheduler)

    def process_block(self, block):
        print("Processing block %s" % block)
