import logging
import luigi
import numpy as np
import peach
import random
import time

logging.basicConfig(level=logging.INFO)
logging.getLogger('peach.tasks').setLevel(logging.DEBUG)

class TestDoneTarget(luigi.Target):

    def __init__(self, block_id):
        self.block_id = block_id

    def exists(self):

        print("Testing if block with ID %s succeeded"%self.block_id)

        with open('test_db_done.dat', 'r') as f:
            f.seek(self.block_id, 0)
            done = f.read(1)

        return done == '1'

class TestTask(peach.BlockTask):

    factor = luigi.IntParameter()

    def run(self):
        print("Running TestTask for %s"%self.read_roi)

        time.sleep(random.random()*5)

        # mark as done
        with open('test_db_done.dat', 'r+') as f:
            f.seek(self.get_block_id(), 0)
            f.write('1')

    def output(self):
        return TestDoneTarget(self.get_block_id())

    def requires(self):
        return [TestDependency()]

class TestDependency(luigi.WrapperTask):
    pass

total_roi = peach.Roi((0, 0, 0), (20, 20, 20))
read_roi = peach.Roi((0, 0, 0), (10, 5, 7))
write_roi = peach.Roi((2, 1, 2), (6, 1, 2))

if __name__ == "__main__":

    # the shared "data base"
    with open('test_db_done.dat', 'w') as f:
        f.write('0'*total_roi.size())

    process_blocks = peach.ProcessBlocks(
        total_roi,
        read_roi,
        write_roi,
        TestTask,
        {'factor': 9})

    luigi.build([process_blocks], log_level='INFO', workers=50)
