import logging
import luigi
import peach
import numpy as np

# logging.shutdown()
# reload(logging)

logging.basicConfig(level=logging.INFO)
# logging.getLogger('peach.tasks').setLevel(logging.DEBUG)

# test: write the sum of read_roi entries to each entry in write_roi
global_db = np.ones((109,))
global_done = np.zeros((109,))

class TestDoneTarget(luigi.Target):

    def __init__(self, write_roi):
        self.write_roi = write_roi

    def exists(self):
        return global_done[
            self.write_roi.get_begin()[0]:self.write_roi.get_end()[0]
        ].sum() == self.write_roi.size()

class TestTask(peach.BlockTask):

    factor = luigi.IntParameter()

    def run(self):
        print("Running TestTask for %s"%self.read_roi)

        # sum over read ROI
        s = global_db[
            self.read_roi.get_begin()[0]:self.read_roi.get_end()[0]
        ].sum()

        # store in write ROI
        global_db[
            self.write_roi.get_begin()[0]:self.write_roi.get_end()[0]
        ] = s*self.factor

        # mark as done
        global_done[
            self.write_roi.get_begin()[0]:self.write_roi.get_end()[0]
        ] = 1

    def output(self):
        return TestDoneTarget(self.write_roi)

if __name__ == "__main__":

    # total_roi: |--------|
    #           100      109
    #
    # block: rrwwr|
    #        0    5
    #
    #            |--------|
    # L0:        rrwwr|
    #                rrwwr|
    # L1:          rrwwr|

    total_roi = peach.Roi((100,), (9,))
    read_roi = peach.Roi((0,), (5,))
    write_roi = peach.Roi((2,), (2,))

    process_blocks = peach.ProcessBlocks(
        total_roi,
        read_roi,
        write_roi,
        TestTask,
        {'factor': 4})

    luigi.build([process_blocks])
