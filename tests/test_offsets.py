import logging
import luigi
import peach
import numpy as np
import time

logging.basicConfig(level=logging.INFO)
# logging.getLogger('peach.tasks').setLevel(logging.DEBUG)

class TestDoneTarget(luigi.Target):

    def __init__(self, write_roi):
        self.write_roi = write_roi

    def exists(self):

        start = self.write_roi.get_begin()[0]
        size = self.write_roi.size()

        print("Testing if block with write ROI %s succeeded"%self.write_roi)
        print("Reading %d bytes from %d"%(size, start))

        with open('test_db_done.dat', 'r') as f:
            f.seek(start, 0)
            done = f.read(size)

        print("Read %s from test_db_done.dat (should be all ones)"%done)

        return done == '1'*size

class TestTask(peach.BlockTask):

    factor = luigi.IntParameter()

    def run(self):
        print("Running TestTask for %s"%self.read_roi)

        # read some, write some, should be conflict free
        with open('test_db.dat', 'r+') as f:

            print("Reading %d bytes from %d"%(self.read_roi.size(),
                self.read_roi.get_begin()[0]))
            f.seek(self.read_roi.get_begin()[0], 0)
            read = f.read(self.read_roi.size())

            print("Writing %d bytes to %d"%(self.write_roi.size(),
                self.write_roi.get_begin()[0]))
            f.seek(self.write_roi.get_begin()[0], 0)
            f.write(('%d'%self.level)*self.write_roi.size())

        time.sleep(0.5)

        # mark as done
        with open('test_db_done.dat', 'r+') as f:
            f.seek(self.write_roi.get_begin()[0], 0)
            f.write('1'*self.write_roi.size())

        time.sleep(0.5)

    def output(self):
        return TestDoneTarget(self.write_roi)

    def requires(self):
        return [TestDependency()]

class TestDependency(luigi.WrapperTask):
    pass

if __name__ == "__main__":

    # total_roi: |------...--|
    #           100         190
    #
    # block: rrrwwrrrrr|
    #        0  3 5    10
    #
    #            |----------------------...---|
    # L0:        rrrwwrrrrr|     rrrwwrrrrr|
    #                    rrrwwrrrrr|
    # L1:          rrrwwrrrrr|     rrrwwrrrrr|
    #                      rrrwwrrrrr|
    # L2:            rrrwwrrrrr|     rrrwwrrrrr|
    #                        rrrwwrrrrr|
    # L3:              rrrwwrrrrr|     rrrwwrrrrr|
    #                          rrrwwrrrrr|

    # the shared "data base"
    with open('test_db.dat', 'w') as f:
        f.write('1'*190)
    with open('test_db_done.dat', 'w') as f:
        f.write('0'*190)

    total_roi = peach.Roi((100,), (90,))
    read_roi = peach.Roi((0,), (10,))
    write_roi = peach.Roi((3,), (2,))

    process_blocks = peach.ProcessBlocks(
        total_roi,
        read_roi,
        write_roi,
        TestTask,
        {'factor': 9})

    luigi.build([process_blocks], log_level='INFO', workers=4)
