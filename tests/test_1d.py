import logging
import luigi
import numpy as np
import daisy
import random
import time

logging.basicConfig(level=logging.INFO)
# logging.getLogger('daisy.tasks').setLevel(logging.DEBUG)

def process(read_roi, write_roi):

    print("Running TestTask for %s"%write_roi)

    # read some, write some, should be conflict free
    with open('test_db.dat', 'r+') as f:

        f.seek(read_roi.get_begin()[0], 0)
        read = f.read(read_roi.size())

        s = sum([ int(d) for d in read ])
        w = s%10

        f.seek(write_roi.get_begin()[0], 0)
        f.write(('%d'%w)*write_roi.size())

    time.sleep(random.random()*5)

    # mark as done
    with open('test_db_done.dat', 'r+') as f:
        f.seek(write_roi.get_begin()[0], 0)
        f.write('1'*write_roi.size())

def check(write_roi):

    start = write_roi.get_begin()[0]
    size = write_roi.size()

    with open('test_db_done.dat', 'r') as f:
        f.seek(start, 0)
        done = f.read(size)

    return done == '1'*size

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

    total_roi = daisy.Roi((100,), (90,))
    read_roi = daisy.Roi((0,), (10,))
    write_roi = daisy.Roi((3,), (2,))

    print("Running with dask:")
    daisy.run_with_dask(
        daisy.Roi((0,), (100,)),
        daisy.Roi((0,), (20,)),
        daisy.Roi((5,), (15,)),
        process,
        check,
        1) # this test only works with one worker, since we have global state

    # reset the "data base"
    with open('test_db.dat', 'w') as f:
        f.write('1'*190)
    with open('test_db_done.dat', 'w') as f:
        f.write('0'*190)

    print("Running with luigi:")
    daisy.run_with_luigi(
        daisy.Roi((0,), (100,)),
        daisy.Roi((0,), (20,)),
        daisy.Roi((5,), (15,)),
        process,
        check,
        1) # this test only works with one worker, since we have global state
