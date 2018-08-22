import logging
import luigi
import numpy as np
import daisy
import random
import time

logging.basicConfig(level=logging.INFO)
logging.getLogger('daisy.blocks').setLevel(logging.DEBUG)

def process(read_roi, write_roi):

    print("Running TestTask for %s"%write_roi)

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

    # the shared "data base"
    with open('test_db.dat', 'w') as f:
        f.write('1'*644)
    with open('test_db_done.dat', 'w') as f:
        f.write('0'*644)

    total_roi = daisy.Roi((-52,), (696,))
    read_roi = daisy.Roi((-52,), (144,))
    write_roi = daisy.Roi((0,), (92,))

    print("Running with dask:")
    daisy.run_with_dask(
        total_roi,
        read_roi,
        write_roi,
        process,
        check,
        1) # this test only works with one worker, since we have global state
