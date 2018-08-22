import logging
import luigi
import numpy as np
import daisy
import random
import time

logging.basicConfig(level=logging.INFO)
logging.getLogger('daisy.tasks').setLevel(logging.DEBUG)

total_roi = daisy.Roi((0, 0, 0), (50, 50, 50))
read_roi = daisy.Roi((0, 0, 0), (10, 5, 7))
write_roi = daisy.Roi((2, 1, 2), (6, 1, 2))

def check(write_roi):

    block_id = 0
    f = 1
    for d in range(write_roi.dims()):
        block_id += write_roi.get_begin()[-1 - d]*f
        f *= total_roi.get_shape()[-1 - d]

    print("Testing if block with ID %s succeeded"%block_id)

    with open('test_db_done.dat', 'r') as f:
        f.seek(block_id, 0)
        done = f.read(1)

    return done == '1'

def process(read_roi, write_roi):

    block_id = 0
    f = 1
    for d in range(write_roi.dims()):
        block_id += write_roi.get_begin()[-1 - d]*f
        f *= total_roi.get_shape()[-1 - d]

    print("Running task with write ROI %s"%write_roi)

    time.sleep(random.random()*5)

    # mark as done
    with open('test_db_done.dat', 'r+') as f:
        f.seek(block_id, 0)
        f.write('1')

if __name__ == "__main__":

    # the shared "data base"
    with open('test_db_done.dat', 'w') as f:
        f.write('0'*total_roi.size())

    daisy.run_with_dask(
        total_roi,
        read_roi,
        write_roi,
        process,
        check,
        50)
