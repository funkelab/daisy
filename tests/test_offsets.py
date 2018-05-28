import logging
import luigi
import peach

logging.basicConfig(level=logging.INFO)
logging.getLogger('peach.tasks').setLevel(logging.DEBUG)

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

    process_blocks = peach.ProcessBlocks(total_roi, read_roi, write_roi)

    luigi.build([process_blocks])
