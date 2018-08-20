from __future__ import absolute_import
from .freezable import Freezable

class Block(Freezable):
    '''Describes a block to process with attributes:

    Attributes:

        read_roi (`class:Roi`):

            The region of interest (ROI) to read from.

        write_roi (`class:Roi`):

            The region of interest (ROI) to write to.

        block_id (``int``):

            A unique ID for this block (within all blocks tiling the total ROI
            to process).

        requested_write_roi (`class:Roi`):

            The write ROI that was actually requested for this block.
            ``write_roi`` might differ if the block was shrunk at the boundary
            of the total ROI.

    Args:

        total_roi(`class:Roi`):

            The total ROI that the blocks are tiling, needed to find unique
            block IDs.

        read_roi (`class:Roi`):

            The region of interest (ROI) to read from.

        write_roi (`class:Roi`):

            The region of interest (ROI) to write to.
    '''

    def __init__(self, total_roi, read_roi, write_roi):

        self.read_roi = read_roi
        self.write_roi = write_roi
        self.requested_write_roi = write_roi.copy()

        self.__compute_block_id(total_roi, write_roi)
        self.freeze()

    def __compute_block_id(self, total_roi, write_roi):

        one = (1,)*total_roi.dims()
        # this is an upper bound on the number of blocks per dimension, the
        # actual number depends on the used fitting strategy
        num_blocks = (
            total_roi.get_shape() +
            write_roi.get_shape() - one
            )/write_roi.get_shape()
        block_index = write_roi.get_offset()/write_roi.get_shape()

        f = 1
        self.block_id = 0
        for d in range(total_roi.dims())[::-1]:
            self.block_id += block_index[d]*f
            f *= num_blocks[d]

    def __repr__(self):

        return "id: %d (read_roi: %s, write_roi %s)"%(
            self.block_id,
            self.read_roi,
            self.write_roi)
