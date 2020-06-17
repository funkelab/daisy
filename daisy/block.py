from __future__ import absolute_import
from .coordinate import Coordinate
from .freezable import Freezable
from funlib.math import cantor_number
import copy


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

    Args:

        total_roi(`class:Roi`):

            The total ROI that the blocks are tiling, needed to find unique
            block IDs.

        read_roi (`class:Roi`):

            The region of interest (ROI) to read from.

        write_roi (`class:Roi`):

            The region of interest (ROI) to write to.

        block_id (``int``, optional):

            The ID to assign to this block. The ID is normally computed from
            the write ROI and the total ROI, such that each block has a unique
            ID.
    '''

    def __init__(self, total_roi, read_roi, write_roi, block_id=None):

        self.read_roi = read_roi
        self.write_roi = write_roi

        if block_id is None:
            self.block_id, self.z_order_id = self.__compute_block_id(
                    total_roi, write_roi)
        else:
            self.block_id = block_id
            self.z_order_id = block_id  # for compatibility
        self.freeze()

    def copy(self):

        return copy.deepcopy(self)

    def __compute_block_id(self, total_roi, write_roi, shift=None):
        block_index = write_roi.get_offset() / write_roi.get_shape()

        # block_id will be the cantor number for this block index
        block_id = int(cantor_number(block_index))

        # calculating Z-order index
        # this index is inexact and is shape agnostic to promote maximum
        # parallelism across tasks
        block_index_z = (write_roi.get_offset() /
                         Coordinate([2048 for _ in range(total_roi.dims())]))
        bit32_constant = 1 << 31
        indices = [int(block_index_z[i]) for i in range(total_roi.dims())]

        n = 0
        z_order_id = 0
        while n < 32:
            for i in range(total_roi.dims()):
                z_order_id = z_order_id >> 1
                if indices[i] & 1:
                    z_order_id += bit32_constant
                indices[i] = indices[i] >> 1
                n += 1

        return block_id, z_order_id

    def __repr__(self):

        return "id: %d (read_roi: %s, write_roi %s)" % (
            self.block_id,
            self.read_roi,
            self.write_roi)
