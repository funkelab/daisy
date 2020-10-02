from __future__ import absolute_import
from .coordinate import Coordinate
from .freezable import Freezable
from funlib.math import cantor_number

import copy
from enum import Enum


class BlockStatus(Enum):
    CREATED = 0
    SUCCESS = 1
    FAILED = 2


class Block(Freezable):
    """Describes a block to process with attributes:

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
    """

    def __init__(self, total_roi, read_roi, write_roi, task_id, block_id=None):

        self.read_roi = read_roi
        self.write_roi = write_roi

        self.task_id = task_id
        if block_id is None:
            block_id = self.__compute_block_id(total_roi, write_roi)
        self.block_id = (task_id, block_id)
        self.status = BlockStatus.CREATED
        self.freeze()

    def copy(self):

        return copy.deepcopy(self)

    def __compute_block_id(self, total_roi, write_roi, shift=None):
        block_index = write_roi.get_offset() / write_roi.get_shape()

        # block_id will be the cantor number for this block index
        block_id = int(cantor_number(block_index))

        return block_id

    def __repr__(self):

        return "id: %s (read_roi: %s, write_roi %s)" % (
            self.block_id,
            self.read_roi,
            self.write_roi,
        )
