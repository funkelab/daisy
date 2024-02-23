from __future__ import absolute_import
from .freezable import Freezable
from enum import Enum
from funlib.math import cantor_number
import copy


class BlockStatus(Enum):
    CREATED = 0
    PROCESSING = 1
    SUCCESS = 2
    FAILED = 3


class Block(Freezable):
    """Describes a block to process with attributes:

    Attributes:

        read_roi (`class:Roi`):

            The region of interest (ROI) to read from.

        write_roi (`class:Roi`):

            The region of interest (ROI) to write to.

        status (``BlockStatus``):

            Stores the processing status of the block. Block status should be
            updated as it goes through the lifecycle of scheduler to client and
            back.

        block_id (``int``):

            A unique ID for this block (within all blocks tiling the total ROI
            to process).

        task_id (``int``):

            The id of the Task that this block belongs to.

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

        task_id (``int``, optional):

            The id of the Task that this block belongs to. Defaults to None.

    """

    def __init__(self, total_roi, read_roi, write_roi, block_id=None, task_id=None):

        self.read_roi = read_roi
        self.write_roi = write_roi
        self.requested_write_roi = write_roi  # save original write_roi

        self.task_id = task_id
        if block_id is None:
            block_id = self.__compute_block_id(total_roi, write_roi)
        self.block_id = (task_id, block_id)
        self.status = BlockStatus.CREATED
        self.started_processing = None
        self.stopped_processing = None
        self.freeze()

    def copy(self):

        return copy.deepcopy(self)

    def __compute_block_id(self, total_roi, write_roi, shift=None):
        block_index = (write_roi.offset - total_roi.offset) / write_roi.shape

        # block_id will be the cantor number for this block index
        block_id = int(cantor_number(block_index))

        return block_id

    def __repr__(self):

        return "%s/%d with read ROI %s and write ROI %s" % (
            self.block_id[0],
            self.block_id[1],
            self.read_roi,
            self.write_roi,
        )

    def __eq__(self, other):
        return other is not None and self.block_id == other.block_id

    def __hash__(self):
        return hash(self.block_id)
