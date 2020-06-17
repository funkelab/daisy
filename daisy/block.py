from __future__ import absolute_import
from .coordinate import Coordinate
from .freezable import Freezable
from funlib.math import cantor_number, inv_cantor_number
from .roi import Roi
import copy
import itertools


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
            self.block_id, self.z_order_id = self.compute_block_id(
                    total_roi, write_roi)
        else:
            self.block_id = block_id
            self.z_order_id = block_id  # for compatibility
        self.freeze()

    def copy(self):

        return copy.deepcopy(self)

    # Class variables to indicate fixes for cantor numbers.
    # Some datasets were processed with cantor number starting from 1
    # instead of from 0
    BLOCK_ID_ADD_ONE_FIX = None
    BLOCK_ID_SUB_ONE_FIX = None

    @staticmethod
    def index2id(block_index):

        block_id = int(cantor_number(block_index))
        if Block.BLOCK_ID_SUB_ONE_FIX:
            block_id = block_id - 1
        elif Block.BLOCK_ID_ADD_ONE_FIX:
            block_id = block_id + 1
        return block_id

    @staticmethod
    def id2index(block_id):

        if Block.BLOCK_ID_SUB_ONE_FIX:
            block_id = block_id + 1
        elif Block.BLOCK_ID_ADD_ONE_FIX:
            block_id = block_id - 1
        return inv_cantor_number(block_id)

    def compute_block_id(self, total_roi, write_roi, shift=None):

        block_index = write_roi.get_offset() / write_roi.get_shape()

        # block_id will be the cantor number for this block index
        block_id = Block.index2id(block_index)

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

    @staticmethod
    def get_chunks(
            block,
            chunk_div=None,
            chunk_shape=None):
        '''Convenient function to divide the given `block` into sub-blocks'''

        if chunk_shape is None:
            chunk_div = Coordinate(chunk_div)

            for j, k in zip(block.write_roi.get_shape(), chunk_div):
                assert (j % k) == 0

            chunk_shape = block.write_roi.get_shape() / Coordinate(chunk_div)

        else:
            chunk_shape = Coordinate(chunk_shape)

        write_offsets = itertools.product(*[
            range(
                block.write_roi.get_begin()[d],
                block.write_roi.get_end()[d],
                chunk_shape[d]
                )
            for d in range(chunk_shape.dims())
        ])

        context_begin = block.write_roi.get_begin()-block.read_roi.get_begin()
        context_end = block.read_roi.get_end()-block.write_roi.get_end()

        dummy_total_roi = Roi((0,)*chunk_shape.dims(),
                              (0,)*chunk_shape.dims())
        chunk_roi = Roi((0,)*chunk_shape.dims(), chunk_shape)
        blocks = []

        for write_offset in write_offsets:
            write_roi = chunk_roi.shift(write_offset).intersect(
                block.write_roi)
            read_roi = write_roi.grow(context_begin, context_end).intersect(
                block.read_roi)

            if not write_roi.empty():
                chunk = Block(dummy_total_roi,
                              read_roi,
                              write_roi)
                blocks.append(chunk)

        return blocks
