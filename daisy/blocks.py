from __future__ import absolute_import
from .block import Block
from .coordinate import Coordinate
from .roi import Roi
from itertools import product
import logging

logger = logging.getLogger(__name__)


class SingleTaskDependencyGraph:
    """Create a dependency graph as a list with elements::

        (block, [upstream_blocks])

    per block, where ``block`` is the block with exclusive write access to its
    ``write_roi`` (and save read access to its ``read_roi``), and
    ``upstream_blocks`` is a list of blocks that need to finish before this
    block can start (``[]`` if there are no upstream dependencies``).

    Args:

        total_roi (`class:daisy.Roi`):

            The region of interest (ROI) of the complete volume to process.

        block_read_roi (`class:daisy.Roi`):

            The ROI every block needs to read data from. Will be shifted over
            the ``total_roi``.

        block_write_roi (`class:daisy.Roi`):

            The ROI every block writes data from. Will be shifted over the
            ``total_roi`` in synchrony with ``block_read_roi``.

        read_write_conflict (``bool``, optional):

            Whether the read and write ROIs are conflicting, i.e., accessing
            the same resource. If set to ``False``, all blocks can run at the
            same time in parallel. In this case, providing a ``read_roi`` is
            simply a means of convenience to ensure no out-of-bound accesses
            and to avoid re-computation of it in each block.

        fit (``string``, optional):

            How to handle cases where shifting blocks by the size of
            ``block_write_roi`` does not tile the ``total_roi``. Possible
            options are:

            "valid": Skip blocks that would lie outside of ``total_roi``. This
            is the default::

                |---------------------------|     total ROI

                |rrrr|wwwwww|rrrr|                block 1
                    |rrrr|wwwwww|rrrr|         block 2
                                                no further block

            "overhang": Add all blocks that overlap with ``total_roi``, even if
            they leave it. Client code has to take care of save access beyond
            ``total_roi`` in this case.::

                |---------------------------|     total ROI

                |rrrr|wwwwww|rrrr|                block 1
                    |rrrr|wwwwww|rrrr|         block 2
                            |rrrr|wwwwww|rrrr|  block 3 (overhanging)

            "shrink": Like "overhang", but shrink the boundary blocks' read and
            write ROIs such that they are guaranteed to lie within
            ``total_roi``. The shrinking will preserve the context, i.e., the
            difference between the read ROI and write ROI stays the same.::

                |---------------------------|     total ROI

                |rrrr|wwwwww|rrrr|                block 1
                    |rrrr|wwwwww|rrrr|         block 2
                            |rrrr|www|rrrr|     block 3 (shrunk)
    """

    def __init__(
        self,
        total_roi,
        block_read_roi,
        block_write_roi,
        read_write_conflict=True,
        fit="valid",
    ):
        self.total_roi = total_roi
        self.block_read_roi = block_read_roi
        self.block_write_roi = block_write_roi
        self.read_write_conflict = read_write_conflict
        self.fit = fit

        # Computed values
        self.level_stride = self.compute_level_stride()
        self.level_offset = self.compute_level_offset()
        self.level_conflicts = self.compute_level_conflicts()

    def compute_level_conflicts(self):
        # create a list of conflict offsets for each level, that span the total
        # ROI

        level_conflict_offsets = []
        prev_level_offset = None

        for level, level_offset in enumerate(self.level_offsets):

            # get conflicts to previous level
            if prev_level_offset is not None and self.read_write_conflict:
                conflict_offsets = self.get_conflict_offsets(
                    level_offset, prev_level_offset, self.level_stride
                )
            else:
                conflict_offsets = []
            prev_level_offset = level_offset

            level_conflict_offsets.append(conflict_offsets)

        return level_conflict_offsets

    def create_dependency_graph(self):

        blocks = []

        for level_offset, level_conflicts in zip(
            self.level_offsets, self.level_conflicts
        ):

            # all block offsets of the current level (relative to total ROI
            # start)
            block_dim_offsets = [
                range(lo, e, s)
                for lo, e, s in zip(
                    level_offset, self.total_roi.get_shape(), self.level_stride
                )
            ]
            # TODO: can we do this part lazily? This might be a lot of
            # Coordinates
            block_offsets = [Coordinate(o) for o in product(*block_dim_offsets)]

            # convert to global coordinates
            block_offsets = [
                o + (self.total_roi.get_begin() - self.block_read_roi.get_begin())
                for o in block_offsets
            ]

            # TODO: can we do this part lazily? This might be a lot of
            # dependencies
            blocks += self.enumerate_dependencies(
                level_conflicts,
                block_offsets,
            )

        return blocks

    def compute_level_stride(self):
        """Get the stride that separates independent blocks in one level."""

        logger.debug(
            "Compute level stride for read ROI %s and write ROI %s.",
            self.block_read_roi,
            self.block_write_roi,
        )

        assert self.block_read_roi.contains(
            self.block_write_roi
        ), "Read ROI must contain write ROI."

        context_ul = self.block_write_roi.get_begin() - self.block_read_roi.get_begin()
        context_lr = self.block_read_roi.get_end() - self.block_write_roi.get_end()

        max_context = Coordinate(
            (max(ul, lr) for ul, lr in zip(context_ul, context_lr))
        )
        logger.debug("max context per dimension is %s", max_context)

        # this stride guarantees that blocks are independent, not be a
        # multiple of the write_roi shape. It would be difficult to tile
        # the output roi with blocks shifted by this min_level_stride
        min_level_stride = max_context + self.block_write_roi.get_shape()

        logger.debug("min level stride is %s", min_level_stride)

        # to avoid overlapping write ROIs, increase the stride to the next
        # multiple of write shape
        write_shape = self.block_write_roi.get_shape()
        level_stride = Coordinate(
            (
                ((level - 1) // w + 1) * w
                for level, w in zip(min_level_stride, write_shape)
            )
        )

        logger.debug("final level stride (multiples of write size) is %s", level_stride)

        return level_stride

    def compute_level_offsets(self):
        """Create a list of all offsets, such that blocks started with these
        offsets plus a multiple of level stride are mutually independent."""

        write_stride = self.block_write_roi.get_shape()

        logger.debug(
            "Compute level offsets for level stride %s and write stride %s.",
            self.level_stride,
            write_stride,
        )

        dim_offsets = [
            range(0, e, step) for e, step in zip(self.level_stride, write_stride)
        ]

        level_offsets = list(reversed([Coordinate(o) for o in product(*dim_offsets)]))

        logger.debug("level offsets: %s", level_offsets)

        return level_offsets

    def get_conflict_offsets(self, level_offset, prev_level_offset, level_stride):
        """Get the offsets to all previous level blocks that are in conflict
        with the current level blocks."""

        offset_to_prev = prev_level_offset - level_offset
        logger.debug("offset to previous level: %s", offset_to_prev)

        conflict_dim_offsets = [
            [op, op + ls] if op < 0 else [op - ls, op]
            for op, ls in zip(offset_to_prev, level_stride)
        ]

        conflict_offsets = [Coordinate(o) for o in product(*conflict_dim_offsets)]
        logger.debug("conflict offsets to previous level: %s", conflict_offsets)

        return conflict_offsets

    def enumerate_dependencies(self, conflict_offsets, block_offsets):

        inclusion_criteria = {
            "valid": lambda b: self.total_roi.contains(b.read_roi),
            "overhang": lambda b: self.total_roi.contains(b.write_roi.get_begin()),
            "shrink": lambda b: self.shrink_possible(b),
        }[self.fit]

        fit_block = {
            "valid": lambda b: b,  # noop
            "overhang": lambda b: b,  # noop
            "shrink": lambda b: self.shrink(b),
        }[self.fit]

        blocks = []

        for block_offset in block_offsets:

            # create a block shifted by the current offset
            block = Block(
                self.total_roi,
                self.block_read_roi + block_offset,
                self.block_write_roi + block_offset,
            )

            logger.debug("considering block: %s", block)

            if not inclusion_criteria(block):
                continue

            # get all blocks in conflict with the current block
            conflicts = []
            for conflict_offset in conflict_offsets:

                conflict = Block(
                    self.total_roi,
                    block.read_roi + conflict_offset,
                    block.write_roi + conflict_offset,
                )

                if not inclusion_criteria(conflict):
                    continue

                logger.debug("in conflict with block: %s", conflict)
                conflicts.append(fit_block(conflict))

            blocks.append((fit_block(block), conflicts))

        logger.debug("found blocks: %s", blocks)

        return blocks

    def shrink_possible(self, block):

        if not self.total_roi.contains(block.write_roi.get_begin()):
            return False

        # test if write roi would be non-empty
        b = self.shrink(self.total_roi, block)
        return all([s > 0 for s in b.write_roi.get_shape()])

    def shrink(self, block):
        """Ensure that read and write ROI are within total ROI by shrinking
        both. Size of context will be preserved."""

        r = self.total_roi.intersect(block.read_roi)
        w = block.write_roi.grow(
            block.read_roi.get_begin() - r.get_begin(),
            r.get_end() - block.read_roi.get_end(),
        )

        shrunk_block = block
        shrunk_block.read_roi = r
        shrunk_block.write_roi = w

        return shrunk_block

    def get_subgraph_blocks(self, sub_roi):
        """Return ids of blocks, as instantiated in the full graph, such that
        their total write rois fully cover `sub_roi`.
        The function API assumes that `sub_roi` and `total_roi` use absolute
        coordinates and `block_read_roi` and `block_write_roi` use relative
        coordinates.
        """

        # first align sub_roi to write roi shape
        full_graph_offset = (
            self.block_write_roi.get_begin()
            + self.total_roi.get_begin()
            - self.block_read_roi.get_begin()
        )

        begin = sub_roi.get_begin() - full_graph_offset
        end = sub_roi.get_end() - full_graph_offset

        # due to the way blocks are enumerated, write_roi can never be negative
        # relative to total_roi and block_read_roi
        begin = Coordinate([max(n, 0) for n in begin])

        aligned_subroi = (
            begin // self.block_write_roi.get_shape(),  # `floordiv`
            -(-end // self.block_write_roi.get_shape()),  # `ceildiv`
        )
        # generate relative offsets of relevant write blocks
        block_dim_offsets = [
            range(lo, e, s)
            for lo, e, s in zip(
                aligned_subroi[0] * self.block_write_roi.get_shape(),
                aligned_subroi[1] * self.block_write_roi.get_shape(),
                self.block_write_roi.get_shape(),
            )
        ]
        # generate absolute offsets
        block_offsets = [
            Coordinate(o)
            + (self.total_roi.get_begin() - self.block_read_roi.get_begin())
            for o in product(*block_dim_offsets)
        ]
        blocks = self.enumerate_dependencies(
            conflict_offsets=[],
            block_offsets=block_offsets,
        )
        return [block.block_id for block, _ in blocks]


def expand_roi_to_grid(sub_roi, total_roi, read_roi, write_roi):
    """Expands given roi so that its write region is aligned to write_roi"""
    offset = write_roi.get_begin() + total_roi.get_begin() - read_roi.get_begin()

    begin = sub_roi.get_begin() - offset
    end = sub_roi.get_end() - offset
    begin = begin // write_roi.get_shape()  # `floordiv`
    end = -(-end // write_roi.get_shape())  # `ceildiv`
    begin = begin * write_roi.get_shape() + offset
    end = end * write_roi.get_shape() + offset

    return Roi(begin, end - begin)


def expand_request_roi_to_grid(req_roi, total_roi, read_roi, write_roi):
    """Expands given roi so that its write region is aligned to write_roi"""
    offset = write_roi.get_begin() + total_roi.get_begin() - read_roi.get_begin()

    begin = req_roi.get_begin() - offset
    end = req_roi.get_end() - offset
    begin = begin // write_roi.get_shape()  # `floordiv`
    end = -(-end // write_roi.get_shape())  # `ceildiv`
    begin = begin * write_roi.get_shape()
    end = end * write_roi.get_shape()
    begin = begin + offset + (read_roi.get_begin() - write_roi.get_begin())
    end = end + offset + (read_roi.get_end() - write_roi.get_end())

    return Roi(begin, end - begin)


def expand_write_roi_to_grid(roi, write_roi):
    """Expands given roi so that its write region is aligned to write_roi"""

    roi = (
        roi.get_begin() // write_roi.get_shape(),  # `floordiv`
        -(-roi.get_end() // write_roi.get_shape()),
    )  # `ceildiv`

    roi = Roi(roi[0] * write_roi.get_shape(), (roi[1] - roi[0]) * write_roi.get_shape())
    return roi
