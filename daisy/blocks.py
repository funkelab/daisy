from __future__ import absolute_import
from .block import Block
from .coordinate import Coordinate
from .roi import Roi
from itertools import product
import logging

logger = logging.getLogger(__name__)


def create_dependency_graph(
        total_roi,
        block_read_roi,
        block_write_roi,
        read_write_conflict=True,
        fit='valid'):
    '''Create a dependency graph as a list with elements::

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
    '''

    level_stride = compute_level_stride(block_read_roi, block_write_roi)
    level_offsets = compute_level_offsets(block_write_roi, level_stride)

    total_shape = total_roi.get_shape()

    # create a list of conflict offsets for each level, that span the total
    # ROI

    level_conflict_offsets = []
    prev_level_offset = None

    for level, level_offset in enumerate(level_offsets):

        # get conflicts to previous level
        if prev_level_offset is not None and read_write_conflict:
            conflict_offsets = get_conflict_offsets(
                level_offset,
                prev_level_offset,
                level_stride)
        else:
            conflict_offsets = []
        prev_level_offset = level_offset

        level_conflict_offsets.append(conflict_offsets)

    blocks = []

    for level in range(len(level_offsets)):

        level_offset = level_offsets[level]

        # all block offsets of the current level (relative to total ROI start)
        block_dim_offsets = [
            range(lo, e, s)
            for lo, e, s in zip(
                level_offset,
                total_shape,
                level_stride)
        ]
        block_offsets = [
            Coordinate(o)
            for o in product(*block_dim_offsets)
        ]

        # convert to global coordinates
        block_offsets = [
            o + (total_roi.get_begin() - block_read_roi.get_begin())
            for o in block_offsets
        ]

        logger.debug(
            "absolute block offsets for level %d: %s", level, block_offsets)

        blocks += enumerate_blocks(
            total_roi,
            block_read_roi,
            block_write_roi,
            level_conflict_offsets[level],
            block_offsets,
            fit)

    return blocks


def compute_level_stride(block_read_roi, block_write_roi):
    '''Get the stride that separates independent blocks in one level.'''

    logger.debug(
        "Compute level stride for read ROI %s and write ROI %s.",
        block_read_roi, block_write_roi)

    assert(block_read_roi.contains(block_write_roi)), (
        "Read ROI must contain write ROI.")

    context_ul = (
        block_write_roi.get_begin() -
        block_read_roi.get_begin()
    )
    context_lr = (
        block_read_roi.get_end() -
        block_write_roi.get_end()
    )

    max_context = Coordinate((
        max(ul, lr)
        for ul, lr in zip(context_ul, context_lr)
    ))
    logger.debug("max context per dimension is %s", max_context)

    # this stride guarantees that blocks are independent, but might be too
    # small for efficient processing due to overlapping write ROIs between
    # different levels
    min_level_stride = max_context + block_write_roi.get_shape()

    logger.debug("min level stride is %s", min_level_stride)

    # to avoid overlapping write ROIs, increase the stride to the next
    # multiple of write shape
    write_shape = block_write_roi.get_shape()
    level_stride = Coordinate((
        ((l - 1)//w + 1)*w
        for l, w in zip(min_level_stride, write_shape)
    ))

    logger.debug(
        "final level stride (multiples of write size) is %s",
        level_stride)

    return level_stride


def compute_level_offsets(block_write_roi, level_stride):
    '''Create a list of all offsets, such that blocks started with these
    offsets plus a multiple of level stride are mutually independent.'''

    write_stride = block_write_roi.get_shape()

    logger.debug(
        "Compute level offsets for level stride %s and write stride %s.",
        level_stride, write_stride)

    dim_offsets = [
        range(0, e, step)
        for e, step in zip(level_stride, write_stride)
    ]

    level_offsets = list(reversed([
        Coordinate(o)
        for o in product(*dim_offsets)
    ]))

    logger.debug("level offsets: %s", level_offsets)

    return level_offsets


def get_conflict_offsets(level_offset, prev_level_offset, level_stride):
    '''Get the offsets to all previous level blocks that are in conflict
    with the current level blocks.'''

    offset_to_prev = prev_level_offset - level_offset
    logger.debug("offset to previous level: %s", offset_to_prev)

    conflict_dim_offsets = [
        [op, op + ls] if op < 0 else [op - ls, op]
        for op, ls in zip(offset_to_prev, level_stride)
    ]

    conflict_offsets = [
        Coordinate(o)
        for o in product(*conflict_dim_offsets)
    ]
    logger.debug("conflict offsets to previous level: %s", conflict_offsets)

    return conflict_offsets


def enumerate_blocks(
        total_roi,
        block_read_roi,
        block_write_roi,
        conflict_offsets,
        block_offsets,
        fit):

    inclusion_criteria = {
        'valid': lambda b: total_roi.contains(b.read_roi),
        'overhang': lambda b: total_roi.contains(b.write_roi.get_begin()),
        'shrink': lambda b: shrink_possible(total_roi, b)
    }[fit]

    fit_block = {
        'valid': lambda b: b,  # noop
        'overhang': lambda b: b,  # noop
        'shrink': lambda b: shrink(total_roi, b)
    }[fit]

    blocks = []

    for block_offset in block_offsets:

        # create a block shifted by the current offset
        block = Block(
            total_roi,
            block_read_roi + block_offset,
            block_write_roi + block_offset)

        logger.debug("considering block: %s", block)

        if not inclusion_criteria(block):
            continue

        # get all blocks in conflict with the current block
        conflicts = []
        for conflict_offset in conflict_offsets:

            conflict = Block(
                total_roi,
                block.read_roi + conflict_offset,
                block.write_roi + conflict_offset
            )

            if not inclusion_criteria(conflict):
                continue

            logger.debug("in conflict with block: %s", conflict)
            conflicts.append(fit_block(conflict))

        blocks.append((fit_block(block), conflicts))

    logger.debug("found blocks: %s", blocks)

    return blocks


def shrink_possible(total_roi, block):

    if not total_roi.contains(block.write_roi.get_begin()):
        return False

    # test if write roi would be non-empty
    b = shrink(total_roi, block)
    return all([s > 0 for s in b.write_roi.get_shape()])


def shrink(total_roi, block):
    '''Ensure that read and write ROI are within total ROI by shrinking both.
    Size of context will be preserved.'''

    r = total_roi.intersect(block.read_roi)
    w = block.write_roi.grow(
        block.read_roi.get_begin() - r.get_begin(),
        r.get_end() - block.read_roi.get_end())

    shrunk_block = block.copy()
    shrunk_block.read_roi = r
    shrunk_block.write_roi = w

    return shrunk_block


def get_subgraph_blocks(
        sub_roi,
        total_roi,
        block_read_roi,
        block_write_roi,
        fit):
    '''Return ids of blocks, as instantiated in the full graph, such that
    their total write rois fully cover `sub_roi`.
    The function API assumes that `sub_roi` and `total_roi` use absolute
    coordinates and `block_read_roi` and `block_write_roi` use relative
    coordinates.
    '''

    # first align sub_roi to write roi shape
    full_graph_offset = (
        block_write_roi.get_begin() +
        total_roi.get_begin() -
        block_read_roi.get_begin())

    begin = sub_roi.get_begin() - full_graph_offset
    end = sub_roi.get_end() - full_graph_offset

    # due to the way blocks are enumerated, write_roi can never be negative
    # relative to total_roi and block_read_roi
    begin = Coordinate([max(n, 0) for n in begin])

    aligned_subroi = (
        begin // block_write_roi.get_shape(),  # `floordiv`
        -(-end // block_write_roi.get_shape())  # `ceildiv`
        )
    # generate relative offsets of relevant write blocks
    block_dim_offsets = [
        range(lo, e, s)
        for lo, e, s in zip(
            aligned_subroi[0] * block_write_roi.get_shape(),
            aligned_subroi[1] * block_write_roi.get_shape(),
            block_write_roi.get_shape())
    ]
    # generate absolute offsets
    block_offsets = [
        Coordinate(o) + (total_roi.get_begin() - block_read_roi.get_begin())
        for o in product(*block_dim_offsets)
    ]
    blocks = enumerate_blocks(
        total_roi,
        block_read_roi,
        block_write_roi,
        conflict_offsets=[],
        block_offsets=block_offsets,
        fit=fit)
    return [block.block_id for block, _ in blocks]


def expand_roi_to_grid(
        sub_roi,
        total_roi,
        read_roi,
        write_roi):
    '''Expands given roi so that its write region is aligned to write_roi
    '''
    offset = (
        write_roi.get_begin() +
        total_roi.get_begin() -
        read_roi.get_begin())

    begin = sub_roi.get_begin() - offset
    end = sub_roi.get_end() - offset
    begin = begin // write_roi.get_shape()  # `floordiv`
    end = -(-end // write_roi.get_shape())  # `ceildiv`
    begin = begin * write_roi.get_shape() + offset
    end = end * write_roi.get_shape() + offset

    return Roi(begin, end - begin)


def expand_request_roi_to_grid(
        req_roi,
        total_roi,
        read_roi,
        write_roi):
    '''Expands given roi so that its write region is aligned to write_roi
    '''
    offset = (
        write_roi.get_begin() +
        total_roi.get_begin() -
        read_roi.get_begin())

    begin = req_roi.get_begin() - offset
    end = req_roi.get_end() - offset
    begin = begin // write_roi.get_shape()  # `floordiv`
    end = -(-end // write_roi.get_shape())  # `ceildiv`
    begin = (begin * write_roi.get_shape())
    end = (end * write_roi.get_shape())
    begin = (begin + offset + (read_roi.get_begin() - write_roi.get_begin()))
    end = (end + offset + (read_roi.get_end() - write_roi.get_end()))

    return Roi(begin, end - begin)


def expand_write_roi_to_grid(
        roi,
        write_roi):
    '''Expands given roi so that its write region is aligned to write_roi
    '''

    roi = (roi.get_begin() // write_roi.get_shape(),  # `floordiv`
           -(-roi.get_end() // write_roi.get_shape()))  # `ceildiv`

    roi = Roi(roi[0] * write_roi.get_shape(),
              (roi[1]-roi[0]) * write_roi.get_shape())
    return roi
