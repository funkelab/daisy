from .coordinate import Coordinate
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

        (read_roi, write_roi, upstream_write_rois)

    per block, where ``write_roi`` is the blocks ROI with exclusive write access,
    ``read_roi`` is the associated ROI for save reading, and
    ``upstream_write_rois`` is a list of write ROIs for other blocks that need
    to finish before this block can start (``[]`` if there are no upstream
    dependencies``).

    Args:

        total_roi (`class:peach.Roi`):

            The region of interest (ROI) of the complete volume to process.

        block_read_roi (`class:peach.Roi`):

            The ROI every block needs to read data from. Will be shifted over
            the ``total_roi``.

        block_write_roi (`class:peach.Roi`):

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
    read_shape = block_read_roi.get_shape()

    block_tasks = []

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

        'valid': lambda r, w: total_roi.contains(r),
        'overhang': lambda r, w: total_roi.contains(w.get_begin()),
        'shrink': lambda r, w: total_roi.contains(w.get_begin())
    }[fit]

    fit_block = {
        'valid': lambda r, w: (r, w), # noop
        'overhang': lambda r, w: (r, w), # noop
        'shrink': lambda r, w: shrink(total_roi, r, w)
    }[fit]

    blocks = []

    for block_offset in block_offsets:

        # shift the block to its destination
        r = block_read_roi + block_offset
        w = block_write_roi + block_offset

        logger.info("considering read roi: %s", r)
        logger.info("considering write roi: %s", w)

        if not inclusion_criteria(r, w):
            continue

        conflicts = [
            fit_block(
                r + conflict_offset,
                w + conflict_offset)[1] # just the write ROI for conflicts
            for conflict_offset in conflict_offsets
        ]

        r, w = fit_block(r, w)

        blocks.append((r, w, conflicts))

    logger.info("found blocks: %s", blocks)

    return blocks

def shrink(total_roi, block_read_roi, block_write_roi):
    '''Ensure that read and write ROI are within total ROI by shrinking both.
    Size of context will be preserved.'''

    r = total_roi.intersect(block_read_roi)
    w = block_write_roi.grow(
        block_read_roi.get_begin() - r.get_begin(),
        r.get_end() - block_read_roi.get_end())

    return (r, w)
