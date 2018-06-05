from coordinate import Coordinate
from itertools import product
import luigi
import logging

logger = logging.getLogger(__name__)

class Parameter(luigi.Parameter):
    pass

class ConflictOffsets(object):

    def __init__(self, offsets):
        self.offsets = offsets

    def __repr__(self):
        return ""

class BlockTask(luigi.Task):
    '''Base-class for block tasks.'''

    read_roi = Parameter()
    write_roi = Parameter()

    # meta-information about the concrete task to run for each block
    block_task = Parameter(significant=False)
    block_task_parameters = Parameter(significant=False)

    # used internally to determine dependenciees
    level = luigi.IntParameter(significant=False)
    total_roi = Parameter(significant=False)
    level_conflict_offsets = Parameter(significant=False)

    def get_block_id(self):
        '''Get a unique ID of this block, depending on the starting
        coordinates of the write ROI.'''

        block_id = 0
        f = 1
        for d in range(self.write_roi.dims()):
            block_id += self.write_roi.get_begin()[-1 - d]*f
            f *= self.total_roi.get_shape()[-1 - d]

        return block_id

    def _requires(self):

        conflict_offsets = self.level_conflict_offsets.offsets[self.level]

        logger.debug("Task %s has conflicts %s", self, conflict_offsets)

        deps = []
        for conflict_offset in conflict_offsets:

            read_roi = self.read_roi + conflict_offset
            write_roi = self.write_roi + conflict_offset

            # skip out-of-bounds dependencies
            if not self.total_roi.contains(read_roi):
                continue

            deps.append(
                self.block_task(
                    read_roi=read_roi,
                    write_roi=write_roi,
                    level=(self.level-1),
                    total_roi=self.total_roi,
                    level_conflict_offsets=self.level_conflict_offsets,
                    block_task=self.block_task,
                    block_task_parameters=self.block_task_parameters,
                    **self.block_task_parameters)
            )

        if self.level == 0:
            deps += self.requires()

        return deps

class ProcessBlocks(luigi.WrapperTask):

    total_roi = Parameter()
    block_read_roi = Parameter()
    block_write_roi = Parameter()
    block_task = Parameter()
    block_task_parameters = Parameter(default=None)

    def compute_level_stride(self):
        '''Get the stride that separates independent blocks in one level.'''

        logger.debug(
            "Compute level stride for read ROI %s and write ROI %s.",
            self.block_read_roi, self.block_write_roi)

        assert(self.block_read_roi.contains(self.block_write_roi)), (
            "Read ROI must contain write ROI.")

        context_ul = (
            self.block_write_roi.get_begin() -
            self.block_read_roi.get_begin()
        )
        context_lr = (
            self.block_read_roi.get_end() -
            self.block_write_roi.get_end()
        )

        max_context = Coordinate((
            max(ul, lr)
            for ul, lr in zip(context_ul, context_lr)
        ))
        logger.debug("max context per dimension is %s", max_context)

        # this stride guarantees that blocks are independent, but might be too
        # small for efficient processing due to overlapping write ROIs between
        # different levels
        min_level_stride = max_context + self.block_write_roi.get_shape()

        logger.debug("min level stride is %s", min_level_stride)

        # to avoid overlapping write ROIs, increase the stride to the next
        # multiple of write shape
        write_shape = self.block_write_roi.get_shape()
        self.level_stride = Coordinate((
            ((l - 1)/w + 1)*w
            for l, w in zip(min_level_stride, write_shape)
        ))

        logger.debug(
            "final level stride (multiples of write size) is %s",
            self.level_stride)

    def compute_level_offsets(self):
        '''Create a list of all offsets, such that blocks started with these
        offsets plus a multiple of level stride are mutually independent.'''

        write_stride = self.block_write_roi.get_shape()

        logger.debug(
            "Compute level offsets for level stride %s and write stride %s.",
            self.level_stride, write_stride)

        dim_offsets = [
            range(0, e, step)
            for e, step in zip(self.level_stride, write_stride)
        ]

        logger.debug("Dim offsets: %s", dim_offsets)

        self.level_offsets = list(reversed([
            Coordinate(o)
            for o in product(*dim_offsets)
        ]))

    def get_conflict_offsets(self, level_offset, prev_level_offset):
        '''Get the offsets to all previous level blocks that are in conflict
        with the current level blocks.'''

        offset_to_prev = prev_level_offset - level_offset
        logger.debug("offset to previous level: %s", offset_to_prev)

        conflict_dim_offsets = [
            [op, op + ls] if op < 0 else [op - ls, op]
            for op, ls in zip(offset_to_prev, self.level_stride)
        ]

        conflict_offsets = [
            Coordinate(o)
            for o in product(*conflict_dim_offsets)
        ]
        logger.debug("conflict offsets to previous level: %s", conflict_offsets)

        return conflict_offsets

    def requires(self):
        '''Create all BlockTasks and inject their dependencies.'''

        self.compute_level_stride()
        self.compute_level_offsets()

        total_shape = self.total_roi.get_shape()
        read_shape = self.block_read_roi.get_shape()

        block_tasks = []

        # create a list of conflict offsets for each level, that span the total
        # ROI

        level_conflict_offsets = []
        prev_level_offset = None

        for level, level_offset in enumerate(self.level_offsets):

            # get conflicts to previous level
            if prev_level_offset is not None:
                conflict_offsets = self.get_conflict_offsets(
                    level_offset,
                    prev_level_offset)
            else:
                conflict_offsets = []
            prev_level_offset = level_offset

            level_conflict_offsets.append(conflict_offsets)

        # start dependecy tree by requesting top-level blocks

        level_offset = self.level_offsets[-1]
        level = len(self.level_offsets) - 1

        # all block offsets of the top level, per dimension
        block_dim_offsets = [
            range(lo, e, s)
            for lo, e, s in zip(
                level_offset,
                total_shape,
                self.level_stride)
        ]

        # all block offsets of the current level (relative to total ROI start)
        block_offsets = [
            Coordinate(o)
            for o in product(*block_dim_offsets)
        ]

        # convert to global coordinates
        block_offsets = [
            o + self.total_roi.get_begin()
            for o in block_offsets
        ]

        logger.debug(
            "absolute block offsets for level %d: %s", level, block_offsets)

        # create top-level block tasks
        if self.block_task_parameters is None:
            self.block_task_parameters = {}
        top_level_tasks = [
            self.block_task(
                read_roi=self.block_read_roi + block_offset,
                write_roi=self.block_write_roi + block_offset,
                level=level,
                total_roi=self.total_roi,
                level_conflict_offsets=ConflictOffsets(level_conflict_offsets),
                block_task=self.block_task,
                block_task_parameters=self.block_task_parameters,
                **self.block_task_parameters)
            for block_offset in block_offsets
            if self.total_roi.contains(self.block_read_roi + block_offset)
        ]

        logger.debug("block tasks for top level: %s", top_level_tasks)

        return top_level_tasks
