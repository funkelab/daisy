from coordinate import Coordinate
from itertools import product
import luigi
import logging

logger = logging.getLogger(__name__)

class Parameter(luigi.Parameter):
    pass

class BlockTask(luigi.Task):
    '''Base-class for block tasks.'''

    read_roi = Parameter()
    write_roi = Parameter()
    level = luigi.IntParameter()
    total_roi = Parameter()

    def set_block_task(self, block_task, parameters):
        self.block_task = block_task
        self.block_task_parameters = parameters

    def set_conflict_offsets(self, conflict_offsets):
        self.conflict_offsets = conflict_offsets

    def requires(self):

        if not hasattr(self, 'conflict_offsets'):
            self.conflict_offsets = []

        logger.debug("Task %s has conflicts %s", self, self.conflict_offsets)

        return [
            self.block_task(
                read_roi=self.read_roi + conflict_offset,
                write_roi=self.write_roi + conflict_offset,
                level=(self.level-1),
                total_roi=self.total_roi,
                **self.block_task_parameters)
            for conflict_offset in self.conflict_offsets
        ]

class ProcessBlocks(luigi.WrapperTask):

    total_roi = Parameter()
    block_read_roi = Parameter()
    block_write_roi = Parameter()
    block_task = Parameter()
    block_task_parameters = Parameter()

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

        self.level_offsets = [
            Coordinate(o)
            for o in product(*dim_offsets)
        ]

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

        prev_level_offset = None
        for level, level_offset in enumerate(self.level_offsets):

            # all block offsets of the current level, per dimension
            block_dim_offsets = [
                range(lo, e + 1, s)
                for e, lo, s in zip(
                    total_shape - read_shape,
                    level_offset,
                    self.level_stride)
            ]

            # all block offsets of the current level (relative to total ROI
            # start)
            block_offsets = [
                Coordinate(o)
                for o in product(*block_dim_offsets)
            ]

            logger.debug(
                "relative block offsets for level %d: %s", level, block_offsets)

            # convert to global coordinates
            block_offsets = [
                o + self.total_roi.get_begin()
                for o in block_offsets
            ]

            logger.debug(
                "absolute block offsets for level %d: %s", level, block_offsets)

            # get conflicts to previous level
            if prev_level_offset is not None:
                conflict_offsets = self.get_conflict_offsets(
                    level_offset,
                    prev_level_offset)
            else:
                conflict_offsets = []
            prev_level_offset = level_offset

            # create block tasks
            level_tasks = []
            for block_offset in block_offsets:

                task = self.block_task(
                    read_roi=self.block_read_roi + block_offset,
                    write_roi=self.block_write_roi + block_offset,
                    level=level,
                    total_roi=self.total_roi,
                    **self.block_task_parameters)

                task.set_conflict_offsets(conflict_offsets)
                task.set_block_task(
                    self.block_task,
                    self.block_task_parameters)

                level_tasks.append(task)

            logger.debug(
                "block tasks for level %d: %s", level, level_tasks)

            block_tasks += level_tasks

        return block_tasks
