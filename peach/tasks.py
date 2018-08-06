from .blocks import compute_level_stride, compute_level_offsets, get_conflict_offsets, create_dependency_graph
from .coordinate import Coordinate
from itertools import product
import logging
import luigi

logger = logging.getLogger(__name__)

class Parameter(luigi.Parameter):
    pass

class BlockTask(luigi.Task):
    '''Base-class for block tasks.'''

    read_roi = Parameter()
    write_roi = Parameter()
    total_roi = Parameter(significant=False)

    # meta-information about the concrete task to run for each block
    block_task = Parameter(significant=False)
    block_task_parameters = Parameter(significant=False)

    # used internally to determine dependencies
    dependencies = Parameter()

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

        return [
            self.block_task(
                read_roi=read_roi,
                write_roi=write_roi,
                total_roi=self.total_roi,
                block_task=self.block_task,
                block_task_parameters=self.block_task_parameters,
                **self.block_task_parameters)
            for read_roi, write_roi in zip(*self.dependencies)
        ]

class ProcessBlocks(luigi.WrapperTask):

    total_roi = Parameter()
    block_read_roi = Parameter()
    block_write_roi = Parameter()
    read_write_conflict = luigi.BoolParameter()
    block_task = Parameter()
    block_task_parameters = Parameter(default=None)

    def requires(self):
        '''Create all BlockTasks and inject their dependencies.'''

        # get all blocks as a dependency graph
        blocks = create_dependency_graph(
            self.total_roi,
            self.block_read_roi,
            self.block_write_roi,
            self.read_write_conflict)

        # convert to dictionary
        blocks = {
            write_roi: (
                read_roi,
                dep_write_rois)
            for (write_roi, read_roi, dep_write_rois) in blocks
        }

        if self.block_task_parameters is None:
            self.block_task_parameters = {}

        # create all block tasks
        tasks = [
            self.block_task(
                read_roi=read_roi,
                write_roi=write_roi,
                total_roi=self.total_roi,
                # the write and read ROIs of dependent tasks
                dependencies=(
                    dep_write_rois,
                    list([ blocks[d][0] for d in dep_write_rois ])),
                block_task=self.block_task,
                block_task_parameters=self.block_task_parameters,
                **self.block_task_parameters)
            for write_roi, (read_roi, dep_write_rois) in blocks.items()
        ]

        logger.debug("created dependency graph with %d task", len(tasks))

        return tasks
