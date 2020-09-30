from __future__ import absolute_import

import collections
import logging
import itertools

from .blocks import create_dependency_graph, get_subgraph_blocks, \
    expand_request_roi_to_grid

logger = logging.getLogger(__name__)


class TaskState():
    def __init__(self):
        self.ready_queue = []
        self.created = False
        self.total_block_count = 0
        self.done_count = 0
        self.failed_count = 0
        self.processing_blocks = set()
        self.failed_blocks = set()
        self.orphaned_blocks = set()


class DependencyGraph():
    '''This class constructs a block-wise dependency graph of a given
    ``Task`` and its dependencies.It provides an interface for the
    scheduler to query for blocks ready to be computed, and also a
    mechanism to retry blocks that have failed.

    User can make a subgraph of certain ROIs of the full graph through
    ``get_subgraph``.
    '''

    def __init__(self):

        # self.leaf_task_id = None
        self.tasks = set()
        self.task_map = {}
        self.created_tasks = {}
        self.task_dependency = collections.defaultdict(set)

        # task states:
        self.task_states = collections.defaultdict(TaskState)

        self.dependents = collections.defaultdict(set)
        self.dependencies = collections.defaultdict(set)
        self.blocks = {}

        self.retry_count = collections.defaultdict(int)


    @property
    def processing_blocks(self):
        set().union(
            *[set((task_id, block_ind) for block_ind in task_state.processed_blocks)
            for task_id, task_state in self.task_states]
        )
    @property
    def orphaned_blocks(self):
        set().union(
            *[set((task_id, block_ind) for block_ind in task_state.orphaned_blocks)
            for task_id, task_state in self.task_states]
        )
    @property
    def failed_blocks(self):
        set().union(
            *[set((task_id, block_ind) for block_ind in task_state.failed_blocks)
            for task_id, task_state in self.task_states]
        )

    def add(self, task, request_roi=None):
        '''Add a ``Task`` to the graph.

        Args:
            task(``Task``):
                Task to be added to the graph. Its dependencies are
                automatically and recursively added to the graph.
        '''
        if task.task_id not in self.task_map:
            self.tasks.add(task)
            self.task_map[task.task_id] = task
            self.add_task_dependencies(task)

            self.__recursively_create_dependency_graph(task.task_id, request_roi)

    def add_task_dependencies(self, task):
        '''Recursively add dependencies of a task to the graph.'''
        for dependency_task in task.requires():

            # Add the required task to tasks and task_map
            self.add(dependency_task)

            # modify task dependency graph
            self.task_dependency[task.task_id].add(dependency_task.task_id)

    def add_to_ready_queue(self, task_id, block_id):
        self.task_states[task_id].ready_queue.append(block_id)

    def get_from_ready_queue(self, task_id):
        return self.task_states[task_id].ready_queue.popleft()

    def __recursively_create_dependency_graph(self, task_id, request_roi):
        '''Create dependency graph for its dependencies first before
        its own'''
        if task_id in self.created_tasks:
            if request_roi is None or self.created_tasks[task_id].contains(request_roi):
                return
            elif request_roi is not None:
                raise NotImplementedError(
                    "Just need to expand the dependency graph to contain extra blocks"
                )
        else:
            self.created_tasks[task_id] = self.created_tasks[task_id].union(request_roi)

        task = self.task_map[task_id]
        dependency_request_roi = None

        # restore original total_roi before modification
        # (second time this task is processed)
        task._daisy.total_roi = task._daisy.orig_total_roi

        if request_roi:
            # request has to be within the total_write_roi
            # TODO: make sure this is correct given the fitting strategy
            # of the dependent task
            if not task._daisy.total_write_roi.contains(request_roi):

                new_request_roi = request_roi.intersect(
                                                task._daisy.total_write_roi)
                logger.info(
                    "Reducing request_roi for Task %s from %s to %s because "
                    "total_write_roi is only %s",
                    task_id, request_roi, new_request_roi,
                    task._daisy.total_write_roi)
                request_roi = new_request_roi

            # reduce total_roi to match request_roi
            # and calculate request_roi for its dependencies
            total_roi = expand_request_roi_to_grid(
                request_roi,
                task._daisy.orig_total_roi,
                task._daisy.read_roi,
                task._daisy.write_roi)

            logger.info(
                "Reducing total_roi for Task %s from %s to %s because of "
                "request %s",
                task_id, task._daisy.orig_total_roi, total_roi, request_roi)

            task._daisy.total_roi = total_roi
            dependency_request_roi = total_roi

        for dependency_task in self.task_dependency[task_id]:
            self.__recursively_create_dependency_graph(dependency_task,
                                                       dependency_request_roi)

        # finally create graph for this task
        # first create the self-contained dependency graph
        blocks = create_dependency_graph(
            task._daisy.total_roi,
            task._daisy.read_roi,
            task._daisy.write_roi,
            task._daisy.read_write_conflict,
            task._daisy.fit)

        self.task_states[task_id].total_block_count += len(blocks)
        self.task_states[task_id].done_count = 0

        # add tasks to block-wise graph, while accounting for intra-task
        # and inter-task dependencies
        for block, block_dependencies in blocks:

            block_id = block.block_id
            assert isinstance(block_id, tuple) and len(block_id) == 2, "block_id should be a unique (task_id, block_index) identifier for a specific block"

            if block_id in self.blocks:
                continue

            self.blocks[block_id] = block

            dependencies = [
                b.block_id for b in block_dependencies
                if b.block_id in self.blocks]

            # add inter-task read-write dependency
            if len(self.task_dependency[task_id]):
                roi = block.read_roi
                for dependent_task in self.task_dependency[task_id]:
                    block_ids = self._get_subgraph_blocks(dependent_task, roi)
                    dependencies.extend([
                        (dependent_task, block_id)
                        for block_id in block_ids
                    ])

            for dep_id in dependencies:
                if dep_id not in self.blocks:
                    raise RuntimeError(
                        "Block dependency %s is not found for task %s." % (
                            dep_id, task_id))
                    continue
                self.dependents[dep_id].add(block_id)
                self.dependencies[block_id].add(dep_id)

            if len(dependencies) == 0:
                # if this block has no dependencies, add it to the ready
                # queue immediately
                self.add_to_ready_queue(task_id, block_id)

    def next(self, waiting_blocks):
        '''Called by the ``scheduler`` to get a `dict` of ready blocks.
        This function blocks when outstanding blocks are empty and
        there is no further ready blocks to issue. This (only) happens
        when there are outstanding, currently executing blocks.

        Return:
            `dict` {task_id: block} for ready task blocks.
            Empty `dict` does not necessarily mean that there is
            no more blocks to be run (though it is the case currently).
            The scheduler should call empty() to really make sure that
            there is no more blocks to run.
        '''

        return_blocks = waiting_blocks
        if self.empty():
            return return_blocks

        for task_id in self.task_states:

            if task_id in return_blocks:
                pass

            elif len(self.task_states[task_id].ready_queue) == 0:
                pass

            else:
                block_id = self.get_from_ready_queue(task_id)
                self.task_states[block_id[0]].processing_blocks.add(
                    block_id[1])
                return_blocks[block_id[0]] = self.blocks[block_id]

        return return_blocks

    def get_tasks(self):
        '''Get all tasks in the graph.'''
        return self.tasks

    def empty(self):
        '''Return ``True`` if there is no more blocks to be executed,
        either because all blocks have been completed, or because there
        are failed blocks that prevent other blocks from running.'''
        return (
            (self.ready_size() == 0) and
            (len(self.processing_blocks) == 0))

    def size(self):
        '''Return the size of the block-wise graph.'''
        return len(self.blocks)

    def ready_size(self):
        '''Return the number of blocks ready to be run.'''
        count = 0
        for task in self.task_states:
            count += len(self.task_states[task].ready_queue)
        return count

    def get_block(self, block_id):
        '''Return a specific block.'''
        return self.blocks[block_id]

    def cancel_and_reschedule(self, block_id):
        '''Used to notify that a block has failed. The block will either
        be rescheduled if within the number of retries, or be marked
        as failed.'''
        if block_id[1] not in self.task_states[block_id[0]]:
            logger.error(
                "Block %d is canceled but was not found", block_id)
            raise

        self.retry_count[block_id] += 1

        self.task_states[block_id[0]].processing_blocks.remove(block_id[1])
        task_id = block_id[0]

        if (
                self.retry_count[block_id] >
                self.task_map[task_id]._daisy.max_retries):

            self.task_states[block_id[0]].add(block_id[1])
            self.task_states[block_id[0]].failed_count += 1
            logger.error(
                "Block {} is canceled and will not be rescheduled."
                .format(block_id))

            if len(self.dependents[block_id]):
                logger.error(
                    "The following blocks are then orphaned and "
                    "cannot be run: {}".format(self.dependents[block_id]))

            self.recursively_check_orphans(block_id)
            # simply leave it canceled at this point

        else:

            self.add_to_ready_queue(task_id, block_id)
            logger.info("Block {} will be rescheduled.".format(block_id))

    def recursively_check_orphans(self, block_id):
        '''Check and mark children of the given block as orphans.'''
        for orphan_id in self.dependents[block_id]:

            if (orphan_id in self.task_states[orphan_id[0]].orphaned_blocks[orphan_id[1]]
                    or orphan_id in self.task_states[orphan_id[0]].failed_blocks[orphan_id[1]]):
                # TODO: isn't this return too early, shouldn't we check all
                # orphan_ids in self.dependents[block_id]?
                return

            self.task_states[orphan_id[0]].orphaned_blocks.add(orphan_id[1])
            self.recursively_check_orphans(orphan_id)

    def remove_and_update(self, block_id):
        '''Removing a finished block and update ready queue.'''

        if block_id[1] not in self.task_states[block_id[0]]:
            logger.error(
                "Block %d is canceled but was not found", block_id)
            raise

        self.task_states[block_id[0]].done_count += 1
        self.task_states[block_id[0]].processing_blocks.remove(block_id[1])

        dependents = self.dependents[block_id]
        for dep in dependents:
            self.dependencies[dep].remove(block_id)
            if len(self.dependencies[dep]) == 0:
                # ready to run
                self.add_to_ready_queue(dep[0], dep)

    def _get_subgraph_blocks(self, task_id, roi):
        '''Return blocks of this task that write to given ROI.'''
        task = self.task_map[task_id]
        return get_subgraph_blocks(
            roi,
            task._daisy.total_roi,
            task._daisy.read_roi,
            task._daisy.write_roi,
            task._daisy.fit)

    def is_task_done(self, task_id):
        '''Return ``True`` if all blocks of a task have completed.'''
        return (self.task_states[task_id].done_count
                == self.task_states[task_id].total_block_count)

    def get_task_size(self, task_id):
        return self.task_states[task_id].total_block_count

    def get_task_done_count(self, task_id):
        return self.task_states[task_id].done_count

    def get_task_failed_count(self, task_id):
        return self.task_states[task_id].failed_count

    def get_task_processing_blocks(self, task_id):
        return self.task_states[task_id].processing_blocks
