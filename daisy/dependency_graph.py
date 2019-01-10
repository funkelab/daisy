from __future__ import absolute_import

import collections
import copy
import logging
import threading
from .blocks import create_dependency_graph, get_subgraph_blocks

logger = logging.getLogger(__name__)


class DependencyGraph():
    '''This class constructs a block-wise dependency graph of a given
    ``Task`` and its dependencies.It provides an interface for the
    scheduler to query for blocks ready to be computed, and also a
    mechanism to retry blocks that have failed.

    User can make a subgraph of certain ROIs of the full graph through
    ``get_subgraph``.
    '''

    def __init__(self, global_config):
        self.global_config = global_config

        self.leaf_task_id = None
        self.tasks = set()
        self.task_map = {}
        self.prepared_tasks = set()
        self.created_tasks = set()
        self.task_dependency = collections.defaultdict(set)

        self.dependents = collections.defaultdict(set)
        self.dependencies = collections.defaultdict(set)
        self.ready_queues = collections.defaultdict(collections.deque)
        self.ready_queue_cv = threading.Condition()
        self.processing_blocks = set()
        self.blocks = {}

        self.retry_count = collections.defaultdict(int)
        self.failed_blocks = set()
        self.orphaned_blocks = set()

        self.task_done_count = collections.defaultdict(int)
        self.task_total_block_count = {}

    def add(self, task):
        '''Add a ``Task`` to the graph.

        Args:
            task(``Task``):
                Task to be added to the graph. Its dependencies are
                automatically and recursively added to the graph.
        '''
        task.init_from_config(self.global_config)

        if self.leaf_task_id is None:
            self.leaf_task_id = task.task_id
        else:
            logger.error("Daisy can only have one leaf task currently.")
            raise

        self.tasks.add(task)
        self.task_map[task.task_id] = task
        self.add_task_dependency(task)

    def add_task_dependency(self, task):
        '''Recursively add dependencies of a task to the graph.'''
        for dependency_task in task.requires():
            dependency_task.init_from_config(self.global_config)
            self.tasks.add(dependency_task)
            # modify task dependency graph
            self.task_dependency[task.task_id].add(dependency_task.task_id)
            self.task_map[dependency_task.task_id] = dependency_task
            # recursively add dependency
            self.add_task_dependency(dependency_task)

    def init(self):
        '''Called by the ``scheduler`` after all tasks have been added.
        Call the prepare() of each task, and create the entire
        block-wise graph.'''
        self.__recursively_prepare(self.leaf_task_id)
        self.__recursively_create_dependency_graph(self.leaf_task_id)

    def __recursively_create_dependency_graph(self, task_id):
        '''Create dependency graph for its dependencies first before
        its own'''
        if task_id in self.created_tasks:
            return
        else:
            self.created_tasks.add(task_id)

        for dependency_task in self.task_dependency[task_id]:
            self.__recursively_create_dependency_graph(dependency_task)

        # finally create graph for this task
        # first create the self-contained dependency graph
        task = self.task_map[task_id]
        blocks = create_dependency_graph(
            task.total_roi,
            task.read_roi,
            task.write_roi,
            task.read_write_conflict,
            task.fit)

        self.task_total_block_count[task_id] = len(blocks)
        self.task_done_count[task_id] = 0

        # some sanity checks
        assert(task.max_retries >= 0)

        # add tasks to block-wise graph, while accounting for intra-task
        # and inter-task dependencies
        for block, block_dependencies in blocks:

            block_id = (task_id, block.block_id)
            self.blocks[block_id] = block

            dependencies = [(task_id, b.block_id) for b in block_dependencies]

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
                self.dependents[dep_id].add(block_id)
                self.dependencies[block_id].add(dep_id)

            if len(dependencies) == 0:
                # if this block has no dependencies, add it to the ready
                # queue immediately
                self.ready_queues[task_id].append(block_id)

    def __recursively_prepare(self, task):

        if task in self.prepared_tasks:
            return
        self.prepared_tasks.add(task)

        for dependency_task in self.task_dependency[task]:
            self.__recursively_prepare(dependency_task)

        self.task_map[task].prepare()

    def next(self, available_actors={}):
        '''Called by the ``scheduler`` to get the next available block.
        Current implementation blocks and waits for outstanding (issued)
        blocks if there is none ready. After an outstanding block
        finished, a new block may become ready and will be returned.

        Return:
            Tuple (task_id, block) if available, or ``None``.
            Returning ``None`` does not necessarily mean that there is
            no more blocks to be run (though it is the case currently).
            The scheduler should call empty() to really make sure that
            there is no more blocks to run.
        '''

        with self.ready_queue_cv:
            # Block release is conducted in 2 phases

            # First, return a block of a task queue if there is
            # available actors.
            for task_type in self.ready_queues:

                if len(self.ready_queues[task_type]) == 0:
                    continue

                if task_type in available_actors:
                    if available_actors[task_type].qsize() == 0:
                        continue

                block_id = self.ready_queues[task_type].popleft()
                self.processing_blocks.add(block_id)
                return (block_id[0], self.blocks[block_id])

            # Otherwise, return any other block. The scheduler will then
            # spawn necessary actors for this block.
            while not self.empty() and self.ready_size() == 0:
                self.ready_queue_cv.wait()

            if self.empty():
                return None

            for task_type in self.ready_queues:
                if len(self.ready_queues[task_type]):
                    block_id = self.ready_queues[task_type].popleft()
                    self.processing_blocks.add(block_id)
                    return (block_id[0], self.blocks[block_id])

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
        for task in self.ready_queues:
            count += len(self.ready_queues[task])
        return count

    def get_orphans(self):
        '''Return the number of blocks cannot be issued due to failed
        dependencies.'''
        return self.orphaned_blocks

    def get_failed_blocks(self):
        '''Return blocks that have failed and won't be retried.'''
        return self.failed_blocks

    def get_block(self, block_id):
        '''Return a specific block.'''
        return self.blocks[block_id]

    def cancel_and_reschedule(self, block_id):
        '''Used to notify that a block has failed. The block will either
        be rescheduled if within the number of retries, or be marked
        as failed.'''
        if block_id not in self.processing_blocks:
            logger.error(
                "Block %d is canceled but was not found", block_id)
            raise

        self.retry_count[block_id] = self.retry_count[block_id] + 1

        with self.ready_queue_cv:

            self.processing_blocks.remove(block_id)
            task_id = block_id[0]

            if (
                    self.retry_count[block_id] >
                    self.task_map[task_id].max_retries):

                self.failed_blocks.add(block_id)
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

                self.ready_queues[task_id].appendleft(block_id)
                logger.info("Block {} will be rescheduled.".format(block_id))

            self.ready_queue_cv.notify()  # in either case, unblock next()

    def recursively_check_orphans(self, block_id):
        '''Check and mark children of the given block as orphans.'''
        for orphan_id in self.dependents[block_id]:

            if (orphan_id in self.orphaned_blocks
                    or orphan_id in self.failed_blocks):
                return

            self.orphaned_blocks.add(orphan_id)
            self.recursively_check_orphans(orphan_id)

    def remove_and_update(self, block_id):
        '''Removing a finished block and update ready queue.'''
        with self.ready_queue_cv:

            self.task_done_count[block_id[0]] += 1
            self.processing_blocks.remove(block_id)

            dependents = self.dependents[block_id]
            for dep in dependents:
                self.dependencies[dep].remove(block_id)
                if len(self.dependencies[dep]) == 0:
                    # ready to run
                    self.ready_queues[dep[0]].append(dep)

            # Unblock next() regardless. If we only unblock for new
            # elements in ready_queue, the program might lock up if
            # this block is the last
            self.ready_queue_cv.notify()

    def get_subgraph(self, roi):
        '''Create a subgraph given a ROI, assuming this ROI is that
        of the leaf task.'''
        subgraph = copy.deepcopy(self)
        subgraph.__create_subgraph(roi)
        return subgraph

    def __create_subgraph(self, roi):
        '''Modify existing graph so that only the minimum number of
        blocks will be computed to cover the given ROI. This is achieved
        by simply recomputing the ready_queue as computed for the full
        graph.'''
        self.ready_queues.clear()

        # get blocks of the leaf task that writes to the given ROI
        to_check = collections.deque()
        to_check.extend([
            (self.leaf_task_id, block)
            for block in self._get_subgraph_blocks(self.leaf_task_id, roi)
        ])

        processed = set()
        while len(to_check) > 0:

            block = to_check.popleft()

            if block in processed:
                continue
            else:
                processed.add(block)

            if len(self.dependencies[block]) == 0:
                self.ready_queues[block[0]].append(block)
            else:
                to_check.extend(self.dependencies[block])

    def _get_subgraph_blocks(self, task_id, roi):
        '''Return blocks of this task that write to given ROI.'''
        task = self.task_map[task_id]
        return get_subgraph_blocks(
            roi,
            task.total_roi,
            task.read_roi,
            task.write_roi,
            task.fit)

    def is_task_done(self, task_id):
        '''Return ``True`` if all blocks of a task have completed.'''
        return (self.task_done_count[task_id]
                == self.task_total_block_count[task_id])
