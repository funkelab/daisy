from __future__ import absolute_import

import collections
import copy
from itertools import product
import logging
import threading
import queue

from .block import Block
from .blocks import *
from .coordinate import Coordinate

logger = logging.getLogger(__name__)


class DependencyGraph():

    global_config = None

    root_task_id = None
    tasks = set()
    task_map = {}
    prepared_tasks = set()
    created_tasks = set()
    task_dependency = collections.defaultdict(set)

    dependents = collections.defaultdict(set)
    dependencies = collections.defaultdict(set)
    ready_queue = collections.deque()
    ready_queue_cv = threading.Condition()
    processing_blocks = set()
    blocks = {}

    retry_count = collections.defaultdict(int)
    failed_blocks = set()
    orphaned_blocks = set()

    task_done_count = collections.defaultdict(int)
    task_total_block_count = {}

    def __init__(self, global_config):
        self.global_config = global_config


    def add(self, task):
        task.init_from_config(self.global_config)

        if self.root_task_id == None:
            self.root_task_id = task.task_id
        else:
            logger.error("Daisy can only have 1 final task currently.")
            raise

        self.tasks.add(task)
        self.task_map[task.task_id] = task
        self.add_task_dependency(task)

    def add_task_dependency(self, task):
        for dependency_task in task.requires():
            dependency_task.init_from_config(self.global_config)
            self.tasks.add(dependency_task)
            # modify task dependency graph
            self.task_dependency[task.task_id].add(dependency_task.task_id)
            self.task_map[dependency_task.task_id] = dependency_task
            # recursively add dependency
            self.add_task_dependency(dependency_task)

    def init(self):
        self.__recursively_prepare(self.root_task_id)
        self.__recursively_create_dependency_graph(self.root_task_id)

        # print(self.blocks)
        # print(self.dependencies)

    def __recursively_create_dependency_graph(self, task_id):
        """ Create dependency graph for its dependencies first before own """

        if task_id in self.created_tasks:
            return
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

        # some sanity check
        assert(task.max_retries >= 0)

        # post process this graph with our own scheduler
        for block, block_dependencies in blocks:
            block_id = (task_id, block.block_id)
            self.blocks[block_id] = block

            dependencies = [(task_id, b.block_id) for b in block_dependencies]

            # add inter-task read-write dependency
            if len(self.task_dependency[task_id]):
                roi = block.read_roi
                for dependent_task in self.task_dependency[task_id]:
                    block_ids = self._get_subgraph_blocks(dependent_task, roi)
                    # print(blocks)
                    dependencies.extend([(dependent_task, block_id) for block_id in block_ids])

            for dep_id in dependencies:
                self.dependents[dep_id].add(block_id)
                self.dependencies[block_id].add(dep_id)

            if len(dependencies) == 0:
                # if no dependencies, add to ready queue immediately
                self.ready_queue.append(block_id)

    def __recursively_prepare(self, task):

        if task in self.prepared_tasks:
            return
        self.prepared_tasks.add(task)

        for dependency_task in self.task_dependency[task]:
            self.__recursively_prepare(dependency_task)

        self.task_map[task].prepare()

    def next(self):
        """ Get the next available block.
            Blocks and waits for outstanding blocks if there is none ready.

            Returns tuple (task_name, block) if available.
            Returns `None` if there will be no more blocks to run.
        """

        with self.ready_queue_cv:
            while not self.empty() and len(self.ready_queue) == 0:
                self.ready_queue_cv.wait()

            if self.empty():
                return None

            # block_id = self.ready_queue.popleft()
            block_id = self.ready_queue.pop()
            self.processing_blocks.add(block_id)
            return (block_id[0], self.blocks[block_id])

    def get_tasks(self):
        return self.tasks

    def empty(self):
        return ((len(self.ready_queue) == 0)
                 and (len(self.processing_blocks) == 0))

    def size(self):
        return len(self.blocks)

    def ready_size(self):
        return len(self.ready_queue)

    def get_orphans(self):
        return self.orphaned_blocks

    def get_failed_blocks(self):
        return self.failed_blocks

    def get_block(self, block_id):
        return self.blocks[block_id]

    def cancel_and_reschedule(self, block_id):
        # assert(block_id in self.processing_blocks)
        if block_id not in self.processing_blocks:
            logger.error("Block {} is canceled but was not found"
                         .format(block_id))
            raise

        self.retry_count[block_id] = self.retry_count[block_id] + 1

        with self.ready_queue_cv:
            self.processing_blocks.remove(block_id)
            task_name = block_id[0]
            if self.retry_count[block_id] > self.task_map[task_name].max_retries:

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
                self.ready_queue.appendleft(block_id)
                logger.info("Block {} will be rescheduled.".format(block_id))

            self.ready_queue_cv.notify()  # in either case, unblock next()

    def recursively_check_orphans(self, block_id):
        for orphan_id in self.dependents[block_id]:

            if (orphan_id in self.orphaned_blocks
                    or orphan_id in self.failed_blocks):
                return

            self.orphaned_blocks.add(orphan_id)
            self.recursively_check_orphans(orphan_id)

    def remove_and_update(self, block_id):
        """Removing a finished block and update ready queue"""
        with self.ready_queue_cv:
            self.task_done_count[block_id[0]] += 1
            self.processing_blocks.remove(block_id)
            dependents = self.dependents[block_id]
            for dep in dependents:
                self.dependencies[dep].remove(block_id)
                if len(self.dependencies[dep]) == 0:
                    # ready to run
                    self.ready_queue.append(dep)
            self.ready_queue_cv.notify()

    def get_subgraph(self, roi):

        subgraph = copy.deepcopy(self)
        subgraph.__create_subgraph(roi)
        return subgraph

    def __create_subgraph(self, roi):
        ''' Modify existing graph so that when executed, only the minimum
            number of blocks will be computed to cover the given ROI.

            This is achieved simply by removing relevant blocks from
            the ready_queue as computed for the full graph
        '''

        # check if everything is initiated correctly
        # get primary task, and create its new roi
        # print(self.ready_queue)

        to_check = collections.deque()
        to_check.extend(
            [(self.root_task_id, block)
                for block in self._get_subgraph_blocks(self.root_task_id, roi)
            ])
            
        # relevant_blocks = set()
        self.ready_queue.clear()
        processed = set()

        while len(to_check) > 0:
            # print(to_check)
            block = to_check.popleft()
            if block in processed:
                continue
            processed.add(block)

            if len(self.dependencies[block]) == 0:
                # relevant_blocks.add(block)
                self.ready_queue.append(block)
            else:
                to_check.extend(self.dependencies[block])

        # print(self.ready_queue)


        # self.ready_queue.clear()
        # self.ready_queue.


        # get the list of blocks that contribute to this roi
        # if it's in the ready block list, add it to a separate list
        # check its dependency, adding them to a to-be-checked list
        # check one task at a time to minimize redundancy
        # particularly at a join, wait for the other path to finish first

    def _get_subgraph_blocks(self, task_id, roi):
        ''' Returns a blocks of this task that write to given ROI '''
        task = self.task_map[task_id]
        return get_subgraph_blocks(
            roi,
            task.total_roi,
            task.read_roi,
            task.write_roi,
            task.fit)

    def is_task_done(self, task_id):
        return self.task_done_count[task_id] == self.task_total_block_count[task_id]