from __future__ import absolute_import
from collections import deque
import threading
from .block import Block
from .blocks import *
from .coordinate import Coordinate
from itertools import product
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)

class DynamicBlocks():

    dependents = defaultdict(set)
    dependencies = defaultdict(set)
    ready_queue = deque()
    ready_queue_cv = threading.Condition()
    processing_blocks = set()
    blocks = {}
    # update_lock = threading.Lock()
    actor_type = {}

    max_retries = 2
    retry_count = defaultdict(int)

    failed_blocks = set()
    orphaned_blocks = set()

    def __init__(
        self,
        total_roi,
        block_read_roi,
        block_write_roi,
        read_write_conflict=True,
        fit='valid',
        max_retries=2):

        self.max_retries = max_retries
        # self.orphan_count = 0
        # self.fail_count = 0

        # first create the full dependency graph
        blocks = create_dependency_graph(
            total_roi,
            block_read_roi,
            block_write_roi,
            read_write_conflict,
            fit)

        # post process this graph with our own scheduler
        for block, block_dependencies in blocks:
            block_id = block.block_id
            self.blocks[block_id] = block

            # if len(dependencies):
            #     for d in dependencies:
            #         dependent_blockid = d.block_id

            for dep_id in [x.block_id for x in block_dependencies]:
                # TODO: is this inefficient?
                if dep_id not in self.dependents:
                    self.dependents[dep_id] = set()
                if block_id not in self.dependencies:
                    self.dependencies[block_id] = set()
                self.dependents[dep_id].add(block_id)
                self.dependencies[block_id].add(dep_id)

            if len(block_dependencies) == 0:
                # if no dependencies, add to ready queue
                self.ready_queue.append(block_id)

            # print(block)
            # print(block_dependencies)
            # print(self.dependents)
            # print(self.dependencies)
            # print(self.ready_queue)
            # print(self.processing_blocks)
            # exit(1)


    def empty(self):
        return (len(self.ready_queue) == 0) and (len(self.processing_blocks) == 0)

    def next(self):
        with self.ready_queue_cv:
            # if len(self.ready_queue):
            #     block_id = self.ready_queue.popleft()
            #     self.processing_blocks.add(block_id)
            #     return self.blocks[block_id]
            # else:
            #     return None
            while not self.empty() and len(self.ready_queue) == 0:
                self.ready_queue_cv.wait()

            if self.empty():
                return None

            block_id = self.ready_queue.popleft()
            self.processing_blocks.add(block_id)
            return self.blocks[block_id]


    def remove_and_update(self, block_id):
        """Removing a finished block and update ready queue"""
        with self.ready_queue_cv:
            # function is expected to be called by multiple threads concurrently
            # print("removing {}".format(block_id))
            self.processing_blocks.remove(block_id)
            dependents = self.dependents[block_id]
            for dep in dependents:
                self.dependencies[dep].remove(block_id)
                if len(self.dependencies[dep]) == 0:
                    # ready to run
                    self.ready_queue.append(dep)
            self.ready_queue_cv.notify()


    def cancel_and_reschedule(self, block_id):
        # assert(block_id in self.processing_blocks)
        if block_id not in self.processing_blocks:
            logger.error("Block {} is error".format(block_id))
            assert(0)

        self.retry_count[block_id] = self.retry_count[block_id] + 1

        with self.ready_queue_cv:
            self.processing_blocks.remove(block_id)
            if self.retry_count[block_id] > self.max_retries:

                self.failed_blocks.add(block_id)
                logger.error("Block {} is canceled and will not be rescheduled.".format(block_id))

                if len(self.dependents[block_id]):
                    logger.error("The following blocks are then orphaned and cannot be run: {}".format(self.dependents[block_id]))

                self.check_orphans(block_id)

                # simply leave it canceled
            else:
                self.ready_queue.append(block_id)
                logger.info("Block {} will be rescheduled.".format(block_id))

            self.ready_queue_cv.notify() # in either case, unblock next()

    def check_orphans(self, block_id):
        for orphan_id in self.dependents[block_id]:
            if orphan_id in self.orphaned_blocks or orphan_id in self.failed_blocks:
                return
            self.orphaned_blocks.add(orphan_id)
            self.check_orphans(orphan_id)

    def get_orphans(self):
        return self.orphaned_blocks

    def get_failed_blocks(self):
        return self.failed_blocks

    def get_block(self, block_id):
        return self.blocks[block_id]

    def size(self):
        return len(self.blocks)

    def get_actor_type(self, actor):
        return self.actor_type[actor]


