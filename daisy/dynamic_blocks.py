from __future__ import absolute_import
from collections import deque
import threading
from .block import Block
from .blocks import *
from .coordinate import Coordinate
from itertools import product
import logging

logger = logging.getLogger(__name__)

class DynamicBlocks():

    class MyDictSet(dict):
        def __missing__(self, key):
            return set()

    dependents = MyDictSet()
    dependencies = MyDictSet()
    ready_queue = deque()
    ready_queue_cv = threading.Condition()
    processing_blocks = set()
    blocks = {}
    # update_lock = threading.Lock()

    def __init__(
        self,
        total_roi,
        block_read_roi,
        block_write_roi,
        read_write_conflict=True,
        fit='valid'):

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

    def get_task(self, block_id):
        return self.blocks[block_id]


