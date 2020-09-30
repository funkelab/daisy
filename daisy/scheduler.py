from __future__ import absolute_import

from .context import Context
from .dependency_graph import DependencyGraph
from .task import Task

from inspect import signature

from typing import List
import collections
from datetime import timedelta
import logging
import os
import queue
import socket
import threading
import time

logger = logging.getLogger(__name__)

# used for automated testing to speed up tests as the status thread takes a
# long time to shutdown
_NO_SPAWN_STATUS_THREAD = False


class Scheduler:
    """This is the main scheduler that tracks states of tasks.

    The Scheduler takes a list of tasks, and upon request will
    provide the next block available for processing.

    Usage:

    .. code:: python

        graph = DependencyGraph(...)
        return Scheduler().distribute(graph)

    See the DependencyGraph class for more information.
    """

    def __init__(self, tasks: List[Task]):
        self.dependency_graph = DependencyGraph()
        for task in tasks:
            self.dependency_graph.add(task)
            self.dependency_graph.init(task.task_id, task._daisy.total_write_roi)

        self.lock = threading.Lock()

        self.available_blocks = {}

        self.last_prechecked = collections.defaultdict(lambda: (None, None))

    def acquire_block(self, task):
        """
        Get a block that is ready to process for task ``task``.

        Args:
            task(``Task``):
                The task for which you want a block
            
        Return:
            ``Block``:
                A block that can be run without worry of
                conflicts.
        """
        if task.task_id in self.available_blocks:
            block = self.available_blocks[task.task_id]
            if self.last_prechecked[task.task_id][0] != block:
                # pre-check and skip blocks if possible
                try:
                    # pre_check can intermittently fail
                    # so we wrap it in a try block
                    pre_check_ret = self.tasks[task.task_id]._daisy.pre_check(block)
                except Exception as e:
                    logger.error(
                        "pre_check() exception for block %s of task %s. "
                        "Exception: %s",
                        block,
                        task,
                        e,
                    )
                    pre_check_ret = False
                finally:
                    self.last_prechecked[task] = (block, pre_check_ret)

            pre_check_ret = self.last_prechecked[task][1]

            if pre_check_ret:
                logger.debug(
                    "Skipping %s block %d; already processed.",
                    task,
                    block.block_id,
                )
                self.available_blocks.pop(task)
                return self.acquire_block(task)

            else:
                return self.available_blocks.pop(task.task_id)

        else:
            # INFINITE LOOP WARNING
            # The possible cases here:
            # 1) This task is done, no more blocks will ever be available
            # 2) All available blocks are being processed, blocks might become available
            # 3) Block is available and continue as normal

            # These cases should be handled by task STATE
            self.available_blocks = self.dependency_graph.next(
                self.available_blocks
            )
            if task.task_id not in self.available_blocks:
                raise Exception("No block available for this task!")
            return self.acquire_block(task)

    def release_block(self, task, block, status):
        """
        Update the dependency graph with the status
        of a given block ``block`` on task ``task``.

        Args:
            task(``Task``):
                Task of interest.

            block(``Block``):
                Block of interest.

            status(``Status``):
                Result of processing given block on given task

        Return:
            ``list``(``Task``):
            Each task returned had its
            state changed by updating the status of the given
            block on the given task. i.e. if a task B was
            dependent on task A, and marking a block in A
            as solved made some blocks in B available for
            processing, task B would update its state from
            waiting to Waiting to Ready and be returned.
        """

        with self.lock:
            self.dependency_graph.remove_and_update((task.task_id, block.block_id))


def run_blockwise():
    raise NotImplementedError()


def distribute():
    raise NotImplementedError()
