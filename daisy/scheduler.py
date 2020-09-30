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

    def acquire_block(self, task_id):
        """
        Get a block that is ready to process for task with given task_id.

        Args:
            task_id(``int``):
                The task for which you want a block

        Return:
            ``Block`` or None:
                A block that can be run without worry of
                conflicts.
            ``TaskState``:
                The state of the task.
        """
        if task_id in self.available_blocks:
            block = self.available_blocks[task_id]
            pre_check_ret = self.precheck(task_id, block)

            if pre_check_ret:
                logger.debug(
                    "Skipping task %s block %d; already processed.",
                    task_id,
                    block.block_id,
                )
                self.available_blocks.pop(task_id)
                return self.acquire_block(task_id)

            else:
                return (
                    self.available_blocks.pop(task_id),
                    self.dependency_graph.task_state(task_id),
                )

        else:
            # get next from dependency graph
            self.available_blocks = self.dependency_graph.next(self.available_blocks)
            if task_id not in self.available_blocks:
                return None, self.dependency_graph.task_state(task_id)
            return self.acquire_block(task_id)

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

    def precheck(self, task_id, block):
        if self.last_prechecked[task_id][0] != block:
            # pre-check and skip blocks if possible
            try:
                # pre_check can intermittently fail
                # so we wrap it in a try block
                pre_check_ret = self.tasks[task_id]._daisy.pre_check(block)
            except Exception as e:
                logger.error(
                    "pre_check() exception for block %s of task %s. " "Exception: %s",
                    block,
                    task_id,
                    e,
                )
                pre_check_ret = False
            finally:
                self.last_prechecked[task_id] = (block, pre_check_ret)

        return self.last_prechecked[task_id][1]


def run_blockwise():
    raise NotImplementedError()


def distribute():
    raise NotImplementedError()
