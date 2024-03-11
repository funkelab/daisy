from __future__ import absolute_import

from .dependency_graph import DependencyGraph
from .ready_surface import ReadySurface
from .task_state import TaskState
from .processing_queue import ProcessingQueue
from .task import Task
from .block import BlockStatus

from typing import Any, Dict, List
import collections
import logging

logger = logging.getLogger(__name__)


class Scheduler:
    """This is the main scheduler that tracks states of tasks.

    The Scheduler takes a list of tasks, and upon request will
    provide the next block available for processing.

    args:
        tasks:
            the list of tasks to schedule. If any of the tasks have
            upstream dependencies these will be recursively enumerated
            and added to the scheduler.
        count_all_orphans: bool:
            Whether to guarantee accurate counting of all orphans. This
            can be inefficient if your dependency tree is particularly
            deep rather than just wide, so consider flipping this to
            False if you are having performance issues.
            If False, orphaned blocks will be counted as "pending" in
            the task state since there is no way to tell the difference
            between the two types without enumerating all orphans.
    """

    def __init__(self, tasks: List[Task], count_all_orphans=True):
        self.dependency_graph = DependencyGraph(tasks)
        self.ready_surface = ReadySurface(
            self.dependency_graph.downstream, self.dependency_graph.upstream
        )

        self.task_map: Dict[Any, Task] = {}
        self.task_states: Dict[Any, TaskState] = collections.defaultdict(TaskState)
        self.task_queues: Dict[Any, ProcessingQueue] = collections.defaultdict(
            ProcessingQueue
        )

        # root tasks is a mapping from task_id -> (num_roots, root_generator)
        roots = self.dependency_graph.roots()
        for task_id, (num_roots, root_gen) in roots.items():
            self.task_states[task_id].ready_count += num_roots
            self.task_queues[task_id] = ProcessingQueue(num_roots, root_gen)

        for task in tasks:
            self.__init_task(task)

        self.count_all_orphans = count_all_orphans

    def get_ready_tasks(self) -> List[Task]:
        """
        Get a list of tasks that currently have blocks available for scheduling
        """
        ready_tasks = []
        for task_id, task_state in self.task_states.items():
            if task_state.ready_count > 0:
                ready_tasks.append(self.task_map[task_id])
        return ready_tasks

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
        """
        while True:
            block = self.task_queues[task_id].get_next()
            if block is not None:

                # update states
                self.task_states[task_id].ready_count -= 1
                self.task_states[task_id].processing_count += 1

                pre_check_ret = self.__precheck(block)
                if pre_check_ret:
                    logger.debug(
                        "Skipping block (%s); already processed.", block.block_id
                    )
                    block.status = BlockStatus.SUCCESS
                    self.task_states[task_id].skipped_count += 1
                    # adding block so release_block() can remove it
                    self.task_queues[task_id].processing_blocks.add(block.block_id)
                    self.release_block(block)
                    continue
                else:
                    self.task_states[task_id].started = True
                    self.task_queues[task_id].processing_blocks.add(block.block_id)
                    return block

            else:
                return None

    def release_block(self, block):
        """
        Update the dependency graph with the status
        of a given block ``block``.

        Args:
            task(``Task``):
                Task of interest.

            block(``Block``):
                Block of interest.

        Return:
            ``dict``(``task_id`` -> ``TaskState``):
            Each task returned had its
            state changed by updating the status of the given
            block on the given task. i.e. if a task B was
            dependent on task A, and marking a block in A
            as solved made some blocks in B available for
            processing, task B would be returned with its state.
        """
        task_id = block.task_id
        self.__remove_from_processing_blocks(block)
        if block.status == BlockStatus.SUCCESS:
            new_blocks = self.ready_surface.mark_success(block)
            self.task_states[block.task_id].completed_count += 1
            updated_tasks = self.__update_ready_queue(new_blocks)
            return updated_tasks
        if block.status == BlockStatus.FAILED:
            if (
                self.task_queues[task_id].block_retries[block.block_id]
                >= self.task_map[task_id].max_retries
            ):
                logger.debug("Marking %s as permanently failed", block)
                orphans = self.ready_surface.mark_failure(
                    block, count_all_orphans=self.count_all_orphans
                )
                logger.debug("Number of orphans is %d", len(orphans))
                self.task_states[block.task_id].failed_count += 1
                for orphan in orphans:
                    self.task_states[orphan.task_id].orphaned_count += 1
                return {}
            else:
                logger.debug("Marking %s as temporarily failed", block)
                self.__queue_ready_block(block)
                self.task_queues[task_id].block_retries[block.block_id] += 1
                return {task_id: self.task_states[task_id]}
        else:
            raise RuntimeError(
                f"Unexpected status for released block: {block.status} {block}"
            )

    def __init_task(self, task):
        if task.task_id not in self.task_map:
            self.task_map[task.task_id] = task
            num_blocks = self.dependency_graph.num_blocks(task.task_id)
            self.task_states[task.task_id].total_block_count = num_blocks

            for upstream_task in task.requires():
                self.__init_task(upstream_task)

    def __queue_ready_block(self, block, index=None):
        if index is None:
            self.task_queues[block.task_id].ready_queue.append(block)
        else:
            self.task_queues[block.task_id].ready_queue.insert(index, block)
        self.task_states[block.task_id].ready_count += 1

    def __remove_from_processing_blocks(self, block):
        self.task_queues[block.task_id].processing_blocks.remove(block.block_id)
        self.task_states[block.task_id].processing_count -= 1

    def __update_ready_queue(self, ready_blocks):
        updated_tasks = {}
        for ready_block in ready_blocks:
            self.__queue_ready_block(ready_block)
            task_state = self.task_states[ready_block.task_id]
            updated_tasks[ready_block.task_id] = task_state
        return updated_tasks

    def __precheck(self, block):
        try:
            # pre_check can intermittently fail
            # so we wrap it in a try block
            if self.task_map[block.task_id].check_function is not None:
                return self.task_map[block.task_id].check_function(block)
            else:
                return False
        except Exception:
            logger.exception(f"pre_check() exception for block {block.block_id}")
            return False
