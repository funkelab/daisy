from __future__ import absolute_import

import collections
import logging
import itertools

from .blocks import (
    create_dependency_graph,
    get_subgraph_blocks,
    expand_request_roi_to_grid,
)

logger = logging.getLogger(__name__)


class TaskState:
    def __init__(self):
        self.ready_queue = []
        self.created = False
        self.total_block_count = 0
        self.done_count = 0
        self.failed_count = 0
        self.processing_blocks = set()
        self.failed_blocks = set()
        self.orphaned_blocks = set()


class PsuedoScheduler:
    """This class is significantly more than just a dependency graph,
    it keeps track of processing, failed, orphaned blocks etc.
    DependencyGraph should just enumerate blocks and their
    upstream/downstream dependencies.
    """

    def __init__(self, tasks):

        # task states:
        self.task_states = collections.defaultdict(TaskState)
        self.block_states = collections.defaultdict(BlockState)

        # dependency graph
        self.dependency_graph = DependencyGraph(tasks)

        self.retry_count = collections.defaultdict(int)

    @property
    def processing_blocks(self):
        set().union(
            *[
                set((task_id, block_ind) for block_ind in task_state.processed_blocks)
                for task_id, task_state in self.task_states
            ]
        )

    @property
    def orphaned_blocks(self):
        set().union(
            *[
                set((task_id, block_ind) for block_ind in task_state.orphaned_blocks)
                for task_id, task_state in self.task_states
            ]
        )

    @property
    def failed_blocks(self):
        set().union(
            *[
                set((task_id, block_ind) for block_ind in task_state.failed_blocks)
                for task_id, task_state in self.task_states
            ]
        )

    def add_to_ready_queue(self, task_id, block_id):
        self.task_states[task_id].ready_queue.append(block_id)

    def get_from_ready_queue(self, task_id):
        return self.task_states[task_id].ready_queue.popleft()

    def next(self, waiting_blocks):
        """Called by the ``scheduler`` to get a `dict` of ready blocks.
        This function blocks when outstanding blocks are empty and
        there is no further ready blocks to issue. This (only) happens
        when there are outstanding, currently executing blocks.

        Return:
            `dict` {task_id: block} for ready task blocks.
            Empty `dict` does not necessarily mean that there is
            no more blocks to be run (though it is the case currently).
            The scheduler should call empty() to really make sure that
            there is no more blocks to run.
        """

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
                self.task_states[block_id[0]].processing_blocks.add(block_id[1])
                return_blocks[block_id[0]] = self.blocks[block_id]

        return return_blocks

    def empty(self):
        """Return ``True`` if there is no more blocks to be executed,
        either because all blocks have been completed, or because there
        are failed blocks that prevent other blocks from running."""
        return (self.ready_size() == 0) and (len(self.processing_blocks) == 0)

    def size(self):
        """Return the size of the block-wise graph."""
        return len(self.blocks)

    def ready_size(self):
        """Return the number of blocks ready to be run."""
        count = 0
        for task in self.task_states:
            count += len(self.task_states[task].ready_queue)
        return count

    def cancel_and_reschedule(self, block_id):
        """Used to notify that a block has failed. The block will either
        be rescheduled if within the number of retries, or be marked
        as failed."""
        if block_id[1] not in self.task_states[block_id[0]]:
            logger.error("Block %d is canceled but was not found", block_id)
            raise

        self.retry_count[block_id] += 1

        self.task_states[block_id[0]].processing_blocks.remove(block_id[1])
        task_id = block_id[0]

        if self.retry_count[block_id] > self.task_map[task_id]._daisy.max_retries:

            self.task_states[block_id[0]].add(block_id[1])
            self.task_states[block_id[0]].failed_count += 1
            logger.error(
                "Block {} is canceled and will not be rescheduled.".format(block_id)
            )

            if len(self.downstream[block_id]):
                logger.error(
                    "The following blocks are then orphaned and "
                    "cannot be run: {}".format(self.downstream[block_id])
                )

            self.recursively_check_orphans(block_id)
            # simply leave it canceled at this point

        else:

            self.add_to_ready_queue(task_id, block_id)
            logger.info("Block {} will be rescheduled.".format(block_id))

    def recursively_check_orphans(self, block_id):
        """Check and mark children of the given block as orphans."""
        for orphan_id in self.downstream[block_id]:

            if (
                orphan_id
                in self.task_states[orphan_id[0]].orphaned_blocks[orphan_id[1]]
                or orphan_id
                in self.task_states[orphan_id[0]].failed_blocks[orphan_id[1]]
            ):
                # TODO: isn't this return too early, shouldn't we check all
                # orphan_ids in self.downstream[block_id]?
                return

            self.task_states[orphan_id[0]].orphaned_blocks.add(orphan_id[1])
            self.recursively_check_orphans(orphan_id)

    def remove_and_update(self, block_id):
        """Removing a finished block and update ready queue."""

        if block_id[1] not in self.task_states[block_id[0]]:
            logger.error("Block %d is canceled but was not found", block_id)
            raise

        self.task_states[block_id[0]].done_count += 1
        self.task_states[block_id[0]].processing_blocks.remove(block_id[1])

        dependents = self.downstream[block_id]
        for dep in dependents:
            self.upstream[dep].remove(block_id)
            if len(self.upstream[dep]) == 0:
                # ready to run
                self.add_to_ready_queue(dep[0], dep)

    def _get_subgraph_blocks(self, task_id, roi):
        """Return blocks of this task that write to given ROI."""
        task = self.task_map[task_id]
        return get_subgraph_blocks(
            roi,
            task._daisy.total_roi,
            task._daisy.read_roi,
            task._daisy.write_roi,
            task._daisy.fit,
        )

    def is_task_done(self, task_id):
        """Return ``True`` if all blocks of a task have completed."""
        return (
            self.task_states[task_id].done_count
            == self.task_states[task_id].total_block_count
        )

    def get_task_size(self, task_id):
        return self.task_states[task_id].total_block_count

    def get_task_done_count(self, task_id):
        return self.task_states[task_id].done_count

    def get_task_failed_count(self, task_id):
        return self.task_states[task_id].failed_count

    def get_task_processing_blocks(self, task_id):
        return self.task_states[task_id].processing_blocks
