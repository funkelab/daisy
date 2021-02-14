from .block import BlockStatus
from .block_bookkeeper import BlockBookkeeper
from .messages import (
    AcquireBlock,
    BlockFailed,
    ClientException,
    ReleaseBlock,
    SendBlock,
    RequestShutdown,
    UnexpectedMessage)
from .scheduler import Scheduler
from .server_observer import ServerObservee
from .task_worker_pools import TaskWorkerPools
from .tcp import TCPServer
from .tcp.exceptions import StreamClosedError
from queue import Queue
import logging

logger = logging.getLogger(__name__)


class Server(ServerObservee):

    def __init__(self):

        super().__init__()

        self.tcp_server = TCPServer()
        self.hostname, self.port = self.tcp_server.address

        logger.debug(
            "Started server listening at %s:%s",
            self.hostname,
            self.port)

    def run_blockwise(self, tasks, scheduler=None):

        if scheduler is None:
            self.scheduler = Scheduler(tasks)
        else:
            self.scheduler = scheduler

        self.worker_pools = TaskWorkerPools(tasks, self)
        self.block_bookkeeper = BlockBookkeeper()
        self.finished_tasks = set()

        self.pending_requests = {
            task.task_id: Queue()
            for task in tasks
        }

        self._recruit_workers()

        try:
            self._event_loop()
        finally:
            self.worker_pools.stop()

    def _event_loop(self):

        self.running = True

        while self.running:
            self._handle_client_messages()
            self._check_for_lost_blocks()
            self.worker_pools.check_worker_health()

    def _get_client_message(self):

        try:
            message = self.tcp_server.get_message(timeout=0.1)
        except StreamClosedError:
            return

        if message is not None:
            return message

        for task_id, requests in self.pending_requests.items():

            if self.pending_requests[task_id].empty():
                continue

            logger.debug("Answering delayed request for task %s", task_id)
            return self.pending_requests[task_id].get()

    def _send_client_message(self, stream, message):

        try:
            stream.send_message(message)
        except StreamClosedError:
            pass

    def _handle_client_messages(self):

        message = self._get_client_message()

        if message is None:
            return

        self._handle_client_message(message)

    def _handle_client_message(self, message):

        if isinstance(message, AcquireBlock):
            self._handle_acquire_block(message)
        elif isinstance(message, ReleaseBlock):
            self._handle_release_block(message)
        elif isinstance(message, ClientException):
            self._handle_client_exception(message)
        else:
            raise UnexpectedMessage(message)

    def _handle_acquire_block(self, message):

        logger.debug("Received block request for task %s", message.task_id)

        task_state = self.scheduler.task_states[message.task_id]

        logger.debug("Current task state: %s", task_state)

        if task_state.ready_count == 0:

            if task_state.pending_count == 0:

                logger.debug(
                    "No more pending blocks for task %s, terminating "
                    "client", message.task_id)

                self._send_client_message(
                    message.stream,
                    RequestShutdown())

                return

            # there are more blocks for this task, but none of them has its
            # dependencies fullfilled
            logger.debug(
                "No currently ready blocks for task %s, delaying "
                "request", message.task_id)
            self.pending_requests[message.task_id].put(message)

        else:

            # now we know there is at least one ready block
            block = self.scheduler.acquire_block(message.task_id)
            assert block is not None

            try:
                logger.debug("Sending block %s to client", block)
                self._send_client_message(
                    message.stream,
                    SendBlock(block))
            finally:
                self.block_bookkeeper.notify_block_sent(block, message.stream)

            self.notify_acquire_block(message.task_id, task_state)

    def _handle_release_block(self, message):

        logger.debug("Client releases block %s", message.block)
        self._safe_release_block(message.block, message.stream)

    def _release_block(self, block):
        '''Returns a block to the scheduler and checks whether all tasks are
        completed.'''

        self.scheduler.release_block(block)
        task_states = self.scheduler.task_states
        task_id = block.task_id
        self.notify_release_block(task_id, task_states[task_id])

        all_done = True

        for task_id, task_state in task_states.items():
            logger.debug("Task state for task %s: %s", task_id, task_state)
            if task_state.is_done():
                if task_id not in self.finished_tasks:
                    self.notify_task_done(task_id)
                    logger.debug("Task %s is done", task_id)
                    self.finished_tasks.add(task_id)
                continue

            all_done = False

            logger.debug(
                "Task %s has %d ready blocks",
                task_id,
                task_state.ready_count)

        if all_done:
            logger.debug("All tasks finished")
            self.running = False

        self._recruit_workers()

    def _safe_release_block(self, block, stream):
        '''Releases a block, if the bookkeeper agrees that this is a valid
        return from the given stream.'''

        valid = self.block_bookkeeper.is_valid_return(block, stream)
        if valid:
            self._release_block(block)
            self.block_bookkeeper.notify_block_returned(block, stream)
        else:
            logger.debug(
                "Attempted to return unexpected block %s from %s",
                block, stream)

    def _handle_client_exception(self, message):

        logger.error("Received ClientException from %s", message.context)

        if isinstance(message, BlockFailed):

            logger.error(
                "Block %s failed on %s with %s",
                message.block,
                message.context,
                repr(message.exception))

            message.block.status = BlockStatus.FAILED

            self._safe_release_block(message.block, message.stream)

            self.notify_block_failure(
                message.block,
                message.exception,
                message.context)

        else:
            raise message.exception

    def _recruit_workers(self):

        ready_tasks = self.scheduler.get_ready_tasks()
        ready_tasks = {task.task_id: task for task in ready_tasks}

        self.worker_pools.recruit_workers(ready_tasks)

    def _check_for_lost_blocks(self):

        lost_blocks = self.block_bookkeeper.get_lost_blocks()

        # mark as failed and release the lost blocks
        for block in lost_blocks:

            logger.error("Block %s was lost, returning it to scheduler", block)
            block.status = BlockStatus.FAILED
            self._release_block(block)
