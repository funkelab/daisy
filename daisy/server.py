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
            self.worker_pools.check_worker_health()

    def _get_client_message(self):

        try:
            return self.tcp_server.get_message(timeout=0.1)
        except StreamClosedError:
            return

    def _send_client_message(self, stream, message):

        try:
            stream.send_message(message)
        except StreamClosedError:
            pass

    def _handle_client_messages(self):

        message = self._get_client_message()

        if message is None:
            return

        try:
            self._handle_client_message(message)
        except StreamClosedError as e:
            pass

    def _handle_client_message(self, message):

        if isinstance(message, AcquireBlock):

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

                logger.debug("Sending block %s to client", block)
                self._send_client_message(
                    message.stream,
                    SendBlock(block))

                self.notify_acquire_block(message.task_id, task_state)

        elif isinstance(message, ReleaseBlock):

            logger.debug("Client releases block %s", message.block)

            self.scheduler.release_block(message.block)
            task_states = self.scheduler.task_states
            task_id = message.block.task_id
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

                for _ in range(task_state.ready_count):

                    if self.pending_requests[task_id].empty():
                        break

                    logger.debug(
                        "Answering delayed request for task %s",
                        task_id)
                    pending_request = self.pending_requests[task_id].get()
                    self._handle_client_message(pending_request)

            if all_done:
                logger.debug("All tasks finished")
                self.running = False

            self._recruit_workers()

        elif isinstance(message, ClientException):

            logger.error("Received ClientException from %s", message.context)
            self._handle_client_exception(message)

        else:

            raise UnexpectedMessage(message)

    def _handle_client_exception(self, message):

        if isinstance(message, BlockFailed):

            logger.error(
                "Block %s failed on %s with %s",
                message.block,
                message.context,
                repr(message.exception))

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
