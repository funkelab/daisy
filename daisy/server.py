from .context import Context
from .scheduler import Scheduler
from .tcp import TCPServer
from .worker_pool import WorkerPool
from .messages import (
    AcquireBlock,
    ClientException,
    ReleaseBlock,
    SendBlock,
    RequestShutdown,
    UnexpectedMessage)
from queue import Queue
import logging

logger = logging.getLogger(__name__)


class Server:

    def __init__(self):

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

        logger.debug("Creating worker pools")
        self.worker_pools = {
            task.task_id: WorkerPool(
                task.spawn_worker_function,
                Context(
                    hostname=self.hostname,
                    port=self.port,
                    task_id=task.task_id))
            for task in tasks
        }

        self.pending_requests = {
            task.task_id: Queue()
            for task in tasks
        }

        self._recruit_workers()

        try:
            self._event_loop()
        finally:
            self._stop_workers()

    def _event_loop(self):

        self.running = True

        while self.running:
            self._handle_client_messages()
            self._check_worker_health()

    def _handle_client_messages(self):

        message = self.tcp_server.get_message(timeout=0.1)

        if message is None:
            return

        self._handle_client_message(message)

    def _handle_client_message(self, message):

        if isinstance(message, AcquireBlock):

            logger.debug("Received block request for task %s", message.task_id)

            task_state = self.scheduler.get_task_states()[message.task_id]

            if task_state.num_ready_blocks == 0:

                if task_state.num_pending_blocks == 0:
                    log.debug(
                        "No more pending blocks for task %s, terminating "
                        "client", message.task_id)
                    message.stream.send_message(RequestShutdown())
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
                message.stream.send_message(SendBlock(block))

        elif isinstance(message, ReleaseBlock):

            logger.debug("Client releases block %s", message.block)

            self.scheduler.release_block(message.block)
            task_states = self.scheduler.get_task_states()

            all_done = True

            for task_id, task_state in task_states.items():

                if task_state.is_done():
                    logger.debug("Task %s is done", task_id)
                    continue

                all_done = False

                logger.debug(
                    "Task %s has %d ready blocks",
                    task_id,
                    task_state.num_ready_blocks)

                for _ in range(task_state.num_ready_blocks):

                    if self.pending_requests[task_id].empty():
                        break

                    logger.debug(
                        "Answering delayed request for task %s",
                        task_id)
                    pending_request = self.pending_requests[task_id].get()
                    self._handle_client_message(pending_request)

            if all_done:
                logger.debug("All tasks finished")
                self.tcp_server.stop()
                self.running = False

            self._recruit_workers()

        elif isinstance(message, ClientException):

            logger.error("Received exception from client %s", message.client)
            raise message.exception

        else:

            raise UnexpectedMessage(message)

    def _recruit_workers(self):

        ready_tasks = self.scheduler.get_ready_tasks()
        ready_tasks = {task.task_id: task for task in ready_tasks}

        for task_id, worker_pool in self.worker_pools.items():
            if task_id in ready_tasks:
                worker_pool.set_num_workers(ready_tasks[task_id].num_workers)
            else:
                worker_pool.stop()

    def _stop_workers(self):

        for worker_pool in self.worker_pools.values():
            worker_pool.stop()

    def _check_worker_health(self):

        for worker_pool in self.worker_pools.values():
            worker_pool.check_for_errors()
