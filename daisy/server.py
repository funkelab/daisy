import logging
from queue import Queue
from .scheduler import Scheduler
from .context import Context
from .tcp import TCPServer

logger = logging.getLogger(__name__)


class Server:

    def __init__(self):

        self.tcp_server = TCPServer()
        self.hostname, self.port = self.tcp_server.address

    def run_blockwise(self, tasks):

        self.scheduler = Scheduler(tasks)

        # create a worker pool per task
        self.worker_pools = {
            task.task_id: WorkerPool(
                task.spawn_worker_function,
                Context(
                    hostname=self.hostname,
                    post=self.port,
                    task_id=task.task_id)
            for task in tasks
        }

        self.pending_requests = {
            task.task_id: Queue()
        }

        # enter server event loop
        self.running = True

        ready_tasks = self.scheduler.get_ready_tasks()
        for task in ready_tasks:
            self.worker_pools[task.task_id].set_num_workers(task.config.num_workers)

        try:
            self._event_loop()
        except ...:
            pass
        finally:
            # tear down all worker pools

    def _event_loop(self):

        # TODO: keep number of workers in sync with scheduler requirements

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

            block, task_state = self.scheduler.acquire_block(message.task_id)

            if block is None:

                if task_state.num_pending_blocks == 0:
                    message.stream.send_message(TerminateClient())

                # there are more blocks for this task, but none of them has its
                # dependencies fullfilled
                self.pending_requests[message.task_id].put(message)

        elif isinstance(message, ReleaseBlock):

            task_state = self.scheduler.release_block(message.block)

            task_id = message.block.task_id

            for _ in range(task_state.num_ready_blocks):

                if self.pending_requests[task_id].empty():
                    break

                pending_request = self.pending_requests[task_id].get()
                self._handle_client_message(pending_request)

        elif isinstance(message, ClientException):

            logger.error("Received exception from client %s", message.client)
            raise message.exception

        else:

            logger.error("Server received unknown message: %s", type(message))

    def _check_worker_health(self):
        for worker_pool in self.worker_pools.values():
            worker_pool.check_for_errors()
