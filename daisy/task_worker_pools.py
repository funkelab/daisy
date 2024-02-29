from .context import Context
from .server_observer import ServerObserver
from .worker_pool import WorkerPool
import logging

logger = logging.getLogger(__name__)


class TaskWorkerPools(ServerObserver):

    def __init__(self, tasks, server, max_block_failures=3):

        super().__init__(server)

        logger.debug("Creating worker pools")
        self.worker_pools = {
            task.task_id: WorkerPool(
                task.spawn_worker_function,
                Context(
                    hostname=server.hostname, port=server.port, task_id=task.task_id
                ),
            )
            for task in tasks
        }
        self.max_block_failures = max_block_failures
        self.failure_counts = {}

    def recruit_workers(self, tasks):

        for task_id, worker_pool in self.worker_pools.items():
            if task_id in tasks:
                logger.debug(
                    "Setting number of workers for task %s to %d",
                    task_id,
                    tasks[task_id].num_workers,
                )
                worker_pool.set_num_workers(tasks[task_id].num_workers)

    def stop(self):

        logger.debug("Stopping all workers")
        for worker_pool in self.worker_pools.values():
            worker_pool.stop()

    def check_worker_health(self):

        for worker_pool in self.worker_pools.values():
            worker_pool.check_for_errors()

    def on_block_failure(self, block, exception, context):

        task_id = context["task_id"]
        worker_id = int(context["worker_id"])

        if task_id not in self.failure_counts:
            self.failure_counts[task_id] = {}

        if worker_id not in self.failure_counts[task_id]:
            self.failure_counts[task_id][worker_id] = 0

        self.failure_counts[task_id][worker_id] += 1

        if self.failure_counts[task_id][worker_id] > self.max_block_failures:

            logger.error(
                "Worker %s failed too many times, restarting this worker...", context
            )

            self.failure_counts[task_id][worker_id] = 0
            worker_pool = self.worker_pools[task_id]
            worker_pool.stop(worker_id)
            worker_pool.inc_num_workers(1)
