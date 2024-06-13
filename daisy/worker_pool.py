from .context import Context
from .worker import Worker
import logging
import threading
import queue
import multiprocessing

logger = logging.getLogger(__name__)


class WorkerPool:
    """Manages a pool of workers in individual processes. All workers are
    spawned by the same user-specified function.

    Args:

        spawn_worker_function (function):

              A function taking no arguments used to spawn a worker. This
              function will be executed inside the worker's individual process
              (i.e., there is no need to spawn a process in this function).

              This function should block as long as the worker is alive.

        context (:class:`daisy.Context`, optional):

              A context to pass on to workers through environment variables.
              Will be augmented with ``worker_id``, a unique ID for each worker
              that is spawned by this pool.
    """

    def __init__(self, spawn_worker_function, context=None):

        if context is None:
            context = Context()

        self.spawn_function = spawn_worker_function
        self.context = context
        self.workers = {}
        self.workers_lock = threading.Lock()

        self.error_queue = multiprocessing.Queue(100)

    def set_num_workers(self, num_workers):
        """Set the number of workers in this pool.

        If higher than the current number of running workers, new workers will
        be spawned using ``spawn_worker_function``.

        If lower, the youngest workers will be terminated.

        Thread-safe.

        Args:

            num_workers (int):

                  The new number of workers for this pool.
        """

        logger.debug("setting number of workers to %d", num_workers)

        with self.workers_lock:

            diff = num_workers - len(self.workers)

            logger.debug("current number of workers: %d", len(self.workers))

            if diff > 0:
                self._start_workers(diff)
            elif diff < 0:
                self._stop_workers(-diff)

    def inc_num_workers(self, num_workers):
        self._start_workers(num_workers)

    def stop(self, worker_id=None):
        """Stop all current workers in this pool (``worker_id == None``) or a
        specific worker."""

        if worker_id is None:
            self.set_num_workers(0)
            return

        if worker_id not in self.workers:
            # worker had been stopped previously
            return

        worker = self.workers[worker_id]
        worker.stop()
        del self.workers[worker_id]

    def check_for_errors(self):
        """If a worker fails with an exception, this exception will be queued
        in this pool to be propagated to the process that created the pool.
        Call this function periodically to check the queue and raise exceptions
        coming from the workers in the calling process.
        """

        try:
            error = self.error_queue.get(block=False)
            logger.debug("Got error: %s", error)
            raise error
        except queue.Empty:
            pass

    def _start_workers(self, n):

        logger.debug("starting %d new workers", n)
        new_workers = [
            Worker(self.spawn_function, self.context, self.error_queue)
            for _ in range(n)
        ]
        self.workers.update({worker.worker_id: worker for worker in new_workers})

    def _stop_workers(self, n):

        logger.debug("stopping %d workers", n)

        sentenced_worker_ids = list(self.workers.keys())[-n:]

        for worker_id in sentenced_worker_ids:
            worker = self.workers[worker_id]
            logger.debug("stopping worker %s", worker)
            worker.stop()
            del self.workers[worker_id]
