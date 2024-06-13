from .context import Context
import daisy.logging as daisy_logging
import logging
import multiprocessing
import os
import queue
import dill

logger = logging.getLogger(__name__)


class Worker:
    """Create and start a worker, running a user-specified function in its own
    process.

    Args:

        spawn_function (function):

              A function taking no arguments used to spawn a worker. This
              function will be executed inside its own process (i.e., there is
              no need to spawn a process in this function).

        context (:class:`daisy.Context`, optional):

              If given, the context will be passed on to the worker via
              environment variables.
    """

    __next_id = multiprocessing.Value("L")
    _spawn_function = None

    @staticmethod
    def get_next_id():
        with Worker.__next_id.get_lock():
            next_id = Worker.__next_id.value
            Worker.__next_id.value += 1
        return next_id

    def __init__(self, spawn_function, context=None, error_queue=None):

        self.spawn_function = spawn_function
        self.worker_id = Worker.get_next_id()
        if context is None:
            self.context = Context()
        else:
            self.context = context.copy()
        self.context["worker_id"] = self.worker_id
        self.error_queue = error_queue
        self.process = None

        self.start()

    @property
    def spawn_function(self):
        return dill.loads(self._spawn_function)

    @spawn_function.setter
    def spawn_function(self, value):
        self._spawn_function = dill.dumps(value)

    def start(self):
        """Start this worker. Note that workers are automatically started when
        created. Use this function to re-start a stopped worker."""

        if self.process is not None:
            return

        self.process = multiprocessing.Process(target=self._spawn_wrapper)
        self.process.start()

    def stop(self):
        """Stop this worker."""

        if self.process is None:
            return

        logger.debug("Terminating %s", self)
        self.process.terminate()

        logger.debug("Joining %s", self)
        self.process.join()

        logger.debug("%s terminated", self)
        self.process = None

    def _spawn_wrapper(self):
        """Thin wrapper around the user-specified spawn function to set
        environment variables, redirect output, and to capture exceptions."""

        try:

            os.environ[self.context.ENV_VARIABLE] = self.context.to_env()

            log_base = daisy_logging.get_worker_log_basename(
                self.worker_id, self.context.get("task_id", None)
            )
            daisy_logging.redirect_stdouterr(log_base)

            self.spawn_function()

        except Exception as e:

            logger.error("%s received exception: %s", self, e)
            if self.error_queue:
                try:
                    self.error_queue.put(e, timeout=1)
                except queue.Full:
                    logger.error(
                        "%s failed to forward exception, error queue is full", self
                    )

        except KeyboardInterrupt:

            logger.debug("%s received ^C", self)

    def __repr__(self):

        return "worker (%s)" % self.context
