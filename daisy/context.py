import os
import logging

logger = logging.getLogger(__name__)


class Context():

    def __init__(self, hostname, port, task_id, worker_id, num_workers):
        self.hostname = hostname
        self.port = port
        self.task_id = task_id
        self.worker_id = worker_id
        self.num_workers = num_workers

    def to_env(self):

        return '%s:%d:%s:%d:%d' % (
            self.hostname,
            self.port,
            self.task_id,
            self.worker_id,
            self.num_workers
        )

    @staticmethod
    def from_env():

        try:
            tokens = os.environ['DAISY_CONTEXT'].split(':')
        except KeyError:
            logger.error(
                "DAISY_CONTEXT environment variable not found!")
            raise

        try:

            return Context(
                tokens[0],
                int(tokens[1]),
                tokens[2],
                int(tokens[3]),
                int(tokens[4]))

        except KeyError:
            logger.error("DAISY_CONTEXT malformed")
            raise
