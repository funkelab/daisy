import logging
import os
import threading
from typing import Dict
import tornado.ioloop

logger = logging.getLogger(__name__)


class IOLooper:
    """Base class for every class that needs access to tornado's IOLoop in a
    separate thread.

    Attributes:

        ioloop (:class:`tornado.ioloop.IOLoop`):

            The IO loop to be used in subclasses. Will run in a singleton
            thread per process.
    """

    threads: Dict[int, threading.Thread] = {}
    ioloops: Dict[int, tornado.ioloop.IOLoop] = {}

    @staticmethod
    def clear():
        IOLooper.threads = {}
        IOLooper.ioloops = {}

    def __init__(self):

        pid = os.getpid()

        if pid not in IOLooper.threads:

            logger.debug("Creating new IOLoop for process %d...", pid)
            self.ioloop = tornado.ioloop.IOLoop()
            self.ioloops[pid] = self.ioloop

            logger.debug("Starting io loop for process %d...", pid)
            IOLooper.threads[pid] = threading.Thread(
                target=self.ioloop.start, daemon=True
            )
            IOLooper.threads[pid].start()

        else:

            logger.debug("Reusing IOLoop for process %d...", pid)
            self.ioloop = self.ioloops[pid]
