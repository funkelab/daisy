import tornado.ioloop
import threading
import logging

logger = logging.getLogger(__name__)


class IOLooper:
    '''Base class for every class that needs access to tornado's IOLoop in a
    separate thread.

    Attributes:

        ioloop (:class:`tornado.ioloop.IOLoop`):

            The IO loop to be used in subclasses. Will run in a singleton
            thread.
    '''

    thread = None

    def __init__(self):

        self.ioloop = tornado.ioloop.IOLoop.current()

        if IOLooper.thread is None:
            logger.debug("Starting io loop...")
            IOLooper.thread = threading.Thread(
                target=self.ioloop.start,
                daemon=True)
            IOLooper.thread.start()
