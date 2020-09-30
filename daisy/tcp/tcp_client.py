import logging
import queue
import sys
import time
import tornado.tcpclient

from .io_looper import IOLooper
from .tcp_stream import TCPStream
from daisy.message import ExceptionMessage

logger = logging.getLogger(__name__)

# TODO: Do we want to close the stream or ioloop or anything?
'''

    def __del__(self):
        Stop spawn ioloop when client is done
        try:
            self.ioloop.add_callback(self.ioloop.stop)
        except Exception:
            # self.ioloop is None
            pass

'''


class TCPClient(IOLooper):
    '''A TCP client to handle client-server communication through
    :class:`Message` objects.

    Args:

        host (string):
        port (int):
            The hostname and port of the :class:`TCPServer` to connect to.
    '''
    def __init__(self, host, port):

        super().__init__()

        logger.debug("Creating new client...")

        self.host = host
        self.port = port
        self.client = tornado.tcpclient.TCPClient()
        self.stream = None
        self.message_queue = queue.Queue()
        self.error_state = False

        logger.debug("Connecting to server...")

        self.ioloop.add_callback(self._connect)

        logger.debug("Waiting for connection to be established...")
        while not self.stream:
            if self.error_state:
                logger.error("Could not connect to server")
                sys.exit(1)
            time.sleep(.1)
        logger.debug("...connected")

        self.ioloop.add_callback(self._receive)

    def send_message(self, message):
        '''Send a message to the server asynchronously. Same as::

            self.stream.send_message(message)

        Args:

            message (:class:`daisy.Message`):

                Message to send over the connection.
        '''
        self.stream.send_message(message)

    def get_message(self, timeout=None):
        '''Get a message that was sent to this client.

        Args:

            timeout (int, optional):

                If set, wait up to `timeout` seconds for a message to arrive.
                If no message is available after the timeout, returns ``None``.
                If not set, wait until a message arrived.
        '''
        try:

            message = self.message_queue.get(block=True, timeout=timeout)
            if isinstance(message, ExceptionMessage):
                raise message.exception
            return message

        except queue.Empty:

            return None

    async def _connect(self):

        logger.debug("Connecting to %s:%d...", self.host, self.port)
        try:
            stream = await self.client.connect(self.host, self.port)
            # TODO: Add a timeout? previously had 60 second timeout and 10
            # retries
        except Exception:
            self.error_state = True
            raise
        self.stream = TCPStream(stream)

        logger.debug("...connected")

    async def _receive(self):
        '''Loop that receives messages from the server.'''

        logger.debug("Entering receive loop")

        while True:
            try:

                message = await self.stream._get_message()
                if message is None:
                    break
                self.message_queue.put(message)

            except Exception as e:

                logger.error("TCPClient %s got Exception %s", self, e)
                break
