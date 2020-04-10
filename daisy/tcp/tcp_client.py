import logging
import queue
import time
import tornado.tcpclient

from .io_looper import IOLooper
from .tcp_stream import TCPStream
from daisy import ExceptionMessage

logger = logging.getLogger(__name__)


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

        self.host = host
        self.port = port
        self.client = tornado.tcpclient.TCPClient()
        self.stream = None
        self.message_queue = queue.Queue()

        self.ioloop.add_callback(self._connect)
        # wait until connected
        while not self.stream:
            time.sleep(.1)

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
        stream = await self.client.connect(self.host, self.port)
        self.stream = TCPStream(stream)

    async def _receive(self):
        '''Loop that receives messages from the server.'''

        while True:
            try:

                message = await self.stream._get_message()
                self.message_queue.put(message)

            except Exception as e:

                logger.error("TCPClient %s got Exception %s", self, e)
                break
