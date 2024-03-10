from .exceptions import NotConnected
from .io_looper import IOLooper
from .tcp_stream import TCPStream
from .internal_messages import AckClientDisconnect, NotifyClientDisconnect
import logging
import queue
import time
import tornado.tcpclient

logger = logging.getLogger(__name__)


class TCPClient(IOLooper):
    """A TCP client to handle client-server communication through
    :class:`TCPMessage` objects.

    Args:

        host (string):
        port (int):

            The hostname and port of the :class:`TCPServer` to connect to.
    """

    def __init__(self, host, port):

        super().__init__()

        logger.debug("Creating new TCP client...")

        self.host = host
        self.port = port
        self.client = tornado.tcpclient.TCPClient()
        self.stream = None
        self.message_queue = queue.Queue()
        self.exception_queue = queue.Queue()

        self.connect()

    def __del__(self):

        if self.connected():
            self.disconnect()

    def connect(self):
        """Connect to the server and start the message receive event loop."""

        logger.debug("Connecting to server at %s:%d...", self.host, self.port)

        self.ioloop.add_callback(self._connect)
        while not self.connected():
            self._check_for_errors()
            time.sleep(0.1)

        logger.debug("...connected")

        self.ioloop.add_callback(self._receive)

    def disconnect(self):
        """Gracefully close the connection to the server."""

        if not self.connected():
            logger.warn("Called disconnect() on disconnected client")
            return

        logger.debug("Notifying server of disconnect...")
        self.stream.send_message(NotifyClientDisconnect())

        while self.connected():
            time.sleep(0.1)

        logger.debug("Disconnected")

    def connected(self):
        """Check whether this client has a connection to the server."""
        return self.stream is not None

    def send_message(self, message):
        """Send a message to the server.

        Args:

            message (:class:`TCPMessage`):

                Message to send over to the server.
        """

        self._check_for_errors()

        if not self.connected():
            raise NotConnected()

        self.stream.send_message(message)

    def get_message(self, timeout=None):
        """Get a message that was sent to this client.

        Args:

            timeout (``float``, optional):

                If set, wait up to `timeout` seconds for a message to arrive.
                If no message is available after the timeout, returns ``None``.
                If not set, wait until a message arrived.
        """

        self._check_for_errors()

        if not self.connected():
            raise NotConnected()

        try:

            return self.message_queue.get(block=True, timeout=timeout)

        except queue.Empty:

            return None

    def _check_for_errors(self):

        try:
            exception = self.exception_queue.get(block=False)
            raise exception
        except queue.Empty:
            return

    async def _connect(self):
        """Async method to connect to the TCPServer."""

        try:
            stream = await self.client.connect(self.host, self.port)
        except Exception as e:
            self.exception_queue.put(e)
            return

        self.stream = TCPStream(stream, (self.host, self.port))

    async def _receive(self):
        """Loop that receives messages from the server."""

        logger.debug("Entering receive loop")

        while self.connected():

            try:

                # raises StreamClosedError
                message = await self.stream._get_message()

                if isinstance(message, AckClientDisconnect):

                    # server acknowledged disconnect, close connection on
                    # our side and break out of event loop
                    try:
                        self.stream.close()
                    finally:
                        self.stream = None
                        return

                else:

                    self.message_queue.put(message)

            except Exception as e:

                try:
                    self.exception_queue.put(e)
                    self.stream.close()
                finally:
                    # mark client as disconnected
                    self.stream = None
                    return
