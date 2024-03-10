from .exceptions import NoFreePort
from .internal_messages import AckClientDisconnect, NotifyClientDisconnect
from .io_looper import IOLooper
from .tcp_stream import TCPStream
import logging
import queue
import socket
import tornado.tcpserver

logger = logging.getLogger(__name__)


class TCPServer(tornado.tcpserver.TCPServer, IOLooper):
    """A TCP server to handle client-server communication through
    :class:`Message` objects.

    Args:

        max_port_tries (int, optional):
            How many times to try to find an empty random port.
    """

    def __init__(self, max_port_tries=1000):

        tornado.tcpserver.TCPServer.__init__(self)
        IOLooper.__init__(self)

        self.message_queue = queue.Queue()
        self.exception_queue = queue.Queue()
        self.client_streams = set()

        # find empty port, start listening
        for i in range(max_port_tries):

            try:

                self.listen(0)  # 0 == random port
                break

            except OSError:

                if i == self.max_port_tries - 1:
                    raise NoFreePort(
                        "Could not find a free port after %d tries "
                        % self.max_port_tries
                    )

        self.address = self._get_address()

    def get_message(self, timeout=None):
        """Get a message that was sent to this server.

        If the stream to any of the connected clients is closed, raises a
        :class:`StreamClosedError` for this client. Other TCP related
        exceptions will also be raised by this function.

        Args:

            timeout (int, optional):

                If set, wait up to `timeout` seconds for a message to arrive.
                If no message is available after the timeout, returns ``None``.
                If not set, wait until a message arrived.
        """

        self._check_for_errors()

        try:

            return self.message_queue.get(block=True, timeout=timeout)

        except queue.Empty:

            return None

    def disconnect(self):
        """Close all open streams to clients."""

        streams = list(self.client_streams)  # avoid set change error
        for stream in streams:
            logger.debug("Closing stream %s", stream)
            stream.close()

    async def handle_stream(self, stream, address):
        """Overrides a function from tornado's TCPServer, and is called
        whenever there is a new IOStream from an incoming connection (not
        whenever there is new data in the IOStream).

        Args:

            stream (:class:`tornado.iostream.IOStream`):

                the incoming stream

            address (tuple):

                host, port that new connection comes from
        """

        logger.debug("Received new connection from %s:%d", *address)
        stream = TCPStream(stream, address)

        self.client_streams.add(stream)

        while True:

            try:

                message = await stream._get_message()

                if isinstance(message, NotifyClientDisconnect):

                    # client notifies that it disconnects, send a response
                    # indicating we are no longer using this stream and break
                    # out of event loop
                    stream.send_message(AckClientDisconnect())
                    self.client_streams.remove(stream)
                    return

                else:

                    self.message_queue.put(message)

            except Exception as e:

                try:
                    self.exception_queue.put(e)
                finally:
                    self.client_streams.remove(stream)
                    return

    def _check_for_errors(self):

        try:
            exception = self.exception_queue.get(block=False)
            raise exception
        except queue.Empty:
            return

    def _get_address(self):
        """Get the host and port of the tcp server"""

        sock = self._sockets[list(self._sockets.keys())[0]]
        port = sock.getsockname()[1]
        outside_sock = None
        try:
            outside_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            outside_sock.connect(("8.8.8.8", port))
            ip = outside_sock.getsockname()[0]
        except Exception:
            logger.error("Could not detect own IP address, returning bogus IP")
            return "8.8.8.8"
        finally:
            if outside_sock:
                outside_sock.close()
        return (ip, port)
