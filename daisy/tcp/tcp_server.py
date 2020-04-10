from tornado.iostream import StreamClosedError
import logging
import socket
import tornado.tcpserver

from .io_looper import IOLooper
from .tcp_stream import TCPStream
from daisy import Message

logger = logging.getLogger(__name__)


class ClientDisconnected(Message):
    pass


class TCPServer(tornado.tcpserver.TCPServer, IOLooper):
    '''A TCP server to handle client-server communication through
    :class:`Message` objects.

    Args:

        max_port_tries (int, optional):
            How many times to try to find an empty random port.
    '''

    def __init__(self, max_port_tries=1000):

        tornado.tcpserver.TCPServer.__init__(self)
        IOLooper.__init__(self)

        self.callbacks = []

        # find empty port, start listening
        for i in range(max_port_tries):
            try:
                self.listen(0)  # 0 == random port
                break
            except OSError:
                if i == self.max_port_tries - 1:
                    raise RuntimeError(
                        "Could not find a free port after %d tries " %
                        self.max_port_tries)
                pass
        self.address = self._get_address()

    def register_message_callback(self, f):
        '''Register a callback for receiving messages sent to this server.

        Args:
            f (function):
                The function to register. ``f`` will be called whenever the
                server recieves a message. Arguments passed to the function
                will be the server, the message, the TCPStream, and the address
                tuple (host, port) of the sender.
        '''
        self.callbacks.append(f)

    async def handle_stream(self, stream, address):
        ''' Overrides a function from tornado's TCPServer, and is called
        whenever there is a new IOStream from an incoming connection (not
        whenever there is new data in the IOStream)

        Args:
            stream (:class:`tornado.iostream.IOStream`):
                the incoming stream
            address (tuple):
                host, port that new connection comes from
        '''
        logger.debug("Received new connection from %s:%d", *address)
        tcpstream = TCPStream(stream)
        # one IO loop scheduler per worker
        while True:
            try:
                message = await tcpstream._get_message()
                for f in self.callbacks:
                    f(self, message, tcpstream, address)

            except StreamClosedError:
                # note: may not be an actual error--workers exits and closes
                # TCP connection without explicitly notifying the scheduler
                logger.debug("Losing connection from %s:%d", *address)
                for f in self.callbacks:
                    f(self, ClientDisconnected, tcpstream, address)
                break

    def _get_address(self):
        '''Get the host and port of the tcp server'''
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
