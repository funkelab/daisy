import tornado.tcpserver
import tornado.ioloop
from tornado.iostream import StreamClosedError
import socket
import logging
import threading
from daisy.tcp_stream import TCPStream


logger = logging.getLogger(__name__)


class TCPServer(tornado.tcpserver.TCPServer):
    def __init__(self, max_port_tries=1000):
        super().__init__()
        self.ioloop = None
        self.address = None
        self.callbacks = []
        self.start(max_port_tries=max_port_tries)

    def start(self, max_port_tries=1000):
        # start io loop for server in separate thread
        self.ioloop = tornado.ioloop.IOLoop.current()
        t = threading.Thread(target=self.ioloop.start, daemon=True)
        t.start()
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
        self.address = self.get_address()

    def get_address(self):
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

    def register_message_callback(self, f):
        '''
        Args:
            f (function):
                The function to register. f will be called
                whenever the server recieves a message.
                Arguments passed to the function will be
                the server, a message, a TCPStream,
                and an address tuple (host, port)
        '''
        self.callbacks.append(f)

    async def handle_stream(self, stream, address):
        ''' Overrides a function from tornado's TCPServer,
        and is called whenever there is a new IOStream
        from an incoming connection (not whenever there is
        new data in the IOStream)

        Args:
            stream (`tornado.iostream.IOStream`):
                the incoming stream
            address (tuple):
                host, port that new connection comes from
        '''
        logger.debug("Received new connection from %s:%d", *address)
        tcpstream = TCPStream(stream)
        # one IO loop scheduler per worker
        while True:
            try:
                message = await tcpstream.get_message()
                for f in self.callbacks:
                    f(self, message, tcpstream, address)

            except StreamClosedError:
                # note: may not be an actual error--workers exits and closes
                # TCP connection without explicitly notifying the scheduler
                logger.debug("Losing connection from %s:%d", *address)
                break
