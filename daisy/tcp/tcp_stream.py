from .exceptions import StreamClosedError
from .io_looper import IOLooper
import logging
import pickle
import struct
import tornado.iostream

logger = logging.getLogger(__name__)


class TCPStream(IOLooper):
    '''Wrapper around :class:`tornado.iostream.IOStream` to send
    :class:`TCPMessage` objects.

    Args:

        stream (:class:`tornado.iostream.IOStream`):
            The tornado stream to wrap.
    '''

    def __init__(self, stream):
        super().__init__()
        self.stream = stream

    def send_message(self, message):
        '''Send a message through this stream asynchronously.

        Args:
            message (:class:`daisy.TCPMessage`):

                Message to send over the stream.
        '''

        if self.stream is None:
            raise StreamClosedError()

        self.ioloop.add_callback(self._send_message, message)

    def close(self):
        '''Close this stream.'''
        try:
            self.stream.close()
        finally:
            self.stream = None

    async def _send_message(self, message):

        pickled_data = pickle.dumps(message)
        message_size_bytes = struct.pack('I', len(pickled_data))
        message_bytes = message_size_bytes + pickled_data

        try:

            await self.stream.write(message_bytes)

        except tornado.iostream.StreamClosedError:

            logger.error("TCPStream lost connection while sending data.")
            self.stream = None

    async def _get_message(self):

        if self.stream is None:
            return

        try:

            size = await self.stream.read_bytes(4)
            size = struct.unpack('I', size)[0]
            assert(size < 65535)  # TODO: parameterize max message size
            pickled_data = await self.stream.read_bytes(size)

        except tornado.iostream.StreamClosedError:

            logger.error(
                "TCPStream lost connection while waiting to receive data")
            self.stream = None
            raise StreamClosedError()

        message = pickle.loads(pickled_data)
        message.stream = self
        return message
