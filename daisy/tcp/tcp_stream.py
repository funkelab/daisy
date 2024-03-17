from .exceptions import StreamClosedError
from .io_looper import IOLooper
import logging
import pickle
import struct
import tornado.iostream

logger = logging.getLogger(__name__)


class TCPStream(IOLooper):
    """Wrapper around :class:`tornado.iostream.IOStream` to send
    :class:`TCPMessage` objects.

    Args:

        stream (:class:`tornado.iostream.IOStream`):
            The tornado stream to wrap.

        address (tuple):
            The address the stream originates from.
    """

    def __init__(self, stream, address):
        super().__init__()
        self.stream = stream
        self.address = address

    def send_message(self, message):
        """Send a message through this stream asynchronously.

        If the stream is closed, raises a :class:`StreamClosedError`.
        Successful return of this function does not guarantee that the message
        was sent. Error messages will be logged, but no exception will be
        visible on the caller side (due to the asynchronous nature of sending
        messages through this stream).

        Args:
            message (:class:`daisy.TCPMessage`):

                Message to send over the stream.
        """

        if self.stream is None:
            raise StreamClosedError(*self.address)

        self.ioloop.add_callback(self._send_message, message)

    def close(self):
        """Close this stream."""
        try:
            self.stream.close()
        except Exception:
            pass
        finally:
            self.stream = None

    def closed(self):
        """True if this stream was closed."""
        if self.stream is None:
            return True
        return self.stream.closed()

    async def _send_message(self, message):

        if self.stream is None:
            logger.error("No TCPStream available, can't send message.")

        pickled_data = pickle.dumps(message)
        message_size_bytes = struct.pack("I", len(pickled_data))
        message_bytes = message_size_bytes + pickled_data

        try:

            await self.stream.write(message_bytes)

        except AttributeError:

            # self.stream can be None even though we check earlier, due to race
            # conditions
            logger.error("No TCPStream available, can't send message.")
            pass

        except tornado.iostream.StreamClosedError:

            logger.error("TCPStream lost connection while sending data.")
            self.stream = None

    async def _get_message(self):

        if self.stream is None:
            raise StreamClosedError(*self.address)

        try:

            size = await self.stream.read_bytes(4)
            size = struct.unpack("I", size)[0]
            assert size < 2**64  # TODO: parameterize max message size
            # assert size < 2**16  # TODO: parameterize max message size
            pickled_data = await self.stream.read_bytes(size)

        except tornado.iostream.StreamClosedError:

            self.stream = None
            raise StreamClosedError(*self.address)

        message = pickle.loads(pickled_data)
        message.stream = self
        return message

    def __repr__(self):

        return f"{self.address[0]}:{self.address[1]}"
