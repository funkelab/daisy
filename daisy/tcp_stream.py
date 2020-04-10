import pickle
import struct
from tornado.iostream import StreamClosedError
import logging
logger = logging.getLogger(__name__)


class TCPStream:
    def __init__(self, iostream):
        self.stream = iostream

    async def send_message(self, message):
        pickled_data = pickle.dumps(message)
        msg_size_bytes = struct.pack('I', len(pickled_data))
        msg_bytes = msg_size_bytes + pickled_data
        try:
            await self.stream.write(msg_bytes)
        except StreamClosedError:
            # might actually be okay if worker exits normally
            logger.debug("Scheduler lost connection while sending data.")

    async def get_message(self):
        try:
            size = await self.stream.read_bytes(4)
        except StreamClosedError:
            logger.debug("stream %s was closed", self.stream)
            raise
        size = struct.unpack('I', size)[0]
        assert(size < 65535)  # TODO: parameterize max message size
        pickled_data = await self.stream.read_bytes(size)
        msg = pickle.loads(pickled_data)
        return msg
