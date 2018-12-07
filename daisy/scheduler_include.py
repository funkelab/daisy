

from enum import Enum
import logging
import pickle
import struct

logger = logging.getLogger(__name__)


class SchedulerMessageType(Enum):
    WORKER_GET_BLOCK = 2,
    WORKER_RET_BLOCK = 3,
    WORKER_EXITING = 4,
    TERMINATE_WORKER = 5,
    NEW_BLOCK = 6,

class ReturnCode(Enum):
    SUCCESS = 0,
    ERROR= 1,
    FAILED_POST_CHECK = 2,
    SKIPPED = 3,
    NETWORK_ERROR = 4,


class SchedulerMessage():

    def __init__(self, type, data=None):
        self.type = type
        self.data = data


async def get_and_unpack_message(stream):
    size = await stream.read_bytes(4)
    size = struct.unpack('I', size)[0]
    assert(size < 65535) # TODO: parameterize max message size
    logger.debug("Receiving {} bytes".format(size))
    pickled_data = await stream.read_bytes(size)
    msg = pickle.loads(pickled_data)
    return msg


def pack_message(data):
    pickled_data = pickle.dumps(data)
    msg_size_bytes = struct.pack('I', len(pickled_data))
    return msg_size_bytes + pickled_data
