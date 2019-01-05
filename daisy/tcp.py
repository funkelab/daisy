
from enum import Enum
import logging
import pickle
import socket
import struct

from tornado.iostream import StreamClosedError
from tornado.tcpserver import TCPServer
from tornado.ioloop import IOLoop

logger = logging.getLogger(__name__)

class DaisyTCPServer(TCPServer):

    def __init__(self):
        super().__init__()
        self.scheduler = None
        self.address_to_stream_mapping = {}
        self.scheduler_closed = False

    async def handle_stream(self, stream, address):

        actor = address
        logger.debug("Received new actor {}".format(actor))
        self.address_to_stream_mapping[address] = stream

        if self.scheduler_closed:
            logger.debug("Closing actor {} because Daisy is done".format(actor))
            stream.close()
            return

        # one IO loop scheduler per worker
        while True:
            try:
                msg = await get_and_unpack_message(stream)
                # logger.debug("Received {}".format(msg))

                if msg.type == SchedulerMessageType.WORKER_GET_BLOCK:
                    self.scheduler.add_idle_actor_callback(
                        actor, task=msg.data)

                elif msg.type == SchedulerMessageType.WORKER_RET_BLOCK:
                    jobid, ret = msg.data
                    self.scheduler.block_return(actor, jobid, ret)

                elif msg.type == SchedulerMessageType.WORKER_EXITING:
                    break

                else:
                    logger.error(
                        "Unknown message from remote actor {}: {}".format(actor, msg))
                    logger.error("Closing connection to {}".format(actor))
                    stream.close()
                    self.scheduler.unexpected_actor_loss_callback(actor)
                    break
                    # assert(0)

            except StreamClosedError:
                logger.warn("Lost connection to actor {}".format(actor))
                self.scheduler.unexpected_actor_loss_callback(actor)
                break

        # done, removing worker from list
        self.scheduler.remove_worker_callback(actor)
        del self.address_to_stream_mapping[actor]


    async def async_send(self, stream, data):

        try:
            await stream.write(data)
        except StreamClosedError:
            logger.error("Unexpected loss of connection while sending data.")


    def send(self, address, data):

        if address not in self.address_to_stream_mapping:
            logger.warn("{} is no longer alive".format(address))
            return

        stream = self.address_to_stream_mapping[address]
        IOLoop.current().spawn_callback(
            self.async_send, stream, pack_message(data))


    def add_handler(self, scheduler):
        self.scheduler = scheduler


    def get_own_ip(self, port):
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.connect(("8.8.8.8", port))
            return sock.getsockname()[0]
        except:
            logger.error("Could not detect own IP address, returning bogus IP")
            return "8.8.8.8"
        finally:
            if sock:
                sock.close()


    def get_identity(self):
        sock = self._sockets[list(self._sockets.keys())[0]]
        port = sock.getsockname()[1]
        ip = self.get_own_ip(port)
        return (ip, port)

    def daisy_close(self):
        self.scheduler_closed = True


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
