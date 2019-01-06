from .actor import Actor
from enum import Enum
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError
from tornado.tcpserver import TCPServer
import logging
import pickle
import socket
import struct

logger = logging.getLogger(__name__)


class DaisyTCPServer(TCPServer):

    def __init__(self):
        super().__init__()
        self.scheduler = None
        self.scheduler_closed = False
        self.connected_actors = set()

    async def handle_stream(self, stream, address):

        logger.debug("Received new connection from %s:%d", *address)

        try:
            msg = await get_and_unpack_message(stream)
        except StreamClosedError:
            logger.error(
                "Lost connection to %s before getting actor ID",
                address)
            return

        if msg.type != SchedulerMessageType.WORKER_HANDSHAKE:
            logger.error("Unexpected message %s received", msg.type)
            return

        actor_id = msg.data
        actor = Actor(actor_id, address, stream)

        if self.scheduler_closed:
            logger.debug(
                "Closing connection to %s:%d, no more blocks to schedule",
                *address)
            stream.close()
            return

        self.connected_actors.add(actor)

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
                        "Unknown message from actor %d: %s",
                        actor.actor_id,
                        msg)
                    logger.error(
                        "Closing connection to %s:%d",
                        *actor.address)
                    stream.close()
                    self.scheduler.unexpected_actor_loss_callback(actor)
                    break
                    # assert(0)

            except StreamClosedError:
                logger.warning("Lost connection to actor {}".format(actor))
                self.scheduler.unexpected_actor_loss_callback(actor)
                break

        # done, removing worker from list
        self.scheduler.remove_worker_callback(actor)
        self.connected_actors.remove(actor)

    async def async_send(self, stream, data):

        try:
            await stream.write(data)
        except StreamClosedError:
            logger.error("Unexpected loss of connection while sending data.")

    def send(self, actor, data):

        if actor not in self.connected_actors:
            logger.warning("actor %d is no longer alive", actor.actor_id)
            return

        IOLoop.current().spawn_callback(
            self.async_send, actor.stream, pack_message(data))

    def add_handler(self, scheduler):
        self.scheduler = scheduler

    def get_own_ip(self, port):
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.connect(("8.8.8.8", port))
            return sock.getsockname()[0]
        except Exception:
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
    WORKER_HANDSHAKE = 1,
    WORKER_GET_BLOCK = 2,
    WORKER_RET_BLOCK = 3,
    WORKER_EXITING = 4,
    TERMINATE_WORKER = 5,
    NEW_BLOCK = 6,


class ReturnCode(Enum):
    SUCCESS = 0,
    ERROR = 1,
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
    assert(size < 65535)  # TODO: parameterize max message size
    logger.debug("Receiving {} bytes".format(size))
    pickled_data = await stream.read_bytes(size)
    msg = pickle.loads(pickled_data)
    return msg


def pack_message(data):
    pickled_data = pickle.dumps(data)
    msg_size_bytes = struct.pack('I', len(pickled_data))
    return msg_size_bytes + pickled_data
