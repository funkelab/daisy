from .worker import Worker
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
        self.connected_workers = set()

    async def handle_stream(self, stream, address):

        logger.debug("Received new connection from %s:%d", *address)

        try:
            msg = await get_and_unpack_message(stream)
        except StreamClosedError:
            logger.error(
                "Lost connection to %s before getting worker ID",
                address)
            return

        if msg.type != SchedulerMessageType.WORKER_HANDSHAKE:
            logger.error("Unexpected message %s received", msg.type)
            return

        worker_id = msg.data
        worker = Worker(worker_id, address, stream)

        if self.scheduler_closed:
            logger.debug(
                "Closing connection to %s:%d, no more blocks to schedule",
                *address)
            stream.close()
            return

        self.connected_workers.add(worker)

        # one IO loop scheduler per worker
        while True:
            try:
                msg = await get_and_unpack_message(stream)

                if msg.type == SchedulerMessageType.WORKER_GET_BLOCK:
                    self.scheduler.add_idle_worker_callback(
                        worker, task=msg.data)

                elif msg.type == SchedulerMessageType.WORKER_RET_BLOCK:
                    jobid, ret = msg.data
                    self.scheduler.block_return(worker, jobid, ret)

                else:
                    logger.error(
                        "Unknown message from worker %d: %s",
                        worker.worker_id,
                        msg)
                    logger.error(
                        "Closing connection to %s:%d",
                        *worker.address)
                    break

            except StreamClosedError:
                # note: may not be an actual error--workers exits and closes
                # TCP connection without explicitly notifying the scheduler
                logger.debug(
                    "Losing connection to worker %s",
                    worker)
                break

        # done, removing worker from list
        stream.close()
        self.scheduler.remove_worker_callback(worker)
        self.connected_workers.remove(worker)

    async def async_send(self, stream, data):

        try:
            await stream.write(data)
        except StreamClosedError:
            # might actually be okay if worker exits normally
            logger.debug("Scheduler lost connection while sending data.")

    def send(self, worker, data):

        if worker not in self.connected_workers:
            logger.warning("worker %d is no longer alive", worker.worker_id)
            return

        IOLoop.current().spawn_callback(
            self.async_send, worker.stream, pack_message(data))

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
    try:
        size = await stream.read_bytes(4)
    except StreamClosedError:
        logger.debug("stream %s was closed", stream)
        raise
    size = struct.unpack('I', size)[0]
    assert(size < 65535)  # TODO: parameterize max message size
    pickled_data = await stream.read_bytes(size)
    msg = pickle.loads(pickled_data)
    return msg


def pack_message(data):
    pickled_data = pickle.dumps(data)
    msg_size_bytes = struct.pack('I', len(pickled_data))
    return msg_size_bytes + pickled_data
