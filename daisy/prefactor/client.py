from .context import Context
from .tcp import pack_message, get_and_unpack_message, \
    SchedulerMessage, SchedulerMessageType, ReturnCode
from collections import deque
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError
from tornado.tcpclient import TCPClient
import asyncio
import logging
import sys
import threading
import time

logger = logging.getLogger(__name__)


class Client():
    '''Client code that runs on a remote worker providing task management
    API for user code. It communicates with the scheduler through TCP/IP.

    Scheduler IP address, port, and other configurations are typically
    passed to ``Client`` through an environment variable named
    'DAISY_CONTEXT'.

    Example usage:

        def blockwise_process(block):
            ...

        def main():
            client = Client()
            while True:
                block = client.acquire_block()
                if block == None:
                    break;
                ret = blockwise_process(block)
                client.release_block(block, ret)
    '''

    def __init__(
            self,
            context=None,
            ioloop=None):
        '''Initialize TCP connection with the scheduler.

        Args:

            context (`class:daisy.Context`, optional):

                If given, will be used to connect to the scheduler. If not
                given, the context will be read from environment variable
                ``DAISY_CONTEXT``.

            ioloop(``tornado.IOLoop``, optional):

                If not passed in, Clientwill start an ioloop in a concurrent
                thread
        '''
        logger.debug("Client init")
        self.context = context
        self.connected = False
        self.error_state = False
        self.stream = None

        if self.context is None:
            self.context = Context.from_env()

        self.ioloop = ioloop
        if self.ioloop is None:
            new_event_loop = asyncio.new_event_loop()
            asyncio._set_running_loop(new_event_loop)
            asyncio.set_event_loop(new_event_loop)
            self.ioloop = IOLoop.current()
            t = threading.Thread(target=self.ioloop.start, daemon=True)
            t.start()

        self.ioloop.add_callback(self._start)

        logger.debug("Waiting for connection to Daisy scheduler...")
        while not self.connected:
            time.sleep(.1)
            if self.error_state:
                logger.error("Cannot connect to Daisy scheduler")
                sys.exit(1)

        self.send(
            SchedulerMessage(
                SchedulerMessageType.WORKER_HANDSHAKE,
                data=self.context.worker_id))

    def __del__(self):
        '''Stop spawn ioloop when client is done'''
        try:
            self.ioloop.add_callback(self.ioloop.stop)
        except Exception:
            # self.ioloop is None
            pass

    async def _start(self):
        '''Start the TCP client.'''
        logger.debug(
            "Connecting to scheduler at %s:%d",
            self.context.hostname,
            self.context.port)

        try:
            self.stream = await self._connect_with_retry()
        except Exception:
            self.error_state = True
            raise

        self.job_queue = deque()
        self.job_queue_cv = threading.Condition()
        self.connected = True
        logger.debug("Connected.")

        await self.async_recv()

    async def _connect_with_retry(self):
        '''Helper method that tries to connect to the scheduler within
        a number of retries.'''
        num_retries = 10
        counter = 0
        while True:
            try:
                logger.debug("calling TCPClient().connect() ...")
                stream = await TCPClient().connect(
                    self.context.hostname,
                    self.context.port,
                    timeout=60)
                return stream
            except TimeoutError:
                logger.error("call to TCPClient().connect() timed out")
                raise
            except Exception:
                logger.debug("TCP connect error, retry...")
                counter += 1
                if (counter > num_retries):
                    # retry for 10 seconds
                    logger.error(
                        "TCP connection failed %d times, giving up",
                        num_retries)
                    raise RuntimeError(
                        "TCP connection could not be established")
                await asyncio.sleep(1)

    async def async_recv(self):
        '''Loop that receives commands from Daisy scheduler.'''
        while True:
            try:
                msg = await get_and_unpack_message(self.stream)
                logger.debug("Received {}".format(msg.data))

                if msg.type == SchedulerMessageType.NEW_BLOCK:
                    block = msg.data
                    with self.job_queue_cv:
                        self.job_queue.append(block)
                        self.job_queue_cv.notify()

                elif msg.type == SchedulerMessageType.TERMINATE_WORKER:
                    break

            except Exception as e:
                logger.error(
                    "Worker %s async_recv got Exception %s",
                    self.context.worker_id, e)
                with self.job_queue_cv:
                    self.job_queue.append(e)
                    self.job_queue_cv.notify()
                break

        # all done, notify client code to exit
        with self.job_queue_cv:
            self.job_queue.append(None)
            self.job_queue_cv.notify()

    async def async_send(self, data):
        '''Send ``data`` to the scheduler. ``data`` must have been formated
        using tcp.pack_message()'''
        try:
            await self.stream.write(data)
        except StreamClosedError:
            logger.error(
                "Worker %s async_send got StreamClosedError" %
                self.context.worker_id)

    def send(self, data):
        '''Non-async wrapper for async_send()'''
        self.ioloop.spawn_callback(self.async_send, pack_message(data))

    def acquire_block(self):
        '''API for client to get a new block. It works by sending a get block
        message to the scheduler, then wait for async_recv() to append to the
        queue.'''
        self.send(
            SchedulerMessage(
                SchedulerMessageType.WORKER_GET_BLOCK,
                data=self.context.task_id))

        with self.job_queue_cv:

            while len(self.job_queue) == 0:
                self.job_queue_cv.wait()

            ret = self.job_queue.popleft()

            if isinstance(ret, StreamClosedError):
                # StreamClosedError can not be distinguished from proper
                # teardown, just tell the client to stop
                ret = None
            elif isinstance(ret, Exception):
                raise ret

            logger.debug(
                "Worker %s received block %s" %
                (self.context.worker_id, ret))
            return ret

    def release_block(self, block, ret):
        '''API for client to return a a block.

        Args:

            block(daisy.Block):

                The block that was acquired with acquire_block()

            ret(``int``):

                Integer return value for the block. Currently only either 0 or
                1 are valid.

        '''
        if ret == 0:
            ret = ReturnCode.SUCCESS
        elif ret == 1:
            ret = ReturnCode.ERROR
        else:
            logger.warning(
                "Daisy user function should return either 0 or 1--given %s",
                ret)
            ret = ReturnCode.SUCCESS

        logger.debug("Releasing block {}".format(block.block_id))

        self.send(
            SchedulerMessage(
                SchedulerMessageType.WORKER_RET_BLOCK,
                data=((self.context.task_id, block.block_id), ret)))
