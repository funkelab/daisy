import asyncio
from collections import deque
import logging
import pickle
import os
import threading
import time

from tornado.ioloop import IOLoop
from tornado.tcpclient import TCPClient
from tornado.iostream import StreamClosedError

from .tcp import *

logger = logging.getLogger(__name__)

class Actor():
    '''Client code that runs on a remote worker providing task management
    API for user code. It communicates with the scheduler through TCP/IP.

    Scheduler IP address, port, and other configurations are typically
    passed to ``Actor`` through an environment variable named
    'DAISY_CONTEXT'.

    Example usage:

        def blockwise_process(block):
            ...

        def main():
            sched = Actor()
            while True:
                block = sched.acquire_block()
                if block == Actor.END_OF_BLOCK:
                    break;
                ret = blockwise_process(block)
                sched.release_block(block, ret)
    '''

    connected = False
    error_state = False
    stream = None
    END_OF_BLOCK = (-1, None)

    def __init__(
        self,
        sched_addr=None,
        sched_port=None,
        task_id=None,
        ioloop=None):
        '''Initialize TCP connection with the scheduler.

        Args:

            sched_addr(``str``, optional):

                Scheduler IP address.

            sched_port(``str``, optional):

                Scheduler port number.

            task_id(``str``, optional):

                Unique ID for this task.

            ioloop(``tornado.IOLoop``, optional):

                If not passed in, Actor will start an ioloop
                in a concurrent thread
        '''
        logger.info("Actor init")

        if sched_addr == None or sched_port == None or task_id == None:
            # attempt to get them through environment variable
            try:
                context = os.environ['DAISY_CONTEXT'].split(':')
            except:
                logger.error(
                    "DAISY_CONTEXT environment variable is not found!")
                raise

            try:
                sched_addr,sched_port,task_id = context
            except:
                logger.error(
                    "DAISY_CONTEXT is found but is incorrectly formatted!")
                raise

        self.ioloop = ioloop
        if self.ioloop == None:
            asyncio.set_event_loop(asyncio.new_event_loop())
            self.ioloop = IOLoop.current()
            t = threading.Thread(target=self.ioloop.start, daemon=True)
            t.start()

        self.sched_addr = sched_addr
        self.sched_port = sched_port
        self.task_id = task_id
        self.ioloop.add_callback(self._start)

        logger.info("Waiting for connection to Daisy scheduler...")
        while not self.connected:
            time.sleep(.2)
            if self.error_state:
                raise Exception("Cannot connect to Daisy scheduler")

    async def _start(self):
        '''Start the TCP client.'''
        logger.info(
            "Connecting to scheduler at {}".format((self.sched_addr,
                                                    self.sched_port)))

        self.stream = await self._connect_with_retry()
        if self.stream == None:
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
        counter = 0
        while True:
            try:
                stream = await TCPClient().connect(self.sched_addr,
                                                   self.sched_port,
                                                   timeout=60)
                return stream
            except:
                logger.debug("TCP connect error, retry...")
                counter = counter + 1
                if (counter > 10):
                    # retry for 10 seconds
                    logger.debug("Timeout, quitting.")
                    return None
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
                    self.send(SchedulerMessage(
                                SchedulerMessageType.WORKER_EXITING))
                    break

            except StreamClosedError:
                logger.error("Unexpected loss of connection to scheduler!")
                break

        # all done, notify client code to exit
        with self.job_queue_cv:
            self.job_queue.append(self.END_OF_BLOCK)
            self.job_queue_cv.notify()

    async def async_send(self, data):
        '''Send ``data`` to the scheduler. ``data`` must have been formated
        using tcp.pack_message()'''
        try:
            await self.stream.write(data)
        except StreamClosedError:
            logger.error("Unexpected loss of connection to scheduler!")

    def send(self, data):
        '''Non-async wrapper for async_send()'''
        self.ioloop.spawn_callback(self.async_send, pack_message(data))

    def acquire_block(self):
        '''API for client to get a new block. It works by sending a get block message to the scheduler, then wait for async_recv() to append to the queue.'''
        self.send(SchedulerMessage(
                    SchedulerMessageType.WORKER_GET_BLOCK, data=self.task_id))

        with self.job_queue_cv:

            while len(self.job_queue) == 0:
                self.job_queue_cv.wait()

            ret = self.job_queue.popleft()
            logger.debug("Received block {}".format(ret))
            return ret

    def release_block(self, block, ret):
        '''API for client to return a a block.

        Args:

            block(daisy.Block):

                The block that was acquired with acquire_block()

            ret(``int``):

                Integer return value for the block. Currently only 
                either 0 or 1 are valid. 

        '''
        if ret == 0:
            ret = ReturnCode.SUCCESS
        elif ret == 1:
            ret = ReturnCode.ERROR
        else:
            logger.warn(
                "Daisy user function should return either 0 or 1--given %s",
                ret)
            ret = ReturnCode.SUCCESS

        logger.debug("Releasing block {}".format(block.block_id))

        self.send(SchedulerMessage(
                    SchedulerMessageType.WORKER_RET_BLOCK,
                    data=((self.task_id, block.block_id), ret)))
