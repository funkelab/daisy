from __future__ import absolute_import
from .blocks import create_dependency_graph
from .dynamic_blocks import DynamicBlocks
from .processes import call_async
from .processes import call_function_async

from tornado.ioloop import IOLoop
from tornado.tcpserver import TCPServer
from tornado.tcpclient import TCPClient
from tornado.iostream import StreamClosedError

import pickle
import struct
import traceback
import logging
import threading
import queue
import time
from collections import deque
import importlib
from threading import Thread
from enum import Enum
import asyncio
from collections import defaultdict
import socket
from inspect import signature
import multiprocessing
import os

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
    # data = ''.join(msg_size_bytes, data)
    pickled_data = pickle.dumps(data)
    msg_size_bytes = struct.pack('I', len(pickled_data))
    return msg_size_bytes + pickled_data


class SchedulerTCPServer(TCPServer):

    handler = None
    address_to_stream_mapping = {}
    daisy_closed = False

    async def handle_stream(self, stream, address):

        actor = address
        logger.debug("Received new actor {}".format(actor))
        self.handler.initialize_actor(actor)
        self.address_to_stream_mapping[address] = stream

        if self.daisy_closed:
            logger.debug("Closing actor {} because Daisy is done".format(actor))
            stream.close()
            return

        # one IO loop handler per worker
        while True:
            try:
                msg = await get_and_unpack_message(stream)
                # logger.debug("Received {}".format(msg))

                if msg.type == SchedulerMessageType.WORKER_GET_BLOCK:
                    self.handler.add_idle_actor(actor)

                elif msg.type == SchedulerMessageType.WORKER_RET_BLOCK:
                    jobid, ret = msg.data
                    self.handler.block_done(actor, jobid, ret)

                elif msg.type == SchedulerMessageType.WORKER_EXITING:
                    break

                else:
                    logger.error("Unknown message from remote actor {}: {}".format(actor, msg))
                    logger.error("Closing connection to {}".format(actor))
                    stream.close()
                    self.handler.unexpected_actor_loss(actor)
                    break
                    # assert(0)

            except StreamClosedError:
                logger.warn("Lost connection to actor {}".format(actor))
                self.handler.unexpected_actor_loss(actor)
                break

        # done, removing worker from list
        self.handler.remove_worker_callback(actor)
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
        IOLoop.current().spawn_callback(self.async_send, stream, pack_message(data))


    def add_handler(self, handler):
        self.handler = handler


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
        self.daisy_closed = True


class Scheduler():

    actors = set()
    actor_list_lock = threading.Lock()
    # num_workers = 4

    idle_actor_queue = queue.Queue() #synchronized queue

    actor_type = {}
    actor_type_cv = threading.Condition()
    idle_workers = []
    idle_workers_lock = threading.Lock()
    dead_actors = set()
    # actor_id = {}

    blocks_actor_processing = defaultdict(set)

    def _start_server (
        self,
        ioloop=None):

        """
            Args:
        """

        self.ioloop = ioloop
        if self.ioloop == None:
            self.ioloop = IOLoop.current()
            t = Thread(target=self.ioloop.start, daemon=True)
            t.start()

        # ioloop.make_current()
        self.tcpserver = SchedulerTCPServer()
        self.tcpserver.add_handler(self)
        # self.tcpserver.listen(9988) # choose random port
        self.tcpserver.listen(0) # choose random port

        self.net_identity = self.tcpserver.get_identity()


    def remove_worker_callback(self, actor):
        logger.debug("Disconnection callback received for actor {}".format(actor))

        with self.actor_list_lock:

            # be careful with this code + lock
            # in conjunction with the scheduling code
            # can lead to dead locks or forgotten blocks if not thought about carefully
            self.dead_actors.add(actor)
            self.actors.remove(actor)
            processing_blocks = self.blocks_actor_processing[actor].copy()

        # notify blocks for reschedule if necessary
        for block_id in processing_blocks:
            self.block_done(actor, block_id, ReturnCode.NETWORK_ERROR)




    def run_blockwise(
        self,
        total_roi,
        read_roi,
        write_roi,
        process_function=None,
        check_function=None,
        read_write_conflict=True,
        num_workers=1,
        max_retries=2,
        fit='valid'):
        '''Run block-wise tasks with dask.

        Args:

            total_roi (`class:daisy.Roi`):

                The region of interest (ROI) of the complete volume to process.

            read_roi (`class:daisy.Roi`):

                The ROI every block needs to read data from. Will be shifted over
                the ``total_roi`` to cover the whole volume.

            write_roi (`class:daisy.Roi`):

                The ROI every block writes data from. Will be shifted over the
                ``total_roi`` to cover the whole volume.

            process_function (function):

                A function that will be called as::

                    process_function(read_roi, write_roi)

                with ``read_roi`` and ``write_roi`` shifted for each block to
                process.

                The callee can assume that there are no read/write concurencies,
                i.e., at any given point in time the ``read_roi`` does not overlap
                with the ``write_roi`` of another process.

            check_function (function, optional):

                A function that will be called as::

                    check_function(write_roi)

                ``write_roi`` shifted for each block to process.

                This function should return ``True`` if the block represented by
                ``write_roi`` was completed. This is used internally to avoid
                processing blocks that are already done and to check if a block was
                correctly processed.

                If a tuple of two functions is given, the first one will be called
                to check if the block needs to be run, and if so, the second after
                it was run to check if the run succeeded.

            read_write_conflict (``bool``, optional):

                Whether the read and write ROIs are conflicting, i.e., accessing
                the same resource. If set to ``False``, all blocks can run at the
                same time in parallel. In this case, providing a ``read_roi`` is
                simply a means of convenience to ensure no out-of-bound accesses
                and to avoid re-computation of it in each block.

            num_workers (int, optional):

                The number of parallel processes or threads to run. Only effective
                if ``client`` is ``None``.

            fit (``string``, optional):

                How to handle cases where shifting blocks by the size of
                ``block_write_roi`` does not tile the ``total_roi``. Possible
                options are:

                "valid": Skip blocks that would lie outside of ``total_roi``. This
                is the default::

                    |---------------------------|     total ROI

                    |rrrr|wwwwww|rrrr|                block 1
                           |rrrr|wwwwww|rrrr|         block 2
                                                      no further block

                "overhang": Add all blocks that overlap with ``total_roi``, even if
                they leave it. Client code has to take care of save access beyond
                ``total_roi`` in this case.::

                    |---------------------------|     total ROI

                    |rrrr|wwwwww|rrrr|                block 1
                           |rrrr|wwwwww|rrrr|         block 2
                                  |rrrr|wwwwww|rrrr|  block 3 (overhanging)

                "shrink": Like "overhang", but shrink the boundary blocks' read and
                write ROIs such that they are guaranteed to lie within
                ``total_roi``. The shrinking will preserve the context, i.e., the
                difference between the read ROI and write ROI stays the same.::

                    |---------------------------|     total ROI

                    |rrrr|wwwwww|rrrr|                block 1
                           |rrrr|wwwwww|rrrr|         block 2
                                  |rrrr|www|rrrr|     block 3 (shrunk)

        Returns:

            True, if all tasks succeeded (or were skipped because they were already
            completed in an earlier run).
        '''

        try:
            os.mkdir('.daisy_logs')
        except:
            pass # log dir exists

        distributed_processing = False
        if len(signature(process_function).parameters) == 0:
            distributed_processing = True

        self._start_server()
        launch_cmd = None
        if distributed_processing:
            launch_cmd = []
            # adding scheduler net identity as env before python call
            env_added = False
            for line in process_function():
                # launch_cmd.append(line.replace("python", "DAISY_SCHED_ADDR={} DAISY_SCHED_PORT={} python".format(self.net_identity[0], self.net_identity[1])))
                if not env_added:
                    if line.find("run_docker") >= 0:
                        launch_cmd.append(line.replace("run_docker", "run_docker -e DAISY_SCHED_ADDR_PORT={}:{} ".format(*self.net_identity)))
                        env_added = True
                    elif line.find("run_lsf") >= 0:
                        launch_cmd.append(line.replace("run_lsf", "run_lsf -e DAISY_SCHED_ADDR_PORT={}:{} ".format(*self.net_identity)))
                        env_added = True
                    elif line.find("python") >= 0:
                        launch_cmd.append(line.replace("python", "DAISY_SCHED_ADDR_PORT={}:{} python".format(*self.net_identity)))
                        env_added = True
                else:
                    launch_cmd.append(line)
            # logger.info("Actor launch command is: {}".format(''.join(launch_cmd)))
            self.new_actor_fn = lambda i: call_async(launch_cmd, ".daisy_logs/actor.{}.out".format(i), ".daisy_logs/actor.{}.err".format(i))

        else:
            multiprocessing.set_start_method('spawn', force=True) # for compatibility with multithreaded (namely ioloop)
            self.new_actor_fn = lambda i: call_function_async(local_actor, [process_function, self.net_identity[1]])

        for i in range(num_workers):
            self.new_actor_fn(i)
            # self.actor_id
        self.num_workers = num_workers
        self.actor_id = self.num_workers




        logger.debug("Creating dynamic blocks")
        blocks = DynamicBlocks(
            total_roi,
            read_roi,
            write_roi,
            read_write_conflict,
            fit,
            max_retries)

        self.blocks = blocks

        if check_function is not None:

            try:
                pre_check, post_check = check_function
            except:
                pre_check = check_function
                post_check = check_function

        else:

            pre_check = lambda _: False
            post_check = lambda _: True

        self.pre_check, self.post_check = (pre_check, post_check)

        self.results = []

        logger.info("Server running at {}".format(self.net_identity))
        logger.info("Scheduling {} tasks to completion.".format(blocks.size()))
        logger.info("Max parallelism seems to be {}.".format(blocks.ready_size()))
        if launch_cmd:
            logger.info("Actor launch command is: {}".format(' '.join(launch_cmd)))


        while not blocks.empty():

            # logger.debug("getting next block...")
            block = blocks.next()
            # logger.debug("got block {}".format(block))

            if block != None:
                scheduled = False
                while not scheduled:
                    actor = self.get_idle_actor()

                    with self.actor_list_lock:
                        if actor not in self.dead_actors:
                            self.blocks_actor_processing[actor].add(block.block_id)
                            scheduled = True
                        else:
                            logger.debug("Actor {} was found dead or disconnected. Skipping.".format(actor))
                            continue

                    self.check_and_run(actor, block, pre_check, post_check)

        self.tcpserver.daisy_close()
        self.close_all_actors()

        succeeded = [ t for t, r in self.results if r == ReturnCode.SUCCESS ]
        skipped = [ t for t, r in self.results if r == ReturnCode.SKIPPED ]
        failed = [ t for t, r in self.results if r == ReturnCode.FAILED_POST_CHECK ]
        errored = [ t for t, r in self.results if r == ReturnCode.ERROR ]
        network_errored = [ t for t, r in self.results if r == ReturnCode.NETWORK_ERROR ]

        logger.info(
            "Ran %d tasks "
            # "(%d retries), "
            "of which %d succeeded, %d were skipped, %d were orphaned (failed dependencies), "
            "%d tasks failed (%d "
            "failed check, %d application errors, %d network failures or app crashes)",
            blocks.size(),
            # len(self.results) - blocks.size(),
            len(succeeded), len(skipped), len(blocks.get_orphans()),
            # len(failed) + len(errored) + len(network_errored),
            len(blocks.get_failed_blocks()),
            len(failed), len(errored), len(network_errored))

        return blocks.size() - len(succeeded) - len(skipped) == 0


    def get_idle_actor(self):
        return self.idle_actor_queue.get()


    def close_all_actors(self):
        for actor in self.get_actors():
            self.send_terminate(actor)

    def send_terminate(self, actor):
        self.tcpserver.send(actor, SchedulerMessage(SchedulerMessageType.TERMINATE_WORKER))

    def send_block(self, actor, block):
        self.tcpserver.send(actor,
            SchedulerMessage(SchedulerMessageType.NEW_BLOCK, data=block),
            )

        # logger.info("Waiting for actors to close...")

        # for actor in self.actors:
        #     with self.actor_type_cv:
        #         while self.actor_type[actor] != None:
        #             self.actor_type_cv.wait()

    def initialize_actor(self, worker):
        logger.info("Activating worker {} as an actor".format(worker))
        with self.actor_list_lock:
            self.actors.add(worker)
            if worker in self.dead_actors:
                # handle aliasing of previous actors
                self.dead_actors.remove(worker)

        self.actor_type[worker] = True


    def add_idle_actor(self, actor):
        logger.debug("Add actor {} to idle queue".format(actor))
        self.idle_actor_queue.put(actor)


    def get_actors(self):
        with self.actor_list_lock:
            return self.actors

    def block_done(self, actor, block_id, ret):

        block = self.blocks.get_block(block_id)

        if ret == ReturnCode.SUCCESS:
            if not self.post_check(block):
                logger.error("Completion check failed for task for block %s.", block)
                ret = ReturnCode.FAILED_POST_CHECK

        if ret in [ReturnCode.ERROR, ReturnCode.NETWORK_ERROR, ReturnCode.FAILED_POST_CHECK]:
            logger.error("Task failed for block %s.", block)
            with self.actor_list_lock:
                self.blocks_actor_processing[actor].remove(block_id)
            self.blocks.cancel_and_reschedule(block_id)

        elif ret in [ReturnCode.SUCCESS, ReturnCode.SKIPPED]:
            # logger.debug("Block {} is done".format(block_id))
            with self.actor_list_lock:
                self.blocks_actor_processing[actor].remove(block_id)
            self.blocks.remove_and_update(block_id)
            # logger.debug("Block {} is done22".format(block_id))

        else:
            raise Exception('Unknown ReturnCode {}'.format(ret))

        self.results.append((block, ret))


    def check_and_run(self, actor, block, pre_check, post_check, *args):

        if pre_check(block):
            logger.info("Skipping task for block %s; already processed.", block)
            ret = ReturnCode.SKIPPED
            self.block_done(actor, block.block_id, ret)
            self.add_idle_actor(actor)
            return

        self.send_block(actor, block)
        logger.info("Pushed block {} to actor {}".format(block.block_id, actor))


    def unexpected_actor_loss(self, actor):

        # TODO: should decide whether to make a new actor or not
        if not self.blocks.empty():
            # logger.info("Creating new actor {}: {}.".format(self.actor_id, actor))
            logger.info("Starting new actor...")
            self.new_actor_fn(self.actor_id)
            # self.actor_id[actor] = self.actor_id
            self.actor_id += 1



class RemoteActor():
    """ Object that runs on a remote worker providing task management API for user code """

    connected = False
    error_state = False
    stream = None
    END_OF_BLOCK = (-1, None)

    def __init__(self, sched_addr=None, sched_port=None, ioloop=None):

        logger.debug("RemoteActor init")

        if sched_addr == None or sched_port == None:
            # attempt to get it through environment variable
            try:
                sched_addr,sched_port = os.environ['DAISY_SCHED_ADDR_PORT'].split(':')
            except:
                logger.error("Can't find daisy scheduler address through DAISY_SCHED_ADDR_PORT environment variables! Exiting...")
                exit(1)

        self.ioloop = ioloop
        if self.ioloop == None:
            self.ioloop = IOLoop.current()
            t = Thread(target=self.ioloop.start, daemon=True)
            t.start()

        self.sched_addr = sched_addr
        self.sched_port = sched_port
        self.ioloop.add_callback(self._start)

        print("Waiting for connection..")
        while self.connected == False:
            time.sleep(.2)


    async def _start(self):

        logger.info("Connecting to scheduler at {}".format((self.sched_addr, self.sched_port)))
        self.stream = await self._connect_with_retry()
        if self.stream == None:
            self.error_state = True
            exit(1)

        self.job_queue = deque()
        self.job_queue_cv = threading.Condition()

        self.connected = True
        logger.debug("Connected.")

        # await gen.multi([self.recv_server()])
        await self.async_recv()


    async def _connect_with_retry(self):

        counter = 0
        while True:
            try:
                stream = await TCPClient().connect(self.sched_addr, self.sched_port, timeout=60)
                return stream
            except:
                logger.debug("TCP connect error, retry...")
                counter = counter + 1
                if (counter > 60):
                    logger.debug("Timeout, quitting.")
                    return None
                await asyncio.sleep(1)



    async def async_recv(self):

        while True:
            try:
                msg = await get_and_unpack_message(self.stream)
                # logger.debug("Received {}".format(msg.data))

                if msg.type == SchedulerMessageType.NEW_BLOCK:
                    block = msg.data
                    with self.job_queue_cv:
                        self.job_queue.append(block)
                        self.job_queue_cv.notify()

                elif msg.type == SchedulerMessageType.TERMINATE_WORKER:
                    self.send(SchedulerMessage(SchedulerMessageType.WORKER_EXITING))
                    break

            except StreamClosedError:
                logger.error("Unexpected loss of connection to scheduler!")
                break

        # worker done, exiting
        with self.job_queue_cv:
            self.job_queue.append(self.END_OF_BLOCK)
            self.job_queue_cv.notify()


    async def async_send(self, data):

        try:
            await self.stream.write(data)
        except StreamClosedError:
            logger.error("Unexpected loss of connection to scheduler!")


    def send(self, data):
        self.ioloop.spawn_callback(self.async_send, pack_message(data))


    def acquire_block(self):

        self.send(SchedulerMessage(
                    SchedulerMessageType.WORKER_GET_BLOCK))

        with self.job_queue_cv:

            while len(self.job_queue) == 0:
                self.job_queue_cv.wait()
                # self.job_queue_cv.wait(timeout=5)

            ret = self.job_queue.popleft()
            return ret


    def release_block(self, block, ret):

        if ret == 0:
            ret = ReturnCode.SUCCESS
        elif ret == 1:
            ret = ReturnCode.ERROR
        else:
            raise Exception('User return code must be either 0 or 1. Given {}'.format(ret))

        self.send(SchedulerMessage(
                    SchedulerMessageType.WORKER_RET_BLOCK,
                    data=(block.block_id, ret)))



def local_actor(user_function, port):
    """ Wrapper for user process function """

    sched = RemoteActor(sched_addr="localhost", sched_port=port)
    while True:
        block = sched.acquire_block()
        if block == RemoteActor.END_OF_BLOCK:
            break;
        ret = user_function(block)
        sched.release_block(block, ret)
