from __future__ import absolute_import
from .blocks import create_dependency_graph
from .dynamic_blocks import DynamicBlocks

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

logger = logging.getLogger(__name__)


class SchedulerMessageType(Enum):
    WORKER_GET_BLOCK = 2,
    WORKER_RET_BLOCK = 3,
    WORKER_EXITING = 4,
    TERMINATE_WORKER = 5,
    NEW_BLOCK = 6,



# def unserialize(data):

class SchedulerMessage():

    def __init__(self, type, data=None):
        self.type = type
        self.data = data
        # if type == SchedulerMessageType.TERMINATE_WORKER:



class SchedulerTCPServer(TCPServer):

    handler = None
    address_to_stream_mapping = {}

    # def __init__(self, ioloop, handler):
    #     self.handler = handler
    #     self.ioloop = ioloop

    async def handle_stream(self, stream, address):

        actor = address
        print("Received new actor {}".format(actor))
        logger.debug("Received new actor {}".format(actor))
        self.handler.initialize_actor(actor)
        self.address_to_stream_mapping[address] = stream

        while True:
            # IO loop per worker
            try:
                size = await stream.read_bytes(4)
                size = struct.unpack('I', size)[0]
                assert(size < 65535) # TODO: parameterize max message size
                logger.debug("Receiving {} bytes".format(size))
                pickled_data = await stream.read_bytes(size)

                msg = pickle.loads(pickled_data)
                logger.debug("Received {}".format(msg))

                if msg.type == SchedulerMessageType.WORKER_GET_BLOCK:
                    self.handler.add_idle_actor(actor)

                elif msg.type == SchedulerMessageType.WORKER_RET_BLOCK:
                    jobid, ret = msg.data
                    self.handler.job_done(jobid)

                elif msg.type == SchedulerMessageType.WORKER_EXITING:
                    break

                else:
                    assert(0)

            except StreamClosedError:
                logger.warn("Lost connection to actor {}".format(actor))
                break

        # done, removing worker from list
        self.handler.remove_worker_callback(actor)
        del self.address_to_stream_mapping[actor]

    async def async_send(self, stream, data):
        await stream.write(data)

    def send(self, address, data):

        if address not in self.address_to_stream_mapping:
            logger.warn("{} is no longer alive".format(address))
            return

        stream = self.address_to_stream_mapping[address]
        # data = ''.join(msg_size_bytes, data)
        pickled_data = pickle.dumps(data)
        msg_size_bytes = struct.pack('I', len(pickled_data))
        IOLoop.current().spawn_callback(
            self.async_send,
            stream,
            msg_size_bytes + pickled_data)

    def add_handler(self, handler):
        self.handler = handler


class Scheduler():

    actors = set()
    actor_list_lock = threading.Lock()
    # worker_actor_mapping = {}
    # dask_client = None
    # num_workers = 4
    finished_scheduling = False

    idle_actor_queue = queue.Queue() #synchronized queue

    actor_type = {}
    actor_type_cv = threading.Condition()
    idle_workers = []
    idle_workers_lock = threading.Lock()
    dead_actors = set()

    def __init__(
        self,
        ioloop=None,
        num_workers=1):

        """
            Args:

            num_workers (int, optional):

                The number of parallel processes or threads to run. Only effective
                if ``client`` is ``None``.
        """

        self.ioloop = ioloop
        # ioloop.make_current()
        self.tcpserver = SchedulerTCPServer()
        self.tcpserver.listen(9988)
        self.tcpserver.add_handler(self)
        # IOLoop.current().start()
        # self.ioloop.start()

        # self.tcp_server = Thread(target=self.ioloop.start, daemon=True)
        # self.tcp_server.start()


        # self.scheduler = Scheduler(loop=self.loop, scheduler_file='/groups/funke/home/nguyent3/programming/daisy/workspace/test_cluster.json')

        # for n in range(num_workers):
        #    future = self.dask_client.submit(RemoteActor, actor=True)



    def remove_worker_callback(self, worker):
        logger.warn("Disconnection callback received for worker {}".format(worker))
        try:
            actor = worker
            self.dead_actors.add(actor)
            with self.actor_list_lock:
                self.actors.remove(actor)

            # TODO: cancel unfinished job, retry, etc...
        except:
            print("remove_worker_callback ERROR???")
            # TODO
            pass # actor might not have been added



    def schedule_blockwise(
        self,
        total_roi,
        read_roi,
        write_roi,
        job_handler=None,
        process_function=None,
        check_function=None,
        process_file=None,
        read_write_conflict=True,
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

        logger.debug("Creating dynamic blocks")
        blocks = DynamicBlocks(
            total_roi,
            read_roi,
            write_roi,
            read_write_conflict,
            fit)

        if check_function is not None:

            try:
                pre_check, post_check = check_function
            except:
                pre_check = check_function
                post_check = check_function

        else:

            pre_check = lambda _: False
            post_check = lambda _: True

        results = []
        tasks = []
        self.blocks = blocks
        self.finished_scheduling = False

        logger.info("Scheduling tasks to completion...")
        while not blocks.empty():
            block = blocks.next()
            # assert(block != None)

            if block != None:
                # print(block)
                block_id = block.block_id
                # tasks = {
                #     block_to_dask_name(block): (
                #         check_and_run,
                #         block,
                #         process_function,
                #         pre_check,
                #         post_check,
                #         [ block_to_dask_name(ups) for ups in upstream_blocks ]
                #     )
                #     for block, upstream_blocks in [(block,[])]
                # }
                # task = ()
                actor = self.get_idle_actor()
                while actor in self.dead_actors:
                    logger.debug("Actor {} was found dead or disconnected. Skipping.".format(actor))
                    actor = self.get_idle_actor()

                # actor.push((block.block_id, process_function, [block])) # TODO: check req completion?
                self.send_block(actor, block.block_id, block)

                logger.debug("Push job {} to actor {}".format(block, actor))
                # check_and_run(actor, block, process_function, pre_check, post_check)

                # futures.append((block_id, client.get(tasks, list(tasks.keys()), sync=False)))
                # futures[block_id] = client.get(tasks, list(tasks.keys()), sync=False)

                    # results.append((blocks.get_task(block_id), future.result()))

        # self.finished_scheduling = True
        self.close_actor_handlers()

        # succeeded = [ t for t, r in zip(tasks, results) if r == 1 ]
        # skipped = [ t for t, r in zip(tasks, results) if r == 0 ]
        # failed = [ t for t, r in zip(tasks, results) if r == -1 ]
        # errored = [ t for t, r in zip(tasks, results) if r == -2 ]
        succeeded = [ t for t, r in results if r == 1 ]
        skipped = [ t for t, r in results if r == 0 ]
        failed = [ t for t, r in results if r == -1 ]
        errored = [ t for t, r in results if r == -2 ]

        logger.info(
            "Ran %d tasks, of which %d succeeded, %d were skipped, %d failed (%d "
            "failed check, %d errored)",
            len(tasks), len(succeeded), len(skipped),
            len(failed) + len(errored), len(failed), len(errored))

        return len(failed) + len(errored) == 0


    def get_idle_actor(self):
        return self.idle_actor_queue.get()


    def close_actor_handlers(self):
        for actor in self.get_actors():
            self.send_terminate(actor)

    def send_terminate(self, actor):
        self.tcpserver.send(actor, SchedulerMessage(SchedulerMessageType.TERMINATE_WORKER))

    def send_block(self, actor, block_id, block):
        self.tcpserver.send(actor,
            SchedulerMessage(SchedulerMessageType.NEW_BLOCK, data=(block_id, block)),
            )

        # logger.info("Waiting for actors to close...")

        # for actor in self.actors:
        #     with self.actor_type_cv:
        #         while self.actor_type[actor] != None:
        #             self.actor_type_cv.wait()

    def initialize_actor(self, worker):
        print("Activating worker {} as an actor".format(worker))
        with self.actor_list_lock:
            self.actors.add(worker)
        self.actor_type[worker] = True


    def add_idle_actor(self, actor):
        logger.debug("Add actor {} to idle queue".format(actor))
        self.idle_actor_queue.put(actor)


    def get_actors(self):
        with self.actor_list_lock:
            return self.actors

    def job_done(self, job_id):
        logger.info("Job {} is finished".format(job_id))
        self.blocks.remove_and_update(job_id)



def check_and_run(actor, block, process_function, pre_check, post_check, *args):

    if pre_check(block):
        logger.info("Skipping task for block %s; already processed.", block)
        return 0

    try:
        # process_function(block)
        actor.push((block.block_id, block)).result()

    except:
        # TODO: proper error handling
        logger.error(
            "Task for block %s failed:\n%s",
            block, traceback.format_exc())
        return -2

    if not post_check(block):
        logger.error(
            "Completion check failed for task for block %s.",
            block)
        return -1

    return 1

class RemoteActor():
    """ Object that runs on a remote worker providing task management API for user code """
    # job_queue = None
    # job_queue_cv = None
    # req_counter = 0

    # return_queue = None
    # return_queue_cv = None

    # close_handler = False

    connected = False
    END_OF_BLOCK = (-1, None)
    # user_worker = None

    def __init__(self, ioloop):
        logger.debug("client init")
        self.ioloop = ioloop
        self.ioloop.add_callback(self._start)

    async def _start(self):
        logger.debug("Connecting to scheduler...")
        self.stream = await TCPClient().connect("localhost", 9988, timeout=60)

        # self.return_queue = queue.Queue()
        # self.return_queue_cv = threading.Condition()
        self.job_queue = deque()
        self.job_queue_cv = threading.Condition()
        # self.run_thread = threading.Thread(target=worker_fn, args=[self])
        # self.close_handler = False
        # if worker_fn != None:
        #     self.run_thread = threading.Thread(target=worker_fn)
        #     self.run_thread.start() # run user provided loop

        # self.daisy_initialized = True

        self.connected = True
        logger.debug("Connected.")

        # await gen.multi([self.recv_server()])
        await self.async_recv()

    async def async_recv(self):

        while True:
            try:
                size = await self.stream.read_bytes(4)
                size = struct.unpack('I', size)[0]
                assert(size < 65535) # TODO: parameterize max message size
                logger.debug("Worker received {} bytes".format(size))
                pickled_data = await self.stream.read_bytes(size)
                msg = pickle.loads(pickled_data)
                logger.debug("Received {}".format(msg.data))

                if msg.type == SchedulerMessageType.NEW_BLOCK:
                    block = msg.data
                    with self.job_queue_cv:
                        self.job_queue.append(block)
                        self.job_queue_cv.notify()

                # elif msg.type == SchedulerMessageType.WORKER_RET_BLOCK:
                    # self.handler.job_done(actor)

                elif msg.type == SchedulerMessageType.TERMINATE_WORKER:
                    self.send(SchedulerMessage(SchedulerMessageType.WORKER_EXITING))
                    break

            except StreamClosedError:
                logger.warn("Lost connection to scheduler!")
                break

        # done
        # TODO
        with self.job_queue_cv:
            self.job_queue.append(self.END_OF_BLOCK)
            self.job_queue_cv.notify()


    async def async_send(self, data):
        await self.stream.write(data)


    def send(self, data):
        # print(data)
        pickled_data = pickle.dumps(data)
        msg_size_bytes = struct.pack('I', len(pickled_data))
        self.ioloop.spawn_callback(self.async_send, msg_size_bytes + pickled_data)


    # def close(self):
    #     with self.job_queue_cv:
    #         self.close_handler = True
    #         self.job_queue_cv.notify()


    def acquire_block(self):

        self.send(SchedulerMessage(
                    SchedulerMessageType.WORKER_GET_BLOCK))

        with self.job_queue_cv:

            while len(self.job_queue) == 0:
                self.job_queue_cv.wait()
                # self.job_queue_cv.wait(timeout=5) # wait for 5 seconds before rechecking

            ret = self.job_queue.popleft()
            # print("got job")
            return ret


    def release_block(self, jobid, res):

        # self.return_queue.put((jobid, res))
        self.send(SchedulerMessage(
                    SchedulerMessageType.WORKER_RET_BLOCK,
                    data=(jobid, res)))




# def get_scheduler():
#     worker = get_worker()
#     print(worker)
#     print(worker.actors)

#     actor_list = list(worker.actors.keys())
#     if len(actor_list) == 0:
#         raise Exception("Actor is not initialized on this node")

#     actor = worker.actors[actor_list[0]]
#     # worker.claimed_actors
#     print("returning actor {}".format(actor))
#     return actor
