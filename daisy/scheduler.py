from __future__ import absolute_import

import collections
import copy
import logging
from inspect import signature
import os
import queue
import socket
import time
import threading

from tornado.ioloop import IOLoop
from tornado.tcpserver import TCPServer
from tornado.iostream import StreamClosedError

from .actor import Actor
from .dependency_graph import DependencyGraph
from .processes import spawn_call
from .processes import spawn_function
from .scheduler_include import SchedulerMessageType, ReturnCode, SchedulerMessage, get_and_unpack_message, pack_message
from .task import Task

logger = logging.getLogger(__name__)

class SchedulerTCPServer(TCPServer):

    handler = None
    address_to_stream_mapping = {}
    daisy_closed = False

    async def handle_stream(self, stream, address):

        actor = address
        logger.debug("Received new actor {}".format(actor))
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
                    self.handler.add_idle_actor(actor, task=msg.data)

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

    tasks = {}

    actors = set()
    actor_list_lock = threading.Lock()
    # num_workers = 4

    idle_actor_queue = collections.defaultdict(queue.Queue)

    actor_type = {}
    actor_type_cv = threading.Condition()
    idle_workers = []
    idle_workers_lock = threading.Lock()
    dead_actors = set()
    # actor_id = {}
    recruit_actor_fn = {}
    manual_recruit_cmd = {}
    local_tasks = set()

    blocks_actor_processing = collections.defaultdict(set)
    launched_tasks = set()
    actor_id = 0

    finished_scheduling = False

    def _start_tcp_server (
        self,
        ioloop=None):
        """Start TCP server to handle remote actor requests.
        """

        self.ioloop = ioloop
        if self.ioloop == None:
            self.ioloop = IOLoop.current()
            t = threading.Thread(target=self.ioloop.start, daemon=True)
            t.start()

        # ioloop.make_current()
        self.tcpserver = SchedulerTCPServer()
        self.tcpserver.add_handler(self)
        # self.tcpserver.listen(9988) # choose random port
        self.tcpserver.listen(0) # choose random port

        self.net_identity = self.tcpserver.get_identity()


    def remove_worker_callback(self, actor):
        logger.debug("Disconnection callback received for actor {}".format(actor))

        if actor not in self.actor_type:
            # actor was not activated, nothing to do
            return

        with self.actor_list_lock:

            # be careful with this code + lock
            # in conjunction with the scheduling code
            # can lead to dead locks or forgotten blocks if not thought about carefully
            self.dead_actors.add(actor)
            self.actors.remove(actor)
            processing_blocks = self.blocks_actor_processing[actor].copy()
            self.actor_type[actor] = None

        # notify blocks for reschedule if necessary
        for block_id in processing_blocks:
            self.block_done(actor, block_id, ReturnCode.NETWORK_ERROR)

    def make_spawn_function(self, function, args, env, log_dir):
        """ This helper function is necessary to disambiguate parameters
            of lambda expressions
        """
        return lambda i: spawn_function(function, args, env, log_dir+"/actor.{}.out".format(i), log_dir+"/actor.{}.err".format(i)) 

    def make_spawn_call(self, launch_cmd, log_dir):
        """ This helper function is necessary to disambiguate parameters
            of lambda expressions
        """
        return lambda i: spawn_call(launch_cmd, log_dir+"/actor.{}.out".format(i), log_dir+"/actor.{}.err".format(i))

    def construct_recruit_functions(self):

        for task in self.tasks:

            log_dir = '.daisy_logs_' + task
            try:
                os.mkdir(log_dir)
            except:
                pass # log dir exists

            new_actor_fn = None
            process_function = self.tasks[task].process_function
            spawn_actors = True
            try:
                if len(signature(process_function).parameters) == 0:
                    spawn_actors = False
            except:
                spawn_actors = True

            # add daisy scheduler context as an env
            # os.environ['DAISY_CONTEXT'] = '{}:{}:{}'.format(
                # *self.net_identity, task)
            env = {'DAISY_CONTEXT': '{}:{}:{}'
                .format(*self.net_identity, task)}

            if spawn_actors:
                new_actor_fn = self.make_spawn_function(
                    process_function,
                    [],
                    env,
                    log_dir)
            else:
                new_actor_fn = self.make_spawn_function(
                    _local_actor_wrapper,
                    [process_function, self.net_identity[1], task], 
                    env,
                    log_dir)

            self.recruit_actor_fn[task] = copy.deepcopy(new_actor_fn)

    def get_idle_actor(self, task):

        if task not in self.launched_tasks:

            logger.info("Launching actors for task {}".format(task))

            num_workers = self.tasks[task].num_workers

            for i in range(num_workers):
                self.recruit_actor_fn[task](self.actor_id)
                self.actor_id += 1

            if task in self.manual_recruit_cmd:
                logger.info("Actor recruit cmd: {}".format(' '.join(self.manual_recruit_cmd[task])))

            self.launched_tasks.add(task)

        return self.idle_actor_queue[task].get()


    def add_idle_actor(self, actor, task):

        if (actor not in self.actor_type) or (self.actor_type[actor] == None):
            self.activate_actor(actor, task)

        logger.debug("Add actor {} to idle queue of task {}".format(actor, task))
        self.idle_actor_queue[task].put(actor)


    def close_all_actors(self):

        with self.actor_list_lock:
            for actor in self.actors:
                self.send_terminate(actor)

    def finish_task(self, task_id):
        # close actors of this task
        with self.actor_list_lock:
            removed = []
            for actor in self.actors:
                if self.actor_type[actor] == task_id:
                    self.send_terminate(actor)
                    removed.append(actor)
            # for actor in removed:
            #     self.actors.remove(actor)
            #     self.dead_actors.add(actor)
            #     self.actor_type[actor] = None

    def send_terminate(self, actor):
        self.tcpserver.send(
            actor,
            SchedulerMessage(SchedulerMessageType.TERMINATE_WORKER))

    def send_block(self, actor, block):
        self.tcpserver.send(
            actor,
            SchedulerMessage(SchedulerMessageType.NEW_BLOCK, data=block))

    def activate_actor(self, actor, task_id):
        logger.info("Activating actor {}".format(actor))
        if self.finished_scheduling:
            self.send_terminate(actor)
            return

        with self.actor_list_lock:
            self.actors.add(actor)
            self.actor_type[actor] = task_id
            if actor in self.dead_actors:
                # handle aliasing of previous actors
                self.dead_actors.remove(actor)

    def block_done(self, actor, block_id, ret):

        block = self.graph.get_block(block_id)
        task_id = block_id[0]

        # run post_check for finishing blocks
        if ret == ReturnCode.SUCCESS:
            try:
                if not self.tasks[task_id].post_check(block):
                    logger.error("Completion check failed for task for block %s.", block)
                    ret = ReturnCode.FAILED_POST_CHECK

            except Exception as e:
                logger.error("Encountered exception while running post_check for block {}. Exception: {}".format(block, e))
                ret = ReturnCode.FAILED_POST_CHECK


        if actor != None:
            with self.actor_list_lock:
                self.blocks_actor_processing[actor].remove(block_id)


        if ret in [ReturnCode.ERROR, ReturnCode.NETWORK_ERROR, ReturnCode.FAILED_POST_CHECK]:
            logger.error("Task failed for block %s.", block)
            self.graph.cancel_and_reschedule(block_id)

        elif ret in [ReturnCode.SUCCESS, ReturnCode.SKIPPED]:
            self.graph.remove_and_update(block_id)

        else:
            raise Exception('Unknown ReturnCode {}'.format(ret))

        if self.graph.is_task_done(task_id):
            self.finish_task(task_id)

        self.results.append((block, ret))

    def unexpected_actor_loss(self, actor):
        # TODO: better algorithm to decide whether to make a new actor or not?
        if not self.graph.empty():

            if actor not in self.actor_type:
                logger.error("Actor was lost before finished initializing. "
                             "It will not be respawned.")
                return

            task_id = self.actor_type[actor] 
            if task_id in self.local_tasks:
                logger.error("Actor {} belongs to a process task, cannot be"
                                " restarted.".format(actor))
                return

            logger.info("Starting new actor due to disconnection")
            self.recruit_actor_fn[task_id](self.actor_id)
            self.actor_id += 1

    def distribute(self, graph):
        self.graph = graph
        all_tasks = graph.get_tasks()
        for task in all_tasks:
            self.tasks[task.task_id] = task

        self._start_tcp_server()

        self.construct_recruit_functions()

        self.results = []

        logger.info("Server running at {}".format(self.net_identity))
        logger.info("Scheduling {} tasks to completion.".format(graph.size()))
        logger.info("Max parallelism seems to be {}.".format(graph.ready_size()))
        # if launch_cmd:
        #     logger.info("Actor launch command is: {}".format(' '.join(launch_cmd)))

        while not graph.empty():

            block = graph.next()
            if block == None: 
                continue

            # print("Got block {}".format(block))

            task_id, block = block

            try:
                # pre_check can intermittently fail
                pre_check_ret = self.tasks[task_id].pre_check(block)
            except Exception as e:
                logger.error(
                    "pre_check() exception for block {}. Exception: {}"
                        .format(block, e))
                pre_check_ret = False

            if pre_check_ret == True:

                logger.info("Skipping {} block {}; already processed.".format(task_id, block.block_id))
                ret = ReturnCode.SKIPPED
                self.block_done(None, (task_id, block.block_id), ret)

            else:

                scheduled = False
                while not scheduled:
                    actor = self.get_idle_actor(task_id)

                    with self.actor_list_lock:
                        if actor not in self.dead_actors:
                            self.blocks_actor_processing[actor].add((task_id, block.block_id))
                            scheduled = True
                        else:
                            logger.debug("Actor {} was found dead or disconnected. Getting new actor.".format(actor))
                            continue

                self.send_block(actor, block)
                logger.info("Pushed block {} of task {} to actor {}. Block data: {}".format(block.block_id, task_id, actor, block))


        self.finished_scheduling = True
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
            graph.size(),
            # len(self.results) - graph.size(),
            len(succeeded), len(skipped), len(graph.get_orphans()),
            # len(failed) + len(errored) + len(network_errored),
            len(graph.get_failed_blocks()),
            len(failed), len(errored), len(network_errored))

        return graph.size() == (len(succeeded) + len(skipped))


def _local_actor_wrapper(received_fn, port, task_id):
    """ Wrapper for local process function """

    sched = Actor(sched_addr="localhost", sched_port=port, task_id=task_id)

    try:
        user_fn, args = received_fn
        fn = lambda b: user_fn(b, *args)
    except:
        fn = received_fn

    while True:
        block = sched.acquire_block()
        if block == Actor.END_OF_BLOCK:
            break;
        ret = fn(block)
        sched.release_block(block, ret)


def run_blockwise(
    total_roi,
    read_roi,
    write_roi,
    process_function=None,
    check_function=None,
    read_write_conflict=True,
    num_workers=1,
    max_retries=2,
    fit='valid'):
    '''Run block-wise tasks.

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

            max_retries (int, optional):

                The maximum number of times a task will be retried if failed (either
                due to failed post_check or application crashes or network failure)

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

    # Scheduler().run_blockwise(*args, **kwargs)

    if check_function is not None:
        try:
            user_pre_check, user_post_check = check_function
        except:
            user_pre_check = check_function
            user_post_check = check_function

    else:
        user_pre_check = lambda _: False
        user_post_check = lambda _: True


    class BlockwiseTask(Task):

        process_function = staticmethod(process_function)
        pre_check = staticmethod(user_pre_check)
        post_check = staticmethod(user_post_check)


    daisy_task = BlockwiseTask()
    daisy_task.num_workers = num_workers
    daisy_task.total_roi = total_roi
    daisy_task.read_roi = read_roi
    daisy_task.write_roi = write_roi
    daisy_task.read_write_conflict = read_write_conflict
    daisy_task.max_retries = max_retries
    daisy_task.fit = fit

    task = {'task': daisy_task}

    dependency_graph = DependencyGraph()

    dependency_graph.add(daisy_task)

    # dependency_graph.init(total_roi, read_roi, write_roi, read_write_conflict, max_retries, fit)
    dependency_graph.init()

    # TODO
    # if request != None:
    #     dependency_graph = dependency_graph.get_subgraph(request)

    return Scheduler().distribute(dependency_graph)



def distribute(tasks, global_config=None):
    ''' Execute tasks in a block-wise fashion using the Task interface

        Args:

            tasks (`list`({'task': `class.Daisy.Task`
                           'request': `class:daisy.Roi`, optional})):

                List of tasks to be executed. Each task is a dictionary mapping
                'task' to a `Task`, and 'request' to a sub-Roi for the task
    '''

    dependency_graph = DependencyGraph(global_config=global_config)

    if len(tasks) > 1:
        raise NotImplementedError(
            "Daisy does not support distribute() multiple tasks yet.")

    task = tasks[0]
    dependency_graph.add(task['task'])
    dependency_graph.init()

    if 'request' in task and task['request'] != None:
        subgraph = dependency_graph.get_subgraph(task['request'])
        dependency_graph = subgraph

    return Scheduler().distribute(dependency_graph)

