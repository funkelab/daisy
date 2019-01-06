from __future__ import absolute_import

from .client_scheduler import ClientScheduler
from .context import Context
from .dependency_graph import DependencyGraph
from .processes import spawn_function
from .task import Task
from .tcp import ReturnCode, SchedulerMessage, SchedulerMessageType, \
    DaisyTCPServer
from inspect import signature
from tornado.ioloop import IOLoop
import asyncio
import collections
import logging
import os
import queue
import threading

logger = logging.getLogger(__name__)


class Scheduler():
    '''This is the main scheduler that tracks states of tasks and actors.

    Given a dependency graph of block-wise tasks, the scheduler executes
    tasks and their dependency to completion.

    Returns True if successfully ran all tasks, False otherwise.

    Usage:

    .. code:: python

        graph = DependencyGraph(...)
        return Scheduler().distribute(graph)

    See the DependencyGraph class for more information.
    '''

    def __init__(self):

        # a copy of tasks from the DependencyGraph
        self.tasks = {}

        # set of currently connected actors
        self.actors = set()

        # actor states
        self.actor_states_lock = threading.Lock()
        self.idle_actors = collections.defaultdict(queue.Queue)
        self.actor_type = {}
        self.dead_actors = set()
        self.actor_outstanding_blocks = collections.defaultdict(set)

        # precomputed recruit functions
        self.actor_recruit_fn = {}

        self.launched_tasks = set()
        self.actor_id = 0
        self.finished_scheduling = False

    def distribute(self, graph):
        self.graph = graph
        all_tasks = graph.get_tasks()
        for task in all_tasks:
            self.tasks[task.task_id] = task
        self.finished_tasks = set()

        self._start_tcp_server()
        self._construct_recruit_functions()

        self.results = []

        logger.info("Server running at %s", self.net_identity)
        logger.info("Scheduling %d tasks to completion.", graph.size())
        logger.info("Max parallelism seems to be %d.", graph.ready_size())

        while not graph.empty():

            block = graph.next()
            if block is None:
                continue

            task_id, block = block
            try:
                # pre_check can intermittently fail
                # so we wrap it in a try block
                pre_check_ret = self.tasks[task_id].pre_check(block)
            except Exception as e:
                logger.error(
                    "pre_check() exception for block %s. Exception: %s",
                    block, e)
                pre_check_ret = False

            if pre_check_ret:
                logger.info(
                    "Skipping %s block %d; already processed.",
                    task_id, block.block_id)
                ret = ReturnCode.SKIPPED
                self.block_return(None, (task_id, block.block_id), ret)

            else:
                scheduled = False
                while not scheduled:
                    actor = self.get_idle_actor(task_id)
                    with self.actor_states_lock:
                        if actor not in self.dead_actors:
                            self.actor_outstanding_blocks[actor].add(
                                (task_id, block.block_id))
                            scheduled = True
                        else:
                            logger.debug(
                                "Actor %s was found dead or disconnected "
                                "Getting new actor.", actor)
                            continue

                self.send_block(actor, block)
                logger.info(
                    "Pushed block %d of task %s to actor %s. \nBlock info: %s",
                    block.block_id, task_id, actor, block)

        self.finished_scheduling = True
        self.tcpserver.daisy_close()
        self.close_all_actors()

        succeeded = [t for t, r in self.results if r == ReturnCode.SUCCESS]
        skipped = [t for t, r in self.results if r == ReturnCode.SKIPPED]
        failed = [
            t for t, r in self.results
            if r == ReturnCode.FAILED_POST_CHECK
        ]
        errored = [t for t, r in self.results if r == ReturnCode.ERROR]
        network_errored = [
            t for t, r in self.results
            if r == ReturnCode.NETWORK_ERROR
        ]

        logger.info(
            "Ran %d tasks of which %d succeeded, %d were skipped, %d were "
            "orphaned (failed dependencies), %d tasks failed (%d "
            "failed check, %d application errors, %d network failures "
            "or app crashes)",
            graph.size(), len(succeeded), len(skipped),
            len(graph.get_orphans()), len(graph.get_failed_blocks()),
            len(failed), len(errored), len(network_errored))

        return graph.size() == (len(succeeded) + len(skipped))

    def _start_tcp_server(self, ioloop=None):
        '''Start TCP server to handle remote actor requests.

        Args:

            ioloop (``tornado.IOLoop``, optional):

                If not passed in, Daisy will start an ioloop
                in a concurrent thread
        '''
        self.ioloop = ioloop
        if self.ioloop is None:
            new_event_loop = asyncio.new_event_loop()
            asyncio._set_running_loop(new_event_loop)
            asyncio.set_event_loop(new_event_loop)
            self.ioloop = IOLoop.current()
            t = threading.Thread(target=self.ioloop.start, daemon=True)
            t.start()
        self.tcpserver = DaisyTCPServer()
        self.tcpserver.add_handler(self)
        self.tcpserver.listen(0)  # 0 == random port
        self.net_identity = self.tcpserver.get_identity()

    def remove_worker_callback(self, actor):
        """ Called by TCP server when connection is lost
        or otherwise exited"""
        logger.debug(
            "Disconnection callback received for actor {}".format(actor))

        if actor not in self.actor_type:
            # actor was not registered
            return

        with self.actor_states_lock:
            # Be careful with this code + lock in conjunction with the loop
            # in distribute(). It can lead to dead locks or forgotten blocks
            # if not thought about carefully
            self.dead_actors.add(actor)
            self.actors.remove(actor)
            outstanding_blocks = self.actor_outstanding_blocks[actor].copy()
            self.actor_type[actor] = None

        # reschedule if necessary
        for block_id in outstanding_blocks:
            self.block_return(actor, block_id, ReturnCode.NETWORK_ERROR)

    def _recruit_actor(
            self,
            function,
            args,
            context,
            log_dir,
            log_to_files,
            log_to_stdout):

        env = {'DAISY_CONTEXT': context.to_env()}

        spawn_function(
            function, args, env,
            log_dir+"/actor.{}.out".format(context.actor_id),
            log_dir+"/actor.{}.err".format(context.actor_id),
            log_to_files, log_to_stdout)

    def _construct_recruit_functions(self):
        '''Construct all actor recruit functions to be used later when
        needed'''
        for task_id in self.tasks:
            log_dir = '.daisy_logs_' + task_id
            try:
                os.mkdir(log_dir)
            except OSError:
                pass  # log dir exists

            new_actor_fn = None
            process_function = self.tasks[task_id].process_function
            log_to_files = self.tasks[task_id].log_to_files
            log_to_stdout = self.tasks[task_id].log_to_stdout

            spawn_actors = False
            try:
                if len(signature(process_function).parameters) == 0:
                    spawn_actors = True
            except Exception:
                spawn_actors = False

            if spawn_actors:
                if log_to_files and log_to_stdout:
                    logger.warn(
                        "It is not possible to log to both files and stdout "
                        "for spawning actors at the moment. File logging "
                        "will be disabled for task %s.", task_id)
                    log_to_files = False

                def new_actor_fn(context):
                    self._recruit_actor(
                        process_function,
                        [],
                        context,
                        log_dir,
                        log_to_files,
                        log_to_stdout)
            else:

                def new_actor_fn(context):
                    self._recruit_actor(
                        _local_actor_wrapper,
                        [process_function, self.net_identity[1], task_id],
                        context,
                        log_dir,
                        log_to_files,
                        log_to_stdout)

            self.actor_recruit_fn[task_id] = new_actor_fn

    def get_idle_actor(self, task_id):
        '''Scheduler loop calls this to get an idle actor ready to accept
        jobs. This function blocks until an actor submits itself into the
        idle queue through TCP. It will also launch ``num_workers`` of
        actors if have not already '''

        if task_id not in self.launched_tasks:

            logger.info("Launching actors for task %s", task_id)
            num_workers = self.tasks[task_id].num_workers

            for i in range(num_workers):

                context = Context(
                    self.net_identity[0],
                    self.net_identity[1],
                    task_id,
                    self.actor_id,
                    num_workers)
                self.actor_recruit_fn[task_id](context)

                self.actor_id += 1

            self.launched_tasks.add(task_id)

        actor = self.idle_actors[task_id].get()
        return actor

    def add_idle_actor_callback(self, actor, task):
        '''TCP server calls this to add an actor to the idle queue'''
        if (actor not in self.actor_type) or (self.actor_type[actor] is None):
            self.register_actor(actor, task)

        logger.debug(
            "Add actor {} to idle queue of task {}".format(actor, task))

        self.idle_actors[task].put(actor)

    def close_all_actors(self):
        '''Send termination message to all available actor. This is called
        when the scheduler loop exits'''
        with self.actor_states_lock:
            for actor in self.actors:
                self.send_terminate(actor)

        for task in self.tasks:
            self.tasks[task].cleanup()

    def finish_task(self, task_id):
        '''Called when a task is completely finished. Currently this function
        closes all actors of this task.'''
        with self.actor_states_lock:
            for actor in self.actors:
                if self.actor_type[actor] == task_id:
                    self.send_terminate(actor)
        self.finished_tasks.add(task_id)
        self.tasks[task_id].cleanup()

    def send_terminate(self, actor):
        '''Send TERMINATE_WORKER command to actor'''
        self.tcpserver.send(
            actor,
            SchedulerMessage(SchedulerMessageType.TERMINATE_WORKER))

    def send_block(self, actor, block):
        '''Send NEW_BLOCK command to actor'''
        self.tcpserver.send(
            actor,
            SchedulerMessage(SchedulerMessageType.NEW_BLOCK, data=block))

    def register_actor(self, actor, task_id):
        '''Register new actor with bookkeeping variables. If scheduler loop
        had finished it will not, instead terminating this new actor.'''
        logger.info("Registering new actor {}".format(actor))
        if self.finished_scheduling:
            self.send_terminate(actor)
            return

        with self.actor_states_lock:
            self.actors.add(actor)
            self.actor_type[actor] = task_id
            if actor in self.dead_actors:
                # handle aliasing of previous actors
                self.dead_actors.remove(actor)

    def block_return(self, actor, block_id, ret):
        '''Called when a block is returned, whether successfully or not'''
        block = self.graph.get_block(block_id)
        task_id = block_id[0]

        # run post_check for finishing blocks
        if ret == ReturnCode.SUCCESS:
            try:
                post_check_success = self.tasks[task_id].post_check(block)
            except Exception as e:
                logger.error(
                    "Encountered exception while running post_check for "
                    "block %s. Exception: %s",
                    block, e)
                post_check_success = False

            if not post_check_success:
                logger.error(
                    "Completion check failed for task for block %s.", block)
                ret = ReturnCode.FAILED_POST_CHECK

        if actor is not None:
            with self.actor_states_lock:
                self.actor_outstanding_blocks[actor].remove(block_id)

        if ret in [ReturnCode.ERROR, ReturnCode.NETWORK_ERROR,
                   ReturnCode.FAILED_POST_CHECK]:
            logger.error("Task failed for block %s.", block)
            self.graph.cancel_and_reschedule(block_id)

        elif ret in [ReturnCode.SUCCESS, ReturnCode.SKIPPED]:
            self.graph.remove_and_update(block_id)

        else:
            raise Exception('Unknown ReturnCode {}'.format(ret))

        if self.graph.is_task_done(task_id):
            # in other words if this is the last block for this task
            self.finish_task(task_id)

        self.results.append((block, ret))

    def unexpected_actor_loss_callback(self, actor):
        '''Called by TCP server when losing connection to an actor
        unexpectedly'''

        # TODO: better algorithm to decide whether to make a new actor or not?
        # if not self.graph.empty():
        if actor not in self.actor_type:
            logger.error(
                "Actor was lost before finished initializing. "
                "It will not be respawned.")
            return

        task_id = self.actor_type[actor]
        num_workers = self.tasks[task_id].num_workers

        # TODO: reuse actor_id
        if task_id not in self.finished_tasks:
            logger.info("Respawning actor %s due to disconnection", actor)
            self.actor_recruit_fn[task_id](actor.actor_id, num_workers)


def _local_actor_wrapper(received_fn, port, task_id):
    '''Simple wrapper for local process function'''

    sched = ClientScheduler()

    try:
        user_fn, args = received_fn

        def fn(b):
            return user_fn(b, *args)

    except Exception:
        fn = received_fn
    while True:
        block = sched.acquire_block()
        if block is None:
            break
        ret = fn(block)
        sched.release_block(block, ret)


def run_blockwise(
        total_roi,
        read_roi,
        write_roi,
        process_function=None,
        check_function=None,
        read_write_conflict=True,
        fit='valid',
        num_workers=1,
        processes=None,
        max_retries=2):
    '''Convenient function to run a single block-wise task.

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

        num_workers (int, optional):

            The number of parallel processes or threads to run. Only effective
            if ``client`` is ``None``.

        processes (bool, optional):

            Deprecated.

        max_retries (int, optional):

            The maximum number of times a task will be retried if failed
            (either due to failed post_check or application crashes or network
            failure)

    Returns:

        True, if all tasks succeeded (or were skipped because they were already
        completed in an earlier run).
    '''
    # make a default task
    class BlockwiseTask(Task):
        def prepare(self):
            self.schedule(
                total_roi=total_roi,
                read_roi=read_roi,
                write_roi=write_roi,
                process_function=process_function,
                check_function=check_function,
                read_write_conflict=read_write_conflict,
                fit=fit,
                num_workers=num_workers,
                )

    return distribute([{'task': BlockwiseTask()}])


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

    if 'request' in task and task['request'] is not None:
        subgraph = dependency_graph.get_subgraph(task['request'])
        dependency_graph = subgraph

    return Scheduler().distribute(dependency_graph)
