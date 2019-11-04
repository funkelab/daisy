from __future__ import absolute_import

from .client import Client
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
from datetime import timedelta
import logging
import os
import queue
import socket
import threading
import time

logger = logging.getLogger(__name__)

# used for automated testing to speed up tests as the status thread takes a
# long time to shutdown
_NO_SPAWN_STATUS_THREAD = False


class Scheduler():
    '''This is the main scheduler that tracks states of tasks and workers.

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

        # set of currently connected workers
        self.workers = set()

        # worker states
        self.worker_states_lock = threading.Lock()
        self.idle_workers = collections.defaultdict(queue.Queue)
        self.worker_type = {}
        self.dead_workers = set()
        self.worker_outstanding_blocks = collections.defaultdict(set)
        self.registered_workers = collections.defaultdict(set)
        self.worker_aliases_count = collections.defaultdict(
            lambda: collections.defaultdict(int))
        # precomputed recruit functions
        self.worker_recruit_fn = {}

        self.launched_tasks = set()
        self.skipped_count = collections.defaultdict(int)
        self.next_worker_id = collections.defaultdict(int)
        self.finished_scheduling = False

        # keeping track of spawned processes so we can force terminate
        # them when finishing the block-wise scheduling
        self.started_processes = set()

        self.status_thread = None
        self.periodic_interval = 10
        self.completion_rate = collections.defaultdict(int)
        self.issue_times = {}

    def distribute(self, graph):
        try:
            return self.__distribute(graph)
        finally:
            # always run clean up
            for task in self.tasks:
                try:
                    self.tasks[task].cleanup()
                except Exception as e:
                    logger.error(e)
                    pass

    def __distribute(self, graph):

        self.graph = graph
        all_tasks = graph.get_tasks()
        for task in all_tasks:
            self.tasks[task.task_id] = task
        self.finished_tasks = set()

        self._start_tcp_server()
        self._construct_recruit_functions()

        self.results = []

        logger.debug("Server running at %s", self.net_identity)
        logger.info("Scheduling %d tasks to completion.", graph.size())
        logger.debug("Max parallelism seems to be %d.", graph.ready_size())

        if not _NO_SPAWN_STATUS_THREAD:
            self._start_status_thread()
        blocks = {}  # {task_id: block_id}
        last_prechecked = collections.defaultdict(lambda: (None, None))
        no_worker_delay = 0.001
        while not graph.empty():

            blocks = graph.next(waiting_blocks=blocks)

            submitted_blocks = []
            for task_id in blocks:
                block = blocks[task_id]

                if last_prechecked[task_id][0] != block:
                    # pre-check and skip blocks if possible
                    try:
                        # pre_check can intermittently fail
                        # so we wrap it in a try block
                        pre_check_ret = self.tasks[task_id]._daisy.pre_check(
                            block)
                    except Exception as e:
                        logger.error(
                            "pre_check() exception for block %s of task %s. "
                            "Exception: %s",
                            block, task_id, e)
                        pre_check_ret = False
                    finally:
                        last_prechecked[task_id] = (block, pre_check_ret)

                pre_check_ret = last_prechecked[task_id][1]

                if pre_check_ret:
                    logger.debug(
                        "Skipping %s block %d; already processed.",
                        task_id, block.block_id)
                    ret = ReturnCode.SKIPPED
                    self.skipped_count[task_id] += 1
                    self.block_return(None, (task_id, block.block_id), ret)
                    submitted_blocks.append(task_id)

                else:
                    worker = self.get_idle_worker(task_id)

                    if worker is not None:

                        with self.worker_states_lock:
                            if worker not in self.dead_workers:
                                self.worker_outstanding_blocks[worker].add(
                                    (task_id, block.block_id))
                                self.issue_times[(task_id, block.block_id)] = (
                                    task_id, worker, time.time())
                            else:
                                logger.debug(
                                    "Worker %s is dead or disconnected. "
                                    "Getting new worker.", worker)
                                continue

                        self.send_block(worker, block)
                        submitted_blocks.append(task_id)

                        logger.debug(
                            "Pushed block %s of task %s to worker %s.",
                            block, task_id, worker)

            scheduled_any = (len(submitted_blocks) > 0)
            if (len(blocks) > 0) and not scheduled_any:
                time.sleep(no_worker_delay)  # wait for workers to come online
                no_worker_delay *= 2
                if (no_worker_delay > 1.0):
                    no_worker_delay = 1.0
            else:
                no_worker_delay = 0.001

            for submitted_task in submitted_blocks:
                blocks.pop(submitted_task)

        self.finished_scheduling = True
        self.tcpserver.daisy_close()
        self.close_all_workers()
        self.ioloop.add_callback(self.ioloop.stop)  # stop Tornado IOLoop
        self._stop_status_thread()

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
        '''Start TCP server to handle remote worker requests.

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
        max_port_tries = 100
        for i in range(max_port_tries):
            try:
                self.tcpserver.listen(0)  # 0 == random port
                break
            except OSError:
                if i == max_port_tries - 1:
                    raise RuntimeError(
                        "Could not find a free port after %d tries " %
                        max_port_tries)
                pass
        self.net_identity = self.tcpserver.get_identity()

    def _start_status_thread(self):
        self.status_thread = threading.Thread(target=self.status_loop)
        self.status_thread.start()

    def _stop_status_thread(self):
        if self.status_thread is not None:
            self.status_thread.join(timeout=20)
            if self.status_thread.is_alive():
                raise RuntimeError("Time out waiting to stop status thread..")

    def status_loop(self):

        last_time = time.time()
        last_completion_rate = self.completion_rate
        SAMPLING_INTERVAL = 120

        while not self.finished_scheduling:
            current_time = time.time()

            # check workers timeout

            with self.worker_states_lock:
                # make a copy to avoid race condition
                issue_times = self.issue_times.copy()

            for block_id in issue_times:

                task_id, worker, issue_time = issue_times[block_id]
                if self.tasks[task_id]._daisy.timeout is None:
                    continue
                if (current_time - issue_time >
                        self.tasks[task_id]._daisy.timeout):
                    # kill worker by shutting down TCP connection and let the
                    # recovery mechanism clean up
                    logger.info("Killing worker %s due to timeout" % worker)
                    try:
                        worker.stream.fileno().shutdown(socket.SHUT_RDWR)
                    except OSError:
                        # TCP socket is somehow already closed; fine
                        pass

            # collecting ETA based on the last 5 minutes

            if (current_time - last_time) > SAMPLING_INTERVAL:
                last_time = current_time
                last_completion_rate = self.completion_rate
                # reset periodic stats
                self.completion_rate = collections.defaultdict(int)

            for task_id in self.tasks:

                pending_count = (
                    self.graph.get_task_size(task_id) -
                    self.graph.get_task_done_count(task_id) -
                    self.graph.get_task_failed_count(task_id) -
                    len(self.graph.get_task_processing_blocks(task_id)))

                # calculate ETA
                blocks_per_sec = 0
                if task_id in last_completion_rate:
                    blocks_per_sec = (float(last_completion_rate[task_id]) /
                                      SAMPLING_INTERVAL)

                eta = "unknown"
                if blocks_per_sec > 0:
                    eta = str(timedelta(
                        seconds=(pending_count / blocks_per_sec)))

                logger.info(
                    "\n\t%s processing %d blocks "
                    "with %d workers (%d aliases online)"
                    "\n\t\t%d finished (%d skipped, %d succeeded, %d failed), "
                    "%d processing, %d pending"
                    "\n\t\tETA: %s",
                    task_id, self.graph.get_task_size(task_id),

                    self.tasks[task_id]._daisy.num_workers,
                    len(self.registered_workers[task_id]),

                    self.graph.get_task_done_count(task_id),
                    self.skipped_count[task_id],
                    (self.graph.get_task_done_count(task_id) -
                        self.skipped_count[task_id]),
                    self.graph.get_task_failed_count(task_id),

                    len(self.graph.get_task_processing_blocks(task_id)),
                    pending_count,

                    eta)

                self.tasks[task_id]._periodic_callback()

            time.sleep(self.periodic_interval)
            logger.info("\n")  # separator

    def remove_worker_callback(self, worker):
        """ Called by TCP server when connection is lost or otherwise
        exited. This happens for both when the worker unexpected
        exits (eg., error, network error) or when it exists normally.
        """
        logger.debug("Worker %s disconnected", worker)

        if worker not in self.worker_type:
            logger.error(
                "Worker %s closed before finished initializing. "
                "It will not be respawned.",
                worker
            )
            return

        task_id = self.worker_type[worker]

        # update worker bookkeeping
        with self.worker_states_lock:
            # Be careful with this code + lock in conjunction with the loop
            # in distribute(). It can lead to dead locks or forgotten
            # blocks if not thought about carefully
            self.dead_workers.add(worker)
            self.workers.remove(worker)
            if self.worker_type[worker] is not None:
                self.registered_workers[task_id].remove(worker)
            self.worker_type[worker] = None

            assert(
              self.worker_aliases_count[task_id][worker.worker_id] > 0)
            self.worker_aliases_count[task_id][worker.worker_id] -= 1

        if ((not self.finished_scheduling) and
                (task_id not in self.finished_tasks)):
            # task is unfinished--keep respawning to finish task

            num_workers = self.tasks[task_id]._daisy.num_workers

            if self.worker_aliases_count[task_id][worker.worker_id] == 0:
                logger.info(
                    "Respawning worker %s due to disconnection", worker)
                context = Context(
                    self.net_identity[0],
                    self.net_identity[1],
                    task_id,
                    worker.worker_id,
                    num_workers)
                self.worker_recruit_fn[task_id](context)

            # reschedule block if necessary
            with self.worker_states_lock:
                outstanding_blocks = (self.worker_outstanding_blocks[worker]
                                      .copy())

            for block_id in outstanding_blocks:
                self.block_return(worker, block_id, ReturnCode.NETWORK_ERROR)

    def _recruit_worker(
            self,
            function,
            args,
            context,
            log_dir,
            log_to_files,
            log_to_stdout):

        env = {'DAISY_CONTEXT': context.to_env()}

        logger.debug(
            "Recruiting worker with:"
            "\n\tenv           %s"
            "\n\tfunction      %s"
            "\n\targs          %s"
            "\n\tlog_to_files  %s"
            "\n\tlog_to_stdout %s",
            env, function, args, log_to_files, log_to_stdout)
        proc = spawn_function(
            function, args, env,
            log_dir+"/worker.{}.out".format(context.worker_id),
            log_dir+"/worker.{}.err".format(context.worker_id),
            log_to_files, log_to_stdout)

        self.started_processes.add(proc)

    def _make_spawn_function(
            self,
            function,
            args,
            log_dir,
            log_to_files,
            log_to_stdout):
        '''This helper function is necessary to disambiguate parameters
        of lambda expressions'''
        return lambda context: self._recruit_worker(
            function, args, context, log_dir,
            log_to_files, log_to_stdout)

    def _construct_recruit_functions(self):
        '''Construct all worker recruit functions to be used later when
        needed'''
        for task_id in self.tasks:
            log_dir = '.daisy_logs_' + task_id
            os.makedirs(log_dir, exist_ok=True)

            new_worker_fn = None
            process_function = self.tasks[task_id]._daisy.process_function
            log_to_files = self.tasks[task_id].log_to_files
            log_to_stdout = self.tasks[task_id].log_to_stdout

            spawn_workers = False
            try:
                if len(signature(process_function).parameters) == 0:
                    spawn_workers = True
            except Exception:
                spawn_workers = False

            if spawn_workers:
                if log_to_files and log_to_stdout:
                    logger.warning(
                        "It is not possible to log to both files and stdout "
                        "for spawning workers at the moment. File logging "
                        "will be disabled for task %s.", task_id)
                    log_to_files = False

                new_worker_fn = self._make_spawn_function(
                        process_function,
                        [],
                        log_dir,
                        log_to_files,
                        log_to_stdout)

            else:

                new_worker_fn = self._make_spawn_function(
                        _local_worker_wrapper,
                        [process_function, self.net_identity[1], task_id],
                        log_dir,
                        log_to_files,
                        log_to_stdout)

            self.worker_recruit_fn[task_id] = new_worker_fn

    def get_idle_worker(self, task_id):
        '''Scheduler loop calls this to get an idle worker ready to accept
        jobs. This function returns `None` if there is no workers available,
        otherwise the worker_id of this `task`.
        It will also launch ``num_workers`` of workers if have not already
        '''

        if task_id not in self.launched_tasks:

            logger.info("Launching workers for task %s", task_id)
            num_workers = self.tasks[task_id]._daisy.num_workers

            for i in range(num_workers):

                context = Context(
                    self.net_identity[0],
                    self.net_identity[1],
                    task_id,
                    self.next_worker_id[task_id],
                    num_workers)
                self.worker_recruit_fn[task_id](context)

                self.next_worker_id[task_id] += 1

            self.launched_tasks.add(task_id)

        try:
            return self.idle_workers[task_id].get(block=False)
        except queue.Empty:
            return None

    def add_idle_worker_callback(self, worker, task):
        '''TCP server calls this to add an worker to the idle queue'''
        if self.worker_type.get(worker, None) is None:
            self.register_worker(worker, task)

        logger.debug(
            "Add worker {} to idle queue of task {}".format(worker, task))

        self.idle_workers[task].put(worker)

    def close_all_workers(self):
        '''Send termination message to all available worker. This is called
        when the scheduler loop exits'''
        with self.worker_states_lock:
            for worker in self.workers:
                self.send_terminate(worker)

        for task in self.tasks:
            self.tasks[task].cleanup()

        # wait up to 60 seconds
        timeout = time.time() + 60

        # join worker processes
        for proc in self.started_processes:
            # give workers time to finish
            proc.join(timeout=max(1, timeout - time.time()))

        # terminate possibly hanging processes
        for proc in self.started_processes:
            proc.terminate()

    def finish_task(self, task_id):
        '''Called when a task is completely finished. Currently this function
        closes all workers of this task.'''
        with self.worker_states_lock:
            for worker in self.workers:
                if self.worker_type[worker] == task_id:
                    self.send_terminate(worker)
        self.finished_tasks.add(task_id)
        self.tasks[task_id].cleanup()

    def send_terminate(self, worker):
        '''Send TERMINATE_WORKER command to worker'''
        self.tcpserver.send(
            worker,
            SchedulerMessage(SchedulerMessageType.TERMINATE_WORKER))

    def send_block(self, worker, block):
        '''Send NEW_BLOCK command to worker'''
        self.tcpserver.send(
            worker,
            SchedulerMessage(SchedulerMessageType.NEW_BLOCK, data=block))

    def register_worker(self, worker, task_id):
        '''Register new worker with bookkeeping variables. If scheduler loop
        had finished it will not, instead terminating this new worker.
        '''
        logger.debug("Registering new worker %s", worker)
        if self.finished_scheduling:
            self.send_terminate(worker)
            return

        with self.worker_states_lock:
            self.workers.add(worker)
            self.worker_type[worker] = task_id
            self.registered_workers[task_id].add(worker)
            if worker in self.dead_workers:
                # handle aliasing of previous workers
                self.dead_workers.remove(worker)
            self.worker_aliases_count[task_id][worker.worker_id] += 1

    def block_return(self, worker, block_id, ret):
        '''Called when a block is returned, whether successfully or not'''
        block = self.graph.get_block(block_id)
        task_id = block_id[0]

        # run post_check for finishing blocks
        if ret == ReturnCode.SUCCESS:
            try:
                post_check_success = (
                    self.tasks[task_id]._daisy.post_check(block))
            except Exception as e:
                logger.error(
                    "Encountered exception while running post_check for "
                    "block %s. Exception: %s",
                    block, e)
                post_check_success = False

            self.completion_rate[task_id] += 1

            if not post_check_success:
                logger.error(
                    "Completion check failed for task for block %s.", block)
                ret = ReturnCode.FAILED_POST_CHECK

        if worker is not None:
            with self.worker_states_lock:
                self.worker_outstanding_blocks[worker].remove(block_id)
                self.issue_times.pop(block_id)

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


def _local_worker_wrapper(received_fn, port, task_id):
    '''Simple wrapper for local process function'''

    client = Client()

    try:
        user_fn, args = received_fn

        def fn(b):
            return user_fn(b, *args)

    except Exception:
        fn = received_fn
    while True:
        block = client.acquire_block()
        if block is None:
            break
        ret = fn(block)
        client.release_block(block, ret)


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
        max_retries=2,
        timeout=None):
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

        timeout (int, optional):

            Time in seconds to wait for a block to be returned from a worker.
            The worker is killed (and block retried) if this time is exceeded.

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
                max_retries=max_retries,
                timeout=timeout
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

    for task in tasks:
        dependency_graph.add(task['task'])

        if 'request' not in task or task['request'] is None:
            dependency_graph.init(task['task'].task_id,
                                  request_roi=[])

        else:
            if len(task['request']) > 1:
                raise NotImplementedError(
                  "Sorry Daisy does not handle more than one request_roi yet.")

            dependency_graph.init(task['task'].task_id,
                                  request_roi=task['request'][0])

    return Scheduler().distribute(dependency_graph)
