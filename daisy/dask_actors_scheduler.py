from __future__ import absolute_import
from .blocks import create_dependency_graph
from .dynamic_blocks import DynamicBlocks
from dask.distributed import Client, LocalCluster
import distributed.diagnostics.plugin
import traceback
import logging
import threading
import queue
import time
from collections import deque
import importlib

from distributed import Scheduler
from tornado.ioloop import IOLoop
from threading import Thread

logger = logging.getLogger(__name__)

class DaskScheduler():

    actors = set()
    actor_list_lock = threading.Lock()
    worker_actor_mapping = {}
    dask_client = None
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
        dask_client=None,
        num_workers=1):

        """
            Args:

            dask_client (optional):

                The dask client to submit jobs to. If ``None``, a client will be
                created from ``dask.distributed.Client`` with ``num_workers``
                workers.

            num_workers (int, optional):

                The number of parallel processes or threads to run. Only effective
                if ``client`` is ``None``.
        """

        self.own_client = dask_client is None
        if self.own_client:

            # if num_workers is not None:
                # print("Creating local cluster with %d workers..."%num_workers)

                # cluster = LocalCluster(
                #     n_workers=num_workers,
                #     threads_per_worker=1,
                #     memory_limit=0,
                #     diagnostics_port=None)
            # dask_client = Client(cluster)
            self.loop = IOLoop.current()
            self.loop_thread = Thread(target=self.loop.start, daemon=True)
            self.loop_thread.start()

            self.scheduler_callback = SchedulerCallback(self)

            self.scheduler = Scheduler(loop=self.loop, scheduler_file='/groups/funke/home/nguyent3/programming/daisy/workspace/test_cluster.json')
            self.scheduler.add_plugin(self.scheduler_callback)
            self.scheduler.start()

            print("starting sched")
            self.dask_client1 = Client(scheduler_file='/groups/funke/home/nguyent3/programming/daisy/workspace/test_cluster.json', set_as_default=False, direct_to_workers=True)
            print("started sched")
            self.dask_client2 = Client(scheduler_file='/groups/funke/home/nguyent3/programming/daisy/workspace/test_cluster.json', set_as_default=False, direct_to_workers=True)
            print("started sched2")

        # self.dask_client = dask_client
        self.dask_client = self.dask_client1

        # for n in range(num_workers):
        #    future = self.dask_client.submit(RemoteActor, actor=True)
        #    # future.result().start(worker_fn)
        #    actor = future.result()
        #    self.actors.append(actor)
        #    self.actor_type[actor] = True

        # for n in range(num_workers):
        #     sys.call("lsf..", "actor.py")

        # for n in range(num_workers):
        #    future = self.dask_client.submit(RemoteActor, actor=True)
        #    # future.result().start(worker_fn)
        #    self.actors.append(future.result())

    def add_worker_callback(self, worker):
        print("Connection callback received for worker {}".format(worker))
        # with self.idle_workers_lock:
        self.idle_workers.append(worker)


    def remove_worker_callback(self, worker):
        print("Disconnection callback received for worker {}".format(worker))
        # with self.idle_workers_lock:
            # self.idle_workers.append(worker)
        try:
            actor = self.worker_actor_mapping[worker]
            self.dead_actors.add(actor)
            with self.actor_list_lock:
                self.actors.remove(actor)
        except:
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

        print("Creating dynamic blocks")

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

        # logger.info("Scheduling %d tasks...", len(tasks))

        # actors = Actors(client, blocks)
        # for actor in self.actors:
        #     print("Initializing actor {}".format(actor))
        #     actor.start()
            # if process_function:
            #     f = actor.start(job_handler)
            # elif process_file:
            #     f = actor.start_file(process_file)
            # f.result()

        # # run all tasks
        req_thread = threading.Thread(target=self.check_req_loop)
        req_thread.start()
        ret_thread = threading.Thread(target=self.check_ret_loop)
        # ret_thread.start()

        # futures = {}
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

                actor.push((block.block_id, process_function, [block])) # TODO: check req completion?

                logger.debug("Push job {} to actor {}".format(block, actor))
                # check_and_run(actor, block, process_function, pre_check, post_check)

                # futures.append((block_id, client.get(tasks, list(tasks.keys()), sync=False)))
                # futures[block_id] = client.get(tasks, list(tasks.keys()), sync=False)

                    # results.append((blocks.get_task(block_id), future.result()))

        self.finished_scheduling = True
        ret_thread.join()
        req_thread.join()
        self.close_actor_handlers()
        if self.own_client:
            self.dask_client.close()

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
            actor.close()

        # logger.info("Waiting for actors to close...")

        # for actor in self.actors:
        #     with self.actor_type_cv:
        #         while self.actor_type[actor] != None:
        #             self.actor_type_cv.wait()

    def initialize_actor(self, worker):
        print("Activating worker {} as an actor".format(worker))
        future = self.dask_client.submit(RemoteActor, workers=[worker], actor=True)
        print("21")
        actor = future.result()
        print("22")
        actor.start().result()
        print("23")
        with self.actor_list_lock:
            # print("23")
            self.actors.add(actor)
            self.worker_actor_mapping[worker] = actor
        self.actor_type[actor] = True
        # print("25")
        print("Activated worker {} as an actor".format(worker))


    def check_req_loop(self):
        query_futures = {}
        while self.finished_scheduling == False:
            # print("1")
            # with self.idle_workers_lock:
                # print("2")
            for worker in self.idle_workers:
                threading.Thread(target=self.initialize_actor, args=[worker]).start()
                # self.initialize_actor(worker)
                # self.
            self.idle_workers = []

            # print("3")
            actors = self.get_actors().copy()
            for actor in actors:
                # print("4")
                if actor in self.dead_actors:
                    continue
                if actor not in query_futures:
                    try:
                        ret = actor.reqs().result(timeout=5)
                        if ret:
                            print("Add actor {} to idle queue".format(actor))
                            self.idle_actor_queue.put(actor)
                    except:
                        print("ERROR: actor req times out!!!!!")
                        # print("Closing scheduler")
                        # self.dask_client.close(timeout=1)
                        # print("Attempting to reconnect to scheduler")
                        # self.dask_client = Client(scheduler_file='/groups/funke/home/nguyent3/programming/daisy/workspace/test_cluster.json')

                        # print("Attempting to reconnect to scheduler")
                        # try:
                        #     self.dask_client._reconnect()
                        # except:
                        #     print("exception reconnecting")

                        # print("Attempting to close scheduler")
                        # try:
                        #     # self.dask_client.close(timeout=1)
                        #     self.dask_client._close()
                        # except:
                        #     print("exception closing")
                        print("Attempting to reconnect to scheduler")
                        # try:
                            # self.dask_client2 = Client(scheduler_file='/groups/funke/home/nguyent3/programming/daisy/workspace/test_cluster.json')
                        # except:
                        #     print("exception starting client")

                        print("Reconnected to scheduler")
                    # query_futures[actor] = actor.reqs()

            # print("5")
            # slow down loop
            time.sleep(.1)

            counter = 0

            done = []
            for actor in query_futures:
                if actor in self.dead_actors:
                    done.append(actor)
                    continue
                future = query_futures[actor]
                try:
                    # print("31: {}".format(actor))
                    # ret = future.result(timeout=.01)
                    ret = future.result(timeout=10)
                    # print("32: {}".format(actor))
                    # print("check ")
                    done.append(actor)
                    for i in range(ret):
                        print("Add actor {} to idle queue".format(actor))
                        self.idle_actor_queue.put(actor)
                        counter += 1
                except:
                    # print("33: {}".format(actor))
                    done.append(actor)
                    # pass # query uncompleted

            if counter != 0:
                print("Added {} actors to ready queue".format(counter))
            # print("Added {} actors to ready queue".format(counter))

            for actor in done:
                del query_futures[actor]


    def check_ret_loop(self):
        query_futures = {}
        while self.finished_scheduling == False:
            # print("1")
            for actor in self.get_actors():
                if actor in self.dead_actors:
                    continue
                if actor not in query_futures:
                    # print("2")
                    query_futures[actor] = actor.get_ret()
                    # print("3")

            # slow down loop
            time.sleep(.2)

            # print("4")
            counter = 0
            done = []
            for actor in query_futures:
                if actor in self.dead_actors:
                    done.append(actor)
                    print("ret from actor {} is dropped".format(actor))
                    continue
                future = query_futures[actor]
                try:
                    # print("5")
                    ret = future.result(timeout=.01)
                    # print("6")
                    done.append(actor)
                except:
                    ret = None # query uncompleted

                # print("7")
                if ret != None:
                    # print("got {}".format(ret))
                    job_id, ret = ret
                    if job_id == -1:
                        # worker task loop existing
                        print("setting actor {} to None".format(actor))
                        with self.actor_type_cv:
                            self.actor_type[actor] = None
                            self.actor_type_cv.notify()
                    else:
                    # print("got {}".format(job_id))
                        self.blocks.remove_and_update(job_id)
                        print("Job {} is finished".format(job_id))
                        counter += 1

            if counter != 0:
                print("Checked {} returns".format(counter))
            # print("Checked {} returns".format(counter))

            for actor in done:
                del query_futures[actor]

    def get_actors(self):
        with self.actor_list_lock:
            return self.actors




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

from dask.distributed import get_worker
class RemoteActor():
    """ Object that runs on a remote worker providing task management API for user code """
    job_queue = None
    job_queue_cv = None
    req_counter = 0

    return_queue = None
    return_queue_cv = None

    close_handler = False
    user_worker = None

    def start(self, worker_fn=None):
        self.return_queue = queue.Queue()
        self.return_queue_cv = threading.Condition()
        self.job_queue = deque()
        self.job_queue_cv = threading.Condition()
        # self.run_thread = threading.Thread(target=worker_fn, args=[self])
        self.close_handler = False
        if worker_fn != None:
            self.run_thread = threading.Thread(target=worker_fn)
            self.run_thread.start() # run user provided loop

        self.daisy_initialized = True


    def start_file(self, worker_file):
        self.return_queue = queue.Queue()
        self.return_queue_cv = threading.Condition()
        self.job_queue = deque()
        self.job_queue_cv = threading.Condition()
        # self.run_thread = thread

        self.user_worker = importlib.import_module(worker_file, package=None)
        # threading.Thread(target=worker_file, args=[self])
        # self.run_thread = threading.Thread(target=self.worker_file_thread, args=[worker_file])
        self.run_thread = threading.Thread(target=self.user_worker.main)
        self.close_handler = False
        print("starting test_actor_new_user")
        self.run_thread.start() # run user provided loop
        print("started test_actor_new_user")


    def close(self):
        with self.job_queue_cv:
            self.close_handler = True
            self.job_queue_cv.notify()


    def reqs(self):
        time.sleep(3)
        ret = self.req_counter
        self.req_counter = 0
        return ret


    # def rets(self):
    #     return len(return_queue)


    def push(self, job):
        with self.job_queue_cv:
            self.job_queue.append(job)
            self.job_queue_cv.notify()


    def acquire_block(self):
        with self.job_queue_cv:
            self.req_counter += 1
            while len(self.job_queue) == 0:

                try:
                    get_worker()
                except:
                    # will except when worker is disconnected
                    # in this case signal to user to break the loop
                    print("Disconnected.. shutdown.")
                    return -1

                if self.close_handler:
                    print("Closing handler")
                    self.close_handler = False
                    self.release_block(-1, None) # signaling done to master
                    return -1 # tells user loop to break

                self.job_queue_cv.wait(timeout=5) # wait for 5 seconds before rechecking

            ret = self.job_queue.popleft()
            # print("got job")
            return ret

    def release_block(self, jobid, res):
        self.return_queue.put((jobid, res))

    def get_ret(self):
        try:
            return self.return_queue.get(block=False)
        except:
            return None



def get_scheduler():
    worker = get_worker()
    print(worker)
    print(worker.actors)

    actor_list = list(worker.actors.keys())
    if len(actor_list) == 0:
        raise Exception("Actor is not initialized on this node")

    actor = worker.actors[actor_list[0]]
    # worker.claimed_actors
    print("returning actor {}".format(actor))
    return actor

class SchedulerCallback(distributed.diagnostics.plugin.SchedulerPlugin):
    sched = None

    def __init__(self, sched):
        self.sched = sched

    def add_worker(self, scheduler=None, worker=None, **kwargs):
        self.sched.add_worker_callback(worker)

    def remove_worker(self, scheduler=None, worker=None, **kwargs):
        self.sched.remove_worker_callback(worker)


