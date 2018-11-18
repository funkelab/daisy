from __future__ import absolute_import
from .blocks import create_dependency_graph
from .dynamic_blocks import DynamicBlocks
from dask.distributed import Client, LocalCluster
import traceback
import logging
import threading
import queue
import time
from collections import deque

logger = logging.getLogger(__name__)

class DaskScheduler():

    actors = []
    dask_client = None
    # num_workers = 4
    finished_scheduling = False

    idle_actor_queue = queue.Queue() #synchronized queue

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

            if num_workers is not None:
                print("Creating local cluster with %d workers..."%num_workers)

                cluster = LocalCluster(
                    n_workers=num_workers,
                    threads_per_worker=1,
                    memory_limit=0,
                    diagnostics_port=None)

            dask_client = Client(cluster)

        self.dask_client = dask_client

        for n in range(num_workers):
           future = self.dask_client.submit(RemoteActor, actor=True)
           # future.result().start(worker_fn)
           self.actors.append(future.result())


    def schedule_blockwise(
        self,
        total_roi,
        read_roi,
        write_roi,
        job_handler,
        process_function,
        check_function=None,
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
        logger.info("Scheduling tasks...")

        # actors = Actors(client, blocks)
        for actor in self.actors:
            print("Running actor {}".format(actor))
            f = actor.start(job_handler)
            f.result()

        # # run all tasks
        req_thread = threading.Thread(target=self.check_req_loop)
        req_thread.start()
        ret_thread = threading.Thread(target=self.check_ret_loop)
        ret_thread.start()

        # futures = {}
        results = []
        tasks = []
        self.blocks = blocks
        self.finished_scheduling = False

        while not blocks.empty():
            block = blocks.next()
            # assert(block != None)

            if block != None:
                print(block)
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
                # actor.push((block.block_id, process_function(block))).result() # TODO: should be async?
                actor.push((block.block_id, process_function, [block])).result() # TODO: should be async?
                # check_and_run(actor, block, process_function, pre_check, post_check)

                # futures.append((block_id, client.get(tasks, list(tasks.keys()), sync=False)))
                # futures[block_id] = client.get(tasks, list(tasks.keys()), sync=False)

                    # results.append((blocks.get_task(block_id), future.result()))

        self.finished_scheduling = True
        ret_thread.join()
        req_thread.join()
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

    # def block_to_dask_name(block):

    #     return '%d'%block.block_id

    def get_idle_actor(self):
        return self.idle_actor_queue.get()

    def check_req_loop(self):
        query_futures = {}
        while self.finished_scheduling == False:
            for actor in self.actors:
                if actor not in query_futures:
                    query_futures[actor] = actor.reqs()

            # slow down loop
            time.sleep(.1)

            done = []
            for actor in query_futures:
                future = query_futures[actor]
                try:
                    ret = future.result(timeout=.01)
                    done.append(actor)
                    for i in range(ret):
                        self.idle_actor_queue.put(actor)
                except:
                    pass # query uncompleted

            for actor in done:
                del query_futures[actor]


    def check_ret_loop(self):
        query_futures = {}
        while self.finished_scheduling == False:
            for actor in self.actors:
                if actor not in query_futures:
                    query_futures[actor] = actor.get_ret()

            # slow down loop
            time.sleep(.1)

            done = []
            for actor in query_futures:
                future = query_futures[actor]
                try:
                    ret = future.result()
                    done.append(actor)
                    if ret != None:
                        job_id, ret = ret
                        self.blocks.remove_and_update(job_id)
                except:
                    pass # query uncompleted

            for actor in done:
                del query_futures[actor]





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
    """ Sample actor framework that would run user script """
    # job_queue = queue.Queue()
    # job_queue = asyncio.Queue()
    job_queue = None
    job_queue_cv = None
    req_counter = 0
    # ret_counter = 0

    return_queue = None
    return_queue_cv = None

    # run_thread = None

    def start(self, worker_fn):
        self.return_queue = queue.Queue()
        self.return_queue_cv = threading.Condition()
        # self.run_thread = threading.Thread(target=self.run, args=[worker_fn])
        self.job_queue = deque()
        self.job_queue_cv = threading.Condition()
        self.run_thread = threading.Thread(target=worker_fn, args=[self])
        self.run_thread.start()

    def reqs(self):
        ret = self.req_counter
        self.req_counter = 0
        return ret

    def rets(self):
        return len(return_queue)

    def push(self, job):
        # print("actor: queue_job1")
        # pass
        with self.job_queue_cv:
            self.job_queue.append(job)
            self.job_queue_cv.notify()
        # print("actor: queue_job2")

    # def queue_size(self):
    #     return len(self.job_queue)

    def next(self, blocking=True):
        # print("actor: next 1")
        with self.job_queue_cv:
            print("actor: next 1")
            self.req_counter += 1
            # if len(self.job_queue) == 0:
            #     if not blocking:
            #         return None
            while len(self.job_queue) == 0:
                self.job_queue_cv.wait()
            # print("actor: next 2")
            ret = self.job_queue.popleft()
            print("remote next {}".format(ret))
            return ret

    def ret(self, jobid, res):
        self.return_queue.put((jobid, res))
        # with self.return_queue_cv:
        #     self.return_queue[jobid] = res
        #     self.return_queue_cv.notify()

    def get_ret(self):
        try:
            return self.return_queue.get(block=False)
        except:
            return None

   # def get_job_result(self, jobid):
   #    # with self.return_queue_cv:
   #       # while jobid not in self.return_queue:
   #       #    self.return_queue_cv.wait()
   #       # ret = self.return_queue[jobid]
   #       # del self.return_queue[jobid]
   #       # return ret
   #    if jobid not in self.return_queue:
   #       return -1

   #    with self.return_queue_cv:
   #       # while jobid not in self.return_queue:
   #          # pass
   #       #    self.return_queue_cv.wait()
   #       ret = self.return_queue[jobid]
   #       del self.return_queue[jobid]
   #       return ret
   #       # return 0

