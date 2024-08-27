from .client import Client
from inspect import getfullargspec


class Task:
    """Definition of a ``daisy`` task that is to be run in a block-wise
    fashion.

    Args:

        name (``string``):

            The unique name of the task.

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

                process_function(block)

            with ``block`` being the shifted read and write ROI for each
            location in the volume.

            If ``read_write_conflict`` is ``True`, the callee can assume that
            there are no read/write concurencies, i.e., at any given point in
            time the ``read_roi`` does not overlap with the ``write_roi`` of
            another process.

        check_function (function, optional):

            A function that will be called as::

                check_function(block)

            This function should return ``True`` if the block was completed.
            This is used internally to avoid processing blocks that are already
            done and to check if a block was correctly processed.

            If a tuple of two functions is given, the first one will be called
            to check if the block needs to be run, and if so, the second one
            will be called after it was run to check if the run succeeded.

        init_callback_fn (function, optional):

            A function that Daisy will call once when the task is started.
            It will be called as::

                init_callback_fn(context)

            Where `context` is the `daisy.Context` string that can be used
            by the daisy clients to connect to the server.

        read_write_conflict (``bool``, optional):

            Whether the read and write ROIs are conflicting, i.e., accessing
            the same resource. If set to ``False``, all blocks can run at the
            same time in parallel. In this case, providing a ``read_roi`` is
            simply a means of convenience to ensure no out-of-bound accesses
            and to avoid re-computation of it in each block.

        fit (``string``, optional):

            How to handle cases where shifting blocks by the size of
            ``write_roi`` does not tile the ``total_roi``. Possible options
            are:

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

            The number of parallel processes to run.

        max_retries (int, optional):

            The maximum number of times a task will be retried if failed
            (either due to failed post check or application crashes or network
            failure)

        timeout (int, optional):

            Time in seconds to wait for a block to be returned from a worker.
            The worker is killed (and the block retried) if this time is
            exceeded.
    """

    def __init__(
        self,
        task_id,
        total_roi,
        read_roi,
        write_roi,
        process_function,
        check_function=None,
        init_callback_fn=None,
        read_write_conflict=True,
        num_workers=1,
        max_retries=2,
        fit="valid",
        timeout=None,
        upstream_tasks=None,
    ):
        self.task_id = task_id
        self.total_roi = total_roi
        self.orig_total_roi = total_roi
        self.read_roi = read_roi
        self.write_roi = write_roi
        self.total_write_roi = self.total_roi.grow(
            -(write_roi.begin - read_roi.begin),
            -(read_roi.end - write_roi.end),
        )
        self.process_function = process_function
        self.check_function = check_function
        self.read_write_conflict = read_write_conflict
        self.fit = fit
        self.num_workers = num_workers
        self.max_retries = max_retries
        self.timeout = timeout
        self.upstream_tasks = []
        if upstream_tasks is not None:
            self.upstream_tasks.extend(upstream_tasks)
        if init_callback_fn is not None:
            self.init_callback_fn = init_callback_fn
        else:
            self.init_callback_fn = self._default_init

        args = getfullargspec(process_function).args
        if "self" in args:
            args.remove("self")
        if len(args) == 0:
            # spawn function
            self.spawn_worker_function = process_function
        elif len(args) == 1:
            # process block function
            self.spawn_worker_function = self._process_blocks
        else:
            raise ValueError(f"daisy does not know what to pass into args: {args}")

    def _default_init(self, context):
        pass

    def _process_blocks(self):
        client = Client()
        while True:
            with client.acquire_block() as block:
                if block is None:
                    break
                self.process_function(block)

    def requires(self):
        return self.upstream_tasks
