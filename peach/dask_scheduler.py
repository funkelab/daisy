from distributed import Client
from blocks import create_dependency_graph

def run_with_dask(
    total_roi,
    read_roi,
    write_roi,
    process_function,
    check_function,
    num_workers,
    read_write_conflict=True,
    client=None):
    '''Run block-wise tasks with dask.

    Args:

        total_roi (`class:peach.Roi`):

            The region of interest (ROI) of the complete volume to process.

        read_roi (`class:peach.Roi`):

            The ROI every block needs to read data from. Will be shifted over
            the ``total_roi`` to cover the whole volume.

        write_roi (`class:peach.Roi`):

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

        check_function (function):

            A function that will be called as::

                check_function(write_roi)

            ``write_roi`` shifted for each block to process.

            This function should return ``True`` if the block represented by
            ``write_roi`` was completed. This is used internally to avoid
            processing blocks that are already done and to check if a block was
            correctly processed.

        num_workers (int):

            The number of parallel processes to run.

        read_write_conflict (``bool``, optional):

            Whether the read and write ROIs are conflicting, i.e., accessing
            the same resource. If set to ``False``, all blocks can run at the
            same time in parallel. In this case, providing a ``read_roi`` is
            simply a means of convenience to ensure no out-of-bound accesses
            and to avoid re-computation of it in each block.

        client (optional):

            The dask client to submit jobs to. If ``None``, a client will be
            created from ``dask.distributed.Client``.
    '''

    blocks = create_dependency_graph(
        total_roi,
        read_roi,
        write_roi,
        read_write_conflict)

    # dask requires strings for task names, string representation of
    # `class:Roi` is assumed to be unique.
    tasks = {
        roi_to_dask_name(write_roi): (
            check_and_run,
            read_roi,
            write_roi,
            process_function,
            check_function,
            [ roi_to_dask_name(ups) for ups in upstream_write_rois ]
        )
        for write_roi, read_roi, upstream_write_rois in blocks
    }

    if client is None:
        client = Client()

    # run all tasks
    client.get(tasks, tasks.keys(), num_workers=num_workers)

def roi_to_dask_name(roi):

    return '_'.join([
        '%d:%d'%(b, e)
        for b, e in zip(roi.get_begin(), roi.get_end())
    ])

def check_and_run(read_roi, write_roi, process_function, check_function, *args):

    if check_function(write_roi):
        logging.info(
            "Skipping task with read ROI %s, write ROI %s; already processed.",
            read_roi, write_roi)
        return

    process_function(read_roi, write_roi)

    if not check_function(write_roi):
        raise RuntimeError(
            "Completion check failed for task with read ROI %s, write ROI "
            "%s."%(read_roi, write_roi))
