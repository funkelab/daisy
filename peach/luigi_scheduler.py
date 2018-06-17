from targets import BlockDoneTarget
from tasks import ProcessBlocks, BlockTask
import luigi

def run_with_luigi(
    total_roi,
    read_roi,
    write_roi,
    process_function,
    check_function,
    num_workers,
    read_write_conflict=True,
    host='localhost'):
    '''Run block-wise tasks with luigi.

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

        host (optional):

            The luigi host to submit jobs to. Defaults to ``localhost``.
    '''

    class ProcessBlock(BlockTask):

        def run(self):
            process_function(self.read_roi, self.write_roi)

        def output(self):
            return BlockDoneTarget(self.write_roi, check_function)

    process_blocks = ProcessBlocks(
        total_roi,
        read_roi,
        write_roi,
        read_write_conflict,
        ProcessBlock)

    luigi.build(
        [process_blocks],
        log_level='INFO',
        workers=num_workers,
        scheduler_host=host)
