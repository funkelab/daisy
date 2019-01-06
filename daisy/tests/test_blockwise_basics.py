from __future__ import absolute_import

from .tmpdir_test import TmpDirTestCase
import daisy
import glob
import os
import logging

logger = logging.getLogger(__name__)


class TestBlockwiseBasics(TmpDirTestCase):

    def test_callback(self):

        total_roi = daisy.Roi((0,), (100,))
        read_roi = daisy.Roi((0,), (5,))
        write_roi = daisy.Roi((0,), (3,))

        outdir = self.path_to('')

        ret = daisy.run_blockwise(
            total_roi=total_roi,
            read_roi=read_roi,
            write_roi=write_roi,
            process_function=lambda b: self.process_block(outdir, b),
            num_workers=10)

        outfiles = glob.glob(os.path.join(outdir, '*.block'))
        block_ids = sorted([
            int(path.split('/')[-1].split('.')[0])
            for path in outfiles
        ])

        self.assertTrue(ret)
        self.assertEqual(block_ids, list(range(32)))

    def test_callback_failure(self):

        total_roi = daisy.Roi((0,), (100,))
        read_roi = daisy.Roi((0,), (5,))
        write_roi = daisy.Roi((0,), (3,))

        outdir = self.path_to('')

        ret = daisy.run_blockwise(
            total_roi=total_roi,
            read_roi=read_roi,
            write_roi=write_roi,
            process_function=lambda b: self.process_block(outdir, b, fail=16),
            num_workers=10)

        outfiles = glob.glob(os.path.join(outdir, '*.block'))
        block_ids = sorted([
            int(path.split('/')[-1].split('.')[0])
            for path in outfiles
        ])

        self.assertFalse(ret)
        expected_block_ids = list(range(32))
        expected_block_ids.remove(16)
        self.assertEqual(block_ids, expected_block_ids)

    def test_worker(self):

        total_roi = daisy.Roi((0,), (100,))
        read_roi = daisy.Roi((0,), (5,))
        write_roi = daisy.Roi((0,), (3,))

        outdir = self.path_to('')

        ret = daisy.run_blockwise(
            total_roi=total_roi,
            read_roi=read_roi,
            write_roi=write_roi,
            process_function=lambda: self.worker(outdir),
            num_workers=10)

        outfiles = glob.glob(os.path.join(outdir, '*.block'))
        block_ids = sorted([
            int(path.split('/')[-1].split('.')[0])
            for path in outfiles
        ])

        self.assertTrue(ret)
        self.assertEqual(block_ids, list(range(32)))

    def test_worker_failure(self):

        total_roi = daisy.Roi((0,), (100,))
        read_roi = daisy.Roi((0,), (5,))
        write_roi = daisy.Roi((0,), (3,))

        outdir = self.path_to('')

        ret = daisy.run_blockwise(
            total_roi=total_roi,
            read_roi=read_roi,
            write_roi=write_roi,
            process_function=lambda: self.worker(outdir, fail=16),
            num_workers=10)

        outfiles = glob.glob(os.path.join(outdir, '*.block'))
        block_ids = sorted([
            int(path.split('/')[-1].split('.')[0])
            for path in outfiles
        ])

        self.assertFalse(ret)
        expected_block_ids = list(range(32))
        expected_block_ids.remove(16)
        self.assertEqual(block_ids, expected_block_ids)

    def process_block(self, outdir, block, fail=None):

        logger.debug("Processing block", block)

        if block.block_id == fail:
            raise RuntimeError("intended failure")

        path = os.path.join(outdir, '%d.block' % block.block_id)
        with open(path, 'w') as f:
            f.write(str(block.block_id))

    def worker(self, outdir, fail=None):

        scheduler = daisy.ClientScheduler()

        while True:

            block = scheduler.acquire_block()
            if block is None:
                break

            self.process_block(outdir, block, fail)

            scheduler.release_block(block, 0)
