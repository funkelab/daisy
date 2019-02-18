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

        outdir = self.path_to()

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

        outdir = self.path_to()

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

        outdir = self.path_to()

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

        outdir = self.path_to()

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

    def test_negative_offset(self):

        logger.warning("A warning")

        total_roi = daisy.Roi(
            (-100,),
            (2369,))
        block_write_roi = daisy.Roi(
            (0,),
            (500,))
        block_read_roi = block_write_roi.grow(
            (100,),
            (100,))

        outdir = self.path_to()

        ret = daisy.run_blockwise(
            total_roi,
            block_read_roi,
            block_write_roi,
            process_function=lambda b: self.process_block(outdir, b),
            num_workers=1,
            fit='shrink')

        outfiles = glob.glob(os.path.join(outdir, '*.block'))
        block_ids = sorted([
            int(path.split('/')[-1].split('.')[0])
            for path in outfiles
        ])

        self.assertTrue(ret)
        self.assertEqual(len(block_ids), 5)

    def test_multidim(self):

        total_roi = daisy.Roi(
            (199, -100, -100, -100),
            (12, 5140, 2248, 2369))
        block_write_roi = daisy.Roi(
            (0, 0, 0, 0),
            (5, 500, 500, 500))
        block_read_roi = block_write_roi.grow(
            (1, 100, 100, 100),
            (1, 100, 100, 100))

        outdir = self.path_to()

        ret = daisy.run_blockwise(
            total_roi,
            block_read_roi,
            block_write_roi,
            process_function=lambda b: self.process_block(outdir, b),
            num_workers=8,
            processes=False,
            fit='shrink')

        outfiles = glob.glob(os.path.join(outdir, '*.block'))
        block_ids = sorted([
            int(path.split('/')[-1].split('.')[0])
            for path in outfiles
        ])

        self.assertTrue(ret)
        self.assertEqual(len(block_ids), 500)

    def process_block(self, outdir, block, fail=None):

        logger.debug("Processing block %s", block)

        if block.block_id == fail:
            raise RuntimeError("intended failure")

        path = os.path.join(outdir, '%d.block' % block.block_id)
        with open(path, 'w') as f:
            f.write(str(block.block_id))

    def worker(self, outdir, fail=None):

        client = daisy.Client()

        while True:

            block = client.acquire_block()
            if block is None:
                break

            self.process_block(outdir, block, fail)

            client.release_block(block, 0)
