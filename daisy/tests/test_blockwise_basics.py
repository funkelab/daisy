from __future__ import absolute_import

from .tmpdir_test import TmpDirTestCase
import daisy
import glob
import os
import logging

logger = logging.getLogger(__name__)


class TestBlockwiseBasics(TmpDirTestCase):

    def test_local(self):

        total_roi = daisy.Roi((0,), (100,))
        read_roi = daisy.Roi((0,), (5,))
        write_roi = daisy.Roi((0,), (3,))

        outdir = self.path_to('')

        ret = daisy.run_blockwise(
            total_roi=total_roi,
            read_roi=read_roi,
            write_roi=write_roi,
            process_function=lambda b: self.process_local(outdir, b),
            num_workers=10)

        outfiles = glob.glob(os.path.join(outdir, '*.block'))
        block_ids = sorted([
            int(path.split('/')[-1].split('.')[0])
            for path in outfiles
        ])

        self.assertTrue(ret)
        self.assertEqual(block_ids, list(range(32)))

    def test_failure(self):

        total_roi = daisy.Roi((0,), (100,))
        read_roi = daisy.Roi((0,), (5,))
        write_roi = daisy.Roi((0,), (3,))

        outdir = self.path_to('')

        ret = daisy.run_blockwise(
            total_roi=total_roi,
            read_roi=read_roi,
            write_roi=write_roi,
            process_function=lambda b: self.process_local(outdir, b, fail=16),
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

    def process_local(self, outdir, block, fail=None):

        logger.debug("Processing block", block)

        if block.block_id == fail:
            raise RuntimeError("intended failure")

        path = os.path.join(outdir, '%d.block' % block.block_id)
        with open(path, 'w') as f:
            f.write(str(block.block_id))
