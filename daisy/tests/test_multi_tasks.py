from __future__ import absolute_import

from .tmpdir_test import TmpDirTestCase
import daisy
import glob
import os
import logging

logger = logging.getLogger(__name__)


class TestMultipleTasks(TmpDirTestCase):

    def test_single(self):
        '''Tests a vanilla task'''
        outdir = self.path_to('')

        # this task generates 0-10
        task = self.LeafTask(outdir=outdir)
        task_spec = {'task': task}

        expected_block_ids = list(range(10))

        ret = daisy.distribute([task_spec])

        outfiles = glob.glob(os.path.join(outdir, '*.block'))
        block_ids = sorted([
            int(path.split('/')[-1].split('.')[0])
            for path in outfiles
        ])

        self.assertTrue(ret)
        self.assertEqual(block_ids, expected_block_ids)

    def test_single_with_request(self):
        '''Tests a task with request for a subset ROI'''
        outdir = self.path_to('')

        task = self.LeafTask(outdir=outdir)
        task_spec = {'task': task, 'request': [daisy.Roi((3,), (2,))]}

        ret = daisy.distribute([task_spec])

        outfiles = glob.glob(os.path.join(outdir, '*.block'))
        block_ids = sorted([
            int(path.split('/')[-1].split('.')[0])
            for path in outfiles
        ])

        self.assertTrue(ret)
        expected_block_ids = list(range(3, 5))
        self.assertEqual(block_ids, expected_block_ids)

    def test_null_request(self):
        '''Tests a task with request for null ROI'''
        outdir = self.path_to('')

        task = self.LeafTask(outdir=outdir)
        task_spec = {'task': task, 'request': [daisy.Roi((3,), (0,))]}

        ret = daisy.distribute([task_spec])

        outfiles = glob.glob(os.path.join(outdir, '*.block'))
        block_ids = sorted([
            int(path.split('/')[-1].split('.')[0])
            for path in outfiles
        ])

        self.assertTrue(ret)
        expected_block_ids = []
        self.assertEqual(block_ids, expected_block_ids)

    def test_multi(self):
        '''Tests multiple different task targets'''
        outdir = self.path_to('')

        # this task generates 0-10
        task0 = self.LeafTask(outdir=outdir)
        expected_block_ids = list(range(0, 10))
        # this task generates 20-30
        task1 = self.LeafTaskAnother(outdir=outdir)
        expected_block_ids += list(range(20, 30))

        task0_spec = {'task': task0}
        task1_spec = {'task': task1}
        ret = daisy.distribute([task0_spec, task1_spec])

        outfiles = glob.glob(os.path.join(outdir, '*.block'))
        block_ids = sorted([
            int(path.split('/')[-1].split('.')[0])
            for path in outfiles
        ])

        self.assertTrue(ret)
        self.assertEqual(block_ids, expected_block_ids)

    def test_multi_with_request(self):
        '''Tests multiple different task targets with requests'''
        outdir = self.path_to('')

        # this task generates 0-10
        task0 = self.LeafTask(outdir=outdir)
        # this task generates 20-30
        task1 = self.LeafTaskAnother(outdir=outdir)

        task0_spec = {'task': task0, 'request': [daisy.Roi((3,), (2,))]}
        expected_block_ids = list(range(3, 5))
        task1_spec = {'task': task1, 'request': [daisy.Roi((27,), (2,))]}
        expected_block_ids += list(range(27, 29))

        ret = daisy.distribute([task0_spec, task1_spec])

        outfiles = glob.glob(os.path.join(outdir, '*.block'))
        block_ids = sorted([
            int(path.split('/')[-1].split('.')[0])
            for path in outfiles
        ])

        self.assertTrue(ret)
        self.assertEqual(block_ids, expected_block_ids)

    def test_multi_with_request_same(self):
        '''Tests multiple same task targets with requests'''
        outdir = self.path_to('')

        # this task generates 0-10
        task0 = self.LeafTask(outdir=outdir)

        task0_spec = {'task': task0, 'request': [daisy.Roi((3,), (2,))]}
        expected_block_ids = list(range(3, 5))
        task1_spec = {'task': task0, 'request': [daisy.Roi((7,), (1,))]}
        expected_block_ids += list(range(7, 8))

        ret = daisy.distribute([task0_spec, task1_spec])

        outfiles = glob.glob(os.path.join(outdir, '*.block'))
        block_ids = sorted([
            int(path.split('/')[-1].split('.')[0])
            for path in outfiles
        ])

        self.assertTrue(ret)
        self.assertEqual(block_ids, expected_block_ids)

    def test_multi_with_request_same_overlapping(self):
        '''Tests multiple same task targets with overlapping requests'''
        outdir = self.path_to('')

        # this task generates 0-10
        task0 = self.LeafTask(outdir=outdir)

        task0_spec = {'task': task0, 'request': [daisy.Roi((3,), (7,))]}
        task1_spec = {'task': task0, 'request': [daisy.Roi((5,), (5,))]}
        expected_block_ids = list(range(3, 10))

        ret = daisy.distribute([task0_spec, task1_spec])

        outfiles = glob.glob(os.path.join(outdir, '*.block'))
        block_ids = sorted([
            int(path.split('/')[-1].split('.')[0])
            for path in outfiles
        ])

        self.assertTrue(ret)
        self.assertEqual(block_ids, expected_block_ids)

    def test_request_outside_error(self):
        '''Tests request that lies outside of total_roi'''
        outdir = self.path_to('')

        # this task generates 0-10
        task0 = self.LeafTask(outdir=outdir)

        task0_spec = {'task': task0, 'request': [daisy.Roi((3,), (17,))]}

        try:
            daisy.distribute([task0_spec])
        except RuntimeError:
            pass
        except Exception as e:
            print(e)
            self.assertFalse(True)  # fail for any other Exceptions

    def test_task_chain(self):
        '''Tests vanilla task chain'''
        outdir = self.path_to('')

        # this task generates 0-10
        task0 = self.ParentTask(outdir=outdir)
        task0_spec = {'task': task0}
        expected_block_ids = list(range(0, 10))

        ret = daisy.distribute([task0_spec])

        outfiles = glob.glob(os.path.join(outdir, '*.block'))
        block_ids = sorted([
            int(path.split('/')[-1].split('.')[0])
            for path in outfiles
        ])

        self.assertTrue(ret)
        self.assertEqual(block_ids, expected_block_ids)

    def test_task_chain_with_request(self):
        '''Tests task chain with request'''
        outdir = self.path_to('')

        # this task generates 0-10
        task0 = self.ParentTask(outdir=outdir)
        task0_spec = {'task': task0, 'request': [daisy.Roi((3,), (2,))]}
        expected_block_ids = list(range(3, 5))

        ret = daisy.distribute([task0_spec])

        outfiles = glob.glob(os.path.join(outdir, '*.block'))
        block_ids = sorted([
            int(path.split('/')[-1].split('.')[0])
            for path in outfiles
        ])

        self.assertTrue(ret)
        self.assertEqual(block_ids, expected_block_ids)

    def test_task_chain_multi(self):
        '''Tests multiple tasks with the same dependency'''
        outdir = self.path_to('')

        # this task generates 0-10
        task0 = self.ParentTask(outdir=outdir)
        task0_spec = {'task': task0}
        # this task also generates 0-10
        task1 = self.ParentTaskAnother(outdir=outdir)
        task1_spec = {'task': task1}
        # their deps are merged
        expected_block_ids = list(range(0, 10))

        ret = daisy.distribute([task0_spec, task1_spec])

        outfiles = glob.glob(os.path.join(outdir, '*.block'))
        block_ids = sorted([
            int(path.split('/')[-1].split('.')[0])
            for path in outfiles
        ])

        self.assertTrue(ret)
        self.assertEqual(block_ids, expected_block_ids)

    def test_task_chain_multi_with_request(self):
        '''Tests multiple tasks with the same dependency and with request'''
        outdir = self.path_to('')

        # this task generates 0-10
        task0 = self.ParentTask(outdir=outdir)
        task0_spec = {'task': task0, 'request': [daisy.Roi((1,), (2,))]}
        # this task also generates 0-10
        task1 = self.ParentTaskAnother(outdir=outdir)
        task1_spec = {'task': task1, 'request': [daisy.Roi((7,), (2,))]}
        # their deps are merged
        expected_block_ids = list(range(1, 3))
        expected_block_ids += list(range(7, 9))

        ret = daisy.distribute([task0_spec, task1_spec])

        outfiles = glob.glob(os.path.join(outdir, '*.block'))
        block_ids = sorted([
            int(path.split('/')[-1].split('.')[0])
            for path in outfiles
        ])

        self.assertTrue(ret)
        self.assertEqual(block_ids, expected_block_ids)

    def test_task_chain_multi_with_overlapping_request(self):
        '''Tests multiple tasks with the same dependency and with request'''
        outdir = self.path_to('')

        # this task generates 0-10
        task0 = self.ParentTask(outdir=outdir)
        task0_spec = {'task': task0, 'request': [daisy.Roi((1,), (5,))]}
        # this task also generates 0-10
        task1 = self.ParentTaskAnother(outdir=outdir)
        task1_spec = {'task': task1, 'request': [daisy.Roi((2,), (5,))]}
        # their deps are merged
        expected_block_ids = list(range(1, 7))

        ret = daisy.distribute([task0_spec, task1_spec])

        outfiles = glob.glob(os.path.join(outdir, '*.block'))
        block_ids = sorted([
            int(path.split('/')[-1].split('.')[0])
            for path in outfiles
        ])

        self.assertTrue(ret)
        self.assertEqual(block_ids, expected_block_ids)

    def test_task_chain_multi_with_mixed_request(self):
        '''Tests multiple tasks with the same dependency and with request'''
        outdir = self.path_to('')

        # this task generates 0-10
        task0 = self.ParentTask(outdir=outdir)
        task0_spec = {'task': task0, 'request': [daisy.Roi((1,), (2,))]}
        # this task also generates 0-10
        task1 = self.ParentTaskAnother(outdir=outdir)
        task1_spec = {'task': task1}
        # their deps are merged
        expected_block_ids = list(range(0, 10))

        ret = daisy.distribute([task0_spec, task1_spec])

        outfiles = glob.glob(os.path.join(outdir, '*.block'))
        block_ids = sorted([
            int(path.split('/')[-1].split('.')[0])
            for path in outfiles
        ])

        self.assertTrue(ret)
        self.assertEqual(block_ids, expected_block_ids)

    def test_task_request_alignment(self):
        '''Tests multiple tasks with the same dependency and with request'''
        outdir = self.path_to('')

        # this task generates 0-5
        task0 = self.TaskWriteRoi2(outdir=outdir)
        task0_spec = {'task': task0}
        expected_block_ids = list(range(0, 5))

        ret = daisy.distribute([task0_spec])

        outfiles = glob.glob(os.path.join(outdir, '*.block'))
        block_ids = sorted([
            int(path.split('/')[-1].split('.')[0])
            for path in outfiles
        ])

        self.assertTrue(ret)
        self.assertEqual(block_ids, expected_block_ids)

    def test_task_request_alignment_with_req(self):
        '''Tests multiple tasks with the same dependency and with request'''
        outdir = self.path_to('')

        # this task generates 0-10
        task0 = self.TaskWriteRoi2(outdir=outdir)
        task0_spec = {'task': task0, 'request': [daisy.Roi((2,), (2,))]}
        # request lies in block 1
        expected_block_ids = list(range(1, 2))

        ret = daisy.distribute([task0_spec])

        outfiles = glob.glob(os.path.join(outdir, '*.block'))
        block_ids = sorted([
            int(path.split('/')[-1].split('.')[0])
            for path in outfiles
        ])

        self.assertTrue(ret)
        self.assertEqual(block_ids, expected_block_ids)

    def test_task_request_alignment_with_req2(self):
        '''Tests multiple tasks with the same dependency and with request'''
        outdir = self.path_to('')

        # this task generates 0-10
        task0 = self.TaskWriteRoi2(outdir=outdir)
        task0_spec = {'task': task0, 'request': [daisy.Roi((1,), (2,))]}
        # request lies between block 0 and block 1
        expected_block_ids = list(range(0, 2))

        ret = daisy.distribute([task0_spec])

        outfiles = glob.glob(os.path.join(outdir, '*.block'))
        block_ids = sorted([
            int(path.split('/')[-1].split('.')[0])
            for path in outfiles
        ])

        self.assertTrue(ret)
        self.assertEqual(block_ids, expected_block_ids)

    def test_task_request_alignment_with_req3(self):
        '''Tests multiple tasks with the same dependency and with request'''
        outdir = self.path_to('')

        # this task generates 0-10
        task0 = self.TaskWriteRoi3(outdir=outdir)
        task0_spec = {'task': task0}
        # request lies between block 0 and block 1
        expected_write_begins = list(range(1, 11, 2))

        ret = daisy.distribute([task0_spec])

        outfiles = glob.glob(os.path.join(outdir, '*.write_roi'))
        block_ids = sorted([
            int(path.split('/')[-1].split('.')[0])
            for path in outfiles
        ])

        self.assertTrue(ret)
        self.assertEqual(block_ids, expected_write_begins)

    def test_task_request_alignment_with_req4(self):
        '''Tests multiple tasks with the same dependency and with request'''
        outdir = self.path_to('')

        # this task generates 0-10
        task0 = self.TaskWriteRoi3(outdir=outdir)
        task0_spec = {'task': task0, 'request': [daisy.Roi((1,), (2,))]}
        expected_write_begins = [1]

        ret = daisy.distribute([task0_spec])

        outfiles = glob.glob(os.path.join(outdir, '*.write_roi'))
        block_ids = sorted([
            int(path.split('/')[-1].split('.')[0])
            for path in outfiles
        ])

        self.assertTrue(ret)
        self.assertEqual(block_ids, expected_write_begins)

    def test_task_request_alignment_with_req5(self):
        '''Tests multiple tasks with the same dependency and with request'''
        outdir = self.path_to('')

        # this task generates 0-10
        task0 = self.TaskWriteRoi3(outdir=outdir)
        task0_spec = {'task': task0, 'request': [daisy.Roi((1,), (6,))]}
        # requesting 1, 2, 3, 4, 5, 6
        # is satisfied with 3 write blocks
        expected_write_begins = [1, 3, 5]

        ret = daisy.distribute([task0_spec])

        outfiles = glob.glob(os.path.join(outdir, '*.write_roi'))
        block_ids = sorted([
            int(path.split('/')[-1].split('.')[0])
            for path in outfiles
        ])

        self.assertTrue(ret)
        self.assertEqual(block_ids, expected_write_begins)

    class TaskWriteRoi3(daisy.Task):

        outdir = daisy.Parameter()

        def prepare(self):

            total_roi = daisy.Roi((1,), (10,))
            read_roi = daisy.Roi((0,), (2,))
            write_roi = daisy.Roi((0,), (2,))

            self.schedule(
                total_roi,
                read_roi,
                write_roi,
                # process_function=TestMultipleTasks.process_block,
                process_function=lambda: TestMultipleTasks.worker(self.outdir),
                max_retries=0,
                fit='shrink')

    class TaskWriteRoi2(daisy.Task):

        outdir = daisy.Parameter()

        def prepare(self):

            total_roi = daisy.Roi((0,), (10,))
            read_roi = daisy.Roi((0,), (2,))
            write_roi = daisy.Roi((0,), (2,))

            self.schedule(
                total_roi,
                read_roi,
                write_roi,
                # process_function=TestMultipleTasks.process_block,
                process_function=lambda: TestMultipleTasks.worker(self.outdir),
                max_retries=0,
                fit='shrink')

    class LeafTask(daisy.Task):

        outdir = daisy.Parameter()

        def prepare(self):

            total_roi = daisy.Roi((0,), (10,))
            read_roi = daisy.Roi((0,), (1,))
            write_roi = daisy.Roi((0,), (1,))

            self.schedule(
                total_roi,
                read_roi,
                write_roi,
                # process_function=TestMultipleTasks.process_block,
                process_function=lambda: TestMultipleTasks.worker(self.outdir),
                max_retries=0,
                fit='shrink')

    class LeafTaskAnother(daisy.Task):

        outdir = daisy.Parameter()

        def prepare(self):

            total_roi = daisy.Roi((20,), (10,))
            read_roi = daisy.Roi((0,), (1,))
            write_roi = daisy.Roi((0,), (1,))

            self.schedule(
                total_roi,
                read_roi,
                write_roi,
                process_function=lambda: TestMultipleTasks.worker(self.outdir),
                max_retries=0,
                fit='shrink')

    class ParentTask(daisy.Task):

        outdir = daisy.Parameter()

        def prepare(self):

            total_roi = daisy.Roi((0,), (10,))
            read_roi = daisy.Roi((0,), (1,))
            write_roi = daisy.Roi((0,), (1,))

            self.schedule(
                total_roi,
                read_roi,
                write_roi,
                process_function=TestMultipleTasks.process_block_null,
                max_retries=0,
                fit='shrink')

        def requires(self):
            return [TestMultipleTasks.LeafTask(outdir=self.outdir)]

    class ParentTaskAnother(daisy.Task):

        outdir = daisy.Parameter()

        def prepare(self):

            total_roi = daisy.Roi((0,), (10,))
            read_roi = daisy.Roi((0,), (1,))
            write_roi = daisy.Roi((0,), (1,))

            self.schedule(
                total_roi,
                read_roi,
                write_roi,
                process_function=TestMultipleTasks.process_block_null,
                max_retries=0,
                fit='shrink')

        def requires(self):
            return [TestMultipleTasks.LeafTask(outdir=self.outdir)]

    def process_block(outdir, block, fail=None):

        logger.debug("Processing block", block)

        if block.block_id == fail:
            raise RuntimeError("intended failure")

        path = os.path.join(outdir, '%d.block' % block.block_id)
        with open(path, 'w') as f:
            f.write(str(block.block_id))

        # print(block.read_roi.get_begin())
        # print(block.write_roi.get_begin()[0])
        path = os.path.join(
            outdir, '%d.write_roi' % block.write_roi.get_begin()[0])
        with open(path, 'w') as f:
            f.write(str(block.write_roi))

    def process_block_null(block):
        return 0

    def worker(outdir, fail=None):

        client = daisy.Client()

        while True:

            block = client.acquire_block()
            if block is None:
                break

            TestMultipleTasks.process_block(outdir, block, fail)

            client.release_block(block, 0)
