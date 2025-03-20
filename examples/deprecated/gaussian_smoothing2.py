import argparse
import numpy as np
import random
import scipy.ndimage
import sys
import os

import daisy
from batch_task import BatchTask

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GaussianSmoothingTask")


def smooth(block, dataset, output, sigma=5):
    logger.debug("Block: %s" % block)

    # read data in block.read_roi
    daisy_array = dataset[block.read_roi]
    data = daisy_array.to_ndarray()
    logger.debug("Got data of shape %s" % str(data.shape))

    # apply gaussian filter
    r = scipy.ndimage.gaussian_filter(data, sigma=sigma, mode="constant")

    # write result to output dataset in block.write_roi
    to_write = daisy.Array(data=r, roi=block.read_roi, voxel_size=dataset.voxel_size)
    output[block.write_roi] = to_write[block.write_roi]
    logger.debug("Done")
    return 0


class GaussianSmoothingTask(BatchTask):

    def _task_init(self):

        # open dataset
        dataset = daisy.open_ds(self.in_file, self.in_ds_name)

        # define total region of interest (roi)
        total_roi = dataset.roi
        ndims = len(total_roi.get_offset())

        # define block read and write rois
        assert (
            len(self.block_read_size) == ndims
        ), "Read size must have same dimensions as in_file"
        assert (
            len(self.block_write_size) == ndims
        ), "Write size must have same dimensions as in_file"
        block_read_size = daisy.Coordinate(self.block_read_size)
        block_write_size = daisy.Coordinate(self.block_write_size)
        block_read_size *= dataset.voxel_size
        block_write_size *= dataset.voxel_size
        context = (block_read_size - block_write_size) / 2
        block_read_roi = daisy.Roi((0,) * ndims, block_read_size)
        block_write_roi = daisy.Roi(context, block_write_size)

        # prepare output dataset
        output_roi = total_roi.grow(-context, -context)
        if self.out_file is None:
            self.out_file = self.in_file
        if self.out_ds_name is None:
            self.out_ds_name = self.in_ds_name + "_smoothed"

        logger.info(f"Processing data to {self.out_file}/{self.out_ds_name}")

        output_dataset = daisy.prepare_ds(
            self.out_file,
            self.out_ds_name,
            total_roi=output_roi,
            voxel_size=dataset.voxel_size,
            dtype=dataset.dtype,
            write_size=block_write_roi.get_shape(),
        )

        # save variables for other functions
        self.total_roi = total_roi
        self.block_read_roi = block_read_roi
        self.block_write_roi = block_write_roi
        self.dataset = dataset
        self.output_dataset = output_dataset

    def prepare_task(self, upstream_tasks=None):
        # call _prepare_task which prepares blockwise task
        worker_filename = os.path.realpath(__file__)
        self._write_config(worker_filename)
        return self._prepare_task(
            self.total_roi,
            self.block_read_roi,
            self.block_write_roi,
            read_write_conflict=False,
            upstream_tasks=upstream_tasks,
        )

    def _worker_impl(self, block):
        """Worker function implementation"""
        smooth(block, self.dataset, self.output_dataset, sigma=self.sigma)


if __name__ == "__main__":

    if len(sys.argv) > 1 and sys.argv[1] == "run_worker":
        task = GaussianSmoothingTask(config_file=sys.argv[2])
        task.run_worker()

    else:

        ap = argparse.ArgumentParser()
        ap.add_argument("in_file", type=str, help="The input container")
        ap.add_argument("in_ds_name", type=str, help="The name of the dataset")
        ap.add_argument(
            "--out_file",
            type=str,
            default=None,
            help="The output container, defaults to be the same as in_file",
        )
        ap.add_argument(
            "--out_ds_name",
            type=str,
            default=None,
            help="The name of the dataset, defaults to be in_ds_name + smoothed",
        )
        ap.add_argument(
            "--sigma",
            "-s",
            type=float,
            help="Sigma to use for gaussian filter",
            default=2,
        )
        ap.add_argument(
            "--block_read_size",
            "-r",
            nargs="+",
            help="Size of block read region",
            default=[20, 200, 200],
        )
        ap.add_argument(
            "--block_write_size",
            "-w",
            nargs="+",
            help="Size of block write region",
            default=[18, 180, 180],
        )

        config = GaussianSmoothingTask.parse_args(ap)
        task = GaussianSmoothingTask(config)
        daisy_task = task.prepare_task()
        done = daisy.run_blockwise([daisy_task])
        if done:
            logger.info("Ran all blocks successfully!")
        else:
            logger.info("Did not run all blocks successfully...")
