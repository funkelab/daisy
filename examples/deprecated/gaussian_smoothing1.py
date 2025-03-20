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


if __name__ == "__main__":

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
        "--sigma", "-s", type=float, help="Sigma to use for gaussian filter", default=2
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
    ap.add_argument(
        "--num_workers", type=int, help="Number of workers to run", default=4
    )

    config = ap.parse_args()

    # open dataset
    dataset = daisy.open_ds(config.in_file, config.in_ds_name)

    # define total region of interest (roi)
    total_roi = dataset.roi
    ndims = len(total_roi.get_offset())

    # define block read and write rois
    assert (
        len(config.block_read_size) == ndims
    ), "Read size must have same dimensions as in_file"
    assert (
        len(config.block_write_size) == ndims
    ), "Write size must have same dimensions as in_file"
    block_read_size = daisy.Coordinate(config.block_read_size)
    block_write_size = daisy.Coordinate(config.block_write_size)
    block_read_size *= dataset.voxel_size
    block_write_size *= dataset.voxel_size
    context = (block_read_size - block_write_size) / 2
    block_read_roi = daisy.Roi((0,) * ndims, block_read_size)
    block_write_roi = daisy.Roi(context, block_write_size)

    # prepare output dataset
    output_roi = total_roi.grow(-context, -context)
    if config.out_file is None:
        config.out_file = config.in_file
    if config.out_ds_name is None:
        config.out_ds_name = config.in_ds_name + "_smoothed"

    logger.info(f"Processing data to {config.out_file}/{config.out_ds_name}")

    output_dataset = daisy.prepare_ds(
        config.out_file,
        config.out_ds_name,
        total_roi=output_roi,
        voxel_size=dataset.voxel_size,
        dtype=dataset.dtype,
        write_size=block_write_roi.get_shape(),
    )

    # make task
    task = daisy.Task(
        "GaussianSmoothingTask",
        total_roi,
        block_read_roi,
        block_write_roi,
        process_function=lambda b: smooth(
            b, dataset, output_dataset, sigma=config.sigma
        ),
        read_write_conflict=False,
        num_workers=config.num_workers,
        fit="shrink",
    )

    # run task
    ret = daisy.run_blockwise([task])

    if ret:
        logger.info("Ran all blocks successfully!")
    else:
        logger.info("Did not run all blocks successfully...")
