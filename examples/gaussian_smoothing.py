import daisy
import argparse
import numpy as np
import random

import matplotlib
import matplotlib.pyplot as plt
import scipy.ndimage

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_sample_image(
        shape=(2000, 2000),
        num_blobs=100,
        blob_radius=10,
        zarr_name="sample_data.zarr",
        zarr_group="blobs_2d"):

    arr = np.zeros(shape, dtype=np.uint8)

    # put blobs into the array
    for i in range(num_blobs):
        point = (random.randint(0, shape[0] - 1),
                 random.randint(0, shape[1] - 1))
        for x in range(
                max(point[0] - blob_radius, 0),
                min(point[0] + blob_radius, shape[0] - 1)):
            for y in range(
                    max(point[1] - blob_radius, 0),
                    min(point[1] + blob_radius, shape[1] - 1)):
                arr[x][y] = random.randint(0, 10)

    # prepare dataset
    total_roi = daisy.Roi((0, 0), shape)
    dataset = daisy.prepare_ds(
            zarr_name,
            zarr_group,
            total_roi=total_roi,
            voxel_size=(1, 1),
            dtype=np.uint8,
            write_size=(200, 200))
    dataset[total_roi] = arr


def smooth(block, dataset, output, sigma=5):
    logger.debug("Block: %s" % block)

    # read data in block.read_roi
    daisy_array = dataset[block.read_roi]
    data = daisy_array.to_ndarray()
    logger.debug("Got data of shape %s" % str(data.shape))

    # apply gaussian filter
    r = scipy.ndimage.gaussian_filter(
            data, sigma=sigma, mode='constant')

    # write result to output dataset in block.write_roi
    to_write = daisy.Array(
            data=r,
            roi=block.read_roi,
            voxel_size=dataset.voxel_size)
    output[block.write_roi] = to_write[block.write_roi]
    logger.debug("Done")
    return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--image', '-i', type=str,
                        help="Path to zarr containing image",
                        default="sample_data.zarr")
    parser.add_argument('--group', '-g', type=str,
                        help="Group or volume name of image within zarr",
                        default="blobs_2d")
    parser.add_argument('--output', '-o', type=str,
                        help="Zarr file to write smoothed image to"
                        "(will use same group/volume name)",
                        default="sample_output.zarr")
    parser.add_argument('--sigma', '-s', type=float,
                        help="Sigma to use for gaussian filter",
                        default=2)
    parser.add_argument('--block_read_size', '-r', nargs='+',
                        help="Size of block read region",
                        default=[200, 200])
    parser.add_argument('--block_write_size', '-w', nargs='+',
                        help="Size of block write region",
                        default=[180, 180])
    parser.add_argument('--num_workers', '-nw', type=int,
                        help="Number of processes to spawn",
                        default=1)
    parser.add_argument('--create_image', action="store_true",
                        help="Flag to create the sample image zarr")
    args = parser.parse_args()

    # open dataset
    dataset = daisy.open_ds(args.image, args.group)

    # write input image to png for visualization
    # note - you usually would have data to big to do this
    # for the whole thing
    plt.imshow(dataset.to_ndarray(), cmap=matplotlib.cm.binary)
    plt.savefig('image_before_gaussian.png')

    # define total region of interest (roi)
    total_roi = dataset.roi
    ndims = len(total_roi.get_offset())

    # define block read and write rois
    assert len(args.block_read_size) == ndims,\
        "Read size must have same dimensions as image"
    assert len(args.block_write_size) == ndims,\
        "Write size must have same dimensions as image"
    block_read_size = daisy.Coordinate(args.block_read_size)
    block_write_size = daisy.Coordinate(args.block_write_size)
    context = (block_read_size - block_write_size) / 2
    block_read_roi = daisy.Roi((0,)*ndims, block_read_size)
    block_write_roi = daisy.Roi(context, block_write_size)

    # prepare output dataset
    output_roi = total_roi.grow(-context, -context)
    output_dataset = daisy.prepare_ds(
            args.output,
            args.group,
            total_roi=output_roi,
            voxel_size=dataset.voxel_size,
            dtype=dataset.dtype,
            write_size=block_write_roi.get_shape())

    # call run_blockwise
    daisy.run_blockwise(
            total_roi,
            block_read_roi,
            block_write_roi,
            process_function=lambda b: smooth(
                b, dataset, output_dataset, sigma=args.sigma),
            read_write_conflict=False,
            num_workers=args.num_workers)

    # write output image to png for visualization
    # note - you usually would have data to big to do this
    # for the whole thing
    plt.imshow(output_dataset.to_ndarray(), cmap=matplotlib.cm.binary)
    plt.savefig('image_after_gaussian.png')
