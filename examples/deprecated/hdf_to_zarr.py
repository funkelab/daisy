import argparse
import logging
import os
import sys
import numpy as np
import skimage.measure

import daisy
from daisy import Coordinate
from batch_task import BatchTask

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("HDF2ZarrTask")


def calculateNearIsotropicDimensions(voxel_size, max_voxel_count):

    dims = len(voxel_size)

    voxel_count = 1
    vol_size = [k for k in voxel_size]
    voxel_dims = [1 for k in voxel_size]

    while voxel_count < max_voxel_count:
        for i in range(0, dims):
            if voxel_count >= max_voxel_count:
                continue
            if vol_size[i] == min(vol_size):
                vol_size[i] *= 2
                voxel_count *= 2
                voxel_dims[i] *= 2

    return voxel_dims


class HDF2ZarrTask(BatchTask):

    def _task_init(self):

        logger.info(f"Accessing {self.in_ds_name} in {self.in_file}")
        try:
            self.in_ds = daisy.open_ds(self.in_file, self.in_ds_name)
        except Exception as e:
            logger.info(f"EXCEPTION: {e}")
            exit(1)

        voxel_size = self.in_ds.voxel_size

        if self.in_ds.n_channel_dims == 0:
            num_channels = None
        elif self.in_ds.n_channel_dims == 1:
            num_channels = self.in_ds.shape[0]
        else:
            raise RuntimeError("more than one channel not yet implemented, sorry...")

        self.ds_roi = self.in_ds.roi

        sub_roi = None
        if self.roi_offset is not None or self.roi_shape is not None:
            assert self.roi_offset is not None and self.roi_shape is not None
            self.schedule_roi = daisy.Roi(tuple(self.roi_offset), tuple(self.roi_shape))
            sub_roi = self.schedule_roi
        else:
            self.schedule_roi = self.in_ds.roi

        if self.chunk_shape_voxel is None:
            self.chunk_shape_voxel = calculateNearIsotropicDimensions(
                voxel_size, self.max_voxel_count
            )
            logger.info(voxel_size)
            logger.info(self.chunk_shape_voxel)
        self.chunk_shape_voxel = Coordinate(self.chunk_shape_voxel)

        self.schedule_roi = self.schedule_roi.snap_to_grid(voxel_size, mode="grow")
        out_ds_roi = self.ds_roi.snap_to_grid(voxel_size, mode="grow")

        self.write_size = self.chunk_shape_voxel * voxel_size

        scheduling_block_size = self.write_size
        self.write_roi = daisy.Roi((0, 0, 0), scheduling_block_size)

        if sub_roi is not None:
            # with sub_roi, the coordinates are absolute
            # so we'd need to align total_roi to the write size too
            self.schedule_roi = self.schedule_roi.snap_to_grid(
                self.write_size, mode="grow"
            )
            out_ds_roi = out_ds_roi.snap_to_grid(self.write_size, mode="grow")

        logger.info(f"out_ds_roi: {out_ds_roi}")
        logger.info(f"schedule_roi: {self.schedule_roi}")
        logger.info(f"write_size: {self.write_size}")
        logger.info(f"voxel_size: {voxel_size}")

        if self.out_file is None:
            self.out_file = ".".join(self.in_file.split(".")[0:-1]) + ".zarr"
        if self.out_ds_name is None:
            self.out_ds_name = self.in_ds_name

        delete = self.overwrite == 2

        self.out_ds = daisy.prepare_ds(
            self.out_file,
            self.out_ds_name,
            total_roi=out_ds_roi,
            voxel_size=voxel_size,
            write_size=self.write_size,
            dtype=self.in_ds.dtype,
            num_channels=num_channels,
            force_exact_write_size=True,
            compressor={"id": "blosc", "clevel": 3},
            delete=delete,
        )

    def prepare_task(self):

        assert len(self.chunk_shape_voxel) == 3

        logger.info(
            "Rechunking %s/%s to %s/%s with chunk_shape_voxel %s (write_size %s, scheduling %s)"
            % (
                self.in_file,
                self.in_ds_name,
                self.out_file,
                self.out_ds_name,
                self.chunk_shape_voxel,
                self.write_size,
                self.write_roi,
            )
        )
        logger.info("ROI: %s" % self.schedule_roi)

        worker_filename = os.path.realpath(__file__)
        self._write_config(worker_filename, extra_config=None)

        return self._prepare_task(
            total_roi=self.schedule_roi,
            read_roi=self.write_roi,
            write_roi=self.write_roi,
            check_fn=lambda b: self.check_fn(b),
        )

    def _worker_impl(self, block):
        """Worker function implementation"""
        self.out_ds[block.write_roi] = self.in_ds[block.write_roi]

    def check_fn(self, block):

        write_roi = self.out_ds.roi.intersect(block.write_roi)
        if write_roi.empty:
            return True

        return super()._default_check_fn(block)


if __name__ == "__main__":

    if len(sys.argv) > 1 and sys.argv[1] == "run_worker":
        task = HDF2ZarrTask(config_file=sys.argv[2])
        task.run_worker()

    else:
        ap = argparse.ArgumentParser(description="Create a zarr/N5 container from hdf.")
        ap.add_argument("in_file", type=str, help="The input container")
        ap.add_argument("in_ds_name", type=str, help="The name of the dataset")
        ap.add_argument(
            "--out_file",
            type=str,
            default=None,
            help="The output container, defaults to be the same as in_file+.zarr",
        )
        ap.add_argument(
            "--out_ds_name",
            type=str,
            default=None,
            help="The name of the dataset, defaults to be in_ds_name",
        )
        ap.add_argument(
            "--chunk_shape_voxel",
            type=int,
            help="The size of a chunk in voxels",
            nargs="+",
            default=None,
        )
        ap.add_argument(
            "--max_voxel_count",
            type=int,
            default=256 * 1024,
            help="If chunk_shape_voxel is not given, use this value to calculate"
            "a near isotropic chunk shape",
        )
        ap.add_argument("--roi_offset", type=int, help="", nargs="+", default=None)
        ap.add_argument("--roi_shape", type=int, help="", nargs="+", default=None)

        config = HDF2ZarrTask.parse_args(ap)
        task = HDF2ZarrTask(config)
        daisy_task = task.prepare_task()
        done = daisy.run_blockwise([daisy_task])
        if done:
            logger.info("Ran all blocks successfully!")
        else:
            logger.info("Did not run all blocks successfully...")
