from __future__ import absolute_import
from .array import Array
from .coordinate import Coordinate
from .ext import zarr, z5py, h5py
from .roi import Roi
import logging
import os
import shutil

logger = logging.getLogger(__name__)

def open_ds(filename, ds_name, mode='r'):

    if filename.endswith('.zarr'):

        ds = zarr.open(filename, mode=mode)[ds_name]

        offset = Coordinate(ds.attrs['offset'])
        voxel_size = Coordinate(ds.attrs['resolution'])
        roi = Roi(offset, voxel_size*ds.shape[-len(voxel_size):])

        return Array(ds, roi, voxel_size)

    elif filename.endswith('.n5'):

        ds = z5py.File(filename, use_zarr_format=False)[ds_name]

        offset = Coordinate(ds.attrs['offset'][::-1])
        voxel_size = Coordinate(ds.attrs['resolution'][::-1])
        roi = Roi(offset, voxel_size*ds.shape[-len(voxel_size):])

        return Array(ds, roi, voxel_size)

    elif filename.endswith('.h5') or filename.endswith('.hdf'):

        ds = h5py.File(filename, mode=mode)[ds_name]

        offset = Coordinate(ds.attrs['offset'])
        voxel_size = Coordinate(ds.attrs['resolution'])
        roi = Roi(offset, voxel_size*ds.shape[-len(voxel_size):])

        return Array(ds, roi, voxel_size)

    else:

        raise RuntimeError("Unknown file format for %s"%filename)

def prepare_ds(filename, ds_name, total_roi, voxel_size, dtype, write_roi=None):

    ds_name = ds_name.lstrip('/')

    if filename.endswith('.h5') or filename.endswith('.hdf'):
        raise RuntimeError("prepare_ds does not support HDF5 files")
    elif filename.endswith('.zarr'):
        file_format = 'zarr'
    elif filename.endswith('.n5'):
        file_format = 'n5'
    else:
        raise RuntimeError("Unknown file format for %s"%filename)

    shape = total_roi.get_shape()/voxel_size

    if write_roi is not None:
        chunk_size = write_roi.get_shape()/voxel_size
    else:
        chunk_size = None

    if not os.path.isdir(filename):

        logger.info("Creating new %s"%filename)

        if file_format == 'zarr':
            zarr.open(filename, mode='w')
        else:
            z5py.File(filename)

    if not os.path.isdir(os.path.join(filename, ds_name)):

        logger.info("Creating new %s in %s"%(ds_name, filename))

        if file_format == 'zarr':
            root = zarr.open(filename, mode='r+')
            comp_arg = {
                'compressor': zarr.get_codec({'id': 'gzip', 'level': 5})
            }
        else:
            root = z5py.File(filename)
            comp_arg = {
                'compressor': 'gzip',
                'level': 5
            }

        ds = root.create_dataset(
            ds_name,
            shape=shape,
            chunks=chunk_size,
            dtype=dtype,
            **comp_arg)

        if file_format == 'zarr':
            ds.attrs['resolution'] = voxel_size
            ds.attrs['offset'] = total_roi.get_begin()
        else:
            ds.attrs['resolution'] = voxel_size[::-1]
            ds.attrs['offset'] = total_roi.get_begin()[::-1]

        return Array(ds, total_roi, voxel_size)

    else:

        logger.debug("Trying to reuse existing dataset %s in %s..."%(ds_name, filename))
        ds = open_ds(filename, ds_name, mode='r+')

        compatible = True

        if ds.shape != shape:
            logger.debug("Shapes differ: %s vs %s"%(ds.shape, shape))
            compatible = False

        if ds.roi != total_roi:
            logger.debug("ROIs differ: %s vs %s"%(ds.roi, total_roi))
            compatible = False

        if ds.voxel_size != voxel_size:
            logger.debug("Voxel sizes differ: %s vs %s"%(ds.voxel_size, voxel_size))
            compatible = False

        if write_roi is not None and ds.data.chunks != chunk_size:
            logger.debug("Chunk sizes differ: %s vs %s"%(ds.data.chunks, chunk_size))
            compatible = False

        if not compatible:

            logger.info("Existing dataset is not compatible, creating new one")

            shutil.rmtree(os.path.join(filename, ds_name))
            return prepare_ds(
                filename,
                ds_name,
                total_roi,
                voxel_size,
                dtype,
                write_roi)

        else:

            logger.info("Reusing existing dataset")
            return ds
