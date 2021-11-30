from __future__ import absolute_import, division
from .array import Array
from .coordinate import Coordinate
from .ext import zarr, h5py
from .klb_adaptor import KlbAdaptor
from .roi import Roi
import json
import logging
import os
import shutil
import tempfile

logger = logging.getLogger(__name__)


# monkey patch to auto set temp file permission so other users can access our
# data, until zarr fixes this issue:
# https://github.com/zarr-developers/zarr/issues/325


def UmaskNamedTemporaryFile(*args, **kargs):
    fdesc = tempfile.NamedTemporaryFile2(*args, **kargs)
    umask = os.umask(0)
    os.umask(umask)
    os.chmod(fdesc.name, 0o666 & ~umask)
    return fdesc


tempfile.NamedTemporaryFile2 = tempfile.NamedTemporaryFile
tempfile.NamedTemporaryFile = UmaskNamedTemporaryFile


def _read_voxel_size_offset(ds, order='C'):

    voxel_size = None
    offset = None
    dims = None

    if 'resolution' in ds.attrs:

        voxel_size = tuple(ds.attrs['resolution'])
        dims = len(voxel_size)

    if 'offset' in ds.attrs:

        offset = tuple(ds.attrs['offset'])

        if dims is not None:
            assert dims == len(offset), (
                "resolution and offset attributes differ in length")
        else:
            dims = len(offset)

    if dims is None:
        dims = len(ds.shape)

    if voxel_size is None:
        voxel_size = (1,)*dims

    if offset is None:
        offset = (0,)*dims

    if order == 'F':
        offset = offset[::-1]
        voxel_size = voxel_size[::-1]

    return Coordinate(voxel_size), Coordinate(offset)


def open_ds(filename, ds_name, mode='r', attr_filename=None):
    '''Open a Zarr, N5, or HDF5 dataset as a :class:`daisy.Array`. If the
    dataset has attributes ``resolution`` and ``offset``, those will be
    used to determine the meta-information of the returned array.

    Args:

        filename (``string``):

            The name of the container "file" (which is a directory for Zarr and
            N5).

        ds_name (``string``):

            The name of the dataset to open.

        attr_filename (``string``):

            KLB only: the name of the attributes json file. Default is
            "attributes.json".

    Returns:

        A :class:`daisy.Array` pointing to the dataset.
    '''

    if filename.endswith('.zarr'):

        logger.debug("opening zarr dataset %s in %s", ds_name, filename)
        try:
            ds = zarr.open(filename, mode=mode)[ds_name]
        except Exception as e:
            logger.error("failed to open %s/%s" % (filename, ds_name))
            raise e

        voxel_size, offset = _read_voxel_size_offset(ds, ds.order)
        shape = Coordinate(ds.shape[-len(voxel_size):])
        roi = Roi(offset, voxel_size*shape)

        chunk_shape = ds.chunks

        logger.debug("opened zarr dataset %s in %s", ds_name, filename)
        return Array(ds, roi, voxel_size, chunk_shape=chunk_shape)

    elif filename.endswith('.n5'):

        logger.debug("opening N5 dataset %s in %s", ds_name, filename)
        ds = zarr.open(filename, mode=mode)[ds_name]

        voxel_size, offset = _read_voxel_size_offset(ds, 'F')
        shape = Coordinate(ds.shape[-len(voxel_size):])
        roi = Roi(offset, voxel_size*shape)

        chunk_shape = ds.chunks

        logger.debug("opened N5 dataset %s in %s", ds_name, filename)
        return Array(ds, roi, voxel_size, chunk_shape=chunk_shape)

    elif filename.endswith('.h5') or filename.endswith('.hdf'):

        logger.debug("opening H5 dataset %s in %s", ds_name, filename)
        ds = h5py.File(filename, mode=mode)[ds_name]

        voxel_size, offset = _read_voxel_size_offset(ds, 'C')
        shape = Coordinate(ds.shape[-len(voxel_size):])
        roi = Roi(offset, voxel_size*shape)

        chunk_shape = ds.chunks

        logger.debug("opened H5 dataset %s in %s", ds_name, filename)
        return Array(ds, roi, voxel_size, chunk_shape=chunk_shape)

    elif filename.endswith('.json'):

        logger.debug("found JSON container spec")
        with open(filename, 'r') as f:
            spec = json.load(f)

        array = open_ds(spec['container'], ds_name, mode)
        return Array(
            array.data,
            Roi(spec['offset'], spec['size']),
            array.voxel_size,
            array.roi.begin,
            chunk_shape=array.chunk_shape)

    elif filename.endswith('.klb'):

        logger.debug("opening KLB dataset %s", filename)
        adaptor = KlbAdaptor(filename, attr_filename=attr_filename)

        return Array(
            adaptor,
            adaptor.roi,
            adaptor.voxel_size,
            adaptor.roi.begin,
            chunk_shape=adaptor.chunk_shape)

    else:

        logger.error("don't know data format of %s in %s", ds_name, filename)
        raise RuntimeError("Unknown file format for %s" % filename)


def prepare_ds(
        filename,
        ds_name,
        total_roi,
        voxel_size,
        dtype,
        write_roi=None,
        write_size=None,
        num_channels=1,
        compressor='default',
        delete=False,
        force_exact_write_size=False):
    '''Prepare a Zarr or N5 dataset.

    Args:

        filename (``string``):

            The name of the container "file" (which is actually a directory).

        ds_name (``string``):

            The name of the dataset to prepare.

        total_roi (:class:`daisy.Roi`):

            The ROI of the dataset to prepare in world units.

        voxel_size (:class:`daisy.Coordinate`):

            The size of one voxel in the dataset in world units.

        write_size (:class:`daisy.Coordinate`):

            The size of anticipated writes to the dataset, in world units. The
            chunk size of the dataset will be set such that ``write_size`` is a
            multiple of it. This allows concurrent writes to the dataset if the
            writes are aligned with ``write_size``.

        num_channels (``int``, optional):

            The number of channels.

        compressor (``string``, optional):

            The compressor to use. See `zarr.get_codec` for available options.
            Defaults to gzip level 5.

        delete (``bool``, optional):

            Whether to delete an existing dataset if it was found to be
            incompatible with the other requirements. The default is not to
            delete the dataset and raise an exception instead.

        force_exact_write_size (``bool``, optional):

            Whether to use `write_size` as-is, or to first process it with
            `get_chunk_size`.

    Returns:

        A :class:`daisy.Array` pointing to the newly created dataset.
    '''

    voxel_size = Coordinate(voxel_size)
    if write_size is not None:
        write_size = Coordinate(write_size)

    assert total_roi.shape.is_multiple_of(voxel_size), (
        "The provided ROI shape is not a multiple of voxel_size")
    assert total_roi.begin.is_multiple_of(voxel_size), (
        "The provided ROI offset is not a multiple of voxel_size")

    if write_roi is not None:

        logger.warning(
            "write_roi is deprecated, please use write_size instead")

        if write_size is None:
            write_size = write_roi.shape

    if write_size is not None:
        assert write_size.is_multiple_of(voxel_size), (
            "The provided write ROI shape is not a multiple of voxel_size")

    if compressor == 'default':
        compressor = {'id': 'gzip', 'level': 5}

    ds_name = ds_name.lstrip('/')

    if filename.endswith('.h5') or filename.endswith('.hdf'):
        raise RuntimeError("prepare_ds does not support HDF5 files")
    elif filename.endswith('.zarr'):
        file_format = 'zarr'
    elif filename.endswith('.n5'):
        file_format = 'n5'
    else:
        raise RuntimeError("Unknown file format for %s" % filename)

    if write_size is not None:
        if not force_exact_write_size:
            chunk_shape = get_chunk_shape(write_size/voxel_size)
        else:
            chunk_shape = write_size/voxel_size
    else:
        chunk_shape = None

    shape = total_roi.shape/voxel_size

    if num_channels > 1:

        shape = (num_channels,) + shape

        if chunk_shape is not None:
            chunk_shape = Coordinate((num_channels,) + chunk_shape)
        voxel_size_with_channels = Coordinate((1,) + voxel_size)

    if not os.path.isdir(filename):

        logger.info("Creating new %s", filename)
        os.makedirs(filename)

        zarr.open(filename, mode='w')

    if not os.path.isdir(os.path.join(filename, ds_name)):

        logger.info("Creating new %s in %s", ds_name, filename)

        if compressor is not None:
            compressor = zarr.get_codec(compressor)

        root = zarr.open(filename, mode='a')
        ds = root.create_dataset(
            ds_name,
            shape=shape,
            chunks=chunk_shape,
            dtype=dtype,
            compressor=compressor)

        if file_format == 'zarr':
            ds.attrs['resolution'] = voxel_size
            ds.attrs['offset'] = total_roi.begin
        else:
            ds.attrs['resolution'] = voxel_size[::-1]
            ds.attrs['offset'] = total_roi.begin[::-1]

        if num_channels > 1:
            chunk_shape = chunk_shape/voxel_size_with_channels
        else:
            chunk_shape = chunk_shape/voxel_size
        return Array(
            ds,
            total_roi,
            voxel_size,
            chunk_shape=chunk_shape)

    else:

        logger.debug(
            "Trying to reuse existing dataset %s in %s...",
            ds_name,
            filename)
        ds = open_ds(filename, ds_name, mode='a')

        compatible = True

        if ds.shape != shape:
            logger.info("Shapes differ: %s vs %s", ds.shape, shape)
            compatible = False

        if ds.roi != total_roi:
            logger.info("ROIs differ: %s vs %s", ds.roi, total_roi)
            compatible = False

        if ds.voxel_size != voxel_size:
            logger.info(
                "Voxel sizes differ: %s vs %s",
                ds.voxel_size,
                voxel_size)
            compatible = False

        if write_size is not None and ds.data.chunks != chunk_shape:
            logger.info(
                "Chunk shapes differ: %s vs %s",
                ds.data.chunks,
                chunk_shape)
            compatible = False

        if dtype != ds.dtype:
            logger.info(
                "dtypes differ: %s vs %s",
                ds.dtype,
                dtype)
            compatible = False

        if not compatible:

            if not delete:
                raise RuntimeError(
                    "Existing dataset is not compatible, please manually "
                    "delete the volume at %s/%s" % (filename, ds_name))

            logger.info("Existing dataset is not compatible, creating new one")

            shutil.rmtree(os.path.join(filename, ds_name))
            return prepare_ds(
                filename=filename,
                ds_name=ds_name,
                total_roi=total_roi,
                voxel_size=voxel_size,
                dtype=dtype,
                write_size=write_size,
                num_channels=num_channels,
                compressor=compressor)

        else:

            logger.info("Reusing existing dataset")
            return ds


def get_chunk_shape(block_shape):
    '''Get a reasonable chunk size that divides the given block size.'''

    chunk_shape = Coordinate(
        get_chunk_size_dim(b, 256)
        for b in block_shape)

    logger.debug("Setting chunk size to %s", chunk_shape)

    return chunk_shape


def get_chunk_size_dim(b, target_chunk_size):

    best_k = None
    best_target_diff = 0

    for k in range(1, b+1):
        if ((b//k)*k) % b == 0:
            diff = abs(b//k - target_chunk_size)
            if best_k is None or diff < best_target_diff:
                best_target_diff = diff
                best_k = k

    return b//best_k
