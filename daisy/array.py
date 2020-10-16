from __future__ import absolute_import
from .coordinate import Coordinate
from .freezable import Freezable
from .roi import Roi
import numpy as np


class Array(Freezable):
    '''A ROI and voxel size annotated ndarray-like. Acts as a view into actual
    data.

    Args:

        data (``ndarray``-like):

            The data to hold. Can be a numpy, HDF5, zarr, etc. array like.
            Needs to have ``shape`` and slicing support for reading/writing. It
            is assumed that slicing returns an ``ndarray``.

        roi (`class:Roi`):

            The region of interest (ROI) represented by this array.

        voxel_size (`class:Coordinate`):

            The size of a voxel.

        data_offset (`class:Coordinate`, optional):

            The start of ``data``, in world units. Defaults to
            ``roi.get_begin()``, if not given.

        chunk_shape (`class:Coordinate`, optional):

            The size of a chunk of the underlying data container in voxels.

        check_write_chunk_align (``bool``, optional):

            If true, assert that each write to this array is aligned with the
            chunks of the underlying array-like.
    '''

    def __init__(
            self,
            data,
            roi,
            voxel_size,
            data_offset=None,
            chunk_shape=None,
            check_write_chunk_align=False):

        self.data = data
        self.roi = roi
        self.voxel_size = Coordinate(voxel_size)
        self.chunk_shape = Coordinate(chunk_shape) if chunk_shape else None
        self.n_channel_dims = len(data.shape) - roi.dims()
        self.check_write_chunk_align = check_write_chunk_align

        assert self.voxel_size.dims() == self.roi.dims(), (
            "dimension of voxel_size (%d) does not match dimension of roi (%d)"
            % (self.voxel_size.dims(), self.roi.dims()))

        if data_offset is None:
            data_offset = roi.get_begin()
        else:
            data_offset = Coordinate(data_offset)

        self.data_roi = Roi(
            data_offset,
            self.voxel_size*self.data.shape[self.n_channel_dims:])

        assert self.roi.get_begin().is_multiple_of(voxel_size), (
            "roi offset %s is not a multiple of voxel size %s" % (
                self.roi.get_begin(), voxel_size))

        assert self.roi.get_shape().is_multiple_of(voxel_size), (
            "roi shape %s is not a multiple of voxel size %s" % (
                self.roi.get_shape(), voxel_size))

        assert data_offset.is_multiple_of(voxel_size), (
            "data offset %s is not a multiple of voxel size %s" % (
                data_offset, voxel_size))

        assert self.data_roi.contains(roi), (
            "data ROI %s does not contain given ROI %s" % (
                self.data_roi, roi))

        self.freeze()

    @property
    def shape(self):
        '''Get the shape in voxels of this array, possibly including channel
        dimensions. This is equivalent to::

            array.to_ndarray().shape()

        but does not actually create the ``ndarray``.
        '''

        view_shape = (self.roi/self.voxel_size).get_shape()
        return self.data.shape[:self.n_channel_dims] + tuple(view_shape)

    @property
    def dtype(self):
        '''Get the dtype of this array.'''
        return self.data.dtype

    def __getitem__(self, key):
        '''Get a sub-array or a single value.

        Args:

            key (`class:Roi` or `class:Coordinate`):

                The ROI specifying the sub-array or a coordinate for a single
                value.

        Returns:

            If ``key`` is a `class:Roi`, returns a `class:Array` that
            represents this ROI. This is a light-weight operation that does not
            access the actual data held by this array. If ``key`` is a
            `class:Coordinate`, the array value (possible multi-channel)
            closest to the coordinate is returned.
        '''

        if isinstance(key, Roi):

            roi = key

            assert self.roi.contains(roi), (
                "Requested roi %s is not contained in this array %s." % (
                    roi,
                    self.roi))

            return Array(
                self.data,
                roi,
                self.voxel_size,
                self.data_roi.get_begin())

        elif isinstance(key, Coordinate):

            coordinate = key

            assert self.roi.contains(coordinate), (
                "Requested coordinate (%s) is not contained "
                "in this array (%s)." % (coordinate, self.roi))

            return self.data[self.__index(coordinate)]

    def __setitem__(self, roi, value):
        '''Set the data of this array within the given ROI.

        Args:

            roi (`class:Roi`):

                The ROI to write to.

            value (`class:Array`, or broadcastable to ``ndarray``):

                The value to write. If an `class:Array`, the ROIs do not have
                to match, however, the shape of ``value`` has to be
                broadcastable to the voxel shape of ``roi``.
        '''

        assert isinstance(roi, Roi), (
            "Roi expected, but got %s" % (type(roi)))

        assert roi.get_begin().is_multiple_of(self.voxel_size), (
            "roi offset %s is not a multiple of voxel size %s" % (
                roi.get_begin(), self.voxel_size))

        assert roi.get_shape().is_multiple_of(self.voxel_size), (
            "roi shape %s is not a multiple of voxel size %s" % (
                roi.get_shape(), self.voxel_size))

        target = self.data
        target_slices = self.__slices(
            roi,
            check_chunk_align=self.check_write_chunk_align)

        if not hasattr(value, '__getitem__'):

            target[target_slices] = value
            return

        if isinstance(value, Array):

            array = value
            source = array.data
            source_slices = array.__slices(array.roi)

        else:

            source = value
            source_slices = slice(None)

        target[target_slices] = source[source_slices]

    def materialize(self):
        '''Copy the data represented by this array to memory. This is
        equivalent to::

            array = Array(array.to_ndarray(), array.roi, array.voxel_size)

        but modifies this array directly.
        '''

        self.data = self.to_ndarray()
        self.data_roi = self.roi.copy()

    def to_ndarray(self, roi=None, fill_value=None):
        '''Copy the data represented by this array into an ``ndarray``.

        Args:

            roi (`class:Roi`, optional):

                If given, copy only the data represented by this ROI. This is
                equivalent to::

                    array[roi].to_ndarray()

            fill_value (scalar, optional):

                If given, allow ``roi`` to be outside of this array's ROI.
                Outside values will be filled with ``fill_value``.
        '''

        if roi is None:
            return self.data[self.__slices(self.roi)]

        if fill_value is None:
            return self[roi].to_ndarray()

        shape = (roi/self.voxel_size).get_shape()
        data = np.zeros(
            self.data.shape[:self.n_channel_dims] + tuple(shape),
            dtype=self.data.dtype)
        if fill_value != 0:
            data[:] = fill_value

        array = Array(data, roi, self.voxel_size)

        shared_roi = self.roi.intersect(roi)

        if not shared_roi.empty():
            array[shared_roi] = self[shared_roi]

        return data

    def intersect(self, roi):
        '''Get a sub-array obtained by intersecting this array with the given
        ROI. This is equivalent to::

            array[array.roi.intersect(roi)]

        Args:

            roi (`class:Roi`):

                The ROI to intersect with.
        '''

        intersection = self.roi.intersect(roi)
        return self[intersection]

    def __slices(self, roi, check_chunk_align=False):
        '''Get the voxel slices for the given roi.'''

        voxel_roi = (roi - self.data_roi.get_begin())/self.voxel_size

        if check_chunk_align:

            for d in range(roi.dims()):

                end_of_array = roi.get_end()[d] == self.roi.get_end()[d]

                begin_align_with_chunks = (
                    voxel_roi.get_begin()[d] % self.chunk_shape[d] == 0)
                shape_align_with_chunks = (
                    voxel_roi.get_shape()[d] % self.chunk_shape[d] == 0)

                assert begin_align_with_chunks and (
                    shape_align_with_chunks or
                    end_of_array), (
                        "ROI %s (in voxels: %s) does not align with chunks of "
                        "size %s (mismatch in dimension %d)"
                        % (roi, voxel_roi, self.chunk_shape, d))

        return (slice(None),)*self.n_channel_dims + voxel_roi.to_slices()

    def __index(self, coordinate):
        '''Get the voxel slices for the given coordinate.'''

        index = (coordinate - self.data_roi.get_begin())/self.voxel_size
        if self.n_channel_dims > 0:
            index = (Ellipsis,) + tuple(index)
        return index
