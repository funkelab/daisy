from __future__ import absolute_import
from .coordinate import Coordinate
from .freezable import Freezable
from .roi import Roi
import numpy as np

class Array(Freezable):
    '''An annotated ndarray-like.

    Args:

        data (``ndarray``-like):

            The data to hold. Can be a numpy, HDF5, zarr, etc. array like.

        roi (`class:Roi`):

            The region of interest (ROI) represented by this array.

        voxel_size (`class:Coordinate`):

            The size of a voxel.

        data_offset (`class:Coordinate`, optional):

            The start of ``data``, in world units. Defaults to
            ``roi.get_begin()``. Setting this to a different value allows
            defining views into ``data``.
    '''

    def __init__(self, data, roi, voxel_size, data_offset=None):

        self.data = data
        self.roi = roi
        self.voxel_size = voxel_size
        self.n_channel_dims = len(data.shape) - roi.dims()
        if data_offset is None:
            data_offset = roi.get_begin()
        self.data_roi = Roi(
            data_offset,
            self.voxel_size*self.data.shape[self.n_channel_dims:])

        assert self.data_roi.contains(roi), (
            "data ROI %s does not contain given ROI %s"%(
            self.data_roi, roi))

        for d in range(roi.dims()):

            assert self.roi.get_begin()[d]%voxel_size[d] == 0, (
                "roi offset %d in dim %d is not a multiple of voxel size %d"%(
                self.roi.get_begin()[d], d, voxel_size[d]))

            assert self.roi.get_shape()[d]%voxel_size[d] == 0, (
                "roi shape %d in dim %d is not a multiple of voxel size %d"%(
                self.roi.get_shape()[d], d, voxel_size[d]))

            assert data_offset[d]%voxel_size[d] == 0, (
                "data offset %d in dim %d is not a multiple of voxel size %d"%(
                data_offset[d], d, voxel_size[d]))

        self.freeze()

    @property
    def shape(self):
        '''Get the shape of this array, possibly including channel dimensions.'''
        return self.data.shape

    def __getitem__(self, key):
        '''Get a sub-array or a single value.

        Args:

            key (`class:Roi` or `class:Coordinate`):

                The ROI specifying the sub-array or a coordinate for a single
                value.
        '''

        if isinstance(key, Roi):

            roi = key

            assert self.roi.contains(roi), (
                "Requested roi is not contained in this array.")

            data = self.data[self.__slices(roi)]

            return Array(data, roi, self.voxel_size)

        elif isinstance(key, Coordinate):

            coordinate = key

            assert self.roi.contains(coordinate), (
                "Requested coordinate is not contained in this array.")

            return self.data[self.__index(coordinate)]

    def __setitem__(self, roi, array):
        '''Set the data of this array to the values of another array within
        roi.

        Args:

            roi (`class:Roi`):

                The ROI to write to.

            array (`class:Array`):

                The array to read from. The ROIs do not have to match, however,
                the shape of ``array`` has to be broadcastable to the voxel
                shape of ``roi``.
        '''

        assert isinstance(roi, Roi), (
            "Roi expected, but got %s"%(type(roi)))

        assert (roi/self.voxel_size).get_shape() == array.shape[-roi.dims():]

        self.data[self.__slices(roi)] = array.data

    def fill(self, roi, fill_value=0):
        '''Get an `class:Array`, filled with the values of this array where
        available. Other values will be set to ``fill_value``.

        Args:

            roi (`class:Roi`):

                The ROI of the array to fill.

            fill_value (``dtype`` of this array):

                The value to use to fill out-of-bounds requests.
        '''

        shape = (roi/self.voxel_size).get_shape()
        data = np.zeros(
            self.data.shape[:self.n_channel_dims] + shape,
            dtype=self.data.dtype)
        if fill_value != 0:
            data[:] = fill_value

        array = Array(data, roi, self.voxel_size)

        shared_roi = self.roi.intersect(roi)

        if not shared_roi.empty():
            array[shared_roi] = self[shared_roi]

        return array

    def intersect(self, roi):
        '''Get a sub-array obtained by intersecting this array with the given
        ROI.

        Args:

            roi (`class:Roi`):

                The ROI to intersect with.
        '''

        intersection = self.roi.intersect(roi)
        return self[intersection]

    def __slices(self, roi):
        '''Get the voxel slices for the given roi.'''

        voxel_roi = (roi - self.data_roi.get_begin())/self.voxel_size
        return (slice(None),)*self.n_channel_dims + voxel_roi.to_slices()

    def __index(self, coordinate):
        '''Get the voxel slices for the given coordinate.'''

        index = (coordinate - self.data_roi.get_begin())/self.voxel_size
        if self.n_channel_dims > 0:
            index = (Ellipsis,) + index
        return index
