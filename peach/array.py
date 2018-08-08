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
    '''

    def __init__(self, data, roi, voxel_size):

        self.data = data
        self.roi = roi
        self.voxel_size = voxel_size
        self.n_channel_dims = len(data.shape) - roi.dims()

        assert (roi/voxel_size).get_shape() == data.shape[-roi.dims():], (
            "data shape %s does not fit to given roi %s and voxel_size %s"%(
                data.shape, roi, voxel_size))

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
            "Roi expected")

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

    def __slices(self, roi):
        '''Get the voxel slices for the given roi.'''

        voxel_roi = (roi - self.roi.get_begin())/self.voxel_size
        return (slice(None),)*self.n_channel_dims + voxel_roi.to_slices()

    def __index(self, coordinate):
        '''Get the voxel slices for the given coordinate.'''

        voxel_coordinate = (coordinate - self.roi.get_begin())/self.voxel_size
        spatial_slices = tuple(slice(c, c + 1) for c in voxel_coordinate)
        return (slice(None),)*self.n_channel_dims + spatial_slices
