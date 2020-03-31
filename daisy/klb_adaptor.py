from daisy.coordinate import Coordinate
from daisy.ext import pyklb
from daisy.roi import Roi
import glob
import json
import numpy as np
import os
import logging

logger = logging.getLogger(__name__)


class KlbAdaptor():

    def __init__(self, filename, attr_filename=None):

        self.files = glob.glob(filename)
        self.files.sort()

        if len(self.files) == 0:
            raise IOError("no KLB files found that match %s" % filename)

        header = pyklb.readheader(self.files[0])
        self.dtype = header['datatype']
        resolution_tzyx = np.delete(header['pixelspacing_tczyx'], 1)
        blocksize_tzyx = np.delete(header['blocksize_tczyx'], 1)
        imagesize_tzyx = np.delete(header['imagesize_tczyx'], 1)
        self.voxel_size = Coordinate(resolution_tzyx)
        self.chunk_shape = Coordinate(blocksize_tzyx)
        self.shape = Coordinate(imagesize_tzyx)
        offset = Coordinate((0,)*len(self.shape))
        logger.debug("Found voxel size %s from header" % str(self.voxel_size))
        logger.debug("Found chunk_shape %s from header"
                     % str(self.chunk_shape))
        logger.debug("Found shape %s from header" % str(self.shape))

        if attr_filename is not None:
            logger.debug("Using attributes from file %s" % attr_filename)
            attributes_file = os.path.join(
                os.path.split(filename)[0],
                attr_filename)

            if not os.path.isfile(attributes_file):
                raise IOError(
                    "no attributes file %s found next to %s"
                    % (attr_filename, filename))

            with open(attributes_file, 'r') as f:

                attributes = json.load(f)
                if 'resolution' in attributes:
                    self.voxel_size = Coordinate(attributes['resolution'])
                    logger.debug("Overwriting voxel size with %s "
                                 % str(self.voxel_size))
                if 'shape' in attributes:
                    self.shape = Coordinate(attributes['shape'])
                    logger.debug("Overwriting shape with %s "
                                 % str(self.shape))
                if 'offset' in attributes:
                    offset = Coordinate(attributes['offset'])
                    logger.debug("Setting offset to %s "
                                 % str(offset))

        self.roi = Roi(
            offset,
            self.shape*self.voxel_size)
        logger.debug("Using ROI %s" % str(self.roi))

    def __getitem__(self, slices):

        if len(self.files) == 1:

            return self.__read_file(self.files[0], slices)

        else:

            file_indices = range(
                slices[0].start,
                slices[0].stop)

            slices = slices[1:]

            return np.array([
                    self.__read_file(self.files[i], slices)
                    for i in file_indices
                ])

    def __read_file(self, filename, slices):

        # pyklb reads max-inclusive, slices are max exclusive ->
        # subtract (1, 1, ...) from max coordinate
        return pyklb.readroi(
            filename,
            tuple(s.start for s in slices),
            tuple(s.stop - 1 for s in slices))
