from daisy.coordinate import Coordinate
from daisy.ext import pyklb
from daisy.roi import Roi
import glob
import json
import numpy as np
import os


class KlbAdaptor():

    def __init__(self, filename, attr_filename=None):

        self.files = glob.glob(filename)
        self.files.sort()

        if len(self.files) == 0:
            raise IOError("no KLB files found that match %s" % filename)

        if attr_filename is None:
            attr_filename = 'attributes.json'

        attributes_file = os.path.join(
            os.path.split(filename)[0],
            attr_filename)

        if not os.path.isfile(attributes_file):
            raise IOError(
                "no attributes file %s found next to %s"
                % (attr_filename, filename))

        with open(attributes_file, 'r') as f:

            attributes = json.load(f)
            self.voxel_size = Coordinate(attributes['resolution'])

            self.shape = Coordinate(attributes['shape'])
            offset = Coordinate(attributes['offset'])

            self.roi = Roi(
                offset,
                self.shape*self.voxel_size)

        header = pyklb.readheader(self.files[0])
        self.dtype = header['datatype']

        # TODO: get chunk shape from KLB headers
        self.chunk_shape = None

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
