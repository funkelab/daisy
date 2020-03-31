import daisy
import pyklb
import numpy as np
import unittest
import json
import os


class TestArray(unittest.TestCase):
    def write_test_klb(self):
        self.klb_file = 'test.klb'
        data = np.arange(1e3).reshape(1, 1, 10, 10, 10)
        pyklb.writefull(
                data,
                self.klb_file,
                numthreads=1,
                pixelspacing_tczyx=[1, 1, 2, 1, 1])

        self.attrs_file = 'attributes.json'
        attrs = {
                'resolution': [1, 1, 2, 2],
                'offset': [0, 100, 10, 0]
                }
        with open(self.attrs_file, 'w') as f:
            json.dump(attrs, f)

    def remove_test_klb(self):
        os.remove(self.klb_file)
        os.remove(self.attrs_file)
        self.klb_file = None
        self.attrs_file = None

    def test_load_klb(self):
        self.write_test_klb()
        data = daisy.open_ds(self.klb_file, None)
        self.assertEqual(data.roi.get_offset(),
                         daisy.Coordinate((0, 0, 0, 0)))
        self.assertEqual(data.roi.get_shape(),
                         daisy.Coordinate((1, 20, 10, 10)))
        self.assertEqual(data.voxel_size,
                         daisy.Coordinate((1, 2, 1, 1)))
        self.remove_test_klb()

    def test_overwrite_attrs(self):
        self.write_test_klb()
        data = daisy.open_ds(self.klb_file,
                             None,
                             attr_filename=self.attrs_file)
        self.assertEqual(data.roi.get_offset(),
                         daisy.Coordinate((0, 100, 10, 0)))
        self.assertEqual(data.roi.get_shape(),
                         daisy.Coordinate((1, 10, 20, 20)))
        self.assertEqual(data.voxel_size,
                         daisy.Coordinate((1, 1, 2, 2)))
        self.remove_test_klb()
