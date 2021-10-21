import daisy
import numpy as np
import unittest


class TestArray(unittest.TestCase):

    def test_constructor(self):

        data = np.zeros((10, 10, 10), dtype=np.float32)
        roi = daisy.Roi((0, 0, 0), (10, 10, 10))

        # consistent configurations
        daisy.Array(data, roi, (1, 1, 1))
        daisy.Array(data, roi, (1, 1, 2))
        daisy.Array(data, roi, (1, 5, 2))
        daisy.Array(data, roi, (10, 5, 2))
        roi = daisy.Roi((1, 1, 1), (10, 10, 10))
        daisy.Array(data, roi, (1, 1, 1), data_offset=(1, 1, 1))
        roi = daisy.Roi((0, 0, 0), (20, 20, 20))
        daisy.Array(data, roi, (2, 2, 2))

        # dims don't match
        with self.assertRaises(AssertionError):
            daisy.Array(data, roi, (1, 1))

        # ROI not multiple of voxel size
        with self.assertRaises(AssertionError):
            daisy.Array(data, roi, (1, 1, 3))
        with self.assertRaises(AssertionError):
            daisy.Array(data, roi, (1, 1, 4))

        # ROI begin doesn't align with voxel size
        roi = daisy.Roi((1, 1, 1), (11, 11, 11))
        with self.assertRaises(AssertionError):
            daisy.Array(data, roi, (1, 1, 2))

        # ROI shape doesn't align with voxel size
        roi = daisy.Roi((0, 0, 0), (11, 11, 11))
        with self.assertRaises(AssertionError):
            daisy.Array(data, roi, (1, 1, 2))

        # ROI outside of provided data
        roi = daisy.Roi((0, 0, 0), (20, 20, 20))
        with self.assertRaises(AssertionError):
            daisy.Array(data, roi, (1, 1, 1))
        with self.assertRaises(AssertionError):
            daisy.Array(data, roi, (2, 2, 1))
        with self.assertRaises(AssertionError):
            daisy.Array(data, roi, (2, 2, 2), data_offset=(0, 0, 2))

    def test_shape(self):

        # ROI fits data

        a1 = daisy.Array(
            np.zeros((10,)),
            daisy.Roi((0,), (10,)),
            (1,))
        a2 = daisy.Array(
            np.zeros((10, 10)),
            daisy.Roi((0, 0), (10, 10)),
            (1, 1))
        a2_3 = daisy.Array(
            np.zeros((3, 10, 10)),
            daisy.Roi((0, 0), (10, 10)),
            (1, 1))
        a5_3_2_1 = daisy.Array(
            np.zeros((1, 2, 3, 4, 4, 4, 4, 4)),
            daisy.Roi((0, 0, 0, 0, 0), (80, 80, 80, 80, 80)),
            (20, 20, 20, 20, 20))

        assert a1.shape == (10,)
        assert a2.shape == (10, 10)
        assert a2_3.shape == (3, 10, 10)
        assert a2_3.roi.dims == 2
        assert a5_3_2_1.shape == (1, 2, 3, 4, 4, 4, 4, 4)

        # ROI subset of data

        a1 = daisy.Array(
            np.zeros((20,)),
            daisy.Roi((0,), (10,)),
            (1,))
        a2 = daisy.Array(
            np.zeros((20, 20)),
            daisy.Roi((0, 0), (10, 10)),
            (1, 1))
        a2_3 = daisy.Array(
            np.zeros((3, 20, 20)),
            daisy.Roi((0, 0), (10, 10)),
            (1, 1))
        a5_3_2_1 = daisy.Array(
            np.zeros((1, 2, 3, 5, 5, 5, 5, 5)),
            daisy.Roi((0, 0, 0, 0, 0), (80, 80, 80, 80, 80)),
            (20, 20, 20, 20, 20))

        assert a1.shape == (10,)
        assert a2.shape == (10, 10)
        assert a2_3.shape == (3, 10, 10)
        assert a2_3.roi.dims == 2
        assert a5_3_2_1.shape == (1, 2, 3, 4, 4, 4, 4, 4)

    def test_dtype(self):

        for dtype in [np.float32, np.uint8, np.uint64]:
            assert daisy.Array(
                np.zeros((1,), dtype=dtype),
                daisy.Roi((0,), (1,)),
                (1,)).dtype == dtype

    def test_getitem(self):

        a = daisy.Array(
            np.arange(0, 10).reshape(2, 5),
            daisy.Roi((0, 0), (2, 5)),
            (1, 1))

        assert a[daisy.Coordinate((0, 0))] == 0
        assert a[daisy.Coordinate((0, 1))] == 1
        assert a[daisy.Coordinate((0, 2))] == 2
        assert a[daisy.Coordinate((1, 0))] == 5
        assert a[daisy.Coordinate((1, 1))] == 6
        with self.assertRaises(AssertionError):
            a[daisy.Coordinate((1, 5))]
        with self.assertRaises(AssertionError):
            a[daisy.Coordinate((2, 5))]
        with self.assertRaises(AssertionError):
            a[daisy.Coordinate((-1, 0))]
        with self.assertRaises(AssertionError):
            a[daisy.Coordinate((0, -1))]

        b = a[daisy.Roi((1, 1), (1, 4))]
        with self.assertRaises(AssertionError):
            b[daisy.Coordinate((0, 0))] == 0
        with self.assertRaises(AssertionError):
            b[daisy.Coordinate((0, 1))] == 1
        with self.assertRaises(AssertionError):
            b[daisy.Coordinate((0, 2))] == 2
        with self.assertRaises(AssertionError):
            b[daisy.Coordinate((1, 0))] == 5
        assert b[daisy.Coordinate((1, 1))] == 6
        assert b[daisy.Coordinate((1, 2))] == 7
        assert b[daisy.Coordinate((1, 3))] == 8
        assert b[daisy.Coordinate((1, 4))] == 9
        with self.assertRaises(AssertionError):
            b[daisy.Coordinate((1, 5))]
        with self.assertRaises(AssertionError):
            b[daisy.Coordinate((2, 5))]
        with self.assertRaises(AssertionError):
            b[daisy.Coordinate((-1, 0))]
        with self.assertRaises(AssertionError):
            b[daisy.Coordinate((0, -1))]

    def test_setitem(self):

        # set entirely with numpy array

        a = daisy.Array(
            np.zeros((2, 5)),
            daisy.Roi((0, 0), (2, 5)),
            (1, 1))

        a[daisy.Roi((0, 0), (2, 5))] = np.arange(0, 10).reshape(2, 5)
        assert a[daisy.Coordinate((0, 0))] == 0
        assert a[daisy.Coordinate((0, 1))] == 1
        assert a[daisy.Coordinate((0, 2))] == 2
        assert a[daisy.Coordinate((1, 0))] == 5
        assert a[daisy.Coordinate((1, 1))] == 6
        assert a[daisy.Coordinate((1, 4))] == 9

        # set entirely with numpy array and channels

        a = daisy.Array(
            np.zeros((3, 2, 5)),
            daisy.Roi((0, 0), (2, 5)),
            (1, 1))

        a[daisy.Roi((0, 0), (2, 5))] = np.arange(0, 3*10).reshape(3, 2, 5)
        np.testing.assert_array_equal(a[daisy.Coordinate((0, 0))], [0, 10, 20])
        np.testing.assert_array_equal(a[daisy.Coordinate((0, 1))], [1, 11, 21])
        np.testing.assert_array_equal(a[daisy.Coordinate((1, 4))], [9, 19, 29])

        # set entirely with scalar

        a = daisy.Array(
            np.zeros((2, 5)),
            daisy.Roi((0, 0), (2, 5)),
            (1, 1))

        a[daisy.Roi((0, 0), (2, 5))] = 42
        assert a[daisy.Coordinate((0, 0))] == 42
        assert a[daisy.Coordinate((1, 4))] == 42

        # set partially with scalar and channels

        a = daisy.Array(
            np.arange(0, 3*10).reshape(3, 2, 5),
            daisy.Roi((0, 0), (2, 5)),
            (1, 1))

        a[daisy.Roi((0, 0), (2, 2))] = 42
        np.testing.assert_array_equal(
            a[daisy.Coordinate((0, 0))], [42, 42, 42])
        np.testing.assert_array_equal(
            a[daisy.Coordinate((0, 1))], [42, 42, 42])
        np.testing.assert_array_equal(
            a[daisy.Coordinate((0, 2))], [2, 12, 22])
        np.testing.assert_array_equal(
            a[daisy.Coordinate((1, 2))], [7, 17, 27])
        np.testing.assert_array_equal(
            a[daisy.Coordinate((1, 3))], [8, 18, 28])
        np.testing.assert_array_equal(
            a[daisy.Coordinate((1, 4))], [9, 19, 29])

        # set partially with Array

        a = daisy.Array(
            np.zeros((2, 5)),
            daisy.Roi((0, 0), (2, 5)),
            (1, 1))
        b = daisy.Array(
            np.arange(0, 10).reshape(2, 5),
            daisy.Roi((0, 0), (2, 5)),
            (1, 1))

        a[daisy.Roi((0, 0), (1, 5))] = b[daisy.Roi((0, 0), (1, 5))]
        assert a[daisy.Coordinate((0, 0))] == 0
        assert a[daisy.Coordinate((0, 1))] == 1
        assert a[daisy.Coordinate((0, 2))] == 2
        assert a[daisy.Coordinate((1, 0))] == 0
        assert a[daisy.Coordinate((1, 1))] == 0
        assert a[daisy.Coordinate((1, 4))] == 0

        a = daisy.Array(
            np.zeros((2, 5)),
            daisy.Roi((0, 0), (2, 5)),
            (1, 1))
        b = daisy.Array(
            np.arange(0, 10).reshape(2, 5),
            daisy.Roi((0, 0), (2, 5)),
            (1, 1))

        a[daisy.Roi((0, 0), (1, 5))] = b[daisy.Roi((1, 0), (1, 5))]
        assert a[daisy.Coordinate((0, 0))] == 5
        assert a[daisy.Coordinate((0, 1))] == 6
        assert a[daisy.Coordinate((0, 4))] == 9
        assert a[daisy.Coordinate((1, 0))] == 0
        assert a[daisy.Coordinate((1, 1))] == 0
        assert a[daisy.Coordinate((1, 2))] == 0

        a[daisy.Roi((1, 0), (1, 5))] = b[daisy.Roi((0, 0), (1, 5))]
        assert a[daisy.Coordinate((0, 0))] == 5
        assert a[daisy.Coordinate((0, 1))] == 6
        assert a[daisy.Coordinate((0, 4))] == 9
        assert a[daisy.Coordinate((1, 0))] == 0
        assert a[daisy.Coordinate((1, 1))] == 1
        assert a[daisy.Coordinate((1, 2))] == 2

    def test_materialize(self):

        a = daisy.Array(
            np.arange(0, 10).reshape(2, 5),
            daisy.Roi((0, 0), (2, 5)),
            (1, 1))

        b = a[daisy.Roi((0, 0), (2, 2))]

        # underlying data did not change
        assert a.data.shape == b.data.shape

        assert b.shape == (2, 2)
        b.materialize()
        assert b.shape == (2, 2)

        assert b.data.shape == (2, 2)

    def test_to_ndarray(self):

        a = daisy.Array(
            np.arange(0, 10).reshape(2, 5),
            daisy.Roi((0, 0), (2, 5)),
            (1, 1))

        # not within ROI of a and no fill value provided
        with self.assertRaises(AssertionError):
            a.to_ndarray(daisy.Roi((0, 0), (5, 5)))

        b = a.to_ndarray(daisy.Roi((0, 0), (1, 5)))
        compare = np.array([[0, 1, 2, 3, 4]])

        b = a.to_ndarray(daisy.Roi((1, 0), (1, 5)))
        compare = np.array([[5, 6, 7, 8, 9]])

        b = a.to_ndarray(daisy.Roi((0, 0), (2, 2)))
        compare = np.array([[0, 1], [5, 6]])

        np.testing.assert_array_equal(b, compare)

        b = a.to_ndarray(daisy.Roi((0, 0), (5, 5)), fill_value=1)
        compare = np.array([
            [0, 1, 2, 3, 4],
            [5, 6, 7, 8, 9],
            [1, 1, 1, 1, 1],
            [1, 1, 1, 1, 1],
            [1, 1, 1, 1, 1]])

        np.testing.assert_array_equal(b, compare)

    def test_intersect(self):

        a = daisy.Array(
            np.arange(0, 10).reshape(2, 5),
            daisy.Roi((0, 0), (2, 5)),
            (1, 1))

        b = a.intersect(daisy.Roi((1, 1), (10, 10)))

        assert b.roi == daisy.Roi((1, 1), (1, 4))
        np.testing.assert_array_equal(b.to_ndarray(), [[6, 7, 8, 9]])
