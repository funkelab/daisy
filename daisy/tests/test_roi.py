import daisy
import unittest

daisy.scheduler._NO_SPAWN_STATUS_THREAD = True


class TestRoi(unittest.TestCase):

    def test_shape(self):

        r = daisy.Roi((0,), (1,))
        assert r.size() == 1
        assert r.empty() is False
        assert r.unbounded() is False

        r = daisy.Roi((0,), (0,))
        assert r.size() == 0
        assert r.empty() is True
        assert r.unbounded() is False

        # unbounded ROI
        r = daisy.Roi((0,), (None,))
        assert r.size() is None
        assert r.empty() is False
        assert r.unbounded() is True
        assert r.get_offset() == (None,)
        assert r.get_end() == (None,)
        assert r.get_shape() == (None,)
        r.set_offset((1,))
        assert r.get_offset() == (None,)
        assert r.get_end() == (None,)
        assert r.get_shape() == (None,)

        # turn into bounded ROI without offset
        r.set_shape((3,))
        assert r.get_offset() == (None,)
        assert r.get_end() == (None,)
        assert r.get_shape() == (3,)

        # turn into regular ROI
        r.set_offset((1,))
        assert r.get_offset() == (1,)
        assert r.get_end() == (4,)
        assert r.get_shape() == (3,)
        assert r.size() == 3

        # turn back into unbounded ROI
        r.set_shape(None)
        assert r.dims() == 1
        assert r.get_offset() == (None,)
        assert r.get_end() == (None,)
        assert r.get_shape() == (None,)
        assert r.size() is None

    def test_operators(self):

        a = daisy.Roi((0, 0, 0), (100, 100, 100))
        b = daisy.Roi((50, 50, 50), (100, 100, 100))

        assert a != b
        assert a.intersects(b)
        assert b.intersects(a)
        assert a.intersects(daisy.Roi((None,)*3, (None,)*3))
        assert a.intersects(daisy.Roi((0, 0, 0), (1, 1, 1)))
        assert not a.intersects(daisy.Roi((0, 0, 0), (0, 0, 0)))
        assert a.intersect(daisy.Roi((100, 100, 100), (1, 1, 1))).empty()

        assert a.get_center() == (50, 50, 50)
        assert b.get_center() == (100, 100, 100)

        assert a.intersect(b) == daisy.Roi((50, 50, 50), (50, 50, 50))
        assert b.intersect(a) == daisy.Roi((50, 50, 50), (50, 50, 50))
        assert a.union(b) == daisy.Roi((0, 0, 0), (150, 150, 150))
        assert b.union(a) == daisy.Roi((0, 0, 0), (150, 150, 150))

        c = daisy.Roi((25, 25, 25), (50, 50, 50))

        assert a.contains(c)
        assert a.contains(c.get_center())
        assert not b.contains(c)

        a = daisy.Roi((0, None, 0), (100, None, 100))
        b = daisy.Roi((50, 50, 50), (100, 100, 100))

        assert a.intersect(b) == daisy.Roi((50, 50, 50), (50, 100, 50))
        assert b.intersect(a) == daisy.Roi((50, 50, 50), (50, 100, 50))
        assert a.union(b) == daisy.Roi((0, None, 0), (150, None, 150))
        assert b.union(a) == daisy.Roi((0, None, 0), (150, None, 150))

        c = daisy.Roi((25, 25, 25), (50, 50, 50))

        assert a.contains(c)
        assert not b.contains(c)
        assert a.contains(daisy.Roi((0, 0, 0), (0, 0, 0)))
        assert not b.contains(daisy.Roi((0, 0, 0), (0, 0, 0)))
        assert not a.contains(daisy.Roi((None,)*3, (None,)*3))

        assert a.grow((1, 1, 1), (1, 1, 1)) == \
            daisy.Roi((-1, None, -1), (102, None, 102))
        assert a.grow((-1, -1, -1), (-1, -1, -1)) == \
            daisy.Roi((1, None, 1), (98, None, 98))
        assert a.grow((-1, -1, -1), None) == \
            daisy.Roi((1, None, 1), (99, None, 99))
        assert a.grow(None, (-1, -1, -1)) == \
            daisy.Roi((0, None, 0), (99, None, 99))

    def test_snap(self):

        a = daisy.Roi((1,), (7,))

        assert a.snap_to_grid((2,), 'grow') == daisy.Roi((0,), (8,))
        assert a.snap_to_grid((2,), 'shrink') == daisy.Roi((2,), (6,))
        assert a.snap_to_grid((2,), 'closest') == daisy.Roi((0,), (8,))

        assert a.snap_to_grid((3,), 'grow') == daisy.Roi((0,), (9,))
        assert a.snap_to_grid((3,), 'shrink') == daisy.Roi((3,), (3,))
        assert a.snap_to_grid((3,), 'closest') == daisy.Roi((0,), (9,))

        a = daisy.Roi((1, None), (7, None))
        assert a.snap_to_grid((2, 1), 'grow') == \
            daisy.Roi((0, None), (8, None))

        with self.assertRaises(RuntimeError):
            a.snap_to_grid((3, 1), 'doesntexist')

    def test_arithmetic(self):

        a = daisy.Roi((1, None), (7, None))

        assert a + (1, 1) == daisy.Roi((2, None), (7, None))
        assert a - (1, 1) == daisy.Roi((0, None), (7, None))
        assert a*2 == daisy.Roi((2, None), (14, None))
        assert a/2 == daisy.Roi((0, None), (3, None))
        assert a//2 == daisy.Roi((0, None), (3, None))
