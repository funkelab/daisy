import daisy
import unittest

daisy.scheduler._NO_SPAWN_STATUS_THREAD = True


class TestCoordinate(unittest.TestCase):

    def test_constructor(self):

        # construct from tuples, lists, and generators

        daisy.Coordinate((0,))
        daisy.Coordinate((0, 1))
        daisy.Coordinate((0, 1, 2))
        daisy.Coordinate((0, 1, 2, 3, 4, 5, 6))
        daisy.Coordinate([0, 1, 2, 3, 4, 5, 6])
        daisy.Coordinate(range(100))

        # convert to integer

        c = daisy.Coordinate((0.1, 0.5, 1.0, 2.0))
        assert c == (0, 0, 1, 2)

    def test_arithmetic(self):

        a = daisy.Coordinate((1, 2, 3))
        b = daisy.Coordinate((4, 5, 6))
        c = daisy.Coordinate((7, 8))

        assert a + b == (5, 7, 9)
        assert a - b == (-3, -3, -3)
        assert b - a == (3, 3, 3)
        assert -a == (-1, -2, -3)
        assert abs(a) == a
        assert abs(-a) == a
        assert a*b == (4, 10, 18)
        assert a/b == (0, 0, 0)
        assert a//b == (0, 0, 0)
        assert b/a == (4, 2, 2)
        assert b//a == (4, 2, 2)

        with self.assertRaises(TypeError):
            a + 1
        with self.assertRaises(TypeError):
            a - 3
        assert a*10 == (10, 20, 30)
        assert b/2 == (2, 2, 3)
        assert b//2 == (2, 2, 3)

        with self.assertRaises(TypeError):
            a + "invalid"
        with self.assertRaises(TypeError):
            a - "invalid"
        with self.assertRaises(TypeError):
            a*"invalid"
        with self.assertRaises(TypeError):
            a/"invalid"
        with self.assertRaises(TypeError):
            a//"invalid"
        with self.assertRaises(AssertionError):
            a + c
        with self.assertRaises(AssertionError):
            a - c
        with self.assertRaises(AssertionError):
            a*c
        with self.assertRaises(AssertionError):
            a/c
        with self.assertRaises(AssertionError):
            a//c

    def test_none(self):

        a = daisy.Coordinate((None, 1, 2))
        b = daisy.Coordinate((3, 4, None))

        assert a + b == (None, 5, None)
        assert a - b == (None, -3, None)
        assert a/b == (None, 0, None)
        assert a//b == (None, 0, None)
        assert b/a == (None, 4, None)
        assert b//a == (None, 4, None)
        assert abs(a) == (None, 1, 2)
        assert abs(-a) == (None, 1, 2)
