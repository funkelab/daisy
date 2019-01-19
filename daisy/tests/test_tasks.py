import daisy
from .tmpdir_test import TmpDirTestCase


class TestBlockwiseBasics(TmpDirTestCase):

    def test_parameters(self):

        class TestTask(daisy.Task):
            a = daisy.Parameter(default=True)
            b = daisy.Parameter(default=False)
            c = daisy.Parameter()

        with self.assertRaises(RuntimeError):
            t = TestTask()

        with self.assertRaises(RuntimeError):
            t = TestTask(d=42)

        t = TestTask(c=42)

        assert t.a is True
        assert t.b is False
        assert t.c == 42

        t = TestTask(global_config={'TestTask': {'a': 23, 'b': 42, 'c': 3.14}})

        assert t.a == 23
        assert t.b == 42
        assert t.c == 3.14
