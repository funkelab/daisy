from datetime import datetime
from tempfile import mkdtemp
from warnings import warn
import os
import shutil
import unittest


class TmpDirTestCase(unittest.TestCase):
    """
    Usage:

    If your test case dumps out any files, use ``self.path_to("path", "to",
    "my.file")`` to get the path to a directory in your temporary directory.
    This will be namespaced by the test class, timestamp and test method, e.g.

    >>> self.path_to("path", "to", "my.file")
    /tmp/daisy_MyTestCase_2018-03-08T18:32:18.967927_r4nd0m/my_test_method/path/to/my.file

    Each test method's data will be deleted after the test case is run
    (regardless of pass, fail or error). To disable test method data deletion,
    set ``self._cleanup = False`` anywhere in the test.

    The test case directory will be deleted after every test method is run,
    unless there is data left in it. Any files written directly to the class
    output directory (rather than the test output subdirectory) should
    therefore be explicitly removed before tearDownClass is called. To disable
    data deletion for the whole class (the test case directory and all tests),
    set ``_cleanup = False`` in the class definition. N.B. doing this in a
    method (``type(self)._cleanup = False``) will have unexpected results
    depending on the order of test execution.

    Subclasses implementing their own setUp, setUpClass, tearDown and
    tearDownClass should explicitly call the ``super`` method in the method
    definition.
    """

    _output_root = ""
    _cleanup = True

    def path_to(self, *args):
        return type(self).path_to_cls(self._testMethodName, *args)

    @classmethod
    def path_to_cls(cls, *args):
        return os.path.join(cls._output_root, *args)

    @classmethod
    def setUpClass(cls):
        timestamp = datetime.now().isoformat()
        cls._output_root = mkdtemp(
            prefix="daisy_{}_{}_".format(cls.__name__, timestamp)
        )

    def setUp(self):
        os.mkdir(self.path_to())

    def tearDown(self):
        path = self.path_to()
        try:
            if self._cleanup:
                shutil.rmtree(path)
            else:
                warn("Directory {} was not deleted".format(path))
        except OSError as e:
            if "[Errno 2]" in str(e):
                pass
            else:
                raise

    @classmethod
    def tearDownClass(cls):
        try:
            if cls._cleanup:
                os.rmdir(cls.path_to_cls())
            else:
                warn("Directory {} was not deleted".format(cls.path_to_cls()))
        except OSError as e:
            if "[Errno 39]" in str(e):
                warn(
                    "Directory {} could not be deleted as it still had data "
                    "in it".format(cls.path_to_cls())
                )
            elif "[Errno 2]" in str(e):
                pass
            else:
                raise
