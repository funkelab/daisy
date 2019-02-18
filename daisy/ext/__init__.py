from __future__ import print_function
import logging
import traceback

import sys

logger = logging.getLogger(__name__)


class NoSuchModule(object):

    def __init__(self, name):

        self.__name = name
        self.__traceback_str = traceback.format_tb(sys.exc_info()[2])
        errtype, value = sys.exc_info()[:2]
        self.__exception = errtype(value)

    def __getattr__(self, item):
        print(self.__traceback_str, file=sys.stderr)
        raise self.__exception


try:
    import h5py
except ImportError:
    h5py = NoSuchModule('h5py')

try:
    import zarr
except ImportError:
    zarr = NoSuchModule('zarr')

try:
    import pyklb
except ImportError:
    pyklb = NoSuchModule('pyklb')
