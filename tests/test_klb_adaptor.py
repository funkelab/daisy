import daisy
from daisy.ext import pyklb, NoSuchModule

import numpy as np
import pytest

import json


@pytest.fixture()
def klb_files(tmpdir):
    klb_file, attrs_file = tmpdir / "test.klb", tmpdir / "attributes.json"
    data = np.arange(1e3).reshape(1, 1, 10, 10, 10)
    pyklb.writefull(data, klb_file, numthreads=1, pixelspacing_tczyx=[1, 1, 2, 1, 1])

    attrs = {"resolution": [1, 1, 2, 2], "offset": [0, 100, 10, 0]}
    with attrs_file.open("w") as f:
        json.dump(attrs, f)

    return klb_file, attrs_file


@pytest.mark.skipIf(isinstance(pyklb, NoSuchModule), "pyklb is not installed")
def test_load_klb(klb_files):
    klb_file, attrs_file = klb_files
    data = daisy.open_ds(klb_file, None)
    assert data.roi.get_offset() == daisy.Coordinate((0, 0, 0, 0))
    assert data.roi.get_shape() == daisy.Coordinate((1, 20, 10, 10))
    assert data.voxel_size == daisy.Coordinate((1, 2, 1, 1))


@pytest.mark.skipIf(isinstance(pyklb, NoSuchModule), "pyklb is not installed")
def test_overwrite_attrs(klb_files):
    klb_file, attrs_file = klb_files
    data = daisy.open_ds(klb_file, None, attr_filename=attrs_file)
    assert data.roi.get_offset() == daisy.Coordinate((0, 100, 10, 0))
    assert data.roi.get_shape() == daisy.Coordinate((1, 10, 20, 20))
    assert data.voxel_size == daisy.Coordinate((1, 1, 2, 2))
