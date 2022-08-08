from .conftest import MONGO_AVAILABLE

import daisy

import pytest


@pytest.mark.parametrize(
    "storage",
    [
        "file",
        pytest.param(
            "mongodb",
            marks=pytest.mark.skipif(
                not MONGO_AVAILABLE, reason="Mongodb not available"
            ),
        ),
    ],
)
def test_graph_read_meta_values(provider_factory):
    roi = daisy.Roi((0, 0, 0), (10, 10, 10))
    provider_factory("w", True, roi)
    graph_provider = provider_factory("r", None, None)
    assert True == graph_provider.directed
    assert roi == graph_provider.total_roi


@pytest.mark.parametrize(
    "storage",
    [
        "file",
        pytest.param(
            "mongodb",
            marks=pytest.mark.skipif(
                not MONGO_AVAILABLE, reason="Mongodb not available"
            ),
        ),
    ],
)
def test_graph_default_meta_values(provider_factory):
    provider = provider_factory("w", None, None)
    assert False == provider.directed
    assert provider.total_roi is None
    graph_provider = provider_factory("r", None, None)
    assert False == graph_provider.directed
    assert graph_provider.total_roi is None


@pytest.mark.parametrize(
    "storage",
    [
        "file",
        pytest.param(
            "mongodb",
            marks=pytest.mark.skipif(
                not MONGO_AVAILABLE, reason="Mongodb not available"
            ),
        ),
    ],
)
def test_graph_nonmatching_meta_values(provider_factory):
    roi = daisy.Roi((0, 0, 0), (10, 10, 10))
    roi2 = daisy.Roi((1, 0, 0), (10, 10, 10))
    provider_factory("w", True, None)
    with pytest.raises(ValueError):
        provider_factory("r", False, None)
        provider_factory("w", None, roi)
    with pytest.raises(ValueError):
        provider_factory("r", None, roi2)
