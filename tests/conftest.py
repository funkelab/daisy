from daisy.logging import set_log_basedir
import daisy

import pytest
import pymongo


@pytest.fixture(autouse=True)
def daisy_test_logging(tmpdir):
    return set_log_basedir(tmpdir)


@pytest.fixture()
def provider_factory(storage, tmpdir):
    # provides a factory function to generate graph provider
    # can provide either mongodb graph provider or file graph provider
    # if file graph provider, will generate graph in a temporary directory
    # to avoid artifacts

    def mongo_provider_factory(mode, directed=None, total_roi=None):
        return daisy.persistence.MongoDbGraphProvider(
            "test_daisy_graph", mode=mode, directed=directed, total_roi=total_roi
        )

    def file_provider_factory(mode, directed=None, total_roi=None):
        return daisy.persistence.FileGraphProvider(
            tmpdir / "test_daisy_graph",
            chunk_size=(10, 10, 10),
            mode=mode,
            directed=directed,
            total_roi=total_roi,
        )

    if storage == "mongodb":
        return mongo_provider_factory
    else:
        return file_provider_factory


try:
    client = pymongo.MongoClient(connectTimeoutMS=1, serverSelectionTimeoutMS=1)
    client.server_info()
    MONGO_AVAILABLE = True
except pymongo.errors.ServerSelectionTimeoutError as e:
    MONGO_AVAILABLE = False
