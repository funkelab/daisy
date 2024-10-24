import pytest

from daisy.logging import set_log_basedir


@pytest.fixture(autouse=True)
def logdir(tmp_path):
    set_log_basedir(tmp_path / "daisy_logs")
