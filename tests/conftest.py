"""Suite-wide test fixtures.

The default log basedir is `./daisy_logs`, which is fine for
real runs but spams the repo root during testing. This module
redirects file logging to pytest's per-test `tmp_path` for the
duration of every test so the working directory stays clean and
log writes are scoped per-test.

Tests that specifically need to inspect log files can override the
basedir explicitly inside the test body — `set_log_basedir(...)`
is idempotent.
"""

import pytest

import daisy.logging as gl


@pytest.fixture(autouse=True)
def _isolate_daisy_logs(tmp_path, monkeypatch):
    """Redirect daisy's per-worker log basedir into the test's
    `tmp_path` and restore the previous value afterward."""
    original = gl.get_log_basedir()
    gl.set_log_basedir(tmp_path / "daisy_logs")
    try:
        yield
    finally:
        gl.set_log_basedir(original)
