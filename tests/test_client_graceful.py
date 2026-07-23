"""Graceful `Client()` when the server is already gone.

A worker that starts after the run finished (a normal straggler race on
batch clusters) must not register as a failure: construction doesn't
raise, `connected` is False, and `acquire_block()` yields the standard
end-of-work signal so the canonical loop exits cleanly. Configuration
errors (missing/malformed DAISY_CONTEXT) still raise.
"""

import os
import socket
import subprocess
import sys

import pytest

import daisy


def _dead_port() -> int:
    """A port that was just freed — connecting to it is refused."""
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _stale_context(port: int) -> daisy.Context:
    return daisy.Context(
        hostname="127.0.0.1",
        port=str(port),
        task_id="gone",
        worker_id="0",
    )


def test_client_survives_dead_server(caplog):
    import logging

    with caplog.at_level(logging.WARNING):
        client = daisy.Client(context=_stale_context(_dead_port()))
    assert client.connected is False
    assert any("not reachable" in r.message for r in caplog.records)

    # canonical worker loop exits cleanly via the end-of-work signal
    blocks_seen = 0
    while True:
        with client.acquire_block() as block:
            if block is None:
                break
            blocks_seen += 1
    assert blocks_seen == 0


def test_straggler_worker_process_exits_zero():
    """A whole worker process launched with a stale DAISY_CONTEXT exits 0."""
    env = dict(os.environ)
    env["DAISY_CONTEXT"] = _stale_context(_dead_port()).to_env()
    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            "import daisy\n"
            "client = daisy.Client()\n"
            "while True:\n"
            "    with client.acquire_block() as block:\n"
            "        if block is None:\n"
            "            break\n",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 0, proc.stderr


def test_missing_context_still_raises():
    env_backup = os.environ.pop("DAISY_CONTEXT", None)
    try:
        with pytest.raises(KeyError, match="DAISY_CONTEXT"):
            daisy.Client()
    finally:
        if env_backup is not None:
            os.environ["DAISY_CONTEXT"] = env_backup


def test_malformed_context_still_raises():
    env_backup = os.environ.get("DAISY_CONTEXT")
    os.environ["DAISY_CONTEXT"] = "this-is-not-a-context"
    try:
        with pytest.raises(ValueError, match="invalid context token"):
            daisy.Client()
    finally:
        if env_backup is None:
            del os.environ["DAISY_CONTEXT"]
        else:
            os.environ["DAISY_CONTEXT"] = env_backup
