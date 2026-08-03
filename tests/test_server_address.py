"""What address the scheduler tells workers to dial.

A run has two addresses that are easy to conflate: the interfaces the
scheduler listens on, and the address workers are told to connect to. daisy
once used one value for both — it bound loopback and advertised `127.0.0.1` —
which works on one machine and cannot work across nodes. Every test here is
about keeping those two roles distinct.
"""

import socket

import pytest

import daisy


def _task(task_id, fn, n_blocks=2):
    return daisy.Task(
        task_id=task_id,
        total_roi=daisy.Roi([0], [n_blocks * 10]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=fn,
        read_write_conflict=False,
        max_workers=1,
    )


def _reporting_worker(out):
    """A 0-arg worker that records the address it was told to dial."""

    def worker():
        from pathlib import Path as _Path

        client = daisy.Client()
        _Path(out, "dialed").write_text(
            f"{client.context['hostname']}:{client.context['port']}"
        )
        while True:
            with client.acquire_block() as block:
                if block is None:
                    return

    return worker


def _routable_address():
    """This host's address on the default-route interface, or None if it has
    only loopback (CI containers, sandboxes).

    Deliberately the routing-table probe rather than resolving
    `gethostname()`: on Debian/Ubuntu the hostname resolves to `127.0.1.1`, so
    a name-based check reports "loopback only" on a machine that has a
    perfectly good interface — and would skip these tests into vacuity. daisy's
    own detection tries the name first and falls through to this for the same
    reason.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # TEST-NET-1; connecting a UDP socket sends nothing, it only asks the
        # kernel which source address it would use.
        sock.connect(("192.0.2.1", 80))
        ip = sock.getsockname()[0]
    except OSError:
        return None
    finally:
        sock.close()
    return None if ip.startswith("127.") else ip


@pytest.mark.timeout(60)
def test_workers_are_told_a_routable_address(tmp_path):
    """By default the advertised address must be one another machine could
    use, and a worker must actually reach it."""
    if _routable_address() is None:
        pytest.skip("host has only loopback")
    out = tmp_path / "seen"
    out.mkdir()

    assert daisy.run_blockwise(
        _task("addr-default", _reporting_worker(str(out))), progress=False
    )
    dialed = (out / "dialed").read_text()
    host = dialed.rsplit(":", 1)[0]
    assert not host.startswith("127."), (
        f"workers were told {host}; nothing off this machine can connect there"
    )


@pytest.mark.timeout(60)
def test_host_argument_overrides_detection(tmp_path):
    """`host=` is for what detection cannot know: a specific fabric on a
    multi-homed node, a container's external name, a DNS name only the compute
    nodes resolve. It is taken verbatim."""
    out = tmp_path / "seen"
    out.mkdir()

    assert daisy.run_blockwise(
        _task("addr-explicit", _reporting_worker(str(out))),
        progress=False,
        host="127.0.0.1",
    )
    assert (out / "dialed").read_text().startswith("127.0.0.1:")


@pytest.mark.timeout(60)
def test_daisy_host_env_var_is_honoured(tmp_path, monkeypatch):
    """The same override, for stacks that don't thread the argument through —
    a pipeline library between the user and daisy usually doesn't, and an
    operator can still set this in a submit script."""
    out = tmp_path / "seen"
    out.mkdir()
    monkeypatch.setenv("DAISY_HOST", "127.0.0.1")

    assert daisy.run_blockwise(
        _task("addr-env", _reporting_worker(str(out))), progress=False
    )
    assert (out / "dialed").read_text().startswith("127.0.0.1:")


@pytest.mark.timeout(60)
def test_explicit_argument_beats_the_env_var(tmp_path, monkeypatch):
    """An argument at the call site is more specific than the environment."""
    explicit = _routable_address()
    if explicit is None:
        pytest.skip("host has only loopback")
    out = tmp_path / "seen"
    out.mkdir()
    monkeypatch.setenv("DAISY_HOST", "127.0.0.1")

    assert daisy.run_blockwise(
        _task("addr-both", _reporting_worker(str(out))),
        progress=False,
        host=explicit,
    )
    assert (out / "dialed").read_text().startswith(f"{explicit}:")


@pytest.mark.timeout(60)
def test_server_class_takes_the_same_argument(tmp_path):
    """`Server.run_blockwise` dropped `host` entirely — the parameter existed
    on the Rust entry point and no caller passed it."""
    out = tmp_path / "seen"
    out.mkdir()

    states = daisy.Server().run_blockwise(
        _task("addr-server", _reporting_worker(str(out))),
        progress=False,
        host="127.0.0.1",
    )
    assert states["addr-server"].completed_count == 2
    assert (out / "dialed").read_text().startswith("127.0.0.1:")
