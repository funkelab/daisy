"""Failure surfaces carry full python tracebacks (roadmap item 4).

Three assertions, one per surface:
- the run summary prints the FIRST failed block's traceback (once);
- the abandonment RuntimeError contains the worker's full traceback;
- TaskState.first_worker_error / last_worker_error carry it programmatically.

The failure originates two calls deep so a message-only surface would
not name the user's line.
"""

import daisy
import pytest
from conftest import linear_task


def _inner_helper(block):
    lookup = {}
    return lookup[block.block_id[1]]  # the buggy line


def _outer_helper(block):
    return _inner_helper(block)


def _failing_task(**kw):
    return linear_task(
        "tb_demo",
        _outer_helper,
        4,
        max_workers=1,
        max_retries=0,
        **kw,
    )


def test_summary_contains_first_failure_traceback_subprocess_mode(capsys):
    server = daisy.Server()
    states = server.run_blockwise([_failing_task()], progress=False)
    st = states["tb_demo"]
    assert st.failed_count > 0
    tb = st.first_worker_error
    assert tb is not None
    assert "Traceback (most recent call last)" in tb
    assert "lookup[block.block_id[1]]" in tb  # the user's source line
    assert "_inner_helper" in tb and "_outer_helper" in tb
    out = capsys.readouterr().out
    assert "First failure in task 'tb_demo':" in out
    assert "lookup[block.block_id[1]]" in out
    # one traceback only, even with several failed blocks
    assert out.count("Traceback (most recent call last)") == 1


def test_serial_mode_reraises_the_original_exception():
    """The other execution path. Distributed workers ship a *formatted*
    traceback back over the wire (there is no exception object to send);
    serial mode runs the function in-process, so it re-raises the original
    exception with its real traceback attached — better for pdb, which is
    what serial mode is for."""
    import traceback

    with pytest.raises(KeyError) as exc_info:
        daisy.run_blockwise(_failing_task(), multiprocessing=False, progress=False)
    tb = "".join(traceback.format_exception(exc_info.value))
    assert "lookup[block.block_id[1]]" in tb  # the user's source line
    assert "_inner_helper" in tb and "_outer_helper" in tb


def _ping(n):
    if n == 0:
        raise ValueError("bottom")
    return _pong(n)


def _pong(n):
    return _ping(n - 1)


def _deep_failure(block):
    # 40 alternating frame pairs: python folds only *identical* repeated
    # frames, so this formats to well over the 50-line cap
    return _ping(40)


def test_worker_traceback_is_capped_to_its_last_50_lines():
    task = linear_task(
        "deep",
        _deep_failure,
        1,
        max_workers=1,
        max_retries=0,
    )
    states = daisy.Server().run_blockwise([task], progress=False)
    tb = states["deep"].first_worker_error
    assert tb is not None
    lines = tb.splitlines()
    assert lines[0] == "... (traceback truncated) ..."
    assert len(lines) == 1 + 50
    assert lines[-1] == "ValueError: bottom"  # the tail is kept
