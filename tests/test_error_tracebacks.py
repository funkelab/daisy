"""Failure surfaces carry full python tracebacks (roadmap item 4).

Three assertions, one per surface:
- the run summary prints the FIRST failed block's traceback (once);
- the abandonment RuntimeError contains the worker's full traceback;
- TaskState.first_worker_error / last_worker_error carry it programmatically.

The failure originates two calls deep so a message-only surface would
not name the user's line.
"""

import io

import pytest

import daisy


def _inner_helper(block):
    lookup = {}
    return lookup[block.block_id[1]]  # the buggy line


def _outer_helper(block):
    return _inner_helper(block)


def _failing_task(**kw):
    return daisy.Task(
        task_id="tb_demo",
        total_roi=daisy.Roi([0], [40]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=_outer_helper,
        read_write_conflict=False,
        max_workers=1,
        max_retries=0,
        **kw,
    )


def _summary_for(states):
    from daisy._progress import _print_execution_summary

    buf = io.StringIO()
    import daisy.logging as _wl

    saved = _wl._saved_stdout
    _wl._saved_stdout = buf
    try:
        _print_execution_summary(states, list(states))
    finally:
        _wl._saved_stdout = saved
    return buf.getvalue()


def test_summary_contains_first_failure_traceback_subprocess_mode():
    server = daisy.Server()
    states = server.run_blockwise([_failing_task()], progress=False)
    st = states["tb_demo"]
    assert st.failed_count > 0
    tb = st.first_worker_error
    assert tb is not None
    assert "Traceback (most recent call last)" in tb
    assert "lookup[block.block_id[1]]" in tb  # the user's source line
    assert "_inner_helper" in tb and "_outer_helper" in tb
    out = _summary_for(states)
    assert "First failure in task 'tb_demo':" in out
    assert "lookup[block.block_id[1]]" in out
    # one traceback only, even with several failed blocks
    assert out.count("Traceback (most recent call last)") == 1


def test_summary_traceback_thread_mode():
    server = daisy.Server()
    states = server.run_blockwise(
        [_failing_task(worker_processes=False)], progress=False
    )
    tb = states["tb_demo"].first_worker_error
    assert tb is not None
    assert "lookup[block.block_id[1]]" in tb


def _crashing_worker():
    raise RuntimeError("bad node: /nfs/models/seg_v4.pt missing")


def test_abandonment_error_contains_full_traceback():
    task = daisy.Task(
        task_id="crash_tb",
        total_roi=daisy.Roi([0], [40]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=_crashing_worker,  # 0-arg spawn fn
        read_write_conflict=False,
        max_workers=1,
        max_retries=0,
        max_worker_restarts=1,
    )
    with pytest.raises(RuntimeError) as exc_info:
        daisy.run_blockwise([task], progress=False)
    msg = str(exc_info.value)
    assert "crash_tb" in msg and "was abandoned" in msg
    assert "bad node: /nfs/models/seg_v4.pt missing" in msg


def test_traceback_capping(monkeypatch):
    import daisy._task as _task_mod

    # python compresses recursive frames, so force a small cap instead
    monkeypatch.setattr(_task_mod, "_TB_MAX_LINES", 5)

    def deep(n):
        if n == 0:
            raise ValueError("bottom")
        return deep(n - 1)

    try:
        deep(20)
    except ValueError:
        tb = _task_mod._capped_traceback()
    assert len(tb.splitlines()) <= 5 + 1
    assert tb.splitlines()[0] == "... (traceback truncated) ..."
    assert "bottom" in tb  # the tail is kept
