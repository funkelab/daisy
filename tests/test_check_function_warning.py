"""Task(check_function=...) should nudge users toward built-in done markers."""

import warnings

import daisy
import pytest


def _mk(**kw):
    return daisy.Task(
        task_id="warn_demo",
        total_roi=daisy.Roi([0], [20]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=lambda b: None,
        read_write_conflict=False,
        max_workers=1,
        **kw,
    )


def test_check_function_warns_and_points_to_done_markers():
    with pytest.warns(UserWarning, match="done markers"):
        _mk(check_function=lambda b: False)


def test_no_check_function_no_warning():
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        _mk()


def test_raising_check_function_is_logged_not_swallowed(caplog):
    """A raising check_function means "not done", so the block (re)runs —
    but that must not be silent: an always-raising check re-runs every done
    block on resume. The exception is logged as a WARNING on the `daisy`
    logger, once per task (not once per block)."""
    import logging

    def broken_check(block):
        raise RuntimeError("simulated broken check")

    calls = []
    task = daisy.Task(
        task_id="warn_demo",
        total_roi=daisy.Roi([0], [20]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=lambda b: calls.append(b.block_id),
        check_function=broken_check,
        read_write_conflict=False,
        max_workers=1,
    )
    with caplog.at_level(logging.WARNING, logger="daisy"):
        states = daisy.run_blockwise(
            task, multiprocessing=False, progress=False, return_states=True
        )
    # every exception is "not done": both blocks still process
    assert len(calls) == 2
    assert states["warn_demo"].completed_count == 2
    assert states["warn_demo"].skipped_count == 0
    logged = [
        r
        for r in caplog.records
        if "check_function" in r.getMessage() and "raised" in r.getMessage()
    ]
    assert len(logged) == 1, [r.getMessage() for r in logged]
    assert "simulated broken check" in logged[0].getMessage()
