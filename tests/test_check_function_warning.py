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
