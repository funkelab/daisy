"""A downstream task that reads beyond its upstream's write region can
never finish (its outer blocks have nothing to wait on); daisy must refuse
it up front instead of waiting forever."""

import pytest

import daisy


def _noop(block):
    return None


def _chain(extend):
    up = daisy.Task(
        "up",
        daisy.Roi([0], [200]),
        daisy.Roi([0], [10]),
        daisy.Roi([0], [10]),
        _noop,
        read_write_conflict=False,
        max_workers=1,
        timeout=30,
    )
    down = daisy.Task(
        "down",
        daisy.Roi([0], [200 + extend]),
        daisy.Roi([0], [10]),
        daisy.Roi([0], [10]),
        _noop,
        read_write_conflict=False,
        max_workers=1,
        timeout=30,
    )
    return up + down


def test_uncovered_downstream_is_refused_before_running():
    with pytest.raises(Exception, match="would wait forever"):
        daisy.run_blockwise(_chain(100), multiprocessing=False)
    with pytest.raises(Exception, match="would wait forever"):
        daisy.run_blockwise(_chain(100), progress=False)
    with pytest.raises(ValueError, match="would wait forever"):
        daisy.Scheduler(_chain(100))


def test_covered_downstream_still_runs():
    assert daisy.run_blockwise(_chain(0), multiprocessing=False)
