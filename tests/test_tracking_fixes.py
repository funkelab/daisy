"""Tracking-store fixes from the 1.x vs 2.0 evaluation review."""

import pytest

import daisy
import daisy.logging as dl


def _task(fn, name="t", n=100, **kw):
    kw.setdefault("max_workers", 1)
    kw.setdefault("timeout", 30)
    return daisy.Task(
        name,
        daisy.Roi([0], [n]),
        daisy.Roi([0], [10]),
        daisy.Roi([0], [10]),
        fn,
        read_write_conflict=False,
        **kw,
    )


def _noop(block):
    return None


def _fail_on_two(block):
    if block.block_id[1] in (3, 7):
        raise RuntimeError("boom")


def test_resource_tracking_without_a_path_falls_back_to_the_log_dir(tmp_path):
    dl.set_log_basedir(tmp_path / "logs")
    task = _task(_noop, resource_tracking=True)
    with pytest.warns(UserWarning, match="run-scoped"):
        assert daisy.run_blockwise([task], progress=False)
    stores = list((tmp_path / "logs" / "t").glob("tracking-*"))
    assert len(stores) == 1
    assert (stores[0] / "wall_seconds").exists()


def test_resource_tracking_without_any_basedir_still_errors(tmp_path):
    dl.set_log_basedir(None)
    task = _task(_noop, resource_tracking=True)
    with pytest.raises(RuntimeError, match="resource_tracking"):
        daisy.run_blockwise([task], progress=False)


def test_failed_blocks_come_back_from_the_store(tmp_path):
    store = tmp_path / "track"
    task = _task(_fail_on_two, tracking_path=str(store), max_retries=0, max_worker_restarts=20)
    assert daisy.run_blockwise([task], progress=False) is False
    failed = sorted(b.block_id[1] for b in task.failed_blocks())
    assert failed == [3, 7]
    # a task without a store has nothing to report
    assert _task(_noop).failed_blocks() == []
