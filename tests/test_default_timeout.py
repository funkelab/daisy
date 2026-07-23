"""Every block always has a timeout (default 600s) — a wedged block function
must never be able to hang a run, or its shutdown, forever."""

import os
import time

import pytest

import daisy


def _task(fn, **kw):
    kw.setdefault("total_roi", daisy.Roi([0], [40]))
    kw.setdefault("read_roi", daisy.Roi([0], [10]))
    kw.setdefault("write_roi", daisy.Roi([0], [10]))
    kw.setdefault("read_write_conflict", False)
    kw.setdefault("max_workers", 1)
    return daisy.Task(process_function=fn, **kw)


def test_default_timeout_applied():
    assert _task(lambda b: None, task_id="d1").timeout_secs == 600.0
    assert _task(lambda b: None, task_id="d2", timeout=None).timeout_secs == 600.0
    assert daisy._daisy.DEFAULT_BLOCK_TIMEOUT_SECS == 600.0


def test_explicit_timeout_respected():
    assert _task(lambda b: None, task_id="e1", timeout=5).timeout_secs == 5.0
    big = _task(lambda b: None, task_id="e2", timeout=86400.0)
    assert big.timeout_secs == 86400.0


@pytest.mark.parametrize("bad", [0, -1, -0.5])
def test_nonpositive_timeout_rejected(bad):
    with pytest.raises(ValueError, match="must be positive"):
        _task(lambda b: None, task_id="bad", timeout=bad)


def test_timeout_attribution_in_summary_and_state(capfd):
    """A run failing on timeouts reports the reclaim count and points at
    Task(timeout=...)."""

    def slow(block):
        time.sleep(2)

    task = _task(slow, task_id="slowpoke", timeout=0.5, max_retries=1)
    states = daisy.run_blockwise([task], return_states=True)
    st = states["slowpoke"]
    assert st.failed_count > 0
    assert st.timeout_reclaim_count > 0
    assert st.timeout_secs == 0.5
    out = capfd.readouterr().out
    assert "exceeded the block timeout" in out
    assert "0.5s" in out
    assert "Task(timeout=...)" in out
    assert "the default" not in out  # explicit value, not the default


def test_zombie_regression_hung_block_cleaned_up(tmp_path):
    """The zombie-audit scenario: a block that hangs 'forever' with a short
    timeout. The run must terminate on its own and leave NO worker process
    behind."""
    pids = tmp_path / "pids"
    pids.mkdir()

    def hang(block):
        open(pids / str(os.getpid()), "w").close()
        time.sleep(30)

    task = _task(hang, task_id="hangs", timeout=1, max_retries=0,
                 max_workers=2)
    t0 = time.monotonic()
    ok = daisy.run_blockwise([task], progress=False)
    wall = time.monotonic() - t0
    assert ok is False
    assert wall < 10, f"run should self-terminate quickly, took {wall:.1f}s"
    time.sleep(0.5)
    survivors = []
    for f in pids.iterdir():
        pid = int(f.name)
        try:
            os.kill(pid, 0)
            survivors.append(pid)
        except ProcessLookupError:
            pass
    for pid in survivors:  # cleanup before asserting
        os.kill(pid, 9)
    assert not survivors, f"worker processes outlived the run: {survivors}"
