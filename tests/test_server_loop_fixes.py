"""Server-loop fixes from the 1.x vs 2.0 evaluation review: a hard error
still shuts the run down, progress observers see skipped blocks, and the
final bar count includes skips."""

import os
import subprocess
import sys
import time

import daisy
import pytest


def _task(fn, name="t", n=40, **kw):
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


class _Recorder:
    def __init__(self):
        self.progress = []
        self.finish = None

    def on_start(self, states):
        pass

    def on_progress(self, states):
        self.progress.append(
            {k: (v.completed_count, v.skipped_count) for k, v in states.items()}
        )

    def on_finish(self, states):
        self.finish = {
            k: (v.completed_count, v.skipped_count) for k, v in states.items()
        }


def test_progress_observer_sees_skipped_blocks(tmp_path):
    task = _task(_noop, tracking_path=str(tmp_path / "track"))
    assert daisy.run_blockwise([task], progress=False)
    rec = _Recorder()
    task2 = _task(_noop, tracking_path=str(tmp_path / "track"))
    assert daisy.run_blockwise([task2], progress=rec)
    assert rec.finish["t"] == (4, 4)
    assert rec.progress and rec.progress[-1]["t"] == (4, 4)


def test_tqdm_bar_reaches_total_on_an_all_skipped_run(tmp_path, capsys):
    from daisy._progress import _TqdmObserver

    task = _task(_noop, tracking_path=str(tmp_path / "track"))
    assert daisy.run_blockwise([task], progress=False)
    obs = _TqdmObserver(["t"])
    task2 = _task(_noop, tracking_path=str(tmp_path / "track"))
    assert daisy.run_blockwise([task2], progress=obs)
    bar = obs._bars["t"]
    assert bar.n == bar.total == 4


def _fire_and_forget(*, context=None):
    # returns at once; the external worker connects after this returned
    env = dict(os.environ)
    env["DAISY_CONTEXT"] = context.to_env()
    subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import time; time.sleep(0.3); import daisy\nc = daisy.Client()\nwhile True:\n    with c.acquire_block() as b:\n        if b is None: break",
        ],
        env=env,
    )


@pytest.mark.timeout(60)
def test_fire_and_forget_error_leaves_no_worker_processes(tmp_path):
    psutil = pytest.importorskip("psutil")
    me = psutil.Process()
    before = {c.pid for c in me.children(recursive=True)}
    task = _task(_fire_and_forget, n=200, max_workers=2)
    with pytest.raises(RuntimeError, match="connected after its spawn function"):
        daisy.run_blockwise([task], progress=False)
    time.sleep(1.0)
    after = {
        c.pid
        for c in me.children(recursive=True)
        if c.is_running() and c.status() != psutil.STATUS_ZOMBIE
    } - before
    assert not after, f"worker processes left behind: {after}"
