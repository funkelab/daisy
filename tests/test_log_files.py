"""Worker log files: live output, one traceback per failure, append across
runs in one process, and an explicit way to clear them."""

import threading
import time
from pathlib import Path

import pytest

import daisy
import daisy.logging as dl


def _task(fn, name="t", n=40, **kw):
    kw.setdefault("max_workers", 1)
    kw.setdefault("timeout", 30)
    kw.setdefault("max_retries", 0)
    return daisy.Task(
        name,
        daisy.Roi([0], [n]),
        daisy.Roi([0], [10]),
        daisy.Roi([0], [10]),
        fn,
        read_write_conflict=False,
        **kw,
    )


def _print_then_sleep(block):
    print(f"BODY {block.block_id[1]}", flush=True)
    time.sleep(0.6)


def _boom(block):
    raise RuntimeError("boom")


def _hello(block):
    print("hello", flush=True)


def _outs(basedir, name="t"):
    return sorted(Path(basedir, name).glob("worker_*.out"))


@pytest.mark.timeout(60)
def test_worker_output_is_visible_while_the_worker_runs(tmp_path):
    basedir = tmp_path / "logs"
    dl.set_log_basedir(basedir)
    seen_at = {}
    stop = threading.Event()

    def watch():
        while not stop.is_set():
            for f in _outs(basedir):
                if "BODY" in f.read_text() and "t" not in seen_at:
                    seen_at["t"] = time.monotonic()
            time.sleep(0.05)

    th = threading.Thread(target=watch, daemon=True)
    th.start()
    t0 = time.monotonic()
    daisy.run_blockwise([_task(_print_then_sleep, n=20)], progress=False)
    t1 = time.monotonic()
    stop.set()
    th.join()
    assert "t" in seen_at, "body output never reached the .out file"
    # two blocks of 0.6 s on one worker: the first print must be visible
    # well before the run ends, not only at worker exit
    assert seen_at["t"] - t0 < (t1 - t0) - 0.4


def test_one_traceback_per_failed_block(tmp_path):
    basedir = tmp_path / "logs"
    dl.set_log_basedir(basedir)
    task = _task(_boom, n=10, max_worker_restarts=0)
    # one block, no retries: the run reports the failure and returns False
    assert daisy.run_blockwise([task], progress=False) is False
    err = "".join(f.read_text() for f in Path(basedir, "t").glob("worker_*.err"))
    assert err.count("Traceback (most recent call last)") == 1
    assert err.count("RuntimeError: boom") == 1


def test_logs_append_across_runs_in_one_process(tmp_path):
    basedir = tmp_path / "logs"
    dl.set_log_basedir(basedir)
    for _ in range(2):
        assert daisy.run_blockwise([_task(_hello, n=20)], progress=False)
    text = "".join(f.read_text() for f in _outs(basedir))
    assert text.count("hello") == 4
    assert "=== daisy run" in text


def test_clear_logs_removes_the_task_directory(tmp_path):
    basedir = tmp_path / "logs"
    dl.set_log_basedir(basedir)
    assert daisy.run_blockwise([_task(_hello, n=20)], progress=False)
    assert _outs(basedir)
    dl.clear_logs("t")
    assert not Path(basedir, "t").exists()
