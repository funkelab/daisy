"""Worker log files: live output, one traceback per failure, append across
runs in one process, and an explicit way to clear them."""

import threading
import time
from pathlib import Path

import daisy
import daisy.logging as dl
import pytest


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


@pytest.mark.parametrize(
    "task_id, removed", [("t", "logs/t"), (None, "logs")], ids=["one-task", "all"]
)
def test_clear_logs_removes_the_log_directory(tmp_path, task_id, removed):
    basedir = tmp_path / "logs"
    dl.set_log_basedir(basedir)
    assert daisy.run_blockwise([_task(_hello, n=20)], progress=False)
    assert len(_outs(basedir)) == 1
    assert dl.clear_logs(task_id) == tmp_path / removed
    assert list(tmp_path.rglob("worker_*")) == []


@pytest.fixture
def log_mode():
    """Set the worker log mode for one test; the default is restored after."""
    yield dl.set_log_mode
    dl.set_log_mode("file")


@pytest.mark.parametrize(
    "mode, in_file, on_console",
    [("file", 2, 0), ("console", 0, 2), ("both", 2, 2)],
)
@pytest.mark.parametrize("multiprocessing", [True, False])
def test_log_mode_decides_where_worker_output_goes(
    tmp_path, capsys, log_mode, mode, in_file, on_console, multiprocessing
):
    basedir = tmp_path / "logs"
    dl.set_log_basedir(basedir)
    log_mode(mode)
    assert dl.get_log_mode() == mode
    assert daisy.run_blockwise(
        [_task(_hello, n=20)],
        multiprocessing=multiprocessing,
        progress=False,
        summary=False,
    )
    assert "".join(f.read_text() for f in _outs(basedir)).count("hello") == in_file
    assert capsys.readouterr().out.count("hello") == on_console


def test_unknown_log_mode_is_rejected():
    with pytest.raises(ValueError, match="mode must be one of"):
        dl.set_log_mode("syslog")


def test_log_mode_is_console_without_a_log_basedir():
    dl.set_log_basedir(None)
    assert dl.get_log_mode() == "console"


@pytest.mark.parametrize(
    "task_id, expected",
    [(None, "logs/worker_3"), ("t", "logs/t/worker_3")],
)
def test_worker_log_basename(tmp_path, task_id, expected):
    dl.set_log_basedir(tmp_path / "logs")
    assert daisy.get_worker_log_basename(3, task_id) == tmp_path / expected


def test_worker_log_basename_is_none_without_a_log_basedir():
    dl.set_log_basedir(None)
    assert daisy.get_worker_log_basename(3) is None


def test_log_level_round_trips():
    import logging

    previous = dl.get_log_level()
    dl.set_log_level("DEBUG")
    try:
        assert dl.get_log_level() == logging.DEBUG
    finally:
        dl.set_log_level(previous)


def _raised(source, filename):
    """The exception raised by running `source` as if it lived in `filename`."""
    try:
        exec(compile(source, filename, "exec"), {})
    except ValueError as e:
        return e
    raise AssertionError("source did not raise")


def test_traceback_style_defaults_to_plain():
    assert dl.get_traceback_style() == "plain"


def test_unknown_traceback_style_is_rejected():
    with pytest.raises(ValueError, match="style must be 'plain' or 'rich'"):
        dl.set_traceback_style("html")


def test_plain_traceback_names_notebook_cells():
    e = _raised("raise ValueError('bad cell')", "<ipython-input-3-0123abcd>")
    text = dl.format_traceback(type(e), e, e.__traceback__)
    assert 'File "[notebook cell 3]", line 1, in <module>' in text
    assert text.endswith("ValueError: bad cell\n")
