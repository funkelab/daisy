"""The execution summary is printed by one context manager for every entry
point: on normal exit, on KeyboardInterrupt (what finished so far) and on
abandonment, but not for errors raised before a run produced states."""

import os
import signal
import threading
import time

import daisy
import pytest


def _noop(block):
    return None


def _slow(block):
    time.sleep(0.005)


def _task(n=40, fn=_noop, **kw):
    kw.setdefault("max_workers", 1)
    kw.setdefault("timeout", 30)
    return daisy.Task(
        "t",
        daisy.Roi([0], [n]),
        daisy.Roi([0], [10]),
        daisy.Roi([0], [10]),
        fn,
        read_write_conflict=False,
        **kw,
    )


def test_server_run_blockwise_prints_summary(capsys):
    daisy.Server().run_blockwise([_task()], progress=False)
    out = capsys.readouterr().out
    assert "Execution Summary" in out
    assert "t ✔" in out


def test_summary_can_be_silenced(capsys):
    daisy.run_blockwise([_task()], progress=False, summary=False)
    daisy.Server().run_blockwise([_task()], progress=False, summary=False)
    daisy.run_blockwise([_task()], multiprocessing=False, summary=False)
    assert "Execution Summary" not in capsys.readouterr().out


def test_serial_and_pipeline_print_once(capsys):
    daisy.run_blockwise([_task()], multiprocessing=False)
    assert capsys.readouterr().out.count("Execution Summary") == 1
    daisy.Pipeline([_task()]).run_blockwise(multiprocessing=False)
    assert capsys.readouterr().out.count("Execution Summary") == 1


def test_keyboard_interrupt_prints_partial_summary(capsys):
    task = _task(n=100_000, fn=_slow, max_workers=4, max_retries=0)
    task = daisy.Task(
        "t",
        daisy.Roi([0], [100_000]),
        daisy.Roi([0], [1]),
        daisy.Roi([0], [1]),
        _slow,
        read_write_conflict=False,
        max_workers=4,
        max_retries=0,
        timeout=30,
    )

    def fire():
        time.sleep(0.7)
        os.kill(os.getpid(), signal.SIGINT)

    threading.Thread(target=fire, daemon=True).start()
    with pytest.raises(KeyboardInterrupt) as info:
        daisy.run_blockwise([task], progress=False)
    out = capsys.readouterr().out
    assert "Run interrupted" in out
    assert "Execution Summary" in out
    states = getattr(info.value, "daisy_states", None)
    assert states is not None and "t" in states


def test_config_error_prints_no_summary(capsys):
    import daisy.logging as dl

    dl.set_log_basedir(None)  # no fallback location for the store either
    task = _task(resource_tracking=True)  # no tracking path anywhere
    with pytest.raises(RuntimeError, match="resource_tracking"):
        daisy.run_blockwise([task], progress=False)
    assert "Execution Summary" not in capsys.readouterr().out


def _boom(block):
    raise RuntimeError("boom")


def test_summary_points_at_the_log_directory_of_every_failed_task(capsys, tmp_path):
    import daisy.logging as dl

    dl.set_log_basedir(tmp_path / "logs")

    def failing(task_id):
        return daisy.Task(
            task_id,
            daisy.Roi([0], [10]),
            daisy.Roi([0], [10]),
            daisy.Roi([0], [10]),
            _boom,
            read_write_conflict=False,
            max_workers=1,
            max_retries=0,
            max_worker_restarts=0,
        )

    daisy.Server().run_blockwise([failing("a"), failing("b")], progress=False)
    out = capsys.readouterr().out
    logs = tmp_path / "logs"
    assert f"    See worker logs for failed tasks under {logs}/\n" in out
    assert f"      {logs / 'a'}/\n      {logs / 'b'}/\n" in out


def test_progress_bar_marks_a_failed_task(capsys):
    task = _task(n=10, fn=_boom, max_retries=0, max_worker_restarts=0)
    daisy.Server().run_blockwise([task], progress=True, summary=False)
    assert "t ✗ ♻=0" in capsys.readouterr().err


def test_empty_task_list_succeeds_and_prints_an_empty_summary(capsys):
    assert daisy.run_blockwise([], multiprocessing=False) is True
    assert capsys.readouterr().out == "\nExecution Summary\n-----------------\n"


def test_run_blockwise_rejects_what_is_not_a_pipeline_task_or_list():
    with pytest.raises(TypeError, match="expected a Pipeline, a Task, or a list"):
        daisy.run_blockwise(42)
