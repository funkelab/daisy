"""Persistent done-marker integration tests.

These exercise the round-trip:
    run 1 — process N blocks, marker records every success
    run 2 — process_function not called for any block, all marked skipped

And the safety check:
    re-opening a marker for a task whose layout changed → error with
    `rm -rf` instructions in the message.
"""

import shutil
from pathlib import Path

import pytest
from daisy._runner import _run_serial

import daisy


def make_task(task_id, marker_path, *, total=400, block=100, rw_conflict=False):
    calls = []

    def process(block):
        calls.append(block.block_id)

    task = daisy.Task(
        task_id=task_id,
        total_roi=daisy.Roi([0, 0], [total, total]),
        read_roi=daisy.Roi([0, 0], [block, block]),
        write_roi=daisy.Roi([0, 0], [block, block]),
        process_function=process,
        read_write_conflict=rw_conflict,
        max_workers=2,
        max_retries=0,
        tracking_path=str(marker_path),
    )
    return task, calls


def test_resume_skips_already_done_blocks(tmp_path):
    marker_path = tmp_path / "done"

    task1, calls1 = make_task("t", marker_path)
    states1 = _run_serial([task1])
    assert states1["t"].is_done()
    assert states1["t"].completed_count == 16
    assert states1["t"].skipped_count == 0
    assert len(calls1) == 16

    # Same task, same layout, fresh process function — nothing should run.
    task2, calls2 = make_task("t", marker_path)
    states2 = _run_serial([task2])
    assert states2["t"].is_done()
    # All 16 blocks should be skipped via the marker on the second run.
    # Skipped blocks count toward `skipped_count` AND `completed_count`
    # (the scheduler routes skips through release_block with Success).
    assert states2["t"].skipped_count == 16
    assert states2["t"].completed_count == 16
    assert len(calls2) == 0


def test_resume_with_multiprocessing_server(tmp_path):
    marker_path = tmp_path / "done_mp"
    # subprocess workers (the default) don't share closures with this
    # process, so record calls through the filesystem instead
    calls_dir = tmp_path / "calls"
    calls_dir.mkdir()

    def make_mp_task(run):
        def process(block):
            from pathlib import Path

            Path(calls_dir, f"{run}-{block.block_id[1]}").touch()

        return daisy.Task(
            task_id="tmp",
            total_roi=daisy.Roi([0, 0], [400, 400]),
            read_roi=daisy.Roi([0, 0], [100, 100]),
            write_roi=daisy.Roi([0, 0], [100, 100]),
            process_function=process,
            read_write_conflict=False,
            max_workers=2,
            max_retries=0,
            tracking_path=str(marker_path),
        )

    daisy.Server().run_blockwise([make_mp_task(1)])
    assert len(list(calls_dir.glob("1-*"))) == 16

    daisy.Server().run_blockwise([make_mp_task(2)])
    # Marker did its job; the multiprocessing path should also skip.
    assert len(list(calls_dir.glob("2-*"))) == 0


def test_layout_mismatch_errors_with_rm_instructions(tmp_path):
    marker_path = tmp_path / "done_mismatch"

    task1, _ = make_task("layout", marker_path, block=100)
    _run_serial([task1])

    # Re-open with a different block size → different task hash.
    task2, _ = make_task("layout", marker_path, block=50)
    raised = None
    try:
        _run_serial([task2])
    except Exception as e:
        raised = e
    assert raised is not None, "expected a layout-mismatch error"
    msg = str(raised)
    assert "rm -rf" in msg
    assert str(marker_path) in msg

    # And after deleting the marker, it should run cleanly.
    shutil.rmtree(marker_path)
    task3, calls3 = make_task("layout", marker_path, block=50)
    states3 = _run_serial([task3])
    assert states3["layout"].is_done()
    assert len(calls3) > 0


def test_global_basedir_resolves_per_task(tmp_path):
    """Setting `set_done_marker_basedir(...)` should auto-resolve markers
    for tasks that don't pass an explicit `done_marker_path`."""
    daisy.set_tracking_basedir(tmp_path / "auto")
    try:
        task1, calls1 = make_task("auto_task", marker_path=None)
        # Override: omit done_marker_path so it falls back to basedir.
        task1 = daisy.Task(
            task_id="auto_task",
            total_roi=daisy.Roi([0, 0], [400, 400]),
            read_roi=daisy.Roi([0, 0], [100, 100]),
            write_roi=daisy.Roi([0, 0], [100, 100]),
            process_function=lambda b: calls1.append(b.block_id),
            read_write_conflict=False,
            max_workers=2,
            max_retries=0,
            # no done_marker_path → uses basedir/task_id
        )
        _run_serial([task1])
        assert len(calls1) == 16
        # The directory should exist now.
        assert (tmp_path / "auto" / "auto_task").is_dir()

        # Second run: nothing called.
        calls2 = []
        task2 = daisy.Task(
            task_id="auto_task",
            total_roi=daisy.Roi([0, 0], [400, 400]),
            read_roi=daisy.Roi([0, 0], [100, 100]),
            write_roi=daisy.Roi([0, 0], [100, 100]),
            process_function=lambda b: calls2.append(b.block_id),
            read_write_conflict=False,
            max_workers=2,
            max_retries=0,
        )
        _run_serial([task2])
        assert len(calls2) == 0
    finally:
        daisy.set_tracking_basedir(None)


def test_per_task_disable_overrides_basedir(tmp_path):
    """`tracking_path=False` should turn the marker OFF for that task
    even when the global basedir is set."""
    daisy.set_tracking_basedir(tmp_path / "auto2")
    try:
        calls1 = []
        task1 = daisy.Task(
            task_id="off",
            total_roi=daisy.Roi([0, 0], [200, 200]),
            read_roi=daisy.Roi([0, 0], [100, 100]),
            write_roi=daisy.Roi([0, 0], [100, 100]),
            process_function=lambda b: calls1.append(b.block_id),
            read_write_conflict=False,
            max_workers=1,
            max_retries=0,
            tracking_path=False,
        )
        _run_serial([task1])
        assert len(calls1) == 4
        # No marker dir created for this task.
        assert not (tmp_path / "auto2" / "off").exists()

        # Re-running re-runs every block.
        calls2 = []
        task2 = daisy.Task(
            task_id="off",
            total_roi=daisy.Roi([0, 0], [200, 200]),
            read_roi=daisy.Roi([0, 0], [100, 100]),
            write_roi=daisy.Roi([0, 0], [100, 100]),
            process_function=lambda b: calls2.append(b.block_id),
            read_write_conflict=False,
            max_workers=1,
            max_retries=0,
            tracking_path=False,
        )
        _run_serial([task2])
        assert len(calls2) == 4
    finally:
        daisy.set_tracking_basedir(None)


def test_no_optin_means_no_tracking(tmp_path, monkeypatch):
    """With no explicit path and no basedir, nothing is tracked: a rerun
    (e.g. after fixing a buggy process function) re-executes every block
    instead of silently resuming from stale markers."""
    monkeypatch.chdir(tmp_path)  # would have caught the old ./daisy_logs fallback
    assert daisy.get_done_marker_basedir() is None

    def run(calls):
        task = daisy.Task(
            task_id="my_pipeline_step",  # same id both runs, like a real script
            total_roi=daisy.Roi([0], [40]),
            read_roi=daisy.Roi([0], [10]),
            write_roi=daisy.Roi([0], [10]),
            process_function=lambda b: calls.append(b.block_id),
            read_write_conflict=False,
            max_workers=1,
            max_retries=0,
        )
        return _run_serial([task])

    buggy_calls, fixed_calls = [], []
    run(buggy_calls)
    run(fixed_calls)  # "fixed the bug, rerun"
    assert len(buggy_calls) == 4
    assert len(fixed_calls) == 4  # every block re-executed
    # worker LOGS may exist under daisy_logs, but no done marker (zarr) may
    assert not list(Path(tmp_path).rglob("zarr.json"))


def test_resume_emits_info_log(tmp_path, caplog):
    """A resumed run (explicit opt-in via basedir) still skips done blocks,
    and reports it as an INFO record on the daisy logger."""
    import logging

    daisy.set_tracking_basedir(tmp_path / "markers")
    try:

        def run(calls):
            task = daisy.Task(
                task_id="resumed_task",
                total_roi=daisy.Roi([0], [40]),
                read_roi=daisy.Roi([0], [10]),
                write_roi=daisy.Roi([0], [10]),
                process_function=lambda b: calls.append(b.block_id),
                read_write_conflict=False,
                max_workers=1,
                max_retries=0,
            )
            return _run_serial([task])

        first, second = [], []
        run(first)
        assert len(first) == 4
        with caplog.at_level(logging.INFO, logger="daisy"):
            states = run(second)
        assert len(second) == 0
        assert states["resumed_task"].skipped_count == 4
        resumed = [r for r in caplog.records if "resumed" in r.getMessage()]
        assert len(resumed) == 1
        msg = resumed[0].getMessage()
        assert "4/4" in msg and "resumed_task" in msg
    finally:
        daisy.set_tracking_basedir(None)


def test_tracking_is_a_zarr_group_with_done_and_failures(tmp_path):
    """The layout is a Zarr v3 group, not a bare array: `done` and
    `failures` are always present as sibling arrays, and the group carries
    the task hash used for layout validation."""
    import json

    task = daisy.Task(
        task_id="grouped",
        total_roi=daisy.Roi([0], [40]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=lambda b: None,
        read_write_conflict=False,
        max_workers=1,
        tracking_path=str(tmp_path / "tracking"),
        worker_processes=False,
    )
    _run_serial([task])

    root = tmp_path / "tracking"
    group_meta = json.loads((root / "zarr.json").read_text())
    assert group_meta["node_type"] == "group"
    assert group_meta["zarr_format"] == 3
    assert group_meta["attributes"]["daisy_task_hash"]

    for child, dtype in (("done", "uint8"), ("failures", "uint32")):
        meta = json.loads((root / child / "zarr.json").read_text())
        assert meta["node_type"] == "array", child
        assert meta["data_type"] == dtype, child
        assert meta["shape"] == [4], child
        # Single chunk, raw little-endian bytes, so daisy can mmap it.
        assert (root / child / "c" / "0").is_file(), child

    # Resource arrays are absent without resource_tracking.
    assert not (root / "cpu_seconds").exists()


def test_legacy_array_layout_is_refused_with_rm_instructions(tmp_path):
    """A tracking directory written by an older daisy is a bare array. It
    must be refused with the actionable message rather than a confusing
    metadata error — the user needs to know to delete it."""
    import json

    root = tmp_path / "legacy"
    root.mkdir()
    (root / "zarr.json").write_text(
        json.dumps(
            {
                "zarr_format": 3,
                "node_type": "array",
                "shape": [4],
                "data_type": "uint8",
                "attributes": {"daisy_task_hash": "hash-from-an-older-daisy"},
            }
        )
    )

    task = daisy.Task(
        task_id="legacy",
        total_roi=daisy.Roi([0], [40]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=lambda b: None,
        read_write_conflict=False,
        max_workers=1,
        tracking_path=str(root),
        worker_processes=False,
    )
    with pytest.raises(Exception) as excinfo:
        _run_serial([task])
    assert "rm -rf" in str(excinfo.value)


def test_done_marker_names_still_work_but_warn(tmp_path):
    """`done_marker_path` / `set_done_marker_basedir` are deprecated aliases
    — the directory outgrew the name — but must keep working."""
    with pytest.warns(DeprecationWarning, match="tracking_path"):
        task = daisy.Task(
            task_id="aliased",
            total_roi=daisy.Roi([0], [20]),
            read_roi=daisy.Roi([0], [10]),
            write_roi=daisy.Roi([0], [10]),
            process_function=lambda b: None,
            read_write_conflict=False,
            max_workers=1,
            done_marker_path=str(tmp_path / "aliased"),
            worker_processes=False,
        )
    _run_serial([task])
    assert (tmp_path / "aliased" / "done").is_dir()

    with pytest.warns(DeprecationWarning, match="set_tracking_basedir"):
        daisy.set_done_marker_basedir(str(tmp_path / "basedir"))
    try:
        with pytest.warns(DeprecationWarning, match="get_tracking_basedir"):
            assert daisy.get_done_marker_basedir() is not None
        # The canonical getter reports the same thing without warning.
        assert daisy.get_tracking_basedir() == (tmp_path / "basedir")
    finally:
        daisy.set_tracking_basedir(None)


def test_passing_both_path_names_is_rejected(tmp_path):
    """Ambiguity is a mistake worth surfacing rather than silently
    preferring one."""
    with pytest.raises(TypeError, match="not both"):
        daisy.Task(
            task_id="both",
            total_roi=daisy.Roi([0], [20]),
            read_roi=daisy.Roi([0], [10]),
            write_roi=daisy.Roi([0], [10]),
            process_function=lambda b: None,
            read_write_conflict=False,
            max_workers=1,
            tracking_path=str(tmp_path / "a"),
            done_marker_path=str(tmp_path / "b"),
        )
