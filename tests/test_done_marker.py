"""Persistent done-marker integration tests.

These exercise the round-trip:
    run 1 — process N blocks, marker records every success
    run 2 — process_function not called for any block, all marked skipped

And the safety check:
    re-opening a marker for a task whose layout changed → error with
    `rm -rf` instructions in the message.
"""

import shutil
import tempfile
from pathlib import Path

import gerbera


def make_task(task_id, marker_path, *, total=400, block=100, rw_conflict=False):
    calls = []

    def process(block):
        calls.append(block.block_id)

    task = gerbera.Task(
        task_id=task_id,
        total_roi=gerbera.Roi([0, 0], [total, total]),
        read_roi=gerbera.Roi([0, 0], [block, block]),
        write_roi=gerbera.Roi([0, 0], [block, block]),
        process_function=process,
        read_write_conflict=rw_conflict,
        num_workers=2,
        max_retries=0,
        done_marker_path=str(marker_path),
    )
    return task, calls


def test_resume_skips_already_done_blocks(tmp_path):
    marker_path = tmp_path / "done"

    task1, calls1 = make_task("t", marker_path)
    states1 = gerbera.SerialServer().run_blockwise([task1])
    assert states1["t"].is_done()
    assert states1["t"].completed_count == 16
    assert states1["t"].skipped_count == 0
    assert len(calls1) == 16

    # Same task, same layout, fresh process function — nothing should run.
    task2, calls2 = make_task("t", marker_path)
    states2 = gerbera.SerialServer().run_blockwise([task2])
    assert states2["t"].is_done()
    # All 16 blocks should be skipped via the marker on the second run.
    # Skipped blocks count toward `skipped_count` AND `completed_count`
    # (the scheduler routes skips through release_block with Success).
    assert states2["t"].skipped_count == 16
    assert states2["t"].completed_count == 16
    assert len(calls2) == 0


def test_resume_with_multiprocessing_server(tmp_path):
    marker_path = tmp_path / "done_mp"

    task1, calls1 = make_task("tmp", marker_path)
    gerbera.Server().run_blockwise([task1])
    assert len(calls1) == 16

    task2, calls2 = make_task("tmp", marker_path)
    gerbera.Server().run_blockwise([task2])
    # Marker did its job; the multiprocessing path should also skip.
    assert len(calls2) == 0


def test_layout_mismatch_errors_with_rm_instructions(tmp_path):
    marker_path = tmp_path / "done_mismatch"

    task1, _ = make_task("layout", marker_path, block=100)
    gerbera.SerialServer().run_blockwise([task1])

    # Re-open with a different block size → different task hash.
    task2, _ = make_task("layout", marker_path, block=50)
    raised = None
    try:
        gerbera.SerialServer().run_blockwise([task2])
    except Exception as e:
        raised = e
    assert raised is not None, "expected a layout-mismatch error"
    msg = str(raised)
    assert "rm -rf" in msg
    assert str(marker_path) in msg

    # And after deleting the marker, it should run cleanly.
    shutil.rmtree(marker_path)
    task3, calls3 = make_task("layout", marker_path, block=50)
    states3 = gerbera.SerialServer().run_blockwise([task3])
    assert states3["layout"].is_done()
    assert len(calls3) > 0


def test_global_basedir_resolves_per_task(tmp_path):
    """Setting `set_done_marker_basedir(...)` should auto-resolve markers
    for tasks that don't pass an explicit `done_marker_path`."""
    gerbera.set_done_marker_basedir(tmp_path / "auto")
    try:
        task1, calls1 = make_task("auto_task", marker_path=None)
        # Override: omit done_marker_path so it falls back to basedir.
        task1 = gerbera.Task(
            task_id="auto_task",
            total_roi=gerbera.Roi([0, 0], [400, 400]),
            read_roi=gerbera.Roi([0, 0], [100, 100]),
            write_roi=gerbera.Roi([0, 0], [100, 100]),
            process_function=lambda b: calls1.append(b.block_id),
            read_write_conflict=False,
            num_workers=2,
            max_retries=0,
            # no done_marker_path → uses basedir/task_id
        )
        gerbera.SerialServer().run_blockwise([task1])
        assert len(calls1) == 16
        # The directory should exist now.
        assert (tmp_path / "auto" / "auto_task").is_dir()

        # Second run: nothing called.
        calls2 = []
        task2 = gerbera.Task(
            task_id="auto_task",
            total_roi=gerbera.Roi([0, 0], [400, 400]),
            read_roi=gerbera.Roi([0, 0], [100, 100]),
            write_roi=gerbera.Roi([0, 0], [100, 100]),
            process_function=lambda b: calls2.append(b.block_id),
            read_write_conflict=False,
            num_workers=2,
            max_retries=0,
        )
        gerbera.SerialServer().run_blockwise([task2])
        assert len(calls2) == 0
    finally:
        gerbera.set_done_marker_basedir(None)


def test_per_task_disable_overrides_basedir(tmp_path):
    """`done_marker_path=False` should turn the marker OFF for that task
    even when the global basedir is set."""
    gerbera.set_done_marker_basedir(tmp_path / "auto2")
    try:
        calls1 = []
        task1 = gerbera.Task(
            task_id="off",
            total_roi=gerbera.Roi([0, 0], [200, 200]),
            read_roi=gerbera.Roi([0, 0], [100, 100]),
            write_roi=gerbera.Roi([0, 0], [100, 100]),
            process_function=lambda b: calls1.append(b.block_id),
            read_write_conflict=False,
            num_workers=1,
            max_retries=0,
            done_marker_path=False,
        )
        gerbera.SerialServer().run_blockwise([task1])
        assert len(calls1) == 4
        # No marker dir created for this task.
        assert not (tmp_path / "auto2" / "off").exists()

        # Re-running re-runs every block.
        calls2 = []
        task2 = gerbera.Task(
            task_id="off",
            total_roi=gerbera.Roi([0, 0], [200, 200]),
            read_roi=gerbera.Roi([0, 0], [100, 100]),
            write_roi=gerbera.Roi([0, 0], [100, 100]),
            process_function=lambda b: calls2.append(b.block_id),
            read_write_conflict=False,
            num_workers=1,
            max_retries=0,
            done_marker_path=False,
        )
        gerbera.SerialServer().run_blockwise([task2])
        assert len(calls2) == 4
    finally:
        gerbera.set_done_marker_basedir(None)
