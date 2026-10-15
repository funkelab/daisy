"""`Task(skip_mask=...)`: a caller-provided mask over the block grid.

The mask is an input to a run, not persisted state. A masked block is
reported as skipped and releases its dependents, nothing is written to
the tracking store for it, and a later run with a different mask runs
it. Cell (0, 0, ...) is the first block regardless of context.
"""

import daisy
import numpy as np
import pytest
from daisy._runner import _run_serial


def make_task(task_id, calls, *, total=400, block=100, context=0, fit="valid", **kw):
    def process(block):
        calls.append(tuple(block.write_roi.offset))

    return daisy.Task(
        task_id=task_id,
        total_roi=daisy.Roi([-context, -context], [total + 2 * context] * 2),
        read_roi=daisy.Roi([-context, -context], [block + 2 * context] * 2),
        write_roi=daisy.Roi([0, 0], [block, block]),
        process_function=process,
        read_write_conflict=False,
        max_workers=1,
        max_retries=0,
        fit=fit,
        tracking_path=kw.pop("tracking_path", False),
        **kw,
    )


def test_block_grid_shape_is_blocks_per_axis():
    assert make_task("t", []).block_grid_shape == [4, 4]
    assert make_task("t", [], context=100).block_grid_shape == [4, 4]
    assert make_task("t", [], total=410).block_grid_shape == [4, 4]
    assert make_task("t", [], total=410, fit="overhang").block_grid_shape == [5, 5]
    assert make_task("t", [], total=410, fit="shrink").block_grid_shape == [5, 5]


def test_no_mask_runs_every_block():
    calls = []
    states = _run_serial([make_task("t", calls)])
    assert states["t"].is_done()
    assert states["t"].skipped_count == 0
    assert len(calls) == 16


def test_masked_blocks_are_skipped_and_counted():
    mask = np.zeros((4, 4), dtype=np.uint8)
    mask[0, 0] = 1
    mask[1, 2] = 1
    mask[3, 3] = 7  # any nonzero value skips
    calls = []
    states = _run_serial([make_task("t", calls, skip_mask=mask)])
    assert states["t"].is_done()
    assert states["t"].skipped_count == 3
    assert states["t"].completed_count == 16
    assert len(calls) == 13
    assert (0, 0) not in calls
    assert (100, 200) not in calls
    assert (300, 300) not in calls


def test_cell_zero_is_the_first_block_with_a_block_wide_context():
    # context == block size: the tracking store's internal grid would put
    # the first block at cell 1; the mask puts it at cell 0.
    mask = np.zeros((4, 4), dtype=np.uint8)
    mask[0, 0] = 1
    calls = []
    _run_serial([make_task("t", calls, context=100, skip_mask=mask)])
    assert len(calls) == 15
    assert (0, 0) not in calls


def test_bool_and_wider_dtypes_are_accepted():
    mask = np.zeros((4, 4), dtype=bool)
    mask[2, 1] = True
    calls = []
    _run_serial([make_task("t", calls, skip_mask=mask)])
    assert (200, 100) not in calls and len(calls) == 15

    calls = []
    _run_serial([make_task("t", calls, skip_mask=mask.astype(np.int64))])
    assert (200, 100) not in calls and len(calls) == 15

    task = make_task("t", [], skip_mask=mask)
    got = task.skip_mask
    assert got.dtype == np.uint8 and got.shape == (4, 4)
    assert got[2, 1] == 1 and got.sum() == 1


def test_wrong_shape_is_rejected_at_construction():
    with pytest.raises(ValueError, match=r"shape \[3, 3\].*block grid.*\[4, 4\]"):
        make_task("t", [], skip_mask=np.zeros((3, 3), dtype=np.uint8))
    with pytest.raises(ValueError, match="block grid"):
        make_task("t", [], skip_mask=np.zeros(16, dtype=np.uint8))


def test_shrink_mask_addresses_the_trailing_block():
    mask = np.zeros((5, 5), dtype=np.uint8)
    mask[4, 4] = 1
    calls = []
    _run_serial([make_task("t", calls, total=410, fit="shrink", skip_mask=mask)])
    assert len(calls) == 24
    assert (400, 400) not in calls


def test_mask_and_done_markers_compose_and_the_mask_is_per_run(tmp_path):
    tracking = tmp_path / "tracking"
    mask = np.zeros((4, 4), dtype=np.uint8)
    mask[0, :] = 1  # skip the first row of blocks

    calls1 = []
    states1 = _run_serial(
        [make_task("t", calls1, skip_mask=mask, tracking_path=str(tracking))]
    )
    assert states1["t"].skipped_count == 4
    assert len(calls1) == 12

    # Masked blocks are not recorded as done: a second run without the
    # mask runs exactly those four and skips the twelve earned ones.
    calls2 = []
    states2 = _run_serial([make_task("t", calls2, tracking_path=str(tracking))])
    assert states2["t"].skipped_count == 12
    assert sorted(calls2) == [(0, 0), (0, 100), (0, 200), (0, 300)]

    # The mask again on a third run: everything is done now, and the
    # store still carries daisy's layout hash.
    calls3 = []
    states3 = _run_serial(
        [make_task("t", calls3, skip_mask=mask, tracking_path=str(tracking))]
    )
    assert states3["t"].skipped_count == 16
    assert calls3 == []
    import json

    attrs = json.loads((tracking / "zarr.json").read_text())["attributes"]
    assert "daisy_task_hash" in attrs


def test_masked_blocks_release_downstream_dependencies():
    # Upstream masks half its blocks; downstream must still run everything.
    mask = np.zeros((4, 4), dtype=np.uint8)
    mask[:2, :] = 1
    up_calls, down_calls = [], []
    up = make_task("up", up_calls, skip_mask=mask)
    down = make_task("down", down_calls)
    states = _run_serial(up + down)
    assert states["up"].is_done() and states["down"].is_done()
    assert states["up"].skipped_count == 8
    assert len(up_calls) == 8
    assert len(down_calls) == 16


def test_mask_works_on_the_multiprocessing_server(tmp_path):
    calls_dir = tmp_path / "calls"
    calls_dir.mkdir()
    mask = np.zeros((4, 4), dtype=np.uint8)
    mask[1, 1] = 1
    mask[2, 3] = 1

    def process(block):
        from pathlib import Path

        off = block.write_roi.offset
        Path(calls_dir, f"{off[0]}-{off[1]}").touch()

    task = daisy.Task(
        task_id="tmp",
        total_roi=daisy.Roi([0, 0], [400, 400]),
        read_roi=daisy.Roi([0, 0], [100, 100]),
        write_roi=daisy.Roi([0, 0], [100, 100]),
        process_function=process,
        read_write_conflict=False,
        max_workers=2,
        max_retries=0,
        tracking_path=False,
        skip_mask=mask,
    )
    states = daisy.Server().run_blockwise([task], summary=False)
    assert states["tmp"].skipped_count == 2
    touched = {p.name for p in calls_dir.iterdir()}
    assert len(touched) == 14
    assert "100-100" not in touched and "200-300" not in touched


def test_copy_keeps_the_mask():
    import copy

    mask = np.eye(4, dtype=np.uint8)
    task = make_task("t", [], skip_mask=mask)
    clone = copy.copy(task)
    assert np.array_equal(clone.skip_mask, mask)
