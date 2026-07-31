"""The end-of-run summary, agglomerated from per-block measurements.

These tests replace an older file that had to pin in-process workers to see
any counts at all — that pin existed *because* only the in-process worker
loop incremented `blocks_processed`. Measurement now happens inside the
worker and rides home on the block, so the numbers do not depend on how the
work was dispatched; the equivalence is asserted here rather than worked
around. Every distributed worker is a subprocess, so the figures are also
per-process, which is what makes peak RSS meaningful.

`linear_trend`'s own maths is unit-tested in Rust; what matters at this
level is that the trend is fed real per-block wall times.
"""

import time

import pytest

import daisy


def _tracked_task(task_id, tmp_path, process_function, **kw):
    return daisy.Task(
        task_id=task_id,
        total_roi=daisy.Roi([0], kw.pop("total", [40])),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=process_function,
        read_write_conflict=False,
        max_workers=kw.pop("max_workers", 2),
        max_retries=0,
        tracking_path=str(tmp_path / f"tracking_{task_id}"),
        resource_tracking=True,
        **kw,
    )


def _run(task):
    server = daisy.Server()
    states = server.run_blockwise([task], progress=False)
    return states, server.last_tracking_summary["per_task"][task.task_id]


def _busy(_block):
    acc = 0
    for i in range(150_000):
        acc += i * i
    return acc


def test_summary_shape(tmp_path):
    """The keys the renderer and users rely on."""
    _, s = _run(_tracked_task("shape", tmp_path, _busy))
    for key in (
        "blocks",
        "failures",
        "has_stats",
        "total_cpu_secs",
        "total_block_secs",
        "max_peak_rss_bytes",
        "io_read_bytes",
        "io_write_bytes",
        "mean_block_ms",
        "block_ms_slope",
    ):
        assert key in s, f"missing summary key {key}"


def _busy_worker():
    """A 0-arg worker driving its own loop — the shape a cluster worker has.
    It measures via `Client.acquire_block` exactly as the shim does."""
    client = daisy.Client()
    while True:
        with client.acquire_block() as block:
            if block is None:
                return
            _busy(block)


@pytest.mark.parametrize(
    "fn, shape",
    [(_busy, "block_function"), (_busy_worker, "worker_function")],
    ids=["block_function", "worker_function"],
)
def test_block_count_matches_the_scheduler_for_either_function_shape(
    tmp_path, fn, shape
):
    """The regression under test: subprocess workers used to report 0 blocks,
    because only the deleted in-process worker loop incremented a counter.

    `blocks` and `completed_count` are the same event counted once, so they
    must agree exactly — whether the user wrote a 1-arg block function or a
    0-arg worker function.
    """
    task = _tracked_task(f"count_{shape}", tmp_path, fn)
    states, s = _run(task)
    assert states[task.task_id].completed_count == 4
    assert int(s["blocks"]) == 4
    assert s["has_stats"], f"{shape} blocks must arrive measured"


def test_cpu_and_wall_are_plausible(tmp_path):
    """CPU-bound work should burn CPU comparable to its wall time; the
    numbers must be real measurements, not zeros or wild values."""
    _, s = _run(_tracked_task("plausible", tmp_path, _busy))
    cpu = float(s["total_cpu_secs"])
    block = float(s["total_block_secs"])
    assert cpu > 0, "busy work must register CPU time"
    assert block > 0
    # A tight arithmetic loop is on-CPU essentially all of its wall time.
    assert 0.4 < cpu / block < 1.6, f"cpu/wall ratio implausible: {cpu}/{block}"
    assert int(s["max_peak_rss_bytes"]) > 0


def test_sleeping_blocks_burn_wall_but_not_cpu(tmp_path):
    """The converse check — this is what distinguishes an IO-bound task in
    the summary, so it must not be conflated with CPU time."""

    def sleeper(_block):
        time.sleep(0.02)

    _, s = _run(_tracked_task("sleepy", tmp_path, sleeper))
    cpu = float(s["total_cpu_secs"])
    block = float(s["total_block_secs"])
    assert block > 0.05, f"4 blocks x 20ms should show up as wall time: {block}"
    assert cpu < block / 2, f"sleeping must not be counted as CPU: {cpu} vs {block}"


def test_constant_workload_reports_near_zero_slope(tmp_path):
    """Uniform blocks → flat trend. Deliberately generous: this asserts the
    trend is fed real data, not that a loaded machine is quiet."""
    _, s = _run(_tracked_task("flat", tmp_path, _busy, total=[200], max_workers=1))
    assert int(s["blocks"]) == 20
    assert float(s["mean_block_ms"]) > 0
    mean = float(s["mean_block_ms"])
    slope = float(s["block_ms_slope"])
    # Slope is ms-per-block; scale-free comparison against the mean.
    assert abs(slope) < mean, f"expected a flat-ish trend: mean={mean} slope={slope}"


def test_slowing_workload_reports_positive_slope(tmp_path):
    """Blocks that get slower must show a positive slope — this is the
    diagnostic the trend exists for."""
    import os

    counter = tmp_path / "seen"
    counter.mkdir()

    def slowing(block):
        # Per-block sleep grows with how many blocks have run before it.
        n = len(os.listdir(counter))
        (counter / str(block.block_id[1])).touch()
        time.sleep(0.005 + 0.004 * n)

    _, s = _run(
        _tracked_task(
            "slowing",
            tmp_path,
            slowing,
            total=[100],
            max_workers=1,
        )
    )
    assert int(s["blocks"]) == 10
    assert float(s["block_ms_slope"]) > 1.0, f"expected a rising trend: {s}"


def test_untracked_task_reports_no_summary(tmp_path):
    """No tracking configured → nothing to summarise, and the renderer
    omits the panel rather than printing zeros."""
    task = daisy.Task(
        task_id="untracked",
        total_roi=daisy.Roi([0], [40]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=_busy,
        read_write_conflict=False,
        max_workers=1,
    )
    server = daisy.Server()
    states = server.run_blockwise([task], progress=False)
    assert states["untracked"].completed_count == 4
    assert server.last_tracking_summary["per_task"] == {}


def _render(summary):
    """Render a summary and return what it printed.

    The renderer writes to the *saved* real stdout on purpose, so that a
    worker-log stdout proxy can't swallow the report. Pointing that at a
    buffer exercises the real path; pytest's own capture replaces
    `sys.__stdout__` and would not see it.
    """
    import io

    from daisy._progress import _print_resource_utilization

    from daisy import logging as _worker_log

    buf = io.StringIO()
    previous = _worker_log._saved_stdout
    _worker_log._saved_stdout = buf
    try:
        _print_resource_utilization(summary)
    finally:
        _worker_log._saved_stdout = previous
    return buf.getvalue()


def test_renderer_omits_panel_without_measurements():
    """Nothing at all when no task measured anything — a table of zeros is
    worse than no table."""
    assert _render(None) == ""
    assert _render({"per_task": {}}) == ""
    assert _render({"per_task": {"t": {"has_stats": False, "blocks": 4}}}) == ""


def test_renderer_prints_measured_totals():
    """And renders real figures when there are measurements."""
    out = _render(
        {
            "per_task": {
                "seg": {
                    "has_stats": True,
                    "blocks": 12,
                    "failures": 2,
                    "total_cpu_secs": 1.5,
                    "total_block_secs": 2.0,
                    "max_peak_rss_bytes": 1024 * 1024,
                    "io_read_bytes": 2048,
                    "io_write_bytes": 4096,
                    "mean_block_ms": 166.7,
                    "block_ms_slope": -0.5,
                }
            }
        }
    )
    assert "Resource Utilization" in out
    assert "blocks measured : 12" in out
    assert "seg" in out
    # CPU-per-block-second is the IO-vs-CPU-bound hint.
    assert "0.75" in out
