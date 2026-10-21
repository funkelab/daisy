"""The end-of-run report, agglomerated from per-block measurements.

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

import daisy
import pytest


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
    return states, server.last_report[task.task_id].blocks


def _busy(_block):
    acc = 0
    for i in range(150_000):
        acc += i * i
    return acc


def test_summary_shape(tmp_path):
    """The fields the renderer and users rely on, and the JSON they
    round-trip through (the same line that lands in runs.jsonl)."""
    _, s = _run(_tracked_task("shape", tmp_path, _busy))
    for key in ("count", "failures", "has_stats", "trend_slope_ms"):
        assert hasattr(s, key), f"missing report field {key}"
    for stat in (s.wall, s.cpu, s.peak_rss, s.io_read, s.io_write, s.overhead):
        assert stat.n == 4, stat
        assert stat.min <= stat.mean <= stat.max
        assert stat.std >= 0
    report = daisy.Server()
    report.run_blockwise([_tracked_task("shape2", tmp_path, _busy)], progress=False)
    report = report.last_report
    again = daisy.RunReport.from_json(report.to_json())
    assert again["shape2"].blocks == report["shape2"].blocks
    assert again["shape2"].workers.by_id.keys() == report["shape2"].workers.by_id.keys()


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
    assert s.count == 4
    assert s.has_stats, f"{shape} blocks must arrive measured"


def test_cpu_and_wall_are_plausible(tmp_path):
    """CPU-bound work should burn CPU comparable to its wall time; the
    numbers must be real measurements, not zeros or wild values."""
    _, s = _run(_tracked_task("plausible", tmp_path, _busy))
    cpu = s.cpu.total
    block = s.wall.total
    assert cpu > 0, "busy work must register CPU time"
    assert block > 0
    # A tight arithmetic loop is on-CPU essentially all of its wall time.
    assert 0.4 < cpu / block < 1.6, f"cpu/wall ratio implausible: {cpu}/{block}"
    assert s.peak_rss.max > 0


def test_sleeping_blocks_burn_wall_but_not_cpu(tmp_path):
    """The converse check — this is what distinguishes an IO-bound task in
    the summary, so it must not be conflated with CPU time."""

    def sleeper(_block):
        time.sleep(0.02)

    _, s = _run(_tracked_task("sleepy", tmp_path, sleeper))
    cpu = s.cpu.total
    block = s.wall.total
    assert block > 0.05, f"4 blocks x 20ms should show up as wall time: {block}"
    assert cpu < block / 2, f"sleeping must not be counted as CPU: {cpu} vs {block}"


def test_constant_workload_reports_near_zero_slope(tmp_path):
    """Uniform blocks → flat trend. Deliberately generous: this asserts the
    trend is fed real data, not that a loaded machine is quiet."""
    _, s = _run(_tracked_task("flat", tmp_path, _busy, total=[200], max_workers=1))
    assert s.count == 20
    assert s.wall.mean > 0
    mean = s.wall.mean * 1000.0
    slope = s.trend_slope_ms
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
    assert s.count == 10
    assert s.trend_slope_ms > 1.0, f"expected a rising trend: {s}"


def test_untracked_task_reports_no_measurements(tmp_path):
    """No tracking configured → the report still exists (worker timing is
    always collected) but carries no resource figures, and the renderer
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
    t = server.last_report["untracked"]
    assert not t.tracked
    assert not t.blocks.has_stats
    assert t.blocks.count == 4
    assert t.workers.count >= 1
    # The store-free figures still print: round trip and worker timing...
    out = server.last_report.render()
    assert "round trip" in out and "workers  startup" in out
    # ...but nothing that needs the store.
    assert "cpu" not in out and "peak RSS" not in out and "trend" not in out


def _render(report):
    """Render a report and return what it printed.

    The renderer writes to the *saved* real stdout on purpose, so that a
    worker-log stdout proxy can't swallow the report. Pointing that at a
    buffer exercises the real path; pytest's own capture replaces
    `sys.__stdout__` and would not see it.
    """
    import io

    from daisy import logging as _worker_log
    from daisy._progress import _print_resource_utilization

    buf = io.StringIO()
    previous = _worker_log._saved_stdout
    _worker_log._saved_stdout = buf
    try:
        _print_resource_utilization(report)
    finally:
        _worker_log._saved_stdout = previous
    return buf.getvalue()


def _stat(n, mean, std=0.0, total=None):
    return {
        "n": n,
        "mean": mean,
        "std": std,
        "min": mean,
        "max": mean,
        "sum": mean * n if total is None else total,
    }


def _report(**tasks):
    return daisy.RunReport.from_dict(
        {tid: {"task_id": tid, **body} for tid, body in tasks.items()}
    )


def test_renderer_omits_panel_without_anything_to_show():
    """Nothing at all when no task has a round trip, a worker or a
    measurement (a serial run without resource tracking) — a table of
    zeros is worse than no table."""
    assert _render(None) == ""
    assert _render(_report()) == ""
    assert _render(_report(t={"blocks": {"has_stats": False, "count": 4}})) == ""


def test_renderer_prints_store_free_figures_without_resource_tracking():
    """Round trip and worker timing need no tracking store, so they print
    for every distributed task; the in-worker measurements do not."""
    out = _render(
        _report(
            plain={
                "blocks": {"count": 4, "round_trip": _stat(4, 0.05, 0.001)},
                "workers": {
                    "count": 1,
                    "startup": _stat(1, 0.1),
                    "idle": _stat(4, 0.001),
                    "compute": _stat(4, 0.05, 0.001),
                    "teardown": _stat(1, 0.02),
                    "exits": {"clean": 1},
                    "by_id": [
                        {
                            "worker_id": 0,
                            "startup": 0.1,
                            "blocks_ok": 4,
                            "idle": _stat(4, 0.001),
                            "compute": _stat(4, 0.05),
                            "teardown": 0.02,
                            "exit": "clean",
                        }
                    ],
                },
            }
        )
    )
    assert "plain   4 blocks, 1 worker" in out
    assert "blocks   round trip 50.0 ms ± 1.0 ms" in out
    assert "workers  startup 100.0 ms   idle 1.0 ms" in out
    assert "cpu" not in out and "peak RSS" not in out and "io read" not in out


def _is_worker_row(line):
    """A per-worker table row: starts with an integer id, ends with the
    exit kind (the `exits N clean` summary line does not start with an id)."""
    parts = line.split()
    return len(parts) > 2 and parts[0].isdigit() and parts[-1] == "clean"


def test_renderer_prints_measured_figures():
    """And renders real figures when there are measurements: the block
    line with its ± figures, the worker line, and one row per worker."""
    out = _render(
        _report(
            seg={
                "started_at": 100.0,
                "finished_at": 104.1,
                "tracked": True,
                "blocks": {
                    "has_stats": True,
                    "count": 12,
                    "failures": 2,
                    "round_trip": _stat(12, 1.003, 0.2),
                    "wall": _stat(12, 1.0, 0.2),
                    "cpu": _stat(12, 0.9, 0.1),
                    "peak_rss": _stat(12, 1024 * 1024 * 1024, 20 * 1024 * 1024),
                    "io_read": _stat(12, 1024.0),
                    "io_write": _stat(12, 2048.0),
                    "overhead": _stat(12, 0.003, 0.001),
                    "trend_slope_ms": -0.5,
                },
                "workers": {
                    "count": 2,
                    "startup": _stat(2, 2.0, 0.5),
                    "idle": _stat(12, 0.1, 0.02),
                    "compute": _stat(12, 1.0, 0.2),
                    "teardown": _stat(2, 0.2, 0.01),
                    "exits": {"clean": 2},
                    "by_id": [
                        {
                            "worker_id": 0,
                            "startup": 2.2,
                            "blocks_ok": 6,
                            "idle": _stat(6, 0.1),
                            "compute": _stat(6, 1.0),
                            "teardown": 0.1,
                            "exit": "clean",
                        },
                        {
                            "worker_id": 1,
                            "startup": 1.8,
                            "blocks_ok": 6,
                            "idle": _stat(6, 0.1),
                            "compute": _stat(6, 1.0),
                            "teardown": 0.3,
                            "exit": "clean",
                        },
                    ],
                },
            }
        )
    )
    assert "Resource Utilization" in out
    assert "seg   12 blocks, 2 workers, 4.10 s wall" in out
    assert "blocks   round trip 1.00 s ± 0.20 s" in out
    assert "wall 1.00 s ± 0.20 s" in out
    assert "cpu 900.0 ms ± 100.0 ms" in out
    assert "peak RSS 1.0 GB ± 20.0 MB" in out
    assert "overhead 3.0 ms ± 1.0 ms" in out
    assert "trend ∠ -0.5000 ms/block" in out
    assert "2 failed attempt(s)" in out
    assert "startup 2.00 s ± 0.50 s" in out
    assert "idle 100.0 ms ± 20.0 ms" in out
    assert "teardown 200.0 ms ± 10.0 ms" in out
    assert "exits 2 clean" in out
    rows = [line for line in out.splitlines() if _is_worker_row(line)]
    assert len(rows) == 2, out
    assert "2.20 s" in rows[0] and "1.80 s" in rows[1]


def test_renderer_caps_the_worker_table():
    """A 500-worker run must not scroll the terminal: ten rows, then a
    pointer at the full table on the report object."""
    rows = [
        {
            "worker_id": i,
            "startup": 1.0,
            "blocks_ok": 1,
            "idle": _stat(1, 0.1),
            "compute": _stat(1, 1.0),
            "teardown": 0.1,
            "exit": "clean",
        }
        for i in range(25)
    ]
    out = _render(
        _report(
            big={
                "tracked": True,
                "blocks": {"has_stats": True, "count": 25, "wall": _stat(25, 1.0)},
                "workers": {"count": 25, "exits": {"clean": 25}, "by_id": rows},
            }
        )
    )
    printed = [line for line in out.splitlines() if _is_worker_row(line)]
    assert len(printed) == 10
    assert "… +15 more" in out
