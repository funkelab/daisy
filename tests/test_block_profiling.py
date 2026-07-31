"""Per-block resource measurement across every execution mode.

The point of the design is that measurement happens *inside* whoever runs
the block, so thread workers, subprocess-shim workers and external cluster
workers all report the same thing. These tests pin that equivalence — the
bug being fixed was subprocess-mode blocks reporting nothing at all.
"""

import subprocess
import sys
import textwrap

import pytest

import daisy


def _require_zarr_v3():
    """The tracking group is Zarr v3; zarr-python 2 cannot open it."""
    zarr = pytest.importorskip("zarr")
    major = int(zarr.__version__.split(".")[0])
    if major < 3:
        pytest.skip(f"needs zarr>=3 to read a v3 group (have {zarr.__version__})")
    return zarr


def _busy(_block):
    acc = 0
    for i in range(200_000):
        acc += i * i
    return acc


def _task(task_id, tmp_path, **kw):
    return daisy.Task(
        task_id=task_id,
        total_roi=daisy.Roi([0], [40]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=kw.pop("process_function", _busy),
        read_write_conflict=False,
        max_workers=kw.pop("max_workers", 2),
        max_retries=0,
        done_marker_path=str(tmp_path / f"tracking_{task_id}"),
        resource_tracking=True,
        **kw,
    )


def _summary(server, task_id):
    return server.last_tracking_summary["per_task"][task_id]


def test_profile_block_attaches_measurements():
    block = daisy.Block(
        daisy.Roi([0], [40]), daisy.Roi([0], [10]), daisy.Roi([0], [10])
    )
    assert block.stats is None
    with daisy.profile_block(block):
        _busy(block)
    stats = block.stats
    assert stats is not None
    assert stats.wall_seconds > 0
    assert stats.cpu_seconds >= 0
    # Reserved until NVML lands — must not be fabricated as 0.0.
    assert stats.gpu_util_pct != stats.gpu_util_pct  # NaN


def test_profile_block_keeps_the_innermost_measurement():
    """Nesting must be harmless: an explicit inner scope wins over an
    automatic outer one rather than being overwritten."""
    block = daisy.Block(
        daisy.Roi([0], [40]), daisy.Roi([0], [10]), daisy.Roi([0], [10])
    )
    with daisy.profile_block(block):
        with daisy.profile_block(block):
            _busy(block)
        inner = block.stats.wall_seconds
        _busy(block)
    assert block.stats.wall_seconds == inner


def test_profile_block_records_a_failing_block():
    """A block that raises still gets measured — what it cost before
    giving up is worth recording — and the exception still propagates."""
    block = daisy.Block(
        daisy.Roi([0], [40]), daisy.Roi([0], [10]), daisy.Roi([0], [10])
    )
    with pytest.raises(ValueError):
        with daisy.profile_block(block):
            raise ValueError("boom")
    assert block.stats is not None


@pytest.mark.parametrize(
    "worker_processes", [False, True], ids=["threads", "subprocess"]
)
def test_blocks_counted_in_both_worker_modes(tmp_path, worker_processes):
    """The regression this overhaul fixes: subprocess mode used to report
    zero blocks because only the in-process worker loop counted."""
    task = _task(
        f"count_{int(worker_processes)}", tmp_path, worker_processes=worker_processes
    )
    server = daisy.Server()
    states = server.run_blockwise([task], progress=False)
    s = _summary(server, task.task_id)

    assert states[task.task_id].completed_count == 4
    assert int(s["blocks"]) == 4, "block count must match what the scheduler completed"
    assert s["has_stats"]
    assert s["total_cpu_secs"] > 0
    assert s["mean_block_ms"] > 0


def test_external_worker_is_measured_without_extra_code(tmp_path):
    """A hand-written cluster worker using the documented
    `daisy.Client()` loop gets measured at the Client seam — no
    `profile_block` call of its own."""
    worker = tmp_path / "worker.py"
    worker.write_text(
        textwrap.dedent("""
        import daisy
        client = daisy.Client()
        while True:
            with client.acquire_block() as block:
                if block is None:
                    break
                acc = 0
                for i in range(200_000):
                    acc += i * i
        """)
    )

    def spawn():
        subprocess.run([sys.executable, str(worker)], check=True)

    task = _task("external", tmp_path, process_function=spawn)
    server = daisy.Server()
    states = server.run_blockwise([task], progress=False)
    s = _summary(server, "external")

    assert states["external"].completed_count == 4
    assert int(s["blocks"]) == 4
    assert s["has_stats"], "external workers must be measured too"


def test_stats_are_readable_from_the_tracking_group(tmp_path):
    """Measurements land in the zarr group, indexed by block grid
    position, readable by anything that speaks zarr v3."""
    zarr = _require_zarr_v3()
    numpy = pytest.importorskip("numpy")

    task = _task("persisted", tmp_path, worker_processes=False)
    server = daisy.Server()
    server.run_blockwise([task], progress=False)

    group = zarr.open_group(str(tmp_path / "tracking_persisted"), mode="r")
    names = sorted(name for name, _ in group.arrays())
    assert {"done", "failures", "cpu_seconds", "wall_seconds"} <= set(names)

    done = group["done"][:]
    assert done.shape == (4,)
    assert int(done.sum()) == 4, "every block marked done"
    wall = group["wall_seconds"][:]
    assert numpy.all(wall > 0), f"per-block wall times should be populated: {wall}"


def test_failures_are_counted_per_block(tmp_path):
    """Failure counts are kept whenever tracking is on, next to done."""
    zarr = _require_zarr_v3()

    def fail_one(block):
        if block.block_id[1] == 0:
            raise RuntimeError("this block always fails")

    task = daisy.Task(
        task_id="failing",
        total_roi=daisy.Roi([0], [40]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=fail_one,
        read_write_conflict=False,
        max_workers=1,
        max_retries=1,
        max_worker_restarts=6,
        done_marker_path=str(tmp_path / "tracking_failing"),
        worker_processes=False,
    )
    server = daisy.Server()
    states = server.run_blockwise([task], progress=False)
    assert states["failing"].failed_count >= 1

    group = zarr.open_group(str(tmp_path / "tracking_failing"), mode="r")
    failures = group["failures"][:]
    # max_retries=1 → the bad block is attempted twice, both counted.
    assert int(failures[0]) >= 2, f"expected repeated failures at block 0: {failures}"
    assert int(failures[1:].sum()) == 0, "healthy blocks must not accrue failures"


def test_resource_tracking_without_a_directory_is_a_config_error(tmp_path):
    """resource_tracking has nowhere to write without tracking enabled;
    say so instead of silently measuring nothing."""
    task = daisy.Task(
        task_id="nowhere",
        total_roi=daisy.Roi([0], [20]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=_busy,
        read_write_conflict=False,
        max_workers=1,
        done_marker_path=False,
        resource_tracking=True,
    )
    with pytest.raises(RuntimeError, match="resource_tracking"):
        daisy.Server().run_blockwise([task], progress=False)


def test_tracking_without_resource_tracking_omits_stat_arrays(tmp_path):
    """Done/failure tracking alone must not create the stat arrays, and
    the summary must report no resource figures."""
    task = daisy.Task(
        task_id="plain",
        total_roi=daisy.Roi([0], [40]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=_busy,
        read_write_conflict=False,
        max_workers=1,
        done_marker_path=str(tmp_path / "tracking_plain"),
        worker_processes=False,
    )
    server = daisy.Server()
    server.run_blockwise([task], progress=False)

    root = tmp_path / "tracking_plain"
    assert (root / "done").is_dir()
    assert (root / "failures").is_dir()
    assert not (root / "cpu_seconds").exists()

    s = _summary(server, "plain")
    assert int(s["blocks"]) == 4, "blocks are counted even without resource stats"
    assert not s["has_stats"]
