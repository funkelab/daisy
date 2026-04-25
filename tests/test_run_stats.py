"""Resource-utilisation report sanity tests.

Each test exercises the dispatcher with a deliberately-shaped workload
and inspects the `last_run_stats` dict captured on the Server.
"""

import time

import gerbera


def _make_server():
    return gerbera.Server()


def test_stats_dict_has_expected_keys():
    server = _make_server()
    task = gerbera.Task(
        task_id="basic",
        total_roi=gerbera.Roi([0], [40]),
        read_roi=gerbera.Roi([0], [10]),
        write_roi=gerbera.Roi([0], [10]),
        process_function=lambda b: None,
        read_write_conflict=False,
        max_workers=2,
        max_retries=0,
    )
    server.run_blockwise([task])
    s = server.last_run_stats
    assert "process" in s
    assert "per_task" in s
    assert "per_worker" in s
    assert "basic" in s["per_task"]
    pt = s["per_task"]["basic"]
    for key in (
        "blocks_processed",
        "max_concurrent_workers",
        "total_block_time_secs",
        "total_wall_time_secs",
        "mean_block_ms",
        "block_ms_slope",
    ):
        assert key in pt, f"missing key {key}"
    assert pt["blocks_processed"] == 4


def test_constant_workload_reports_near_zero_slope():
    server = _make_server()
    task = gerbera.Task(
        task_id="flat",
        total_roi=gerbera.Roi([0], [200]),
        read_roi=gerbera.Roi([0], [10]),
        write_roi=gerbera.Roi([0], [10]),
        process_function=lambda b: time.sleep(0.001),
        read_write_conflict=False,
        max_workers=1,  # serial-ish for clean trend
        max_retries=0,
    )
    server.run_blockwise([task])
    s = server.last_run_stats["per_task"]["flat"]
    # 1 ms per block; slope should be tiny.
    assert s["mean_block_ms"] >= 0.5  # we know we waited at least 1ms
    assert abs(s["block_ms_slope"]) < 0.05, (
        f"expected near-zero slope on a constant workload, got {s['block_ms_slope']}"
    )


def test_slowing_workload_reports_positive_slope():
    """A workload whose per-block sleep grows linearly with the call
    index should produce a clearly positive slope."""
    counter = {"i": 0}

    def slowing(block):
        i = counter["i"]
        counter["i"] += 1
        # Block i sleeps (1 + 0.05 * i) ms — slope of +0.05 ms/block.
        time.sleep(0.001 + 0.00005 * i)

    server = _make_server()
    task = gerbera.Task(
        task_id="slowing",
        total_roi=gerbera.Roi([0], [400]),
        read_roi=gerbera.Roi([0], [10]),
        write_roi=gerbera.Roi([0], [10]),
        process_function=slowing,
        read_write_conflict=False,
        max_workers=1,  # serial so the trend is unambiguous
        max_retries=0,
    )
    server.run_blockwise([task])
    s = server.last_run_stats["per_task"]["slowing"]
    assert s["blocks_processed"] == 40
    # The injected slope is 0.05 ms/block; in practice we measure a bit
    # more (TCP roundtrip floor), but the *sign* and order of magnitude
    # should be right.
    assert s["block_ms_slope"] > 0.02, (
        f"expected positive slope on slowing workload, got {s['block_ms_slope']}"
    )


def test_per_worker_stats_present_and_blocks_account():
    server = _make_server()
    task = gerbera.Task(
        task_id="multi",
        total_roi=gerbera.Roi([0], [80]),
        read_roi=gerbera.Roi([0], [10]),
        write_roi=gerbera.Roi([0], [10]),
        process_function=lambda b: time.sleep(0.001),
        read_write_conflict=False,
        max_workers=4,
        max_retries=0,
    )
    server.run_blockwise([task])
    s = server.last_run_stats
    workers = s["per_worker"]
    assert all(w["task_id"] == "multi" for w in workers)
    total_processed = sum(int(w["blocks_processed"]) for w in workers)
    assert total_processed == 8
    # Every worker should have a non-zero wall_time recorded.
    assert all(w["wall_time_secs"] > 0 for w in workers)
