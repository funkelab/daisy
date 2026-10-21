"""Per-worker lifecycle timing in the run report.

The server stamps every worker-lifecycle event it already sees (spawn,
first acquire, each dispatch and return, the shutdown reply, the spawn
thread's exit) and folds them into startup / idle / compute / teardown per
worker. Nothing crosses the wire for this, so these tests drive real
subprocess workers and check the figures are plausible, consistent with
the block counts, and persisted (``worker_id`` / ``sent_at`` /
``returned_at`` arrays and ``runs.jsonl``).
"""

import json
import time
from pathlib import Path

import daisy
import pytest


def _sleepy(_block):
    time.sleep(0.05)


def _task(task_id, tmp_path, fn=_sleepy, *, tracked=True, n=80, **kw):
    kwargs = dict(
        task_id=task_id,
        total_roi=daisy.Roi([0], [n]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=fn,
        read_write_conflict=False,
        max_workers=kw.pop("max_workers", 2),
        max_retries=0,
    )
    if tracked:
        kwargs["tracking_path"] = str(tmp_path / f"tracking_{task_id}")
        kwargs["resource_tracking"] = True
    kwargs.update(kw)
    return daisy.Task(**kwargs)


def _run(task, **kw):
    server = daisy.Server()
    states = server.run_blockwise([task], progress=False, **kw)
    return states, server.last_report[task.task_id]


def test_every_worker_gets_a_full_lifecycle(tmp_path):
    """Two workers draining eight sleeping blocks: both connect (startup),
    both take blocks, both exit cleanly after the shutdown reply
    (teardown), and their block counts add up to the task's."""
    states, t = _run(_task("life", tmp_path))
    assert states["life"].completed_count == 8
    w = t.workers
    assert w.count == 2
    assert w.exits == {
        "clean": 2,
        "error": 0,
        "timeout": 0,
        "interrupted": 0,
        "running": 0,
    }
    assert w.unmanaged_clients == 0
    assert sorted(w.by_id) == [0, 1]
    for rec in w.by_id.values():
        assert rec.exit == "clean"
        assert rec.startup is not None and rec.startup > 0, rec
        assert rec.teardown is not None and rec.teardown >= 0, rec
        assert rec.blocks_failed == 0
        assert rec.idle.n == rec.compute.n == rec.blocks_ok, rec
    assert sum(r.blocks_ok for r in w.by_id.values()) == 8
    assert w.startup.n == 2 and w.teardown.n == 2
    # Pooled per-block samples: one per block handed out / returned.
    assert w.idle.n == 8 and w.compute.n == 8


def test_compute_idle_and_overhead_are_plausible(tmp_path):
    """Blocks that sleep 50 ms must show ≈50 ms of round-trip compute per
    block, a per-block overhead (round trip minus the worker's own wall
    measurement) far smaller than the block, and idle gaps that are small
    because the scheduler always has a block ready."""
    _, t = _run(_task("plausible", tmp_path))
    w, b = t.workers, t.blocks
    assert 0.045 < w.compute.mean < 0.5, w.compute
    assert w.compute.min >= 0.045, w.compute
    assert b.overhead.n == 8
    assert 0 <= b.overhead.mean < 0.05, b.overhead
    # overhead = round trip − wall, so the round trip is the larger one
    assert w.compute.mean >= b.wall.mean, (w.compute, b.wall)
    assert w.idle.mean < 0.2, w.idle
    assert w.startup.mean > 0.005, w.startup  # an interpreter has to come up


def _crash_on_connect():
    """A 0-arg worker that dies before ever acquiring a block."""
    raise RuntimeError("simulated crash at startup")


def test_crashed_worker_has_an_error_exit_and_no_teardown(tmp_path):
    """A worker that dies before connecting has no startup and no teardown,
    and is counted under ``exits["error"]``; the report still covers it."""
    task = _task(
        "crashy",
        tmp_path,
        fn=_crash_on_connect,
        tracked=False,
        max_workers=1,
        max_worker_restarts=1,
    )
    states, t = _run(task)
    assert states["crashy"].abandoned
    w = t.workers
    assert w.count == 2  # the first worker and its one allowed restart
    assert w.exits["error"] == 2, w.exits
    for rec in w.by_id.values():
        assert rec.exit == "error"
        assert rec.startup is None
        assert rec.teardown is None
        assert rec.blocks == 0
    assert w.startup.n == 0 and w.teardown.n == 0


def _record_my_worker(block):
    """Write this worker's id next to the block's spatial id so the test can
    check the server's `worker_id` array against ground truth."""
    import os

    import daisy

    out = Path(os.environ["DAISY_TEST_OUT"])
    wid = daisy.Context.from_env()["worker_id"]
    (out / f"block-{block.block_id[1]}").write_text(str(wid))
    time.sleep(0.01)


def test_worker_id_on_disk_matches_who_ran_each_block(tmp_path, monkeypatch):
    out = tmp_path / "who"
    out.mkdir()
    monkeypatch.setenv("DAISY_TEST_OUT", str(out))
    task = _task("who", tmp_path, fn=_record_my_worker, max_workers=3)
    _, t = _run(task)

    seen = set()
    for i in range(8):
        truth = int((out / f"block-{i}").read_text())
        rec = t.block((i,))
        assert rec is not None and rec.done
        assert rec.worker_id == truth, (i, rec)
        assert rec.sent_at is not None and rec.returned_at is not None
        assert rec.returned_at - rec.sent_at >= 0.009, rec  # the 10 ms sleep
        assert t.started_at <= rec.sent_at <= rec.returned_at <= t.finished_at
        seen.add(truth)
    assert seen <= set(t.workers.by_id)
    # ...and the per-worker rows agree with the per-block stamps.
    counted = {wid: 0 for wid in t.workers.by_id}
    for i in range(8):
        counted[int((out / f"block-{i}").read_text())] += 1
    assert {wid: r.blocks_ok for wid, r in t.workers.by_id.items()} == counted

    # The per-block table: one record per block, in grid order, with the
    # same stamps as the single lookups and timings consistent with them.
    rows = t.block_records()
    assert [r.block_id for r in rows] == [("who", i) for i in range(8)]
    for i, r in enumerate(rows):
        assert r.done and r.failures == 0
        assert r.worker_id == int((out / f"block-{i}").read_text())
        assert r.round_trip is not None and r.round_trip >= r.wall_seconds >= 0.009
        assert r.sent_at == t.block((i,)).sent_at
    assert t.blocks.round_trip.n == len(rows)
    assert abs(sum(r.round_trip for r in rows) - t.blocks.round_trip.total) < 1e-6

    # The arrays are plain zarr v3, readable without daisy.
    zarr = pytest.importorskip("zarr")
    if int(zarr.__version__.split(".")[0]) >= 3:
        g = zarr.open_group(str(tmp_path / "tracking_who"), mode="r")
        wids = g["worker_id"][:]
        assert [int(w) for w in wids] == [
            int((out / f"block-{i}").read_text()) for i in range(8)
        ]
        assert (g["returned_at"][:] >= g["sent_at"][:]).all()


def test_block_lookup_forms_agree_and_degrade_honestly(tmp_path):
    """`TaskReport.block` takes a Block, a grid coordinate or a spatial id;
    an untracked task has nothing to look up; a report rebuilt from JSON
    has no task to read through and says so."""
    _, t = _run(_task("forms", tmp_path, max_workers=1))
    block = daisy.Block(
        daisy.Roi([0], [80]), daisy.Roi([30], [10]), daisy.Roi([30], [10])
    )
    by_block = t.block(block)
    by_coord = t.block((3,))
    by_id = t.block(3)

    def key(rec):
        # BlockStats compares by identity; compare what it holds instead
        return (
            rec.block_id,
            rec.done,
            rec.failures,
            rec.worker_id,
            rec.sent_at,
            rec.returned_at,
            rec.wall_seconds,
            rec.peak_rss_bytes,
        )

    assert key(by_block) == key(by_coord) == key(by_id)
    assert by_block.block_id == ("forms", 3)
    assert by_block.wall_seconds >= 0.045
    assert by_block.peak_rss_bytes > 0
    assert by_block.worker_id == 0
    with pytest.raises(ValueError, match="grid coordinate"):
        t.block((1, 2))
    with pytest.raises(TypeError):
        t.block("3")

    _, plain = _run(_task("plain", tmp_path, tracked=False, max_workers=1))
    assert plain.block((0,)) is None
    assert plain.block_records() == []

    rebuilt = daisy.TaskReport.from_dict(plain.to_dict())
    with pytest.raises(RuntimeError, match="no task attached"):
        rebuilt.block((0,))


def test_runs_jsonl_gets_one_line_per_run_and_round_trips(tmp_path):
    task = _task("history", tmp_path, max_workers=1)
    _, first = _run(task)
    _, second = _run(task)  # resumed: every block skipped via done markers
    path = tmp_path / "tracking_history" / "runs.jsonl"
    lines = path.read_text().splitlines()
    assert len(lines) == 2
    rows = [json.loads(line) for line in lines]
    assert daisy.TaskReport.from_dict(rows[0]).to_dict() == first.to_dict()
    assert daisy.TaskReport.from_dict(rows[1]).to_dict() == second.to_dict()
    assert rows[0]["workers"]["count"] == 1
    assert rows[1]["started_at"] >= rows[0]["finished_at"]
    # task.report() is the last line, read from the file.
    assert task.report().to_dict() == second.to_dict()
    assert task.tracking_path == str(tmp_path / "tracking_history")
    # The rerun executed nothing: count still folds the skipped blocks in
    # (as the execution summary's `completed` does), `skipped` says so, and
    # there are no timing samples for it; the per-block records keep the
    # stamps of the run that actually did the work.
    assert first.blocks.skipped == 0 and first.blocks.round_trip.n == 8
    assert second.blocks.count == 8 and second.blocks.skipped == 8
    assert second.blocks.round_trip.n == 0 and second.blocks.wall.n == 0
    assert "8 blocks (8 skipped)" in second.render()
    assert second.block((3,)).sent_at == first.block((3,)).sent_at
    assert [r.block_id for r in second.block_records()] == [
        ("history", i) for i in range(8)
    ]


def test_serial_mode_has_no_workers(tmp_path):
    task = _task("serial", tmp_path, max_workers=1)
    ok = daisy.run_blockwise([task], multiprocessing=False, progress=False)
    assert ok
    t = task.report()
    assert t.tracked and t.blocks.has_stats and t.blocks.count == 8
    assert t.workers.count == 0
    assert t.workers.by_id == {}
    assert not t.blocks.round_trip  # nothing was dispatched over TCP
    assert not t.blocks.overhead  # no round trip to subtract from
    assert "workers" not in t.render() and "round trip" not in t.render()
    assert "wall" in t.render()

    plain = _task("serial_plain", tmp_path, tracked=False, max_workers=1)
    daisy.run_blockwise([plain], multiprocessing=False, progress=False)
    section = plain.report().render()
    assert section.strip().startswith("serial_plain   8 blocks")
    assert (
        "workers" not in section
        and "round trip" not in section
        and "wall " not in section
    )


def test_panel_always_prints_store_free_figures(tmp_path, capfd):
    """Round trip and worker timing need no tracking store, so every
    summary shows them; the in-worker measurements (wall, CPU, RSS, IO,
    overhead, trend) appear only with resource_tracking=True."""
    daisy.run_blockwise([_task("measured", tmp_path)], progress=False)
    out = capfd.readouterr().out
    assert "Resource Utilization" in out
    assert "blocks   round trip" in out
    assert "wall" in out and "cpu" in out and "peak RSS" in out
    assert "workers  startup" in out
    assert "  exit" in out and "clean" in out

    daisy.run_blockwise([_task("plain", tmp_path, tracked=False)], progress=False)
    out = capfd.readouterr().out
    assert "Execution Summary" in out
    assert "Resource Utilization" in out
    assert "blocks   round trip" in out
    assert "workers  startup" in out and "clean" in out
    for needs_store in ("cpu", "peak RSS", "io read", "overhead", "trend"):
        assert needs_store not in out, needs_store
    t = _last_run_task("plain", tmp_path).report()
    assert t.blocks.round_trip.n == 8
    assert 0.045 < t.blocks.round_trip.mean < 0.5
    assert not t.blocks.wall


def _last_run_task(task_id, tmp_path):
    """A fresh Task object for a task that already ran: what a later
    process would build to call `report()` — no in-memory state at all."""
    return _task(task_id, tmp_path, tracked=False)


def test_report_is_persisted_for_every_entry_point_and_readable_later(tmp_path):
    """Every entry point leaves runs.jsonl behind — in the tracking store
    for tracked tasks, next to the worker logs otherwise — and a brand-new
    Task object reads it back, as a later process would."""
    import daisy.logging as dl

    daisy.run_blockwise([_task("a", tmp_path, tracked=False)], progress=False)
    daisy.Server().run_blockwise([_task("b", tmp_path, tracked=False)], progress=False)
    daisy.run_blockwise(
        [_task("c", tmp_path, tracked=False)], multiprocessing=False, progress=False
    )
    daisy.run_blockwise([_task("d", tmp_path)], progress=False)

    for tid in "abc":
        path = dl.get_log_basedir() / tid / "runs.jsonl"
        assert path.is_file(), tid
        fresh = _last_run_task(tid, tmp_path)
        assert fresh.tracking_path is None
        t = fresh.report()
        assert t is not None and t.task_id == tid and t.blocks.count == 8
    assert _last_run_task("c", tmp_path).report().workers.count == 0
    assert _last_run_task("a", tmp_path).report().workers.count >= 1

    # The tracked task's report is in its store, not under the logs.
    assert not (dl.get_log_basedir() / "d" / "runs.jsonl").exists()
    assert (tmp_path / "tracking_d" / "runs.jsonl").is_file()
    fresh = _task("d", tmp_path)
    t = fresh.report()
    assert t.tracked and t.blocks.has_stats
    assert t.block((0,)).done  # the store lookup works through the fresh task

    # Two runs → two lines; report() is the latest.
    daisy.run_blockwise([_task("a", tmp_path, tracked=False)], progress=False)
    lines = (dl.get_log_basedir() / "a" / "runs.jsonl").read_text().splitlines()
    assert len(lines) == 2
    assert (
        _last_run_task("a", tmp_path).report().started_at
        == json.loads(lines[1])["started_at"]
    )

    # Nothing to persist (no store, file logging off): None from a fresh
    # task, the in-memory copy from the task that ran.
    dl.set_log_basedir(None)
    try:
        ran = _task("e", tmp_path, tracked=False)
        daisy.run_blockwise([ran], progress=False)
        assert _last_run_task("e", tmp_path).report() is None
        assert ran.report() is not None and ran.report().blocks.count == 8
    finally:
        dl.set_log_basedir(tmp_path / "daisy_logs")


def test_task_run_blockwise_and_report(tmp_path):
    """The one-object form: build a task, run it, read its figures off it."""
    task = _task("solo", tmp_path, tracked=False)
    assert task.report() is None
    assert task.run_blockwise(progress=False) is True
    t = task.report()
    assert t is not None and t.task_id == "solo"
    assert t.blocks.count == 8 and t.blocks.round_trip.n == 8
    assert t.workers.count >= 1 and t.workers.exits["clean"] == t.workers.count
    assert t.worker(0).startup > 0

    # Abandonment raises, but the partial report still lands on the task.
    crashy = _task(
        "crashy_solo",
        tmp_path,
        fn=_crash_on_connect,
        tracked=False,
        max_workers=1,
        max_worker_restarts=0,
    )
    with pytest.raises(RuntimeError, match="abandoned"):
        crashy.run_blockwise(progress=False)
    assert crashy.report().workers.exits["error"] == 1
    assert crashy.report().blocks.count == 0
