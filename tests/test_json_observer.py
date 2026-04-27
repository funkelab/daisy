"""`JsonProgressObserver` — streaming JSON progress for dashboards.

The observer writes one JSON object per task per `on_progress` /
`on_start` / `on_finish` event. The schema is fixed (see the
docstring on the class) — these tests pin the contract so dashboards
written against it don't quietly break.
"""

import io
import json

import daisy


def test_json_observer_writes_lines_to_stream():
    """Run a small task with `JsonProgressObserver` against an
    in-memory `StringIO` and verify the output is line-delimited
    JSON, one event per task per call, with the expected keys."""
    buf = io.StringIO()

    def fine(block):
        pass

    task = daisy.Task(
        task_id="json_test",
        total_roi=daisy.Roi([0], [40]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=fine,
        read_write_conflict=False,
        max_workers=1,
        max_retries=0,
    )

    obs = daisy.JsonProgressObserver(stream=buf)
    server = daisy.Server()
    server.run_blockwise([task], progress=obs)

    lines = [ln for ln in buf.getvalue().splitlines() if ln.strip()]
    assert lines, "expected at least one JSON line emitted"

    events = [json.loads(ln) for ln in lines]
    # Schema sanity per line.
    required = {
        "t", "event", "task", "total", "ready", "processing",
        "completed", "skipped", "failed", "orphaned",
        "restarts", "failures",
    }
    for ev in events:
        assert required.issubset(ev.keys()), f"missing keys in {ev}"
        assert ev["task"] == "json_test"
        assert ev["event"] in ("start", "progress", "finish")

    # Must have exactly one start event and one finish event for the
    # one task in the run.
    starts = [ev for ev in events if ev["event"] == "start"]
    finishes = [ev for ev in events if ev["event"] == "finish"]
    assert len(starts) == 1
    assert len(finishes) == 1

    # The finish event must reflect the completed counts.
    assert finishes[0]["completed"] == 4
    assert finishes[0]["failed"] == 0


def test_json_observer_writes_to_file_path(tmp_path):
    """The `path=` constructor opens, writes, and closes the file."""
    out = tmp_path / "progress.jsonl"

    def fine(block):
        pass

    task = daisy.Task(
        task_id="path_test",
        total_roi=daisy.Roi([0], [20]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=fine,
        read_write_conflict=False,
        max_workers=1,
        max_retries=0,
    )

    obs = daisy.JsonProgressObserver(path=str(out))
    server = daisy.Server()
    server.run_blockwise([task], progress=obs)

    assert out.exists()
    text = out.read_text()
    assert text, "expected non-empty output file"
    # Every line must parse as JSON.
    for line in text.splitlines():
        if not line.strip():
            continue
        ev = json.loads(line)
        assert ev["task"] == "path_test"


def test_json_observer_path_and_stream_are_mutually_exclusive():
    """Passing both `path=` and `stream=` is a configuration error
    we catch up front rather than silently picking one."""
    import pytest
    buf = io.StringIO()
    with pytest.raises(ValueError):
        daisy.JsonProgressObserver(path="/tmp/x.jsonl", stream=buf)
