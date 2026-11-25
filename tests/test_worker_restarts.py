"""Worker-restart cap and task-abandonment tests.

The runner replaces a crashed worker until `Task.max_worker_restarts`
is hit, after which it stops respawning. If the task still has
unprocessed blocks at that point it is *abandoned* — the runner
accounts the remainder as failed so `is_done()` becomes true and the
run loop can exit.

Worker-function mode is used here because it gives us a clean way to
crash a worker thread (raising from the 0-arg callable causes
`PySpawnWorker::spawn` to return `Err`, which counts as a dirty exit).
"""

import time

import daisy
import pytest
from conftest import linear_task


def _always_crashing_worker():
    """Always raises, so the worker thread exits dirty every time."""
    time.sleep(0.01)
    raise RuntimeError("simulated crash")


def test_restart_cap_terminates_task_and_marks_remaining_failed():
    """A worker-function task whose worker always crashes should stop
    respawning after `max_worker_restarts` and abandon the rest."""
    task = linear_task(
        "crashy",
        _always_crashing_worker,
        8,
        max_workers=1,
        max_retries=0,
        max_worker_restarts=3,  # tight cap so the test stays fast
    )

    server = daisy.Server()
    states = server.run_blockwise([task], progress=False)

    state = states["crashy"]
    assert state.is_done(), "expected task to terminate, run loop hung"
    # The cap caps *restarts*, not deaths. With max_workers=1 and
    # cap=3, the runner spawns the worker, lets it die, restarts up
    # to 3 times, then stops. That's 4 dirty exits total — initial
    # spawn + 3 restarts.
    assert state.worker_restart_count == 3, state.worker_restart_count
    assert state.worker_failure_count == 4, state.worker_failure_count
    # Remaining blocks accounted as orphaned by the abandonment
    # transition. They never got attempted-and-failed; the runner
    # just gave up, so they go in the orphan bucket — `failed_count`
    # is reserved for blocks that hit `max_retries` during real
    # processing.
    assert state.total_block_count == 8
    assert state.orphaned_count > 0
    assert state.failed_count == 0
    assert state.completed_count == 0
    # ...and the state says why the task stopped
    assert state.abandoned
    assert "restart cap" in state.abandon_reason
    assert "simulated crash" in state.last_worker_error


def test_task_abandonment_does_not_block_other_tasks():
    """An abandoned upstream task with `max_worker_restarts=0` should
    still let an independent peer task complete normally."""

    def good_worker(block):
        time.sleep(0.001)

    bad = linear_task(
        "bad",
        _always_crashing_worker,
        4,
        max_workers=1,
        max_retries=0,
        max_worker_restarts=0,  # zero — first failure abandons
    )

    good = linear_task(
        "good",
        good_worker,
        4,
        max_workers=2,
        max_retries=0,
    )

    server = daisy.Server()
    states = server.run_blockwise([bad, good], progress=False)

    # Abandoned task's remaining blocks are accounted as orphaned,
    # not failed — see abandon.md.
    assert states["bad"].orphaned_count > 0
    assert states["bad"].is_done()

    assert states["good"].is_done()
    assert states["good"].failed_count == 0
    assert states["good"].completed_count == 4


def test_abandoned_upstream_unblocks_downstream():
    """When an upstream task is abandoned (cap exhausted, no alive
    workers, blocks remaining), `is_done()` would otherwise hang
    forever for any downstream task whose input was never produced.
    The runner must propagate the abandonment so downstream tasks'
    remaining blocks count as orphaned and `is_done()` returns true."""
    upstream = linear_task(
        "dead_upstream",
        _always_crashing_worker,
        4,
        max_workers=1,
        max_retries=0,
        max_worker_restarts=1,
    )

    def good_worker(block):
        pass

    downstream = linear_task(
        "downstream",
        good_worker,
        4,
        max_workers=2,
        max_retries=0,
        upstream_tasks=[upstream],
    )

    server = daisy.Server()
    # Hard timeout via signal would be cleaner but pytest-timeout
    # isn't installed. The bug surfaces as the run hanging until
    # SIGINT, so a long runtime here would still flag a regression
    # under CI's wall-clock limit. In practice this test completes
    # in a few hundred ms.
    states = server.run_blockwise([upstream, downstream], progress=False)

    assert states["dead_upstream"].is_done()
    # Both the directly-abandoned upstream and its transitively-
    # abandoned downstream account remaining blocks as orphaned.
    assert states["dead_upstream"].orphaned_count > 0
    assert states["dead_upstream"].failed_count == 0

    # The downstream task never got input, so is_done() must still
    # become true via orphan accounting.
    assert states["downstream"].is_done(), (
        "downstream hung after upstream was abandoned"
    )
    assert states["downstream"].orphaned_count > 0
    assert states["downstream"].completed_count == 0


def _block_holding_worker():
    """0-arg worker that acquires blocks one at a time and crashes
    while holding one. Reproduces the race between the in-flight
    block's release message and the worker-thread exit signal."""
    import time

    import daisy as g

    client = g.Client()
    while True:
        with client.acquire_block() as block:
            if block is None:
                return
            time.sleep(0.005)
            raise RuntimeError("crash holding a block")


def test_abandonment_handles_in_flight_block_release_race():
    """Regression: workers that crash while holding blocks send a
    `release_block` TCP message that races with the thread-exit
    signal on the server's mpsc channels. If `abandon_exhausted_tasks`
    runs first (worker_exit arm fires before msg_rx), the late
    release would otherwise push counts past `total_block_count` and
    flip `is_done()` back to false, hanging the run loop forever.

    Run several iterations to flush out the timing-dependent race.
    """
    for _ in range(3):
        task = linear_task(
            "race_test",
            _block_holding_worker,
            20,
            max_workers=4,
            max_retries=0,
            max_worker_restarts=2,
        )

        server = daisy.Server()
        states = server.run_blockwise([task], progress=False)
        state = states["race_test"]
        assert state.is_done(), (
            "run hung — late block release flipped is_done() back to false"
        )
        assert state.worker_restart_count == 2


def _always_failing_block(block):
    raise RuntimeError("simulated block failure")


def test_block_function_failure_kills_worker_and_drives_abandonment():
    """A 1-arg `process_function` that always raises should drive
    the same restart-cap → abandonment cycle as a worker-function
    task. Each failed block exits the worker dirty, the runner
    counts it as a death, and refills until the cap is hit.

    Without this, a buggy block function would silently retry every
    block to its `max_retries` cap and the run would finish with
    every block accounted as failed but `worker_restart_count`
    stuck at zero. The user wouldn't see the abandonment they
    expected; the run would just slog through all blocks.
    """
    task = linear_task(
        "buggy_block_fn",
        _always_failing_block,
        8,
        max_workers=1,
        max_retries=0,
        max_worker_restarts=3,
    )

    server = daisy.Server()
    states = server.run_blockwise([task], progress=False)

    state = states["buggy_block_fn"]
    assert state.is_done()
    # Each failed block is one worker death. With max_retries=0 and
    # max_worker_restarts=3, the runner allows 3 restarts before
    # abandoning — i.e. up to 4 worker lifetimes, each consuming
    # exactly one block.
    assert state.worker_restart_count == 3
    assert state.worker_failure_count == 4
    # 4 blocks attempted-and-failed; the rest abandoned as orphaned.
    assert state.failed_count == 4
    assert state.orphaned_count == 4
    assert state.completed_count == 0


def _instantly_crashing_worker():
    """0-arg worker that crashes before acquiring any block."""
    raise RuntimeError("downstream worker crash on startup")


def test_healthy_upstream_does_not_repopulate_abandoned_downstream():
    """Race 2 from abandon.md.

    Downstream is directly abandoned while upstream is alive and
    still producing. Each upstream `release_block(Success)` would
    normally generate a ready block for downstream — without the
    `queue_ready_block` typestate gate, those bumps would push
    `downstream.ready_count` past zero after the abandon transition,
    flipping `is_done(downstream)` back to false and hanging the
    run loop.

    With the gate, blocks targeted at a non-running task are dropped
    silently; downstream stays `is_done()` and the run completes.
    """

    def healthy(block):
        pass

    upstream = linear_task(
        "healthy_upstream",
        healthy,
        8,
        max_workers=2,
        max_retries=0,
    )

    downstream = linear_task(
        "crashy_downstream",
        _instantly_crashing_worker,
        8,
        max_workers=1,
        max_retries=0,
        max_worker_restarts=0,
        upstream_tasks=[upstream],
    )

    server = daisy.Server()
    states = server.run_blockwise([upstream, downstream], progress=False)

    assert states["healthy_upstream"].is_done()
    assert states["crashy_downstream"].is_done(), (
        "downstream's ready_count was repopulated by upstream releases"
    )
    # Downstream's blocks accounted as orphaned by abandonment.
    assert states["crashy_downstream"].orphaned_count == 8
    assert states["crashy_downstream"].failed_count == 0


def test_abandonment_raises_from_run_blockwise_with_cause():
    """The convenience `run_blockwise` must raise on abandonment and the
    message must carry the task id, restart accounting, and the original
    worker error. `Server.run_blockwise` (tested above) stays non-raising
    for introspection."""

    def crash(block):
        raise ValueError("broken import on node xyz")

    task = linear_task(
        "crashy_raise",
        crash,
        8,
        max_workers=1,
        max_retries=0,
        max_worker_restarts=3,
    )

    with pytest.raises(RuntimeError) as excinfo:
        daisy.run_blockwise([task], progress=False)

    msg = str(excinfo.value)
    assert "crashy_raise" in msg and "was abandoned" in msg
    assert "3 restarts" in msg
    assert "broken import on node xyz" in msg
    assert "max_worker_restarts" in msg  # actionable advice


def test_worker_start_budget_bounds_clean_exit_churn(tmp_path):
    """A spawn function that returns cleanly without its worker ever
    processing a block (e.g. `subprocess.run(..., check=False)` around
    a command that can't start) must not respawn forever: total worker
    starts are hard-capped at max_workers + max_worker_restarts, no
    matter how or why previous workers exited.

    Starts are counted with one file each: the worker function runs in its
    own process, so an in-process counter would be incremented in the child
    and read as zero here."""
    startdir = tmp_path / "starts"
    startdir.mkdir()
    out = str(startdir)

    def broken_spawn():
        import os as _os
        import subprocess as _subprocess
        import sys as _sys
        from pathlib import Path as _Path

        _Path(out, f"start-{_os.getpid()}").touch()
        _subprocess.run(
            [_sys.executable, "/nonexistent/worker.py"],
            check=False,
            capture_output=True,
        )

    task = linear_task(
        "clean_churn",
        broken_spawn,
        8,
        max_workers=2,
        max_retries=0,
        max_worker_restarts=3,
    )
    server = daisy.Server()
    # The run must TERMINATE (pytest-timeout is the backstop).
    states = server.run_blockwise([task], progress=False)

    state = states["clean_churn"]
    assert state.is_done(), "expected task to terminate, run loop hung"
    starts = list(startdir.iterdir())
    assert len(starts) == 5, f"budget is 2 + 3 = 5 starts, saw {len(starts)}"
    assert state.worker_restart_count == 3
    # clean exits are not failures — worker_failure_count reports
    # dirty exits only
    assert state.worker_failure_count == 0
    assert state.completed_count == 0
    assert state.orphaned_count > 0
    assert state.failed_count == 0


def test_worker_recycling_consumes_the_start_budget():
    """Workers are expected to be long-running: a worker that exits
    cleanly after processing blocks still consumes start budget, and a
    task whose workers recycle themselves runs out of starts. This is
    intended semantics — fix the worker (or raise max_worker_restarts /
    resume via done markers), don't recycle it."""

    def one_block_then_quit():
        client = daisy.Client()
        # process exactly one block, then exit cleanly
        with client.acquire_block() as _block:  # noqa: F841 — acquiring IS the point
            pass

    task = linear_task(
        "recycle",
        one_block_then_quit,
        16,
        max_workers=2,
        max_retries=0,
        max_worker_restarts=3,  # start budget: 2 + 3 = 5
    )
    server = daisy.Server()
    states = server.run_blockwise([task], progress=False)

    state = states["recycle"]
    assert state.is_done()
    # 5 starts x 1 block each, then the task is abandoned with the
    # remaining 11 blocks orphaned.
    assert state.completed_count == 5, state.completed_count
    assert state.worker_restart_count == 3
    assert state.orphaned_count == 11, state.orphaned_count
    assert state.failed_count == 0


def test_queue_drained_exits_do_not_abandon():
    """Workers that exit because the queue drained must not push a
    completed task toward abandonment, even when far more workers were
    requested than there was work for them."""

    def fine(b):
        pass

    task = linear_task(
        "overprovisioned",
        fine,
        4,
        max_workers=16,  # >> block count
        max_retries=0,
        max_worker_restarts=0,
    )
    server = daisy.Server()
    states = server.run_blockwise([task], progress=False)
    state = states["overprovisioned"]
    assert state.is_done()
    assert state.completed_count == 4
    assert state.orphaned_count == 0
    assert state.failed_count == 0
    assert state.worker_failure_count == 0
    assert state.worker_restart_count == 0
    assert not state.abandoned
    assert state.abandon_reason is None
    assert state.last_worker_error is None


def test_no_more_worker_starts_than_available_work():
    """Requesting far more workers than there are blocks must not
    launch them all: workers beyond ready + processing can never be
    fed, and launching them just burns start budget and wall-clock
    (the run waits for every launched worker to connect and shut
    down). 8 blocks with max_workers=16 -> at most 8 starts."""

    def quick_worker():
        client = daisy.Client()
        while True:
            with client.acquire_block() as block:
                if block is None:
                    break

    task = linear_task(
        "overprovisioned_spawn",
        quick_worker,
        8,
        max_workers=16,
        max_retries=0,
    )
    server = daisy.Server()
    states = server.run_blockwise([task], progress=False)

    state = states["overprovisioned_spawn"]
    assert state.is_done()
    assert state.completed_count == 8
    assert state.orphaned_count == 0
    # never spawn workers that provably have nothing to do
    assert state.worker_start_count <= 8, state.worker_start_count


def test_downstream_task_ramps_up_as_upstream_completes():
    """The ready+processing spawn cap must not strand a downstream
    task: its ready_count starts at 0 and grows as upstream blocks
    complete; later rebalance calls (state changes / health tick) must
    keep spawning workers until the pipeline finishes."""
    upstream = linear_task(
        "ramp_upstream",
        lambda b: None,
        8,
        max_workers=2,
        max_retries=0,
    )
    downstream = linear_task(
        "ramp_downstream",
        lambda b: None,
        8,
        max_workers=4,
        max_retries=0,
    )
    pipeline = upstream + downstream
    server = daisy.Server()
    states = server.run_blockwise(pipeline, progress=False)

    assert states["ramp_upstream"].completed_count == 8
    assert states["ramp_downstream"].completed_count == 8
    assert states["ramp_downstream"].orphaned_count == 0
