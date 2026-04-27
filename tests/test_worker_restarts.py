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

import pytest

import gerbera


def _always_crashing_worker():
    """Always raises, so the worker thread exits dirty every time."""
    time.sleep(0.01)
    raise RuntimeError("simulated crash")


def test_restart_cap_terminates_task_and_marks_remaining_failed():
    """A worker-function task whose worker always crashes should stop
    respawning after `max_worker_restarts` and abandon the rest."""
    task = gerbera.Task(
        task_id="crashy",
        total_roi=gerbera.Roi([0], [80]),
        read_roi=gerbera.Roi([0], [10]),
        write_roi=gerbera.Roi([0], [10]),
        process_function=_always_crashing_worker,
        read_write_conflict=False,
        max_workers=1,
        max_retries=0,
        max_worker_restarts=3,  # tight cap so the test stays fast
    )

    server = gerbera.Server()
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


def test_task_abandonment_does_not_block_other_tasks():
    """An abandoned upstream task with `max_worker_restarts=0` should
    still let an independent peer task complete normally."""

    def good_worker(block):
        time.sleep(0.001)

    bad = gerbera.Task(
        task_id="bad",
        total_roi=gerbera.Roi([0], [40]),
        read_roi=gerbera.Roi([0], [10]),
        write_roi=gerbera.Roi([0], [10]),
        process_function=_always_crashing_worker,
        read_write_conflict=False,
        max_workers=1,
        max_retries=0,
        max_worker_restarts=0,  # zero — first failure abandons
    )

    good = gerbera.Task(
        task_id="good",
        total_roi=gerbera.Roi([0], [40]),
        read_roi=gerbera.Roi([0], [10]),
        write_roi=gerbera.Roi([0], [10]),
        process_function=good_worker,
        read_write_conflict=False,
        max_workers=2,
        max_retries=0,
    )

    server = gerbera.Server()
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
    upstream = gerbera.Task(
        task_id="dead_upstream",
        total_roi=gerbera.Roi([0], [40]),
        read_roi=gerbera.Roi([0], [10]),
        write_roi=gerbera.Roi([0], [10]),
        process_function=_always_crashing_worker,
        read_write_conflict=False,
        max_workers=1,
        max_retries=0,
        max_worker_restarts=1,
    )

    def good_worker(block):
        pass

    downstream = gerbera.Task(
        task_id="downstream",
        total_roi=gerbera.Roi([0], [40]),
        read_roi=gerbera.Roi([0], [10]),
        write_roi=gerbera.Roi([0], [10]),
        process_function=good_worker,
        read_write_conflict=False,
        max_workers=2,
        max_retries=0,
        upstream_tasks=[upstream],
    )

    server = gerbera.Server()
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
    assert states["downstream"].is_done(), \
        "downstream hung after upstream was abandoned"
    assert states["downstream"].orphaned_count > 0
    assert states["downstream"].completed_count == 0


def _block_holding_worker():
    """0-arg worker that acquires blocks one at a time and crashes
    while holding one. Reproduces the race between the in-flight
    block's release message and the worker-thread exit signal."""
    import gerbera as g
    import time
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
    for _ in range(5):
        task = gerbera.Task(
            task_id="race_test",
            total_roi=gerbera.Roi([0], [200]),
            read_roi=gerbera.Roi([0], [10]),
            write_roi=gerbera.Roi([0], [10]),
            process_function=_block_holding_worker,
            read_write_conflict=False,
            max_workers=4,
            max_retries=0,
            max_worker_restarts=2,
        )

        server = gerbera.Server()
        states = server.run_blockwise([task], progress=False)
        state = states["race_test"]
        assert state.is_done(), \
            "run hung — late block release flipped is_done() back to false"
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
    task = gerbera.Task(
        task_id="buggy_block_fn",
        total_roi=gerbera.Roi([0], [80]),
        read_roi=gerbera.Roi([0], [10]),
        write_roi=gerbera.Roi([0], [10]),
        process_function=_always_failing_block,
        read_write_conflict=False,
        max_workers=1,
        max_retries=0,
        max_worker_restarts=3,
    )

    server = gerbera.Server()
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


def test_block_function_success_does_not_kill_worker():
    """The worker only exits dirty on failure. A clean run should
    leave `worker_restart_count` and `worker_failure_count` at zero
    regardless of how many blocks went through."""
    def fine(block):
        pass

    task = gerbera.Task(
        task_id="clean_block_fn",
        total_roi=gerbera.Roi([0], [40]),
        read_roi=gerbera.Roi([0], [10]),
        write_roi=gerbera.Roi([0], [10]),
        process_function=fine,
        read_write_conflict=False,
        max_workers=2,
        max_retries=0,
    )

    server = gerbera.Server()
    states = server.run_blockwise([task], progress=False)
    state = states["clean_block_fn"]
    assert state.is_done()
    assert state.completed_count == 4
    assert state.worker_failure_count == 0
    assert state.worker_restart_count == 0


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

    upstream = gerbera.Task(
        task_id="healthy_upstream",
        total_roi=gerbera.Roi([0], [80]),
        read_roi=gerbera.Roi([0], [10]),
        write_roi=gerbera.Roi([0], [10]),
        process_function=healthy,
        read_write_conflict=False,
        max_workers=2,
        max_retries=0,
    )

    downstream = gerbera.Task(
        task_id="crashy_downstream",
        total_roi=gerbera.Roi([0], [80]),
        read_roi=gerbera.Roi([0], [10]),
        write_roi=gerbera.Roi([0], [10]),
        process_function=_instantly_crashing_worker,
        read_write_conflict=False,
        max_workers=1,
        max_retries=0,
        max_worker_restarts=0,
        upstream_tasks=[upstream],
    )

    server = gerbera.Server()
    states = server.run_blockwise([upstream, downstream], progress=False)

    assert states["healthy_upstream"].is_done()
    assert states["crashy_downstream"].is_done(), \
        "downstream's ready_count was repopulated by upstream releases"
    # Downstream's blocks accounted as orphaned by abandonment.
    assert states["crashy_downstream"].orphaned_count == 8
    assert states["crashy_downstream"].failed_count == 0


def test_clean_run_does_not_count_failures():
    """Workers that exit cleanly (drained queue) should not bump
    `worker_failure_count` regardless of how many run."""
    def fine(b):
        pass

    task = gerbera.Task(
        task_id="clean",
        total_roi=gerbera.Roi([0], [40]),
        read_roi=gerbera.Roi([0], [10]),
        write_roi=gerbera.Roi([0], [10]),
        process_function=fine,
        read_write_conflict=False,
        max_workers=4,
        max_retries=0,
    )
    server = gerbera.Server()
    states = server.run_blockwise([task], progress=False)
    assert states["clean"].is_done()
    assert states["clean"].completed_count == 4
    assert states["clean"].worker_failure_count == 0
