"""Tests for `Task(worker_processes=True)` — the subprocess-worker shim.

The default distributed mode runs 1-arg process functions on threads that
share the GIL, so CPU-bound python work does not parallelize. The shim
serializes the function (dill when available) and runs it in real worker
subprocesses launched via `python -m daisy._subprocess_worker`, with the
payload passed over an anonymous stdin pipe.
"""

import time

import pytest

import daisy


def _busy(n):
    acc = 0
    for i in range(n):
        acc += i * i
    return acc


def _calibrate(target_s):
    probe = 200_000
    t0 = time.perf_counter()
    _busy(probe)
    dt = time.perf_counter() - t0
    return max(1, int(probe * target_s / dt))


def _task(fn, workers, n_blocks=16, worker_processes=True, **kwargs):
    return daisy.Task(
        task_id=f"wp-{workers}-{n_blocks}",
        total_roi=daisy.Roi([0], [n_blocks * 10]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=fn,
        read_write_conflict=False,
        max_workers=workers,
        worker_processes=worker_processes,
        **kwargs,
    )


@pytest.mark.timeout(60)
def test_lambda_gets_real_parallelism():
    """A CPU-bound *lambda* must actually scale with worker count.

    Fixed-iteration busy work (never wall-clock-based: under GIL-shared
    threads a wall-clock loop just does less work per call and fakes a
    speedup)."""
    iters = _calibrate(0.15)

    def timed(workers):
        fn = lambda block: _busy(iters)  # noqa: E731 — the lambda IS the point
        # best of two: a single sample is noisy when the whole suite's
        # worker churn loads the machine
        walls = []
        for _ in range(2):
            t0 = time.perf_counter()
            assert daisy.run_blockwise([_task(fn, workers)], progress=False)
            walls.append(time.perf_counter() - t0)
        return min(walls)

    t_serial = timed(1)
    t_parallel = timed(8)
    # 16 blocks over 8 workers = 2 rounds; ideal ~t_serial/8 plus startup.
    # Require better than half the serial time — generous, but fails hard
    # if workers are GIL-serialized (ratio would be ~1) while robust to a
    # loaded CI machine.
    assert t_parallel < t_serial / 2, (t_serial, t_parallel)


@pytest.mark.timeout(60)
def test_closure_and_def_side_effects(tmp_path):
    """Closures and module-level defs both work, and the blocks really are
    processed by *other processes* (side effects land on disk, not in this
    process's memory)."""
    out = str(tmp_path)

    def write_block_marker(block):  # closure over `out`
        from pathlib import Path

        Path(out, f"{block.block_id[1]}").touch()

    assert daisy.run_blockwise(
        [_task(write_block_marker, 4, n_blocks=8)], progress=False
    )
    assert len([f for f in tmp_path.iterdir() if f.name.isdigit()]) == 8


@pytest.mark.timeout(60)
def test_raising_block_function_is_a_dirty_exit():
    """A process_function that raises crashes its worker subprocess ->
    dirty exit -> restart cap -> abandonment. Matches thread-worker
    semantics (see tests/test_worker_restarts.py): persistent block-
    function bugs surface as worker failures, not an endless slog
    through every block's retries."""

    def boom(block):
        raise ValueError("simulated crash")

    task = _task(boom, 1, n_blocks=8, max_retries=0, max_worker_restarts=2)
    server = daisy.Server()
    states = server.run_blockwise([task], progress=False)
    state = list(states.values())[0]
    assert state.is_done(), "run did not terminate"
    assert state.worker_failure_count >= 1
    assert state.completed_count == 0


def test_worker_processes_requires_block_function():
    """0-arg spawn functions already manage their own processes; the shim
    must reject them loudly rather than double-wrap."""

    def spawn():
        pass

    with pytest.raises(TypeError, match="1-argument"):
        _task(spawn, 1)


def test_serialization_happens_at_construction():
    """The payload is built eagerly so unserializable functions fail at
    Task construction, not minutes later on a cluster node."""
    captured = []
    task = _task(lambda block: captured.append(block), 1)
    # if we got here, a lambda serialized fine (dill available in tests);
    # the wrapped spawn function is a 0-arg callable
    assert task.process_function is not None


@pytest.mark.timeout(60)
def test_v1_compat_surface_accepts_worker_processes(tmp_path):
    """`import daisy` (the v1-compat surface) forwards the new kwarg."""
    out = str(tmp_path)

    def mark(block):
        from pathlib import Path

        Path(out, f"{block.block_id[1]}").touch()

    task = daisy.Task(
        task_id="wp-compat",
        total_roi=daisy.Roi([0], [40]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=mark,
        read_write_conflict=False,
        num_workers=2,  # v1 kwarg alias
        worker_processes=True,
    )
    assert daisy.run_blockwise([task], progress=False)
    assert len([f for f in tmp_path.iterdir() if f.name.isdigit()]) == 4


@pytest.mark.timeout(60)
def test_subprocess_workers_are_the_default(tmp_path):
    """1-arg process functions run in worker subprocesses BY DEFAULT:
    blocks are processed by multiple distinct pids, none of which is the
    server process."""
    import os

    out = str(tmp_path)

    def record_pid(block):
        import time as _time
        from pathlib import Path
        import os as _os

        Path(out, f"pid-{_os.getpid()}-{block.block_id[1]}").touch()
        _time.sleep(0.05)  # keep blocks around long enough for all workers

    task = daisy.Task(
        task_id="wp-default",
        total_roi=daisy.Roi([0], [160]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=record_pid,
        read_write_conflict=False,
        max_workers=4,
        # NOTE: no worker_processes kwarg — this tests the default
    )
    assert daisy.run_blockwise([task], progress=False)
    pids = {
        int(f.name.split("-")[1])
        for f in tmp_path.iterdir()
        if f.name.startswith("pid-")
    }
    assert os.getpid() not in pids, "blocks ran in the server process"
    assert len(pids) > 1, f"expected multiple worker processes, got {pids}"


@pytest.mark.timeout(60)
def test_worker_processes_false_runs_in_server_process(tmp_path):
    """Thread mode remains available as the explicit opt-out."""
    import os

    out = str(tmp_path)

    def record_pid(block):
        from pathlib import Path
        import os as _os

        Path(out, f"pid-{_os.getpid()}-{block.block_id[1]}").touch()

    task = daisy.Task(
        task_id="wp-threads",
        total_roi=daisy.Roi([0], [40]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=record_pid,
        read_write_conflict=False,
        max_workers=2,
        worker_processes=False,
    )
    assert daisy.run_blockwise([task], progress=False)
    pids = {
        int(f.name.split("-")[1])
        for f in tmp_path.iterdir()
        if f.name.startswith("pid-")
    }
    assert pids == {os.getpid()}


@pytest.mark.timeout(60)
def test_serial_mode_unaffected_by_default(tmp_path):
    """multiprocessing=False runs the ORIGINAL function in-process — even
    an unserializable one — regardless of the subprocess default."""
    import os
    import threading

    lock = threading.Lock()  # unserializable closure capture
    seen_pids = []

    def unserializable(block):
        with lock:
            seen_pids.append(os.getpid())

    task = daisy.Task(
        task_id="wp-serial",
        total_roi=daisy.Roi([0], [40]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=unserializable,
        read_write_conflict=False,
        max_workers=2,
    )
    assert daisy.run_blockwise([task], multiprocessing=False, progress=False)
    assert seen_pids and set(seen_pids) == {os.getpid()}
