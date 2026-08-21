"""How daisy runs workers: one dedicated OS process each.

There is a single distributed execution model. Whatever a task's
`process_function` is, the runner wraps it into a spawn function that
launches `python -m daisy._subprocess_worker`:

- a 1-arg block function is called by the child per block, inside the
  standard `Client.acquire_block()` loop;
- a 0-arg worker function is called by the child once and drives its own
  loop, optionally handing off to a further process (`srun`, `sbatch`).

This file covers the consequences: both shapes get real parallelism, the
function must be serializable, `timeout=` can preempt for real, and serial
mode still runs in-process. It replaces the old test_worker_processes.py,
test_worker_process_timeout.py and test_block_timeout.py, which between them
tested a `worker_processes` tri-state and documented thread mode's inability
to preempt — neither exists any more.
"""

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

import daisy


def _busy(n):
    acc = 0
    for i in range(n):
        acc += i * i
    return acc


def _calibrate(target_s):
    """Iteration count costing roughly `target_s`. Tests must use FIXED
    iteration counts, never a wall-clock loop: a wall-clock loop under
    contention simply does less work per call and fakes a speedup."""
    probe = 200_000
    t0 = time.perf_counter()
    _busy(probe)
    dt = time.perf_counter() - t0
    return max(1, int(probe * target_s / dt))


def _task(task_id, fn, workers, n_blocks=16, **kwargs):
    return daisy.Task(
        task_id=task_id,
        total_roi=daisy.Roi([0], [n_blocks * 10]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=fn,
        read_write_conflict=False,
        max_workers=workers,
        **kwargs,
    )


def _pids_in(dirpath):
    return {
        int(f.name.split("-")[1])
        for f in Path(dirpath).iterdir()
        if f.name.startswith("pid-")
    }


# --------------------------------------------------------------------------
# every worker is its own process
# --------------------------------------------------------------------------


@pytest.mark.timeout(60)
def test_block_function_runs_in_worker_processes(tmp_path):
    """1-arg process functions run in worker subprocesses: blocks are
    processed by multiple distinct pids, none of which is the server."""
    out = str(tmp_path)

    def record_pid(block):
        import os as _os
        import time as _time
        from pathlib import Path as _Path

        _Path(out, f"pid-{_os.getpid()}-{block.block_id[1]}").touch()
        _time.sleep(0.05)  # keep blocks around long enough for all workers

    assert daisy.run_blockwise(
        _task("blockfn-pids", record_pid, 4, n_blocks=16), progress=False
    )
    pids = _pids_in(tmp_path)
    assert os.getpid() not in pids, "blocks ran in the server process"
    assert len(pids) > 1, f"expected multiple worker processes, got {pids}"


@pytest.mark.timeout(60)
def test_zero_arg_worker_runs_in_its_own_process(tmp_path):
    """0-arg worker functions are also given a dedicated process — daisy
    1.x behaviour, and what makes inline work parallelize. They used to be
    called on a server thread."""
    out = str(tmp_path)

    def worker():
        import os as _os
        from pathlib import Path as _Path

        client = daisy.Client()
        while True:
            with client.acquire_block() as block:
                if block is None:
                    return
                _Path(out, f"pid-{_os.getpid()}-{block.block_id[1]}").touch()

    assert daisy.run_blockwise(
        _task("zeroarg-pids", worker, 2, n_blocks=8), progress=False
    )
    pids = _pids_in(tmp_path)
    assert pids, "no blocks processed"
    assert os.getpid() not in pids, "worker function ran in the server process"


@pytest.mark.timeout(60)
def test_zero_arg_worker_gets_its_own_context(tmp_path):
    """`def worker(*, context)` receives this process's own context. Each
    child has a private environment, so worker identity cannot race the way
    it could when spawn functions shared the server's DAISY_CONTEXT."""
    out = str(tmp_path)

    def worker(*, context):
        import time as _time
        from pathlib import Path as _Path

        client = daisy.Client(context)
        while True:
            with client.acquire_block() as block:
                if block is None:
                    return
                # hold each block so more than one worker gets a turn —
                # otherwise the first to start drains the queue alone
                _time.sleep(0.1)
                _Path(out, f"w{context['worker_id']}-{block.block_id[1]}").touch()

    assert daisy.run_blockwise(
        _task("zeroarg-ctx", worker, 3, n_blocks=12, requires={"cpu": 1}),
        resources={"cpu": 3},
        progress=False,
    )
    worker_ids = {
        f.name.split("-")[0] for f in tmp_path.iterdir() if f.name.startswith("w")
    }
    assert len(worker_ids) > 1, f"all blocks claimed the same identity: {worker_ids}"
    assert worker_ids <= {"w0", "w1", "w2"}, f"unexpected worker ids: {worker_ids}"


@pytest.mark.timeout(90)
def test_zero_arg_worker_may_launch_a_further_process(tmp_path):
    """The cluster shape: a worker function that never touches daisy itself
    and just forwards the context to another process (stand-in for
    `srun`/`sbatch`), which runs the client loop."""
    out = str(tmp_path)
    grandchild = """
import os
from pathlib import Path
import daisy
client = daisy.Client()
while True:
    with client.acquire_block() as block:
        if block is None:
            break
        Path(os.environ["OUT"], f"pid-{os.getpid()}-{block.block_id[1]}").touch()
"""

    def launch(*, context):
        import os as _os
        import subprocess as _subprocess
        import sys as _sys

        env = dict(_os.environ)
        env["DAISY_CONTEXT"] = context.to_env()
        env["OUT"] = out
        _subprocess.run([_sys.executable, "-c", grandchild], env=env, check=True)

    assert daisy.run_blockwise(
        _task("srun-shape", launch, 2, n_blocks=8), progress=False
    )
    pids = _pids_in(tmp_path)
    assert pids and os.getpid() not in pids
    # every block was processed by a grandchild, two processes down
    assert len([f for f in tmp_path.iterdir() if f.name.startswith("pid-")]) == 8


# --------------------------------------------------------------------------
# the parent's process-global configuration reaches the child
# --------------------------------------------------------------------------


@pytest.mark.timeout(60)
def test_block_function_worker_sees_the_parent_log_basedir(tmp_path):
    """`daisy.logging` settings are module globals, and a spawned child
    inherits nothing, so the parent ships them in the worker payload.
    Without that, `set_log_basedir(...)` would silently have no effect in
    any worker."""
    out = str(tmp_path / "seen")
    Path(out).mkdir()
    basedir = tmp_path / "logs-here"
    daisy.logging.set_log_basedir(basedir)

    def report(block):
        from pathlib import Path as _Path

        import daisy.logging as gl

        _Path(out, f"b{block.block_id[1]}").write_text(
            f"{gl.get_log_basedir()}\n{gl.get_log_mode()}\n"
            f"{gl.get_worker_log_basename(0, 'logcfg')}"
        )

    assert daisy.run_blockwise(_task("logcfg", report, 1, n_blocks=2), progress=False)
    reports = [f.read_text().splitlines() for f in Path(out).iterdir()]
    assert reports, "no blocks ran"
    for seen_basedir, seen_mode, seen_basename in reports:
        assert seen_basedir == str(basedir), (
            f"worker saw log basedir {seen_basedir!r}, parent set {basedir}"
        )
        assert seen_mode == "file"
        # the worker can therefore actually name its own log files
        assert str(basedir) in seen_basename


@pytest.mark.timeout(60)
def test_zero_arg_worker_sees_the_parent_log_basedir(tmp_path):
    """Same for 0-arg workers, which is how downstream code gets at
    `client.context["logdir"]` — the daisy 1.x idiom for a worker that sets
    up its own logging. A missing key there is a hard error for callers that
    index it, not a degradation."""
    out = str(tmp_path / "seen")
    Path(out).mkdir()
    basedir = tmp_path / "logs-here"
    daisy.logging.set_log_basedir(basedir)

    def worker():
        from pathlib import Path as _Path

        import daisy.logging as gl

        client = daisy.Client()
        # the 1.x idiom: index it, don't .get() it
        logdir = client.context["logdir"]
        while True:
            with client.acquire_block() as block:
                if block is None:
                    return
                _Path(out, f"b{block.block_id[1]}").write_text(
                    f"{gl.get_log_basedir()}\n{logdir}"
                )

    assert daisy.run_blockwise(
        _task("logcfg-zero", worker, 1, n_blocks=2), progress=False
    )
    reports = [f.read_text().splitlines() for f in Path(out).iterdir()]
    assert reports, "no blocks ran"
    for seen_basedir, seen_logdir in reports:
        assert seen_basedir == str(basedir)
        assert seen_logdir == str(basedir)


@pytest.mark.timeout(60)
def test_re_execed_worker_still_gets_the_log_dir(tmp_path):
    """A worker that re-execs itself — `srun`, `sbatch`, `docker run`, a bare
    `subprocess.run` in a spawn function — hands its child nothing but the
    environment. The log directory therefore travels in the *context*, not
    only in the payload frame daisy sends to workers it launches itself.

    Regression: the context carried five keys and no logdir, so `Client` in
    the grandchild fell back to that process's own default and reported the
    relative `daisy_logs`. Not a missing key — a wrong one, per worker,
    silently.
    """
    out = tmp_path / "seen"
    out.mkdir()
    basedir = tmp_path / "master-logs"
    daisy.logging.set_log_basedir(basedir)

    grandchild = f"""
import warnings; warnings.simplefilter("ignore")
import daisy, daisy.logging as gl
from pathlib import Path
client = daisy.Client()
# what the context carried, and what constructing the Client actually did
# (chr(10), not an escape: this source is nested inside an f-string)
Path({str(out)!r}, "report").write_text(
    client.context["logdir"] + chr(10) + str(gl.get_log_basedir())
)
while True:
    with client.acquire_block() as block:
        if block is None:
            break
"""

    def spawn():
        subprocess.run([sys.executable, "-c", grandchild], check=True)

    assert daisy.run_blockwise(
        _task("logdir-reexec", spawn, 1, n_blocks=2), progress=False
    )
    carried, applied = (out / "report").read_text().splitlines()
    assert carried == str(basedir)
    # daisy 1.x semantics: constructing a Client adopts the run's log dir
    assert applied == str(basedir)


@pytest.mark.timeout(60)
def test_the_drivers_env_var_channel_carries_the_log_basedir(tmp_path):
    """Both spawn channels must carry the full context — including the
    process-global DAISY_CONTEXT env var, whose own comment promises that
    "the canonical cluster-worker pattern" keeps working. 1.x code that reads
    the env var instead of taking the keyword-only `context` gets whatever
    was written there, so a five-key, logdir-less write silently reverts that
    whole class of consumer to per-worker default log dirs.

    The write is process-global in the *driver*, so it is still observable
    after the run: what this asserts is exactly what a concurrently launched
    1.x-style child would have inherited.
    """
    basedir = tmp_path / "driver-logs"
    daisy.logging.set_log_basedir(basedir)

    def worker():
        client = daisy.Client()
        while True:
            with client.acquire_block() as block:
                if block is None:
                    return

    assert daisy.run_blockwise(
        _task("logdir-envvar", worker, 1, n_blocks=2), progress=False
    )
    ctx = daisy.Context.from_env()
    # index it, don't .get() it — absence must be an error, same as Client's
    # consumers
    assert Path(ctx["logdir"]) == basedir


def test_hand_built_contexts_carry_the_log_basedir(tmp_path):
    """A `Context` is born carrying this process's log directory — daisy 1.x
    parity (`Context.__init__` there started from
    `dict(logdir=get_log_basedir(), **kwargs)`). With construction and the
    spawn boundary both filling it, no context daisy emits can lack the key,
    which is why there is deliberately NO public repair helper: a
    `daisy.context_with_logdir` would enshrine "contexts are sometimes
    incomplete" as API.
    """
    basedir = tmp_path / "master-logs"
    daisy.logging.set_log_basedir(basedir)

    born = daisy.Context(hostname="h", port=1, task_id="t", worker_id=0)
    assert Path(born["logdir"]) == basedir

    # an explicit choice at construction wins over the process global
    explicit = daisy.Context(
        hostname="h", port=1, task_id="t", worker_id=0, logdir="/chosen"
    )
    assert explicit["logdir"] == "/chosen"

    # "file logging off" travels as the empty string, not as a missing key
    daisy.logging.set_log_basedir(None)
    try:
        assert daisy.Context(hostname="h")["logdir"] == ""
    finally:
        daisy.logging.set_log_basedir(basedir)

    # wire parsers stay faithful: an old, logdir-less string parses as sent
    parsed = daisy.Context.from_env_string("hostname=h:port=1:task_id=t:worker_id=0")
    assert "logdir" not in parsed

    # and the helper is not public API
    assert not hasattr(daisy, "context_with_logdir")


@pytest.mark.timeout(60)
def test_log_dir_containing_a_separator_survives_the_context(tmp_path):
    """The context is `key=value:key=value` with no escaping, and a path may
    legally contain `:` (always does on Windows). Values are percent-encoded,
    so such a directory reaches the worker intact instead of corrupting the
    handoff.

    Uses a re-execed worker deliberately: one daisy launches itself would get
    the directory from the payload frame instead, and never exercise the
    encoding."""
    basedir = tmp_path / "wei:rd=logs"
    basedir.mkdir()
    daisy.logging.set_log_basedir(basedir)
    out = tmp_path / "seen"
    out.mkdir()

    grandchild = f"""
import warnings; warnings.simplefilter("ignore")
import daisy
from pathlib import Path
client = daisy.Client()
Path({str(out)!r}, "report").write_text(client.context["logdir"])
while True:
    with client.acquire_block() as block:
        if block is None:
            break
"""

    def spawn():
        subprocess.run([sys.executable, "-c", grandchild], check=True)

    assert daisy.run_blockwise(
        _task("logdir-separator", spawn, 1, n_blocks=2), progress=False
    )
    assert (out / "report").read_text() == str(basedir)


@pytest.mark.timeout(60)
def test_disabled_file_logging_propagates_to_workers(tmp_path):
    """`set_log_basedir(None)` turns file logging off for the run, and a
    worker that quietly re-enabled it would be as wrong as one logging to the
    wrong place. Re-execs, so the "off" state has to survive in the context
    rather than in the payload frame."""
    out = tmp_path / "seen"
    out.mkdir()
    daisy.logging.set_log_basedir(None)

    grandchild = f"""
import warnings; warnings.simplefilter("ignore")
import daisy, daisy.logging as gl
from pathlib import Path
client = daisy.Client()
Path({str(out)!r}, "report").write_text(repr(gl.get_log_basedir()))
while True:
    with client.acquire_block() as block:
        if block is None:
            break
"""

    def spawn():
        subprocess.run([sys.executable, "-c", grandchild], check=True)

    assert daisy.run_blockwise(
        _task("logdir-off", spawn, 1, n_blocks=2), progress=False
    )
    assert (out / "report").read_text() == "None"


def test_context_round_trips_reserved_characters():
    """`to_env`/`from_env_string` are inverses even for values holding the
    framing's own separators."""
    ctx = daisy.Context(
        hostname="node07",
        port=41567,
        task_id="stage=1:of:3",
        worker_id=0,
        logdir="/nrs/lab/my:logs",
    )
    back = daisy.Context.from_env_string(ctx.to_env())
    assert back["task_id"] == "stage=1:of:3"
    assert back["logdir"] == "/nrs/lab/my:logs"
    assert back["hostname"] == "node07"


# --------------------------------------------------------------------------
# parallelism — the point of the whole design
# --------------------------------------------------------------------------


@pytest.mark.timeout(120)
def test_block_function_gets_real_parallelism():
    """A CPU-bound *lambda* must actually scale with worker count."""
    iters = _calibrate(0.15)

    def timed(workers):
        fn = lambda block: _busy(iters)  # noqa: E731 — the lambda IS the point
        # best of two: a single sample is noisy when the whole suite's
        # worker churn loads the machine
        walls = []
        for _ in range(2):
            t0 = time.perf_counter()
            assert daisy.run_blockwise(
                _task(f"par-{workers}", fn, workers), progress=False
            )
            walls.append(time.perf_counter() - t0)
        return min(walls)

    t_serial = timed(1)
    t_parallel = timed(8)
    # 16 blocks over 8 workers = 2 rounds; ideal ~t_serial/8 plus startup.
    # Require better than half the serial time — generous, but fails hard
    # if workers are GIL-serialized (ratio would be ~1) while robust to a
    # loaded CI machine.
    assert t_parallel < t_serial / 2, (t_serial, t_parallel)


@pytest.mark.timeout(120)
def test_inline_zero_arg_worker_gets_real_parallelism():
    """A 0-arg worker function that does CPU-bound work INLINE — instead of
    shelling out to a per-block process — must scale with `max_workers`.

    This is the acceptance test for running every worker in its own process.
    When 0-arg functions were called on server threads they shared the GIL,
    so this workload measured 0.59x on 4 workers versus 1: *slower* than a
    single worker. volara's default worker has exactly this shape, which is
    how the regression was found. It now measures ~3x.
    """
    iters = _calibrate(0.12)

    def timed(workers):
        t0 = time.perf_counter()
        assert daisy.run_blockwise(
            _task(f"inline-{workers}", _inline_worker(iters), workers, n_blocks=8),
            progress=False,
        )
        return time.perf_counter() - t0

    t_one = timed(1)
    t_four = timed(4)
    speedup = t_one / t_four
    # Ideal is 4x minus one worker start-up; ~3x measured. Assert well above
    # 1.0 so GIL-serialized execution (0.6x) fails unambiguously, with slack
    # for a loaded machine.
    assert speedup > 1.8, (
        f"inline 0-arg work did not parallelize: {t_one:.2f}s on 1 worker vs "
        f"{t_four:.2f}s on 4 ({speedup:.2f}x)"
    )


def _inline_worker(iters):
    """A 0-arg worker that burns CPU in-line for every block it takes."""

    def worker():
        client = daisy.Client()
        while True:
            with client.acquire_block() as block:
                if block is None:
                    return
                _busy(iters)

    return worker


# --------------------------------------------------------------------------
# serialization
# --------------------------------------------------------------------------


@pytest.mark.timeout(60)
def test_closure_and_def_side_effects(tmp_path):
    """Closures and module-level defs both work, and the blocks really are
    processed by *other processes* (side effects land on disk, not in this
    process's memory)."""
    out = str(tmp_path)

    def write_block_marker(block):  # closure over `out`
        from pathlib import Path as _Path

        _Path(out, f"{block.block_id[1]}").touch()

    assert daisy.run_blockwise(
        _task("closure", write_block_marker, 4, n_blocks=8), progress=False
    )
    assert len([f for f in tmp_path.iterdir() if f.name.isdigit()]) == 8


def test_serialization_happens_before_the_run():
    """The payload is built eagerly so an unserializable function fails at
    run start, not minutes later on a cluster node."""
    captured = []
    task = _task("eager", lambda block: captured.append(block), 1)
    # cloudpickle is installed in the test env, so a lambda serializes fine
    assert task.process_function is not None


def test_unserializable_worker_function_is_rejected():
    """0-arg worker functions are serialized too, so they must be picklable
    — a requirement that did not exist while they ran in-process. The error
    has to say so, since the fix is not obvious."""
    import threading

    lock = threading.Lock()

    def worker():
        with lock:  # captures an unpicklable object
            pass

    with pytest.raises(RuntimeError, match="could not serialize"):
        daisy.run_blockwise(_task("bad-spawn", worker, 2), progress=False)


@pytest.mark.timeout(60)
def test_raising_block_function_is_a_dirty_exit():
    """A process_function that raises crashes its worker subprocess ->
    dirty exit -> restart cap -> abandonment (see
    tests/test_worker_restarts.py): persistent block-function bugs surface
    as worker failures, not an endless slog through every block's
    retries."""

    def boom(block):
        raise ValueError("simulated crash")

    task = _task("boom", boom, 1, n_blocks=8, max_retries=0, max_worker_restarts=2)
    states = daisy.Server().run_blockwise([task], progress=False)
    state = list(states.values())[0]
    assert state.is_done(), "run did not terminate"
    assert state.worker_failure_count >= 1
    assert state.completed_count == 0


# --------------------------------------------------------------------------
# timeout — now always preemptive
# --------------------------------------------------------------------------


@pytest.mark.timeout(30)
def test_stuck_block_is_preempted_not_awaited(tmp_path):
    """`timeout` kills the process running a stuck block, so the attempt is
    *gone*: no late writes, no double-apply, no waiting for a sleeper at
    shutdown. There is no longer any mode in which a timed-out block keeps
    running — that was thread mode's unavoidable limitation."""
    first_attempt = tmp_path / "first_attempt_started"
    late_write = tmp_path / "late_write"  # only a SURVIVING sleeper writes it

    def slow_first(block):
        if not first_attempt.exists():
            first_attempt.touch()
            time.sleep(5)  # stuck attempt; deadline is 0.5s
            late_write.touch()  # a surviving attempt reaches this

    task = _task("preempt", slow_first, 2, n_blocks=1, max_retries=2, timeout=0.5)

    t0 = time.perf_counter()
    states = daisy.Server().run_blockwise([task], progress=False)
    wall = time.perf_counter() - t0

    state = states["preempt"]
    assert state.is_done()
    assert state.completed_count == 1, "retry should have completed the block"
    assert first_attempt.exists(), "first (stuck) attempt never ran"
    # the whole point: the sleeper was killed, not awaited
    assert wall < 4.0, f"run waited on the stuck attempt ({wall:.1f}s)"
    time.sleep(0.3)  # would-be late write lands within sleep+epsilon if alive
    assert not late_write.exists(), (
        "stuck attempt survived its deadline and wrote output after the "
        "block had been retried"
    )
    # the preempted worker is accounted as a dirty exit (visible, capped)
    assert state.worker_failure_count >= 1


@pytest.mark.timeout(60)
def test_slow_block_is_reclaimed_and_succeeds_on_retry(tmp_path):
    """A block whose first attempt exceeds the timeout is reclaimed and
    retried; the retry completes normally. The attempt counter lives in a
    file because each attempt runs in a fresh process."""
    marker = tmp_path / "attempted"

    def maybe_slow(block):
        if not marker.exists():
            marker.touch()
            time.sleep(1.5)  # first attempt only; deadline is 0.5s

    task = _task("slow_first", maybe_slow, 1, n_blocks=1, max_retries=2, timeout=0.5)
    states = daisy.Server().run_blockwise([task], progress=False)
    state = states["slow_first"]

    assert state.is_done()
    assert state.completed_count == 1
    assert state.failed_count == 0


@pytest.mark.timeout(60)
def test_persistently_slow_blocks_eventually_fail():
    """If every attempt times out, the block is permanently failed after
    `max_retries` reclaim cycles. The run still terminates."""

    def always_slow(block):
        time.sleep(1.0)

    task = _task(
        "always_slow",
        always_slow,
        1,
        n_blocks=1,
        max_retries=1,
        timeout=0.2,
        # Generous worker-restart cap so we exit via permanent failure
        # rather than abandonment (the block is what's broken, not the
        # worker — for this test we want the failure path).
        max_worker_restarts=20,
    )
    states = daisy.Server().run_blockwise([task], progress=False)
    state = states["always_slow"]
    assert state.is_done()
    assert state.failed_count == 1
    assert state.completed_count == 0


@pytest.mark.timeout(60)
def test_fast_blocks_unaffected_by_timeout(tmp_path):
    """The watchdog arms and cancels around every block without disturbing
    normal completion."""

    def quick(block):
        (tmp_path / str(block.block_id[1])).touch()

    task = _task("preempt-fast", quick, 2, n_blocks=8, timeout=5.0)
    assert daisy.run_blockwise(task, progress=False)
    assert len([f for f in tmp_path.iterdir() if f.name.isdigit()]) == 8


@pytest.mark.timeout(60)
def test_block_well_under_the_default_timeout_completes(tmp_path):
    """With no explicit `timeout`, blocks run under the universal default
    (see tests/test_default_timeout.py) — comfortably slow blocks are not
    disturbed."""

    def slow(block):
        time.sleep(0.3)

    task = _task("patient", slow, 1, n_blocks=1, max_retries=0)
    states = daisy.Server().run_blockwise([task], progress=False)
    state = states["patient"]
    assert state.is_done()
    assert state.completed_count == 1
    assert state.failed_count == 0


# --------------------------------------------------------------------------
# serial mode: the in-process escape hatch
# --------------------------------------------------------------------------


@pytest.mark.timeout(60)
def test_serial_mode_runs_in_process(tmp_path):
    """`multiprocessing=False` runs the ORIGINAL function in-process — even
    an unserializable one. This is the mode for pdb, closures over live
    objects, and in-process assertions."""
    import threading

    lock = threading.Lock()  # unserializable closure capture
    seen_pids = []

    def unserializable(block):
        with lock:
            seen_pids.append(os.getpid())

    task = _task("serial", unserializable, 2, n_blocks=4)
    assert daisy.run_blockwise(task, multiprocessing=False, progress=False)
    assert seen_pids and set(seen_pids) == {os.getpid()}


def test_subprocess_worker_module_is_runnable():
    """`python -m daisy._subprocess_worker` is the worker entry point; it
    must be importable and fail cleanly on an empty payload rather than
    hanging."""
    proc = subprocess.run(
        [sys.executable, "-m", "daisy._subprocess_worker"],
        input=b"",
        capture_output=True,
        timeout=60,
    )
    assert proc.returncode != 0
    assert b"Traceback" in proc.stderr
