"""User-facing pure-data classes (Task, Scheduler, Client, Context).

`Task` and `Scheduler` are direct re-exports of their `_rs.*`
PyO3-backed counterparts — no Python wrapper. `Client` keeps a thin
Python wrapper because its `acquire_block()` context manager performs
per-worker traceback logging through `daisy.logging`, which is the
"logging integration" carve-out that's deliberately Python-side.

Backwards-compat aliases (e.g. `num_workers=` on Task) live in
`daisy.v1_compat`, not here.
"""

import copy
import inspect
import logging
import warnings
from contextlib import contextmanager
from pathlib import Path
from typing import cast

import daisy._daisy as _rs
from daisy import logging as _worker_log

logger = logging.getLogger(__name__)

# Traceback strings that ride in BlockFailed messages are capped so a
# pathological stack (recursion, huge locals in repr) can't bloat the
# wire message or the run summary. Keep the END of the traceback — the
# raise site is the useful part.
_TB_MAX_LINES = 50
_TB_MAX_BYTES = 8192


def _capped_traceback() -> str:
    import traceback

    tb = traceback.format_exc()
    lines = tb.splitlines()
    if len(lines) > _TB_MAX_LINES:
        lines = ["... (traceback truncated) ..."] + lines[-_TB_MAX_LINES:]
        tb = "\n".join(lines)
    if len(tb) > _TB_MAX_BYTES:
        tb = "... (traceback truncated) ...\n" + tb[-_TB_MAX_BYTES:]
    return tb


def set_tracking_basedir(path) -> None:
    """Set the global base directory for per-task block tracking.

    When a `Task` is constructed with `tracking_path=None` (the default)
    and a basedir is set, that task's tracking group lives at
    `<basedir>/<task_id>`: which blocks are done, how often each failed,
    and — with `resource_tracking=True` — what each block cost. Set to
    `None` to disable auto-resolution, which leaves tracking off unless a
    task passes an explicit path.

    Tracking is opt-in on purpose: persistent skip-state anchored to a
    default location meant a rerun of changed code could silently skip
    everything.
    """
    _rs.set_done_marker_basedir(str(path) if path is not None else None)


def get_tracking_basedir() -> Path | None:
    """The global block-tracking base directory, or None if unset."""
    p = _rs.get_done_marker_basedir()
    return p if p is None else Path(p)


def set_done_marker_basedir(path) -> None:
    """Deprecated alias for `set_tracking_basedir`.

    Renamed because the directory holds more than done state now —
    failure counts and per-block resource measurements live beside it.
    """
    warnings.warn(
        "set_done_marker_basedir() is deprecated; use set_tracking_basedir(). "
        "The directory now holds failure counts and resource measurements "
        "alongside the done markers.",
        DeprecationWarning,
        stacklevel=2,
    )
    set_tracking_basedir(path)


def get_done_marker_basedir() -> Path | None:
    """Deprecated alias for `get_tracking_basedir`."""
    warnings.warn(
        "get_done_marker_basedir() is deprecated; use get_tracking_basedir().",
        DeprecationWarning,
        stacklevel=2,
    )
    return get_tracking_basedir()


# Direct re-exports — no Python wrappers. The Rust types provide the
# full user-facing surface (constructor, getters, reset, pipeline
# operators, copy.copy support, attribute setters for process_function
# / upstream_tasks so `_wrap_for_worker_logging` and Pipeline
# materialize work).
Roi = _rs.Roi
Coordinate = _rs.Coordinate
Block = _rs.Block
BlockStatus = _rs.BlockStatus
BlockwiseDependencyGraph = _rs.BlockwiseDependencyGraph
DependencyGraph = _rs.DependencyGraph
TaskState = _rs.TaskState
Scheduler = _rs.Scheduler
Context = _rs.Context

# positional index of `check_function` in the `_rs.Task` constructor
# signature (task_id, total_roi, read_roi, write_roi, process_function,
# check_function, ...)
_CHECK_FN_ARG_INDEX = 5


class Task(_rs.Task):
    """`_rs.Task` plus Python-side construction-time checks and the
    ``tracking_path`` alias.

    Every worker on the distributed run paths is a dedicated OS process:
    a 1-arg block function is serialized and run by
    ``daisy._subprocess_worker`` in the standard acquire/process/release
    loop, and a 0-arg worker function is serialized and called in its own
    process (where it may in turn ``srun``/``sbatch`` something else).
    There is no in-process worker mode, so CPU-bound python scales with
    ``max_workers`` and ``timeout=`` can preempt a stuck block for real.

    Serial execution (``run_blockwise(..., multiprocessing=False)``)
    calls the original function in-process, single-threaded — the mode for
    pdb, closures over live objects, and in-process assertions.
    """

    def __new__(cls, *args, **kwargs):
        # `tracking_path` is the canonical name; the Rust constructor still
        # takes `done_marker_path`, kept as a deprecated alias because the
        # directory now holds failure counts and resource measurements too.
        if "tracking_path" in kwargs:
            if "done_marker_path" in kwargs:
                raise TypeError(
                    "pass either tracking_path or done_marker_path, not both "
                    "(done_marker_path is the deprecated alias)"
                )
            kwargs["done_marker_path"] = kwargs.pop("tracking_path")
        elif "done_marker_path" in kwargs:
            warnings.warn(
                "Task(done_marker_path=...) is deprecated; use "
                "Task(tracking_path=...). The directory now holds failure "
                "counts and resource measurements alongside the done markers.",
                DeprecationWarning,
                stacklevel=2,
            )
        check_fn = kwargs.get("check_function")
        if check_fn is None and len(args) > _CHECK_FN_ARG_INDEX:
            check_fn = args[_CHECK_FN_ARG_INDEX]
        if check_fn is not None:
            warnings.warn(
                "Task(check_function=...) runs your callable on the server "
                "for EVERY block, and its result persists nowhere — every "
                "rerun pays the full check cost again. For resuming "
                "interrupted or repeated runs, prefer the built-in done "
                "markers (Task(tracking_path=...) or "
                "daisy.set_tracking_basedir(...)): one mmap'd byte per "
                "block, written on completion, checked in ~microseconds. "
                "Keep check_function only when the ground truth genuinely "
                "lives in your output data (e.g. verifying non-empty zarr "
                "chunks written by an earlier pipeline).",
                UserWarning,
                stacklevel=2,
            )
        return cast("Task", super().__new__(cls, *args, **kwargs))

    def __init__(self, *args, **kwargs):
        # PyO3 constructs via __new__; override so object.__init__
        # doesn't reject the constructor kwargs.
        pass


def _block_fn_arity(fn):
    """Positional arity of a process function: 0 means a worker function
    that drives its own loop, anything else means a block function called
    once per block. Mirrors the dispatch in `py_task.rs::to_core`."""
    if not callable(fn):
        return 0
    try:
        fn_args = inspect.getfullargspec(fn).args
    except TypeError:
        return 0  # builtin or C callable with no introspectable signature
    return len([a for a in fn_args if a != "self"])


def _wrap_for_subprocess_workers(task):
    """Return a shallow clone of `task` whose `process_function` is replaced
    by a 0-arg spawn function that runs the original in a dedicated worker
    subprocess.

    Applied by the *distributed* run paths to every task, whatever the
    function's arity — subprocesses are the only distributed execution
    model. A 1-arg block function is run by the child in the standard
    `Client.acquire_block()` loop; a 0-arg worker function is simply called
    by the child, which keeps daisy 1.x's process-per-worker semantics and
    lets the function shell out to `srun`/`sbatch` from there.

    Serial execution runs the original function in-process instead, so it
    never comes through here.
    """
    fn = task.process_function
    if fn is None:
        return task

    from daisy._worker_processes import make_spawn_function

    arity = 0 if _block_fn_arity(fn) == 0 else 1
    clone = copy.copy(task)
    clone.process_function = make_spawn_function(
        fn, arity=arity, timeout=task.timeout_secs
    )
    return clone


_V1_UPSTREAM_ATTR = "_v1_upstream_tasks"


def _record_task_upstream(task, upstream):
    """Record v1.x-style `upstream_tasks=` on the task itself.
    `_rs.Task` carries `__dict__` (pyclass(dict)), so we just stash
    the list as a Python attribute. Lifetime ties to the task —
    GC of the task naturally clears the attribute too."""
    if upstream:
        setattr(task, _V1_UPSTREAM_ATTR, list(upstream))


def _get_task_upstream(task):
    """Return the recorded upstream list (or None) for a task."""
    return getattr(task, _V1_UPSTREAM_ATTR, None)


def _to_pipeline(x):
    """Coerce a `Pipeline`, a `Task`, or a list/tuple of tasks into a
    `_rs.Pipeline`. List inputs honour `Task(upstream_tasks=[...])`
    via the v1.x-compat side-table."""
    if isinstance(x, _rs.Pipeline):
        return x
    if isinstance(x, _rs.Task):
        return _rs.Pipeline.from_task(x)
    if isinstance(x, (list, tuple)):
        return _build_pipeline_from_tasks(x)
    raise TypeError(
        f"expected a Pipeline, a Task, or a list of tasks; got {type(x).__name__}"
    )


def _build_pipeline_from_tasks(tasks):
    """Build a `_rs.Pipeline` from a flat list of tasks, walking the
    side-table for v1.x-style upstream declarations. Used by
    `run_blockwise([task1, task2, ...])` to bridge the v1.x calling
    convention to the v2 Pipeline-only runtime.

    Strategy: collect every task transitively reachable through the
    upstream side-table; start with a parallel union of all tasks (no
    edges); then for each edge (up, down) union in a sequential
    pair-pipeline `up + down`. The pipeline composition operators
    deduplicate tasks by Python identity, so each edge contributes
    exactly the (up, down) edge to the merged DAG without
    duplicating tasks.
    """
    seen: dict[int, _rs.Task] = {}
    edges: list[tuple[int, int]] = []

    def visit(t):
        if id(t) in seen:
            return
        seen[id(t)] = t
        for up in _get_task_upstream(t) or ():
            edges.append((id(up), id(t)))
            visit(up)

    for t in tasks:
        visit(t)

    if not seen:
        return _rs.Pipeline()
    task_iter = iter(seen.values())
    pipe = _rs.Pipeline.from_task(next(task_iter))
    for t in task_iter:
        pipe = pipe | _rs.Pipeline.from_task(t)
    for up_id, down_id in edges:
        up = seen[up_id]
        down = seen[down_id]
        # Parallel-union an `up + down` pair-pipeline. The `+`
        # introduces the edge; the `|` (vs. `+`) avoids creating
        # spurious edges from `pipe`'s current outputs to `up`.
        pipe = pipe | (_rs.Pipeline.from_task(up) + _rs.Pipeline.from_task(down))
    return pipe


class Client:
    """Worker-side client. Reads connection info from the
    `DAISY_CONTEXT` env var (or accepts an explicit `Context`),
    connects to the scheduler over TCP, and yields blocks via a
    context manager that handles status bookkeeping and routes
    failure tracebacks into the per-worker log.

    Every acquired block is watched automatically: when the task has a
    block timeout (`Task(timeout=...)`, default 600s), a watchdog kills
    this worker process if the block is still unreleased after that
    long — true preemption, even for code stuck inside C. This covers
    every loop built on this Client, including hand-written cluster
    workers; the server reclaims and retries the block on the same
    deadline.

    A server that is already gone at construction time (connection
    refused) is treated as "the run has ended": construction does NOT
    raise — a WARNING is logged, `connected` is False, and
    `acquire_block()` yields None immediately, so the canonical worker
    loop exits cleanly. This is the normal fate of straggler cluster
    jobs that start after the run drained; it must not register as a
    worker failure. A missing or malformed `DAISY_CONTEXT` still
    raises (`KeyError` / `ValueError`) — that's a configuration error,
    not a race. Mid-run connection losses also still raise.

    The acquire_block context manager is implemented in Python
    because it integrates with `daisy.logging` (the "logging"
    carve-out for the otherwise all-Rust runtime)."""

    def __init__(self, context=None):
        if context is None:
            context = Context.from_env()
        # Constructing a Client adopts the run's log directory, exactly as
        # daisy 1.x did (`daisy/client.py`: `set_log_basedir(
        # self.context["logdir"])`). The server puts the master's directory in
        # every worker context, so this holds for every worker whatever
        # launched it — daisy's own subprocess workers, a job at the far end
        # of an `srun`, a hand-written worker on a cluster node. Without it,
        # `set_log_basedir(...)` in the driver would configure only the
        # processes daisy happens to spawn itself, and workers that connect on
        # their own would scatter logs beside whatever cwd they started in.
        #
        # An empty value means the master disabled file logging. An absent
        # key can only come from `from_env` of a wire string older than the
        # key (hand-built contexts get logdir at construction now); this
        # process's own setting stands there, and applying it is a no-op. A
        # worker that wants its own location calls `set_log_basedir(...)`
        # after constructing its Client, as in 1.x.
        from daisy.logging import get_log_basedir, set_log_basedir

        if "logdir" not in context:
            basedir = get_log_basedir()
            context["logdir"] = "" if basedir is None else str(basedir)
        set_log_basedir(context["logdir"] or None)
        self.context = context
        self.host = context["hostname"]
        self.port = int(context["port"])
        self.worker_id = int(context["worker_id"])
        self.task_id = context["task_id"]
        # Measure blocks only when the task asked for it. Without this the
        # server would receive stats it discards, so every worker would pay
        # for a feature nobody enabled. Absent key (a hand-built Context, or
        # an older server) means "off".
        self.resource_tracking = str(context.get("resource_tracking", "0")) in (
            "1",
            "true",
            "True",
        )
        try:
            # The worker_id rides along so the server can tie this TCP
            # peer back to the spawn call it is blocking on (worker-slot
            # retirement on block timeout, fire-and-forget spawn
            # detection). Hand-built contexts with ids outside the
            # server-assigned range are simply never matched.
            self._client = _rs.SyncClient(
                self.host,
                self.port,
                self.task_id,
                self.worker_id if self.worker_id >= 0 else None,
            )
        except ConnectionRefusedError:
            self._client = None
            logger.warning(
                "daisy server at %s:%s is not reachable; assuming the run "
                "has ended — this worker will exit without processing "
                "blocks",
                self.host,
                self.port,
            )

    @property
    def connected(self) -> bool:
        """False if the server was already gone at construction, or after
        `disconnect()`."""
        return self._client is not None and self._client.is_connected()

    @contextmanager
    def acquire_block(self):
        if self._client is None:
            # server was gone at construction: behave exactly like the
            # normal end-of-work signal
            yield None
            return
        block = self._client.acquire_block()
        if block is None:
            yield None
            return
        reported = False
        # Measure the block here so every worker that goes through this
        # loop is covered: daisy's own subprocess shim and any hand-written
        # cluster worker alike. Measuring inside the worker is what makes
        # the numbers mode-independent, and doing it at this single seam is
        # why 0-arg workers need no extra code. A user who wants tighter
        # scoping can still use `daisy.profile_block(block)` inside their
        # function — the first measurement attached wins.
        profiler = _rs.profile_block(block) if self.resource_tracking else None
        if profiler is not None:
            profiler.__enter__()
        try:
            yield block
            if block.status == BlockStatus.PROCESSING:
                block.status = BlockStatus.SUCCESS
        except BaseException as e:
            block.status = BlockStatus.FAILED
            try:
                _worker_log.logger.warning(
                    "block %s failed: %s",
                    block.block_id,
                    e,
                )
                _worker_log.emit_failure(
                    f"block {block.block_id} failed:\n"
                    + _worker_log.format_traceback(
                        type(e),
                        e,
                        e.__traceback__,
                    )
                )
            except Exception:
                pass
            # Send the failure WITH its formatted traceback so the
            # server (and the run summary / abandonment error) can show
            # the cause without a trip to the worker logs. BlockFailed
            # is a complete block return — do not also release below.
            try:
                self._client.report_failure(block, _capped_traceback())
                reported = True
            except Exception:
                pass
            raise
        finally:
            # Attach before the block goes back: the server reads stats off
            # the released block, and a failed block's cost is worth
            # recording too.
            if profiler is not None:
                profiler.__exit__(None, None, None)
            if block.status != BlockStatus.SUCCESS:
                block.status = BlockStatus.FAILED
            if not reported:
                self._client.release_block(block)

    def __del__(self):
        try:
            if self._client is not None:
                self._client.disconnect()
        except Exception:
            pass


def _wrap_for_worker_logging(task):
    """Return a shallow copy of `task` (a `_rs.Task`) whose
    `process_function` is wrapped so that stdout/stderr emitted during
    the call go to the worker's log files, and any exception is
    routed through `daisy.logging.emit_failure` before re-raising.

    Operates on `_rs.Task` instances. Tasks are pure data with no
    inter-task knowledge in v2 — DAG dependencies live on the
    Pipeline, so this function only needs to wrap the single task's
    process_function (no upstream recursion).
    """
    if task.process_function is None:
        return task

    orig = task.process_function
    task_id = task.task_id
    argspec = inspect.getfullargspec(orig)
    nargs = len([a for a in argspec.args if a != "self"])
    # spawn functions may declare a keyword-only `context` parameter
    # (race-free worker identity); mirror it so the Rust caller still
    # sees the request and forward it through.
    orig_wants_context = "context" in (argspec.kwonlyargs or [])

    if nargs == 0:

        def wrapped(*, context=None):
            with _worker_log._WorkerLogContext(task_id):
                try:
                    if orig_wants_context:
                        return orig(context=context)
                    return orig()
                except BaseException as e:
                    _worker_log.logger.warning(
                        "worker function %s failed: %s",
                        task_id,
                        e,
                    )
                    _worker_log.emit_failure(
                        _worker_log.format_traceback(
                            type(e),
                            e,
                            e.__traceback__,
                        )
                    )
                    raise
    else:

        def wrapped(block):
            with _worker_log._WorkerLogContext(task_id):
                try:
                    return orig(block)
                except BaseException as e:
                    _worker_log.logger.warning(
                        "block %s failed: %s",
                        block.block_id,
                        e,
                    )
                    _worker_log.emit_failure(
                        f"block {block.block_id} failed:\n"
                        + _worker_log.format_traceback(
                            type(e),
                            e,
                            e.__traceback__,
                        )
                    )
                    raise

    wrapped.__name__ = getattr(orig, "__name__", "process_function")
    wrapped.__qualname__ = getattr(orig, "__qualname__", wrapped.__name__)

    clone = copy.copy(task)
    clone.process_function = wrapped
    return clone
