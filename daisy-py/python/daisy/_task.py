"""User-facing pure-data classes (Task, Scheduler, Client, Context).

`Task` and `Scheduler` are direct re-exports of their `_rs.*`
PyO3-backed counterparts — no Python wrapper. `Client` keeps a thin
Python wrapper because its `acquire_block()` context manager performs
per-worker traceback logging through `daisy.logging`, which is the
"logging integration" carve-out that's deliberately Python-side.

Backwards-compat aliases (e.g. `num_workers=` on Task) live in
`daisy.v1_compat`, not here.
"""

from contextlib import contextmanager
import copy
import inspect
import logging
import warnings
from pathlib import Path

import daisy._daisy as _rs
from daisy import logging as _worker_log

logger = logging.getLogger(__name__)


def set_done_marker_basedir(path) -> None:
    """Set the global base directory for per-task done-marker arrays.

    When a `Task` is constructed with `done_marker_path=None` (the
    default) and a basedir is set, the marker for that task lives at
    `<basedir>/<task_id>`. Set to `None` to disable auto-resolution
    against this basedir — `daisy.logging.LOG_BASEDIR` is then the
    final fallback before "no marker".
    """
    _rs.set_done_marker_basedir(str(path) if path is not None else None)


def get_done_marker_basedir() -> Path | None:
    p = _rs.get_done_marker_basedir()
    return p if p is None else Path(p)


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

# positional indices in the `_rs.Task` constructor signature (task_id,
# total_roi, read_roi, write_roi, process_function, check_function,
# read_write_conflict, fit, max_workers, max_retries, timeout, ...)
_PROCESS_FN_ARG_INDEX = 4
_CHECK_FN_ARG_INDEX = 5
_TIMEOUT_ARG_INDEX = 10


class Task(_rs.Task):
    """`_rs.Task` plus the Python-side ``worker_processes`` option.

    ``Task(process_function=fn, worker_processes=True, max_workers=N)``
    runs the 1-arg block function in N real worker *subprocesses*
    (via ``daisy._worker_processes``) instead of the default GIL-sharing
    threads — real CPU parallelism for lambdas and closures, without
    hand-writing a worker script and spawn function. Everything else
    delegates to the Rust constructor unchanged.
    """

    def __new__(cls, *args, worker_processes=False, **kwargs):
        check_fn = kwargs.get("check_function")
        if check_fn is None and len(args) > _CHECK_FN_ARG_INDEX:
            check_fn = args[_CHECK_FN_ARG_INDEX]
        if check_fn is not None:
            warnings.warn(
                "Task(check_function=...) runs your callable on the server "
                "for EVERY block, and its result persists nowhere — every "
                "rerun pays the full check cost again. For resuming "
                "interrupted or repeated runs, prefer the built-in done "
                "markers (Task(done_marker_path=...) or "
                "daisy.set_done_marker_basedir(...)): one mmap'd byte per "
                "block, written on completion, checked in ~microseconds. "
                "Keep check_function only when the ground truth genuinely "
                "lives in your output data (e.g. verifying non-empty zarr "
                "chunks written by an earlier pipeline).",
                UserWarning,
                stacklevel=2,
            )
        if worker_processes:
            from daisy._worker_processes import make_spawn_function

            if len(args) > _PROCESS_FN_ARG_INDEX:
                fn = args[_PROCESS_FN_ARG_INDEX]
            else:
                fn = kwargs.get("process_function")
            nargs = None
            if callable(fn):
                fn_args = inspect.getfullargspec(fn).args
                nargs = len([a for a in fn_args if a != "self"])
            if nargs != 1:
                raise TypeError(
                    "worker_processes=True requires a 1-argument (block) "
                    "process_function; 0-argument spawn functions already "
                    "manage their own worker processes"
                )
            if len(args) > _TIMEOUT_ARG_INDEX:
                timeout = args[_TIMEOUT_ARG_INDEX]
            else:
                timeout = kwargs.get("timeout")
            spawn = make_spawn_function(fn, timeout=timeout)
            if len(args) > _PROCESS_FN_ARG_INDEX:
                args = (
                    args[:_PROCESS_FN_ARG_INDEX]
                    + (spawn,)
                    + args[_PROCESS_FN_ARG_INDEX + 1:]
                )
            else:
                kwargs["process_function"] = spawn
        return super().__new__(cls, *args, **kwargs)

    def __init__(self, *args, **kwargs):
        # PyO3 constructs via __new__; override so object.__init__
        # doesn't reject the constructor kwargs.
        pass


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
        "expected a Pipeline, a Task, or a list of tasks; "
        f"got {type(x).__name__}"
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

    The acquire_block context manager is implemented in Python
    because it integrates with `daisy.logging` (the "logging"
    carve-out for the otherwise all-Rust runtime)."""

    def __init__(self, context=None):
        if context is None:
            context = Context.from_env()
        # daisy 1.x callers reach for `client.context["logdir"]` to set up
        # per-worker logging themselves. v2 owns logging internally and no
        # longer puts logdir in the worker context, so fall back to the
        # process-global `get_log_basedir()` (which fork-inheriting workers
        # see as the value the master set) when the key is missing.
        if "logdir" not in context:
            from daisy.logging import get_log_basedir
            basedir = get_log_basedir()
            if basedir is not None:
                context["logdir"] = str(basedir)
        self.context = context
        self.host = context["hostname"]
        self.port = int(context["port"])
        self.worker_id = int(context["worker_id"])
        self.task_id = context["task_id"]
        self._client = _rs.SyncClient(self.host, self.port, self.task_id)

    @contextmanager
    def acquire_block(self):
        block = self._client.acquire_block()
        if block is None:
            yield None
            return
        try:
            yield block
            if block.status == BlockStatus.PROCESSING:
                block.status = BlockStatus.SUCCESS
        except BaseException as e:
            block.status = BlockStatus.FAILED
            try:
                _worker_log.logger.warning(
                    "block %s failed: %s", block.block_id, e,
                )
                _worker_log.emit_failure(
                    f"block {block.block_id} failed:\n"
                    + _worker_log.format_traceback(
                        type(e), e, e.__traceback__,
                    )
                )
            except Exception:
                pass
            raise
        finally:
            if block.status != BlockStatus.SUCCESS:
                block.status = BlockStatus.FAILED
            self._client.release_block(block)

    def __del__(self):
        try:
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
    nargs = len([a for a in inspect.getfullargspec(orig).args if a != "self"])

    if nargs == 0:
        def wrapped():
            with _worker_log._WorkerLogContext(task_id):
                try:
                    return orig()
                except BaseException as e:
                    _worker_log.logger.warning(
                        "worker function %s failed: %s", task_id, e,
                    )
                    _worker_log.emit_failure(
                        _worker_log.format_traceback(
                            type(e), e, e.__traceback__,
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
                        "block %s failed: %s", block.block_id, e,
                    )
                    _worker_log.emit_failure(
                        f"block {block.block_id} failed:\n"
                        + _worker_log.format_traceback(
                            type(e), e, e.__traceback__,
                        )
                    )
                    raise

    wrapped.__name__ = getattr(orig, "__name__", "process_function")
    wrapped.__qualname__ = getattr(orig, "__qualname__", wrapped.__name__)

    clone = copy.copy(task)
    clone.process_function = wrapped
    return clone
