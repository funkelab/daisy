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
Task = _rs.Task
Scheduler = _rs.Scheduler
Context = _rs.Context


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
