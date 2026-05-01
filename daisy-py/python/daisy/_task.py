"""User-facing Task / Scheduler / Client / Context classes.

These are the constructors users hold; they wrap (or pass-through)
the Rust-defined types in `daisy._daisy` and `_rs.SyncClient`.

This module has no scheduling logic, no progress-display code, and
no run-loop entry points. Those live in `_progress.py` and
`_runner.py`.
"""

from contextlib import contextmanager
import copy
import inspect
import logging
import os
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

    Thin wrapper over `_rs.set_done_marker_basedir`; the storage
    lives in Rust so all paths (Python wrappers, raw `_rs.Task`,
    Pipeline materialization) see the same value.
    """
    _rs.set_done_marker_basedir(str(path) if path is not None else None)


def get_done_marker_basedir() -> Path | None:
    p = _rs.get_done_marker_basedir()
    return p if p is None else Path(p)


# Types with identical API — no wrapping needed.
Roi = _rs.Roi
Coordinate = _rs.Coordinate
Block = _rs.Block
BlockStatus = _rs.BlockStatus
BlockwiseDependencyGraph = _rs.BlockwiseDependencyGraph
DependencyGraph = _rs.DependencyGraph
TaskState = _rs.TaskState


class Task:
    """User-facing Task constructor. Thin wrapper over `_rs.Task`."""

    def __init__(
        self,
        task_id,
        total_roi,
        read_roi,
        write_roi,
        process_function=None,
        check_function=None,
        init_callback_fn=None,
        read_write_conflict=True,
        max_workers=1,
        max_retries=2,
        fit="valid",
        timeout=None,
        upstream_tasks=None,
        done_marker_path=None,
        requires=None,
        max_worker_restarts=10,
    ):
        self.task_id = task_id
        self.total_roi = total_roi
        self.read_roi = read_roi
        self.write_roi = write_roi
        self.process_function = process_function
        self.check_function = check_function
        self.init_callback_fn = init_callback_fn
        self.read_write_conflict = read_write_conflict
        self.max_workers = int(max_workers)
        self.max_retries = max_retries
        self.fit = fit
        self.timeout = timeout
        self.upstream_tasks = upstream_tasks or []
        # Three states for `done_marker_path`:
        #   - explicit string/Path  → use it verbatim
        #   - False                 → disabled for this task (override basedir)
        #   - None (default)        → fall back to global basedir + task_id
        self.done_marker_path = done_marker_path
        # Per-worker resource cost; empty dict (or None) disables
        # resource accounting for this task — the worker count is bounded
        # purely by `max_workers`.
        self.requires = dict(requires) if requires else {}
        # Cap on the number of times a worker for this task may exit
        # with an error before the runner stops respawning. Once
        # reached, any unprocessed blocks are accounted as orphaned.
        self.max_worker_restarts = int(max_worker_restarts)

    def reset(self) -> Path | None:
        """Delete this task's done-marker, so the next run starts fresh
        for every block of this task. Returns the path that was cleared
        (or `None` if no marker was configured / nothing existed).

        Does *not* cascade to upstream or downstream tasks. If a
        downstream task has a marker recording "done based on this
        task's previous output," its marker still says done — call
        `reset()` on those tasks too if you want the downstream re-run.

        Filesystem operation lives in Rust (`daisy_core::DoneMarker::clear`);
        this wrapper just resolves the path through the normal precedence
        chain (per-task `done_marker_path` → global basedir → log basedir)
        and delegates.
        """
        cleared = self._to_rs().reset()
        return Path(cleared) if cleared is not None else None

    def _to_rs(self):
        # Pass `done_marker_path` through verbatim — `_rs.Task` handles
        # the 3-state spec (None/False/str) and resolves Auto against
        # the global basedir / `daisy.logging.LOG_BASEDIR` at convert
        # time inside `convert_task_tree`.
        upstream_rs = [t._to_rs() if isinstance(t, Task) else t for t in self.upstream_tasks]
        timeout_secs = None
        if self.timeout is not None:
            if hasattr(self.timeout, "total_seconds"):
                timeout_secs = float(self.timeout.total_seconds())
            else:
                timeout_secs = float(self.timeout)
        return _rs.Task(
            task_id=self.task_id,
            total_roi=self.total_roi,
            read_roi=self.read_roi,
            write_roi=self.write_roi,
            process_function=self.process_function,
            check_function=self.check_function,
            read_write_conflict=self.read_write_conflict,
            fit=self.fit,
            num_workers=self.max_workers,
            max_retries=self.max_retries,
            upstream_tasks=upstream_rs if upstream_rs else None,
            done_marker_path=self.done_marker_path,
            requires=self.requires if self.requires else None,
            max_worker_restarts=self.max_worker_restarts,
            timeout_secs=timeout_secs,
        )

    def requires(self):
        return self.upstream_tasks

    def __add__(self, other):
        """`a + b` — sequential pipeline composition. Delegates to
        the Rust `_rs.Pipeline`; `b`'s blocks depend on `a`'s outputs."""
        return _rs.Pipeline.from_task(self).__add__(other)

    def __or__(self, other):
        """`a | b` — parallel pipeline composition. Delegates to
        the Rust `_rs.Pipeline`; union of two DAGs with no new edges."""
        return _rs.Pipeline.from_task(self).__or__(other)


def _convert_tasks(tasks):
    return [t._to_rs() if isinstance(t, Task) else t for t in tasks]


class Scheduler:
    """Maps daisy's Scheduler(tasks) to _rs.Scheduler."""

    def __init__(self, tasks, count_all_orphans=True):
        self._inner = _rs.Scheduler(_convert_tasks(tasks))

    def acquire_block(self, task_id):
        return self._inner.acquire_block(task_id)

    def release_block(self, block):
        self._inner.release_block(block)

    @property
    def task_states(self):
        return self._inner.task_states

    @property
    def dependency_graph(self):
        return self._inner.dependency_graph


# `Context` is implemented in Rust as `_rs.Context`. Exposed as a
# top-level alias for the same dict-like env-var encoder the Python
# class used to provide.
Context = _rs.Context


class Client:
    """Maps daisy's Client(context) to _rs.SyncClient."""

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
            # Mark the block failed, emit a structured warning, write
            # the traceback to the worker's log destinations (one
            # consolidated call that bypasses `sys.stderr` so it can't
            # be picked up by host-environment hooks), and re-raise so
            # the caller can decide whether to continue.
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
    """Return a shallow copy of `task` whose `process_function` is wrapped
    so that stdout/stderr emitted during the call go to the worker's log
    files (see `daisy.logging`)."""
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
                        "worker function %s failed: %s",
                        task_id, e,
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
    # Upstream tasks get wrapped too so a chained pipeline logs uniformly.
    clone.upstream_tasks = [
        _wrap_for_worker_logging(up) if isinstance(up, Task) else up
        for up in task.upstream_tasks
    ]
    return clone


