"""Run-loop entry points.

The orchestration (topo order, dispatch, execution-summary printing,
bool result computation) lives in Rust as `_rs._run_blockwise_orchestrator`.
This module contains thin Python shims that handle the one piece that
genuinely belongs in Python: applying `_wrap_for_worker_logging` to
each user-supplied process function so per-worker stdout/stderr
routing and traceback emission go through the existing Python logging
machinery.
"""

import daisy._daisy as _rs
from daisy import logging as _worker_log

from daisy._task import Task, _convert_tasks, _wrap_for_worker_logging


def _prepare(tasks):
    """Materialize a Pipeline if needed, wrap process_functions for
    per-worker logging, and convert to `_rs.Task` for the runner."""
    if isinstance(tasks, _rs.Pipeline):
        tasks = tasks.materialize()
    wrapped = [_wrap_for_worker_logging(t) if isinstance(t, Task) else t
               for t in tasks]
    return _convert_tasks(wrapped)


class Server:
    """Coordinator. Thin shim over `_rs._run_distributed_server` that
    wraps process functions with per-worker logging context first.

    Most users go through `daisy.run_blockwise(...)`. Instantiate this
    class directly when you want access to `last_run_stats` or
    `last_task_order` after the run.
    """

    def __init__(self, stop_event=None):
        self._stop_event = stop_event
        self.hostname = None
        self.port = None
        self.last_run_stats = None
        self.last_task_order = None

    def run_blockwise(self, tasks, resources=None, progress=True, block_tracking=True):
        rs_tasks = _prepare(tasks)
        order = _rs._topo_order(rs_tasks)
        self.last_task_order = order
        # Resolve progress observer (Python class for tqdm).
        observer = _resolve_observer(progress, order)
        try:
            states, run_stats = _rs._run_distributed_server(
                rs_tasks, resources, observer, block_tracking=block_tracking,
            )
            self.last_run_stats = run_stats
            return states
        finally:
            _worker_log.close_all_log_files()


def _run_serial(tasks, block_tracking=True):
    """Single-threaded execution path (no TCP, no workers). Used
    when `daisy.run_blockwise(..., multiprocessing=False)`."""
    rs_tasks = _prepare(tasks)
    try:
        return _rs._run_serial(rs_tasks, block_tracking=block_tracking)
    finally:
        _worker_log.close_all_log_files()


def _resolve_observer(progress, task_order):
    """Map `progress` arg to a Python observer object (or None).
    `True` → `_TqdmObserver(task_order)`. `False`/`None` → None.
    Anything else is used verbatim."""
    if progress is None or progress is False:
        return None
    if progress is True:
        from daisy._progress import _TqdmObserver
        return _TqdmObserver(task_order=task_order)
    return progress


def run_blockwise(
    tasks,
    multiprocessing=True,
    resources=None,
    progress=True,
    block_tracking=True,
):
    """Run the given tasks to completion.

    Returns `True` only if every block of every task either completed
    successfully in this run or was skipped because a prior run already
    marked it done. Any permanently failed or orphaned blocks cause
    this to return `False`. Inspect the per-task `TaskState` returned
    from `Server.run_blockwise` for the full counter breakdown.

    `resources`: optional `dict[str, int]` global budget consumed by
    tasks whose `requires=...` declares non-empty entries. Ignored in
    serial mode.

    `progress`:
      - `True` (default): show a `tqdm.auto` progress bar per task.
      - `False` / `None`: disable.
      - object with `on_start`/`on_progress`/`on_finish`: custom observer.
      Always disabled in serial mode.

    `block_tracking`: when True (default), per-task done markers are
    enabled. Resolution: explicit `done_marker_path` → global basedir
    (`set_done_marker_basedir`) → `daisy.logging.LOG_BASEDIR`. When
    False, marker tracking is disabled for this run.

    Orchestration (topo order, dispatch, summary printing, bool result)
    runs in Rust via `_rs._run_blockwise_orchestrator`. This Python
    shim only wraps process functions for per-worker logging before
    handing off.
    """
    rs_tasks = _prepare(tasks)
    try:
        return _rs._run_blockwise_orchestrator(
            rs_tasks,
            multiprocessing=multiprocessing,
            resources=resources,
            progress=progress,
            block_tracking=block_tracking,
        )
    finally:
        _worker_log.close_all_log_files()
