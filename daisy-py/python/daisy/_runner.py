"""Run-loop entry points.

`Server` is the user-facing coordinator class; `_run_serial` is the
private serial path. The top-level `run_blockwise` here is the
helper most users actually call.
"""

import daisy._daisy as _rs
from daisy import logging as _worker_log

from daisy._task import Task, _convert_tasks, _wrap_for_worker_logging
from daisy._progress import (
    _print_execution_summary,
    _print_resource_utilization,
    _resolve_progress,
    _topo_order,
)


class Server:
    """Coordinator. Spawns and manages Rust worker threads internally.

    Most users go through the top-level `daisy.run_blockwise(...)`
    helper. Instantiate this class directly when you want access to
    `last_run_stats` or `last_task_order` after the run, or when
    you're embedding daisy in a larger orchestrator.
    """

    def __init__(self, stop_event=None):
        self._stop_event = stop_event
        self.hostname = None
        self.port = None

    def run_blockwise(self, tasks, resources=None, progress=True):
        wrapped = [_wrap_for_worker_logging(t) if isinstance(t, Task) else t
                   for t in tasks]
        order = _topo_order(tasks)
        self.last_task_order = order
        observer = _resolve_progress(progress, task_order=order)
        try:
            states, run_stats = _rs._run_distributed_server(
                _convert_tasks(wrapped), resources, observer,
            )
            self.last_run_stats = run_stats
            return states
        finally:
            _worker_log.close_all_log_files()


def _run_serial(tasks):
    """Single-threaded execution path (no TCP, no workers). Used
    when `daisy.run_blockwise(..., multiprocessing=False)`."""
    wrapped = [_wrap_for_worker_logging(t) if isinstance(t, Task) else t
               for t in tasks]
    try:
        return _rs._run_serial(_convert_tasks(wrapped))
    finally:
        _worker_log.close_all_log_files()


def run_blockwise(tasks, multiprocessing=True, resources=None, progress=True):
    """Run the given tasks to completion.

    `resources` is an optional `dict[str, int]` global budget consumed by
    tasks whose `requires=...` declares non-empty entries. Tasks with
    empty `requires` are bounded only by their own `max_workers` cap.
    Hard-errors at startup if any task's `requires` exceeds the budget.
    Ignored in serial mode.

    `progress`:
      - `True` (default in multiprocessing mode): show a `tqdm.auto`
        progress bar per task, with live counts in the postfix.
      - `False` / `None`: disable.
      - object with `on_start`/`on_progress`/`on_finish` methods: a
        custom observer (e.g. for logging into a file or pushing to a
        dashboard).
      Always disabled in serial mode.
    """
    order = _topo_order(tasks)
    if multiprocessing:
        server = Server()
        states = server.run_blockwise(tasks, resources=resources, progress=progress)
        run_stats = getattr(server, "last_run_stats", None)
    else:
        states = _run_serial(tasks)
        run_stats = None
    _print_execution_summary(states, task_order=order)
    _print_resource_utilization(run_stats, task_order=order)
    return all(s.is_done() for s in states.values())
