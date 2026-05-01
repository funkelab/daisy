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

    def run_blockwise(self, tasks, resources=None, progress=True, block_tracking=True):
        from daisy._pipeline import Pipeline
        if isinstance(tasks, Pipeline):
            tasks = tasks.materialize()
        wrapped = [_wrap_for_worker_logging(t) if isinstance(t, Task) else t
                   for t in tasks]
        order = _topo_order(tasks)
        self.last_task_order = order
        observer = _resolve_progress(progress, task_order=order)
        try:
            states, run_stats = _rs._run_distributed_server(
                _convert_tasks(wrapped),
                resources,
                observer,
                block_tracking=block_tracking,
            )
            self.last_run_stats = run_stats
            return states
        finally:
            _worker_log.close_all_log_files()


def _run_serial(tasks, block_tracking=True):
    """Single-threaded execution path (no TCP, no workers). Used
    when `daisy.run_blockwise(..., multiprocessing=False)`."""
    from daisy._pipeline import Pipeline
    if isinstance(tasks, Pipeline):
        tasks = tasks.materialize()
    wrapped = [_wrap_for_worker_logging(t) if isinstance(t, Task) else t
               for t in tasks]
    try:
        return _rs._run_serial(_convert_tasks(wrapped), block_tracking=block_tracking)
    finally:
        _worker_log.close_all_log_files()


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
    marked it done. Any permanently failed or orphaned blocks (i.e.
    blocks that ran out of retries, or whose upstream dependencies
    failed) cause this to return `False` — even if the runner itself
    finished cleanly. Inspect the per-task `TaskState` returned from
    `Server.run_blockwise` for the full counter breakdown, or read the
    "Execution Summary" printed to stderr after each run.

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

    `block_tracking`: when True (default), per-task done markers are
    enabled. The marker location is resolved per task in this order:
    explicit `done_marker_path` on the task → global basedir set via
    `set_done_marker_basedir` → the daisy log basedir
    (`./daisy_logs`, or whatever `daisy.logging.set_log_basedir` was
    given). With markers on, re-running the same tasks after an
    aborted or partial run skips already-completed blocks via the
    scheduler pre-check. When False, marker tracking is disabled for
    this run regardless of any per-task or global configuration —
    every block is processed afresh.
    """
    from daisy._pipeline import Pipeline
    if isinstance(tasks, Pipeline):
        tasks = tasks.materialize()
    order = _topo_order(tasks)
    if multiprocessing:
        server = Server()
        states = server.run_blockwise(
            tasks, resources=resources, progress=progress,
            block_tracking=block_tracking,
        )
        run_stats = getattr(server, "last_run_stats", None)
    else:
        states = _run_serial(tasks, block_tracking=block_tracking)
        run_stats = None
    _print_execution_summary(states, task_order=order)
    _print_resource_utilization(run_stats, task_order=order)
    return all(
        s.completed_count == s.total_block_count
        for s in states.values()
    )
