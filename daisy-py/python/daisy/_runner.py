"""Run-loop entry points.

The orchestration (topo order, dispatch, summary printing, bool
result) lives in Rust as `_rs._run_blockwise_orchestrator`. This
module supplies the thin Python shims that handle the one piece
that genuinely belongs in Python: applying `_wrap_for_worker_logging`
to each user-supplied process function so per-worker stdout/stderr
routing and traceback emission go through the existing Python
logging machinery.
"""

import daisy._daisy as _rs
from daisy import logging as _worker_log

from daisy._task import _to_pipeline, _wrap_for_worker_logging


def _coerce_pipeline(pipeline_or_tasks):
    """Coerce input to a `_rs.Pipeline`. See `_task._to_pipeline`."""
    return _to_pipeline(pipeline_or_tasks)


def _prepare(pipeline):
    """Wrap every task in the pipeline with per-worker logging,
    returning a new Pipeline with the same edge structure but
    process-functions wrapped through `_wrap_for_worker_logging`."""
    wrapped = [_wrap_for_worker_logging(t) for t in pipeline.tasks]
    # Edges are exposed as (Task, Task) pairs of the original tasks;
    # remap them to the wrapped clones (same index in the new task list).
    index = {id(orig): wrapped[i] for i, orig in enumerate(pipeline.tasks)}
    new_edges = [(index[id(u)], index[id(d)]) for u, d in pipeline.edges]
    return _rs.Pipeline(wrapped, new_edges)


class Server:
    """Coordinator. Thin shim over `_rs._run_distributed_server` that
    wraps process functions with per-worker logging context first.

    Most users go through `daisy.run_blockwise(...)`. Instantiate this
    class directly when you want access to `last_run_stats` or
    `last_task_order` after the run."""

    def __init__(self, stop_event=None):
        self._stop_event = stop_event
        self.hostname = None
        self.port = None
        self.last_run_stats = None
        self.last_task_order = None

    def run_blockwise(self, pipeline, resources=None, progress=True, block_tracking=True):
        pipeline = _prepare(_coerce_pipeline(pipeline))
        order = _rs._topo_order(pipeline)
        self.last_task_order = order
        observer = _resolve_observer(progress, order)
        try:
            states, run_stats = _rs._run_distributed_server(
                pipeline, resources, observer, block_tracking=block_tracking,
            )
            self.last_run_stats = run_stats
            return states
        finally:
            _worker_log.close_all_log_files()


def _run_serial(pipeline, block_tracking=True):
    """Single-threaded execution path (no TCP, no workers)."""
    pipeline = _prepare(_coerce_pipeline(pipeline))
    try:
        return _rs._run_serial(pipeline, block_tracking=block_tracking)
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
    pipeline,
    multiprocessing=True,
    resources=None,
    progress=True,
    block_tracking=True,
):
    """Run the given pipeline to completion.

    `pipeline` must be a `daisy.Pipeline` (or a single `Task`, which
    is sugar for a singleton pipeline). The legacy form
    `run_blockwise([task1, task2])` is supported by `daisy.v1_compat`,
    which converts a list of tasks into a Pipeline by reading each
    task's v1.x-style `upstream_tasks=` kwarg.

    Returns `True` only if every block of every task either completed
    successfully in this run or was skipped because a prior run
    already marked it done. Permanently failed or orphaned blocks
    cause this to return `False`.
    """
    pipeline = _prepare(_coerce_pipeline(pipeline))
    try:
        return _rs._run_blockwise_orchestrator(
            pipeline,
            multiprocessing=multiprocessing,
            resources=resources,
            progress=progress,
            block_tracking=block_tracking,
        )
    finally:
        _worker_log.close_all_log_files()
