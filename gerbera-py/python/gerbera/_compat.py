"""Re-export shim. The implementation lives in `_task.py`,
`_progress.py`, and `_runner.py`. This file exists so existing
`from gerbera._compat import Task` imports keep working.
"""

from gerbera._task import (
    Block,
    BlockStatus,
    BlockwiseDependencyGraph,
    Client,
    Context,
    Coordinate,
    DependencyGraph,
    Roi,
    Scheduler,
    Task,
    TaskState,
    _convert_tasks,
    _wrap_for_worker_logging,
    get_done_marker_basedir,
    set_done_marker_basedir,
)

from gerbera._progress import (
    _format_bytes,
    _ordered_states,
    _print_execution_summary,
    _print_resource_utilization,
    _resolve_progress,
    _topo_order,
    _TqdmObserver,
)

from gerbera._runner import (
    Server,
    _run_serial,
    run_blockwise,
)

__all__ = [
    "Block",
    "BlockStatus",
    "BlockwiseDependencyGraph",
    "Client",
    "Context",
    "Coordinate",
    "DependencyGraph",
    "Roi",
    "Scheduler",
    "Server",
    "Task",
    "TaskState",
    "get_done_marker_basedir",
    "run_blockwise",
    "set_done_marker_basedir",
]
