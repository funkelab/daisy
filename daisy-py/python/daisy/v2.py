"""Direct v2 API surface.

Exposes the Rust-defined types from `daisy._daisy` where they're usable
as-is (`Pipeline`, `TaskState`, `Context`), `funlib.geometry` ROIs and
Coordinates, a Python `Block`, plus the small Python wrappers needed to do
real Python-side work — per-worker log routing, progress
observers, execution-summary printing, and resolving the marker
path against `daisy.logging.LOG_BASEDIR`. No daisy 1.x
backwards-compatibility shims live here; for that, use
`daisy.v1_compat` (which is also what bare `import daisy`
re-exports by default).

Typical use::

    import daisy.v2 as daisy
    task = daisy.Task(..., max_workers=4)
    daisy.run_blockwise([task])
"""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _version

import daisy._daisy as _rs
from daisy._progress import JsonProgressObserver
from daisy._report import RunReport, TaskReport, last_report
from daisy._runner import Server, run_blockwise
from daisy._task import (
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
    get_done_marker_basedir,
    get_tracking_basedir,
    profile_block,
    set_done_marker_basedir,
    set_tracking_basedir,
)

# Pipeline is implemented entirely in Rust; the operators on Python
# `Task` delegate to it. Exposed directly here.
Pipeline = _rs.Pipeline

# `Roi` and `Coordinate` are funlib.geometry's classes; `Block` is the
# Python class in `daisy.block`. The Rust carriers stay inside `daisy._daisy`.
BlockStats = _rs.BlockStats
TaskState = _rs.TaskState

try:
    __version__ = _version("daisy")
except PackageNotFoundError:
    __version__ = "unknown"

__all__ = [
    "Block",
    "BlockStatus",
    "BlockwiseDependencyGraph",
    "Client",
    "Context",
    "Coordinate",
    "DependencyGraph",
    "JsonProgressObserver",
    "Pipeline",
    "Roi",
    "RunReport",
    "Scheduler",
    "Server",
    "Task",
    "TaskReport",
    "TaskState",
    "__version__",
    "BlockStats",
    "last_report",
    "get_done_marker_basedir",
    "get_tracking_basedir",
    "run_blockwise",
    "profile_block",
    "set_done_marker_basedir",
    "set_tracking_basedir",
]
