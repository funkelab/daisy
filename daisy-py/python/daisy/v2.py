"""Direct v2 API surface.

Exposes the Rust-defined types from `daisy._daisy` directly where
they're usable as-is, plus the small Python wrappers needed to do
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

from importlib.metadata import PackageNotFoundError, version as _version

import daisy._daisy as _rs
from daisy._progress import JsonProgressObserver
from daisy._runner import Server, run_blockwise
from daisy._task import (
    Client,
    Context,
    Scheduler,
    Task,
    get_done_marker_basedir,
    set_done_marker_basedir,
)

# Pipeline is implemented entirely in Rust; the operators on Python
# `Task` delegate to it. Exposed directly here.
Pipeline = _rs.Pipeline

# Pure value / algorithmic types from Rust — no Python ergonomics
# needed, expose them directly:
Roi = _rs.Roi
Coordinate = _rs.Coordinate
Block = _rs.Block
BlockStatus = _rs.BlockStatus
TaskState = _rs.TaskState
BlockwiseDependencyGraph = _rs.BlockwiseDependencyGraph
DependencyGraph = _rs.DependencyGraph

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
    "Scheduler",
    "Server",
    "Task",
    "TaskState",
    "__version__",
    "get_done_marker_basedir",
    "run_blockwise",
    "set_done_marker_basedir",
]
