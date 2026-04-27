from importlib.metadata import PackageNotFoundError, version as _version

from daisy._compat import (
    Block,
    BlockStatus,
    BlockwiseDependencyGraph,
    Client,
    Context,
    Coordinate,
    DependencyGraph,
    Roi,
    Scheduler,
    Server,
    Task,
    TaskState,
    get_done_marker_basedir,
    run_blockwise,
    set_done_marker_basedir,
)
from daisy._progress import JsonProgressObserver

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
