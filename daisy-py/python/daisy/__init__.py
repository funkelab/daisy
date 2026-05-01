"""daisy v2.0.0 — blockwise task scheduler for large volumetric data.

The top-level namespace re-exports `daisy.v1_compat` so existing
daisy 1.x scripts run unchanged wherever the v2 API didn't break
compatibility.

Two explicit alternatives:

- ``import daisy.v2 as daisy`` — v2-native surface, no compat shims,
  Rust types exposed directly.
- ``import daisy.v1_compat as daisy`` — same module the bare
  `import daisy` resolves to; spell it out when you want to be
  explicit about depending on the compat layer.
"""

from daisy.v1_compat import (
    Block,
    BlockStatus,
    BlockwiseDependencyGraph,
    Client,
    Context,
    Coordinate,
    DependencyGraph,
    JsonProgressObserver,
    Roi,
    Scheduler,
    SerialServer,
    Server,
    Task,
    TaskState,
    __version__,
    get_done_marker_basedir,
    run_blockwise,
    set_done_marker_basedir,
)

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
    "SerialServer",
    "Server",
    "Task",
    "TaskState",
    "__version__",
    "get_done_marker_basedir",
    "run_blockwise",
    "set_done_marker_basedir",
]
