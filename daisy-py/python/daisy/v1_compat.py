"""daisy 1.x backwards-compatibility surface.

Re-exports the v2 API (`daisy.v2`) plus a small set of shims for
users coming from daisy 1.x:

- `Task(num_workers=N)` is accepted as an alias for `max_workers=N`.
- `SerialServer` exists as a thin shim over the unified `Server` /
  serial path; daisy v2 dropped the separate class.

Both shims emit a `DeprecationWarning` pointing at the v2-native
equivalent so users see a migration hint at the call site (without
breaking existing code).

Top-level `import daisy` re-exports this module by default, so
existing 1.x scripts run on daisy 2.0 wherever the v2 API didn't
deliberately break compatibility. Things that *did* break (for
example `daisy.persistence`, `daisy.Array`, `daisy.open_ds`,
`daisy.prepare_ds`, the funlib.geometry Roi/Coordinate types) are
documented in `MIGRATION.md` and are not provided here.

For the v2-native surface (no compat shims, no deprecation
warnings), use ``import daisy.v2 as daisy``.
"""

import warnings

from daisy import v2 as _v2
from daisy._runner import _run_serial
from daisy.v2 import (
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
    Server,
    TaskState,
    __version__,
    get_done_marker_basedir,
    run_blockwise,
    set_done_marker_basedir,
)


class Task(_v2.Task):
    """v1.x-compatible `Task` — accepts `num_workers=N` as an alias
    for daisy v2's `max_workers=N`. Passing `num_workers=` emits a
    `DeprecationWarning`; passing `max_workers=` (or omitting both)
    is silent."""

    def __init__(self, *args, num_workers=None, max_workers=None, **kwargs):
        if num_workers is not None and max_workers is not None:
            raise TypeError(
                "pass either max_workers (v2) or num_workers (v1.x), not both"
            )
        if num_workers is not None:
            warnings.warn(
                "Task(num_workers=N) is the daisy 1.x kwarg; daisy v2 uses "
                "max_workers=N. This compat alias keeps your code working "
                "but for the v2-native API use `import daisy.v2 as daisy`.",
                DeprecationWarning,
                stacklevel=2,
            )
            max_workers = num_workers
        if max_workers is None:
            max_workers = 1
        super().__init__(*args, max_workers=max_workers, **kwargs)


class SerialServer:
    """v1.x compat shim. daisy 1.x had a separate `SerialServer`
    class for in-process execution; v2 unified that into the single
    `Server` class with `multiprocessing` selected at run time. This
    class keeps the 1.x name working — its `run_blockwise(tasks)`
    runs serially without TCP or worker spawning.

    Instantiation emits a `DeprecationWarning`; prefer
    ``daisy.run_blockwise(tasks, multiprocessing=False)``, or
    ``import daisy.v2 as daisy`` for the v2-native API.

    Returns the per-task `TaskState` dict, mirroring v2 `Server`."""

    def __init__(self):
        warnings.warn(
            "daisy.SerialServer is a v1.x compat shim. daisy v2 unified "
            "serial and parallel execution into a single `Server`; prefer "
            "`daisy.run_blockwise(tasks, multiprocessing=False)`, or "
            "`import daisy.v2 as daisy` for the v2-native API.",
            DeprecationWarning,
            stacklevel=2,
        )

    def run_blockwise(self, tasks, block_tracking=True):
        return _run_serial(tasks, block_tracking=block_tracking)


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
