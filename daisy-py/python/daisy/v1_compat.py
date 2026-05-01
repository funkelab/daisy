"""daisy 1.x backwards-compatibility surface.

Re-exports the v2 API (`daisy.v2`) plus a small set of shims for
users coming from daisy 1.x:

- `Task(num_workers=N)` is accepted as an alias for `max_workers=N`.
- `Task(upstream_tasks=[other_task])` is accepted as the v1.x way to
  declare per-task dependencies. v2 moved DAG dependencies onto the
  `Pipeline` (and its `+`/`|` operators); the v1 form is recorded on
  the returned `Task` via a side dict so `run_blockwise` /
  `Server.run_blockwise` can build a Pipeline transparently.
- `SerialServer` is a thin shim over the unified `Server` / serial
  path; daisy v2 dropped the separate class.
- `run_blockwise([task1, task2, ...])` accepts a list of tasks (the
  v1.x shape) and builds a Pipeline by walking each task's
  `upstream_tasks` declared at construction time.

Each shim emits a `DeprecationWarning` pointing at the v2-native
equivalent so users see a migration hint at the call site without
breaking existing code.

Top-level `import daisy` re-exports this module by default, so
existing 1.x scripts run on daisy 2.0 wherever the v2 API didn't
deliberately break compatibility. Removed APIs (`daisy.persistence`,
`daisy.Array`, etc.) are documented in `MIGRATION.md`.
"""

import warnings

from daisy import v2 as _v2
from daisy._runner import _run_serial
from daisy._task import _record_task_upstream
from daisy.v2 import (
    Block,
    BlockStatus,
    BlockwiseDependencyGraph,
    Client,
    Context,
    Coordinate,
    DependencyGraph,
    JsonProgressObserver,
    Pipeline,
    Roi,
    Scheduler,
    Server,
    TaskState,
    __version__,
    get_done_marker_basedir,
    set_done_marker_basedir,
)
from daisy.v2 import run_blockwise as _v2_run_blockwise


class Task(_v2.Task):
    """v1.x-compatible `Task` — a Python subclass of `_rs.Task` that
    accepts the daisy 1.x kwargs `num_workers=N` (alias for
    `max_workers=N`) and `upstream_tasks=[other_task, ...]` (declares
    per-task block-level dependencies, which v2 expresses via
    `Pipeline` composition).

    Passing either kwarg emits a `DeprecationWarning`. The
    `upstream_tasks` are stored on this Python subclass instance
    (Python subclasses of `#[pyclass(subclass)]` get their own
    `__dict__`); `daisy._task._build_pipeline_from_tasks` reads this
    attribute when bridging a v1.x list-of-tasks call into a v2
    Pipeline."""

    def __new__(
        cls,
        *args,
        num_workers=None,
        max_workers=None,
        upstream_tasks=None,
        **kwargs,
    ):
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
        if upstream_tasks:
            warnings.warn(
                "Task(upstream_tasks=[...]) is the daisy 1.x way to declare "
                "block-level dependencies. daisy v2 expresses this via "
                "`Pipeline` composition: `upstream_task + downstream_task`. "
                "This compat alias keeps your code working — `run_blockwise` "
                "will build a Pipeline from the upstream list automatically.",
                DeprecationWarning,
                stacklevel=2,
            )
        instance = super().__new__(cls, *args, max_workers=max_workers, **kwargs)
        _record_task_upstream(instance, upstream_tasks or [])
        return instance

    def __init__(self, *args, **kwargs):
        # PyO3 handles construction via __new__; nothing to do here.
        # Override kept so super().__init__() doesn't reject our kwargs.
        pass


# v2's run_blockwise already accepts Pipeline / Task / list-of-tasks
# via `_runner._coerce_pipeline`. The list path reads the
# `_v1_upstream_tasks` attribute populated by this Task subclass.
run_blockwise = _v2_run_blockwise


def Scheduler(tasks_or_pipeline) -> _v2.Scheduler:
    """v1.x-compatible `Scheduler` factory. Accepts a Pipeline, a
    Task, or a list of tasks (with `upstream_tasks=` declarations
    honoured) and returns an `_rs.Scheduler`.

    daisy v2 internalised the scheduler to consume a Pipeline only;
    this shim builds a Pipeline from the list-of-tasks form by
    walking the v1.x upstream side-table on each task."""
    from daisy._task import _to_pipeline
    return _v2.Scheduler(_to_pipeline(tasks_or_pipeline))


class SerialServer:
    """v1.x compat shim. daisy 1.x had a separate `SerialServer` class
    for in-process execution; v2 unified that into the single `Server`
    with `multiprocessing` selected at run time."""

    def __init__(self):
        warnings.warn(
            "daisy.SerialServer is a v1.x compat shim. daisy v2 unified "
            "serial and parallel execution; prefer "
            "`daisy.run_blockwise(pipeline, multiprocessing=False)`, or "
            "`import daisy.v2 as daisy`.",
            DeprecationWarning,
            stacklevel=2,
        )

    def run_blockwise(self, tasks, block_tracking=True):
        # `_run_serial` goes through `_coerce_pipeline`, which already
        # handles list/tuple/Task/Pipeline.
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
    "Pipeline",
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
