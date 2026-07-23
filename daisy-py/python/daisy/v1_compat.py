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


def _coerce_roi(value):
    """Coerce funlib.geometry.Roi (or anything with .offset/.shape) into
    a daisy.Roi. Pass-through for daisy.Roi and None."""
    if value is None or isinstance(value, _v2.Roi):
        return value
    offset = getattr(value, "offset", None)
    shape = getattr(value, "shape", None)
    if offset is not None and shape is not None:
        return _v2.Roi(tuple(offset), tuple(shape))
    return value


def _coerce_coord(value):
    """Coerce funlib.geometry.Coordinate (an iterable of ints) into a
    daisy.Coordinate. Pass-through for daisy.Coordinate and None."""
    if value is None or isinstance(value, _v2.Coordinate):
        return value
    try:
        return _v2.Coordinate(tuple(value))
    except TypeError:
        return value


def _install_funlib_compat():
    """daisy 1.x callers passed `funlib.geometry.Roi` / `funlib.geometry.Coordinate`
    to `funlib.persistence.Array` indexing methods. funlib's `isinstance(key, Roi)`
    /  `isinstance(key, Coordinate)` checks reject daisy 2.x's Rust types (they
    aren't subclasses), so the access falls through to the numpy/zarr indexing
    path and dies. Monkey-patch the funlib entry points to convert at the
    boundary. No-op if funlib isn't installed."""
    try:
        from funlib.persistence.arrays.array import Array
        from funlib.geometry import Roi as _FRoi, Coordinate as _FCoord
    except ImportError:
        return

    def _to_funlib(x):
        if isinstance(x, _v2.Roi):
            return _FRoi(tuple(x.offset), tuple(x.shape))
        if isinstance(x, _v2.Coordinate):
            return _FCoord(tuple(x))
        return x

    _orig_setitem = Array.__setitem__
    _orig_getitem = Array.__getitem__
    _orig_to_ndarray = Array.to_ndarray
    _orig_to_pixel = Array.to_pixel_space
    _orig_to_world = Array.to_world_space

    def setitem(self, key, value):
        return _orig_setitem(self, _to_funlib(key), value)

    def getitem(self, key):
        return _orig_getitem(self, _to_funlib(key))

    def to_ndarray(self, roi=None, **kwargs):
        return _orig_to_ndarray(self, roi=_to_funlib(roi), **kwargs)

    def to_pixel_space(self, world_loc):
        return _orig_to_pixel(self, _to_funlib(world_loc))

    def to_world_space(self, pixel_loc):
        return _orig_to_world(self, _to_funlib(pixel_loc))

    Array.__setitem__ = setitem
    Array.__getitem__ = getitem
    Array.to_ndarray = to_ndarray
    Array.to_pixel_space = to_pixel_space
    Array.to_world_space = to_world_space


_install_funlib_compat()


class _BlockProxy:
    """Wraps a v2 `_rs.Block` so user code (e.g. daisy 1.x `process_function`s
    like volara's `process_block`) sees `block.write_roi` / `block.read_roi`
    as `funlib.geometry.Roi` instances rather than daisy's Rust `Roi`.

    All other attribute reads/writes proxy through to the underlying block.
    Status mutations (set by daisy's own `acquire_block` context manager
    after process_function returns) are forwarded to the inner block so
    daisy's bookkeeping is unaffected."""

    __slots__ = ("_block",)

    def __init__(self, block):
        object.__setattr__(self, "_block", block)

    def _to_funlib_roi(self, r):
        if r is None:
            return None
        try:
            from funlib.geometry import Roi as _FRoi
        except ImportError:
            return r
        return _FRoi(tuple(r.offset), tuple(r.shape))

    @property
    def write_roi(self):
        return self._to_funlib_roi(self._block.write_roi)

    @property
    def read_roi(self):
        return self._to_funlib_roi(self._block.read_roi)

    @property
    def status(self):
        return self._block.status

    @status.setter
    def status(self, value):
        self._block.status = value

    @property
    def block_id(self):
        return self._block.block_id

    def __getattr__(self, name):
        return getattr(self._block, name)

    def __repr__(self):
        return f"_BlockProxy({self._block!r})"


def _install_block_proxy():
    """Monkey-patch `daisy.Client.acquire_block` to yield blocks wrapped in a
    `_BlockProxy`. Volara and other daisy 1.x callers only ever obtain blocks
    through `Client.acquire_block`, so this single boundary is enough to keep
    daisy's Rust Roi/Coordinate types out of user code — they always see
    funlib.geometry types instead.

    With this in place, the daisy v2 Rust Roi/Coordinate can stay strict (no
    duck-typing, no shape setters, no arithmetic shims) and v1.x consumers
    keep working."""
    from contextlib import contextmanager
    from daisy._task import Client

    _orig_acquire = Client.acquire_block

    @contextmanager
    def acquire_block(self):
        with _orig_acquire(self) as block:
            if block is None:
                yield None
            else:
                yield _BlockProxy(block)

    Client.acquire_block = acquire_block


_install_block_proxy()


def _wrap_block_fn(fn):
    """Wrap a 1-arg (block-taking) process function so it receives
    `_BlockProxy`-wrapped blocks. 0-arg spawn functions and anything whose
    signature can't be inspected pass through unchanged. The wrapper is a
    plain 1-arg `def` so the Rust side's getfullargspec-based arity
    detection still classifies it as a block processor."""
    import inspect

    if fn is None:
        return None
    try:
        argspec = inspect.getfullargspec(fn)
    except TypeError:
        return fn
    nargs = len([a for a in argspec.args if a != "self"])
    if nargs != 1:
        return fn

    def _compat_block_fn(block):
        return fn(_BlockProxy(block))

    return _compat_block_fn


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
        # daisy 1.x callers pass funlib.geometry Roi / Coordinate; daisy
        # v2's Rust Task constructor requires its own native types.
        for k in ("total_roi", "read_roi", "write_roi"):
            if k in kwargs:
                kwargs[k] = _coerce_roi(kwargs[k])
        # 1-arg (block-taking) process functions on the compat surface
        # receive `_BlockProxy`-wrapped blocks (funlib-typed ROIs), matching
        # what spawn-mode workers get from the patched Client.acquire_block.
        # Without this, the Rust block-fn path hands v1.x user code native
        # ROIs that fail isinstance/equality against funlib types.
        if "process_function" in kwargs:
            kwargs["process_function"] = _wrap_block_fn(kwargs["process_function"])
        elif len(args) >= 5 and args[4] is not None:
            args = (*args[:4], _wrap_block_fn(args[4]), *args[5:])
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
