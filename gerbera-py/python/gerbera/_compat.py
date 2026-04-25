"""Daisy-compatible Python wrappers over gerbera's Rust core.

Each class maps daisy's constructor signature to gerbera's Rust types.
All scheduling, block lifecycle, worker management, and TCP coordination
lives in Rust.
"""

from contextlib import contextmanager
import copy
import inspect
import logging
import os
from pathlib import Path

import gerbera._gerbera as _rs
from gerbera import logging as _worker_log

logger = logging.getLogger(__name__)


# Global default base directory for per-task done-marker arrays. When set,
# any `Task(..., done_marker_path=None)` (the default) resolves to
# `<basedir>/<task_id>`. Set to `None` to disable (no auto-marker).
_DONE_MARKER_BASEDIR: Path | None = None


def set_done_marker_basedir(path) -> None:
    """Set the global base directory for per-task done-marker arrays.

    When a `Task` is constructed with `done_marker_path=None` (the
    default) and a basedir is set, the marker for that task lives at
    `<basedir>/<task_id>`. Set to `None` to disable auto-resolution —
    tasks without an explicit `done_marker_path` will then run without
    a marker.
    """
    global _DONE_MARKER_BASEDIR
    _DONE_MARKER_BASEDIR = Path(path) if path is not None else None


def get_done_marker_basedir() -> Path | None:
    return _DONE_MARKER_BASEDIR

# Types with identical API — no wrapping needed.
Roi = _rs.Roi
Coordinate = _rs.Coordinate
Block = _rs.Block
BlockStatus = _rs.BlockStatus
BlockwiseDependencyGraph = _rs.BlockwiseDependencyGraph
DependencyGraph = _rs.DependencyGraph
TaskState = _rs.TaskState


class Task:
    """Maps daisy's positional constructor to gerbera's keyword-only Rust Task."""

    _NUM_WORKERS_SENTINEL = object()

    def __init__(
        self,
        task_id,
        total_roi,
        read_roi,
        write_roi,
        process_function=None,
        check_function=None,
        init_callback_fn=None,
        read_write_conflict=True,
        num_workers=_NUM_WORKERS_SENTINEL,
        max_retries=2,
        fit="valid",
        timeout=None,
        upstream_tasks=None,
        done_marker_path=None,
        max_workers=None,
        requires=None,
    ):
        self.task_id = task_id
        self.total_roi = total_roi
        self.read_roi = read_roi
        self.write_roi = write_roi
        self.process_function = process_function
        self.check_function = check_function
        self.init_callback_fn = init_callback_fn
        self.read_write_conflict = read_write_conflict
        # `num_workers` is the legacy (daisy-compatible) name; `max_workers`
        # is the gerbera name and matches `requires`-based resource
        # accounting. Accept both, prefer `max_workers` when set, emit a
        # DeprecationWarning if the legacy name is passed.
        if num_workers is not Task._NUM_WORKERS_SENTINEL:
            import warnings
            warnings.warn(
                "Task(num_workers=...) is deprecated; use max_workers=... instead. "
                "Both currently behave identically — max_workers is a hard cap on "
                "concurrent workers per task and composes with the runner's "
                "global `resources=` budget when `requires=` is set.",
                DeprecationWarning,
                stacklevel=2,
            )
            if max_workers is None:
                max_workers = num_workers
        self.max_workers = 1 if max_workers is None else int(max_workers)
        self.max_retries = max_retries
        self.fit = fit
        self.timeout = timeout
        self.upstream_tasks = upstream_tasks or []
        # Three states for `done_marker_path`:
        #   - explicit string/Path  → use it verbatim
        #   - False                 → disabled for this task (override basedir)
        #   - None (default)        → fall back to global basedir + task_id
        self.done_marker_path = done_marker_path
        # Per-worker resource cost; empty dict (or None) disables
        # resource accounting for this task — the worker count is bounded
        # purely by `max_workers`.
        self.requires = dict(requires) if requires else {}

    @property
    def num_workers(self):
        """Legacy alias for `max_workers`."""
        return self.max_workers

    @num_workers.setter
    def num_workers(self, value):
        self.max_workers = int(value)

    def _resolve_done_marker_path(self) -> str | None:
        """Resolve the effective marker path string (or None to disable)."""
        if self.done_marker_path is False:
            return None
        if self.done_marker_path is not None:
            return str(self.done_marker_path)
        basedir = get_done_marker_basedir()
        if basedir is None:
            return None
        return str(Path(basedir) / self.task_id)

    def _to_rs(self):
        upstream_rs = [t._to_rs() if isinstance(t, Task) else t for t in self.upstream_tasks]
        return _rs.Task(
            task_id=self.task_id,
            total_roi=self.total_roi,
            read_roi=self.read_roi,
            write_roi=self.write_roi,
            process_function=self.process_function,
            check_function=self.check_function,
            read_write_conflict=self.read_write_conflict,
            fit=self.fit,
            num_workers=self.max_workers,
            max_retries=self.max_retries,
            upstream_tasks=upstream_rs if upstream_rs else None,
            done_marker_path=self._resolve_done_marker_path(),
            requires=self.requires if self.requires else None,
        )

    def requires(self):
        return self.upstream_tasks


def _convert_tasks(tasks):
    return [t._to_rs() if isinstance(t, Task) else t for t in tasks]


class Scheduler:
    """Maps daisy's Scheduler(tasks) to _rs.Scheduler."""

    def __init__(self, tasks, count_all_orphans=True):
        self._inner = _rs.Scheduler(_convert_tasks(tasks))

    def acquire_block(self, task_id):
        return self._inner.acquire_block(task_id)

    def release_block(self, block):
        self._inner.release_block(block)

    @property
    def task_states(self):
        return self._inner.task_states

    @property
    def dependency_graph(self):
        return self._inner.dependency_graph


class Context:
    """Env-var encoding for passing server address to worker processes."""

    ENV_VARIABLE = "GERBERA_CONTEXT"

    def __init__(self, **kwargs):
        self._dict = dict(**kwargs)

    def copy(self):
        return copy.deepcopy(self)

    def to_env(self):
        return ":".join(f"{k}={v}" for k, v in self._dict.items())

    def __setitem__(self, k, v):
        self._dict[str(k)] = str(v)

    def __getitem__(self, k):
        return self._dict[k]

    def get(self, k, v=None):
        return self._dict.get(k, v)

    def __repr__(self):
        return self.to_env()

    @staticmethod
    def from_env():
        try:
            tokens = os.environ[Context.ENV_VARIABLE].split(":")
        except KeyError:
            logger.error("%s not found!", Context.ENV_VARIABLE)
            raise
        context = Context()
        for token in tokens:
            k, v = token.split("=")
            context[k] = v
        return context


class Client:
    """Maps daisy's Client(context) to _rs.SyncClient."""

    def __init__(self, context=None):
        if context is None:
            context = Context.from_env()
        self.context = context
        self.host = context["hostname"]
        self.port = int(context["port"])
        self.worker_id = int(context["worker_id"])
        self.task_id = context["task_id"]
        self._client = _rs.SyncClient(self.host, self.port, self.task_id)

    @contextmanager
    def acquire_block(self):
        block = self._client.acquire_block()
        if block is None:
            yield None
            return
        try:
            yield block
            if block.status == BlockStatus.PROCESSING:
                block.status = BlockStatus.SUCCESS
        except BaseException as e:
            # Mark the block failed, emit a structured warning, write
            # the traceback to the worker's log destinations (one
            # consolidated call that bypasses `sys.stderr` so it can't
            # be picked up by host-environment hooks), and re-raise so
            # the caller can decide whether to continue.
            block.status = BlockStatus.FAILED
            try:
                _worker_log.logger.warning(
                    "block %s failed: %s", block.block_id, e,
                )
                _worker_log.emit_failure(
                    f"block {block.block_id} failed:\n"
                    + _worker_log.format_traceback(
                        type(e), e, e.__traceback__,
                    )
                )
            except Exception:
                pass
            raise
        finally:
            if block.status != BlockStatus.SUCCESS:
                block.status = BlockStatus.FAILED
            self._client.release_block(block)

    def __del__(self):
        try:
            self._client.disconnect()
        except Exception:
            pass


def _wrap_for_worker_logging(task):
    """Return a shallow copy of `task` whose `process_function` is wrapped
    so that stdout/stderr emitted during the call go to the worker's log
    files (see `gerbera.logging`)."""
    if task.process_function is None:
        return task

    orig = task.process_function
    task_id = task.task_id
    nargs = len([a for a in inspect.getfullargspec(orig).args if a != "self"])

    if nargs == 0:
        def wrapped():
            with _worker_log._WorkerLogContext(task_id):
                try:
                    return orig()
                except BaseException as e:
                    _worker_log.logger.warning(
                        "worker function %s failed: %s",
                        task_id, e,
                    )
                    _worker_log.emit_failure(
                        _worker_log.format_traceback(
                            type(e), e, e.__traceback__,
                        )
                    )
                    raise
    else:
        def wrapped(block):
            with _worker_log._WorkerLogContext(task_id):
                try:
                    return orig(block)
                except BaseException as e:
                    _worker_log.logger.warning(
                        "block %s failed: %s", block.block_id, e,
                    )
                    _worker_log.emit_failure(
                        f"block {block.block_id} failed:\n"
                        + _worker_log.format_traceback(
                            type(e), e, e.__traceback__,
                        )
                    )
                    raise

    wrapped.__name__ = getattr(orig, "__name__", "process_function")
    wrapped.__qualname__ = getattr(orig, "__qualname__", wrapped.__name__)

    clone = copy.copy(task)
    clone.process_function = wrapped
    # Upstream tasks get wrapped too so a chained pipeline logs uniformly.
    clone.upstream_tasks = [
        _wrap_for_worker_logging(up) if isinstance(up, Task) else up
        for up in task.upstream_tasks
    ]
    return clone


def _print_execution_summary(states):
    """Daisy-style post-run report. Writes to the real stdout even if the
    per-worker log proxy is currently installed."""
    import sys
    out = _worker_log._saved_stdout or sys.__stdout__ or sys.stdout

    def p(s=""):
        print(s, file=out)

    p()
    p("Execution Summary")
    p("-----------------")

    if not states:
        return

    rows = []
    failed_tasks = []
    for task_id in sorted(states.keys()):
        state = states[task_id]
        total = state.total_block_count
        completed = state.completed_count
        failed = state.failed_count
        orphaned = state.orphaned_count
        skipped = state.skipped_count
        processing = state.processing_count
        pending = state.pending_count

        if failed > 0:
            status = "✗"
            failed_tasks.append(task_id)
        elif orphaned > 0:
            status = "∅"
        elif pending > 0 or processing > 0:
            status = "…"
        else:
            status = "✔"

        rows.append((task_id, status, total, completed, skipped, failed, orphaned))

    name_w = max(len("task"), max(len(r[0]) for r in rows))
    cols = [
        (name_w + 2,  f"{'task':<{name_w + 2}}"),   # name + status symbol
        (7,           f"{'blocks':>7}"),
        (10,          f"{'completed':>10}"),
        (7,           f"{'skipped':>7}"),
        (6,           f"{'failed':>6}"),
        (8,           f"{'orphaned':>8}"),
    ]
    p()
    p("    " + "  ".join(text for _, text in cols))
    p("    " + "  ".join("─" * w for w, _ in cols))
    for task_id, status, total, completed, skipped, failed, orphaned in rows:
        first = f"{task_id} {status}".ljust(name_w + 2)
        p(f"    {first}  {total:>7}  {completed:>10}  "
          f"{skipped:>7}  {failed:>6}  {orphaned:>8}")

    log_basedir = _worker_log.get_log_basedir()
    files_written = log_basedir is not None and _worker_log.get_log_mode() != "console"
    if failed_tasks and files_written:
        p()
        if len(failed_tasks) == 1:
            p(f"    See worker logs for details under "
              f"{log_basedir / failed_tasks[0]}/")
        else:
            p(f"    See worker logs for failed tasks under {log_basedir}/")
            for tid in failed_tasks:
                p(f"      {log_basedir / tid}/")


class SerialServer:
    """Maps daisy's SerialServer().run_blockwise(tasks) to Rust."""

    def run_blockwise(self, tasks, resources=None):
        # `resources` is accepted for API parity with `Server` but is
        # ignored — the serial runner is single-threaded so resource
        # accounting has no effect.
        wrapped = [_wrap_for_worker_logging(t) if isinstance(t, Task) else t
                   for t in tasks]
        try:
            return _rs._run_serial(_convert_tasks(wrapped))
        finally:
            _worker_log.close_all_log_files()


class Server:
    """Maps daisy's Server().run_blockwise(tasks) to Rust.

    Worker threads are spawned and managed entirely by the Rust server.
    """

    def __init__(self, stop_event=None):
        self._stop_event = stop_event
        self.hostname = None
        self.port = None

    def run_blockwise(self, tasks, resources=None):
        wrapped = [_wrap_for_worker_logging(t) if isinstance(t, Task) else t
                   for t in tasks]
        try:
            states, run_stats = _rs._run_distributed_server(
                _convert_tasks(wrapped), resources,
            )
            self.last_run_stats = run_stats
            return states
        finally:
            _worker_log.close_all_log_files()


def _format_bytes(n):
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024.0:
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} PB"


def _print_resource_utilization(stats):
    """Daisy-style post-run report of resource utilisation."""
    if stats is None:
        return
    import sys
    out = _worker_log._saved_stdout or sys.__stdout__ or sys.stdout

    def p(s=""):
        print(s, file=out)

    process = stats.get("process") or {}
    per_task = stats.get("per_task") or {}
    if not per_task and (process.get("unavailable", False) or not process):
        return

    p()
    p("Resource Utilization")
    p("--------------------")

    wall = process.get("wall_time_secs", 0.0)
    cpu_total = process.get("total_cpu_time_secs", 0.0)
    rss = process.get("peak_rss_bytes", 0)
    disk_r = process.get("disk_read_bytes", 0)
    disk_w = process.get("disk_write_bytes", 0)
    cpu_eff = (cpu_total / wall) if wall > 0 else 0.0

    p()
    p("  Process:")
    p(f"    peak RSS       : {_format_bytes(rss)}")
    p(f"    total CPU time : {cpu_total:.2f} s   (across all threads)")
    p(f"    wall time      : {wall:.2f} s")
    p(f"    cpu efficiency : {cpu_eff:.2f}x   (≈ {cpu_eff:.1f} cores busy on average)")
    p(f"    disk read      : {_format_bytes(disk_r)}")
    p(f"    disk write     : {_format_bytes(disk_w)}")

    if not per_task:
        return

    p()
    p("  Per-task:")
    p(f"    {'task':<14}{'blocks':>8}{'max conc':>10}"
      f"    {'mean ms ∠ slope':<22}{'cpu busy':>10}{'wall':>10}")
    p(f"    {'─' * 14}{'─' * 8}{'─' * 10}    "
      f"{'─' * 22}{'─' * 10}{'─' * 10}")
    for task_id in sorted(per_task.keys()):
        t = per_task[task_id]
        blocks = int(t.get("blocks_processed", 0))
        max_conc = int(t.get("max_concurrent_workers", 0))
        mean_ms = float(t.get("mean_block_ms", 0.0))
        slope = float(t.get("block_ms_slope", 0.0))
        # CPU busy = sum(block durations) / sum(worker wall) — what
        # fraction of the time a worker had a block in hand vs. idle.
        block_total = float(t.get("total_block_time_secs", 0.0))
        worker_wall = float(t.get("total_wall_time_secs", 0.0))
        busy = (block_total / worker_wall * 100.0) if worker_wall > 0 else 0.0
        wall_t = worker_wall
        trend = f"{mean_ms:6.2f} ∠ {slope:+.4f}"
        p(f"    {task_id:<14}{blocks:>8}{max_conc:>10}"
          f"    {trend:<22}{busy:>9.0f}%{wall_t:>9.2f}s")


def run_blockwise(tasks, multiprocessing=True, resources=None):
    """Run the given tasks to completion.

    `resources` is an optional `dict[str, int]` global budget consumed by
    tasks whose `requires=...` declares non-empty entries. Tasks with
    empty `requires` are bounded only by their own `max_workers` cap.
    Hard-errors at startup if any task's `requires` exceeds the budget.
    Ignored in serial mode.
    """
    server = Server() if multiprocessing else SerialServer()
    states = server.run_blockwise(tasks, resources=resources)
    _print_execution_summary(states)
    _print_resource_utilization(getattr(server, "last_run_stats", None))
    return all(s.is_done() for s in states.values())
