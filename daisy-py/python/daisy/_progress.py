"""Progress display: tqdm bars, JSON observer, post-run summary,
resource report, and the topological ordering that drives them.

Imports nothing from `_runner.py`; both `_runner.py` and the public
`__init__.py` import from here.
"""

import heapq
import json
import sys
import time

from daisy import logging as _worker_log


def _topo_order(tasks):
    """Topological order of `tasks` with alphabetical tiebreaker on
    the *ready set*.

    The rule: roots (no upstream) become candidates first, sorted
    alphabetically. After printing a task, any of its children whose
    upstream dependencies are now all printed become candidates.
    From the candidate set, always pick the alphabetically smallest.

    Example: `(A → B), (C → D), ((B, D) → E)` produces `[A, B, C, D, E]`
    — B comes right after A because it has only A upstream and `B < C`,
    then C is the next-smallest root, then D unlocks, then E unlocks
    once both its parents are seen.
    """
    # Walk the task graph (handles upstream tasks not in the input list).
    all_tasks = {}
    upstream_map = {}

    # `_rs.Task` no longer carries `upstream_tasks`; v2 expresses
    # task DAGs via `Pipeline`. For users coming from daisy 1.x via
    # `Task(upstream_tasks=[...])`, the v1_compat factory records the
    # upstream list in a side-table that we read here.
    from daisy._task import _get_task_upstream

    def visit(t):
        if t.task_id in all_tasks:
            return
        all_tasks[t.task_id] = t
        ups = list(_get_task_upstream(t) or [])
        upstream_map[t.task_id] = {u.task_id for u in ups}
        for u in ups:
            visit(u)

    for t in tasks:
        visit(t)

    downstream_map = {tid: [] for tid in all_tasks}
    for tid, ups in upstream_map.items():
        for u in ups:
            downstream_map[u].append(tid)

    visited = set()
    order = []
    ready = []
    for tid, ups in upstream_map.items():
        if not ups:
            heapq.heappush(ready, tid)

    while ready:
        tid = heapq.heappop(ready)
        if tid in visited:
            continue
        visited.add(tid)
        order.append(tid)
        for child in downstream_map.get(tid, []):
            if child in visited:
                continue
            if all(u in visited for u in upstream_map[child]):
                heapq.heappush(ready, child)

    return order


def _ordered_states(states, task_order):
    """Honour caller-supplied topological order; fall back to
    alphabetical for unknown task ids."""
    if task_order is None:
        return sorted(states.keys())
    seen = set(task_order)
    return [t for t in task_order if t in states] + sorted(
        tid for tid in states if tid not in seen
    )


def _print_execution_summary(states, task_order=None):
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
    for task_id in _ordered_states(states, task_order):
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


class _TqdmObserver:
    """Default progress observer — one `tqdm.auto` bar per task.

    The bar's description reads `f"{task_id} {symbol} ♻={restarts}"`
    where `symbol` flips from `▶` (running) to `✔` / `✗` / `∅`
    (success / failure / orphaned) at finish, and `restarts` is
    `worker_restart_count` for the task.
    """

    def __init__(self, task_order=None):
        self._bars = {}
        self._last_desc = {}
        # Order in which to create / iterate bars. Set by the caller
        # (typically `_resolve_progress`) from the task DAG. None
        # falls back to dict insertion order.
        self._task_order = list(task_order) if task_order else None

    def _bar(self, task_id, total):
        if task_id not in self._bars:
            from tqdm.auto import tqdm
            self._bars[task_id] = tqdm(
                total=total, desc=self._desc(task_id, "▶", 0),
                unit="block", leave=True, dynamic_ncols=True,
            )
        return self._bars[task_id]

    @staticmethod
    def _desc(task_id, symbol, restarts):
        return f"{task_id} {symbol} ♻={restarts}"

    def _maybe_update_desc(self, task_id, symbol, restarts, refresh=False):
        desc = self._desc(task_id, symbol, restarts)
        if self._last_desc.get(task_id) != desc:
            self._bars[task_id].set_description(desc, refresh=refresh)
            self._last_desc[task_id] = desc

    def _ordered_items(self, states):
        """Yield `(task_id, state)` in `self._task_order`, then any
        leftovers in alphabetical order. This is what determines bar
        creation order on the first `on_start` call — once a `tqdm`
        bar is created its display position is fixed."""
        if self._task_order is None:
            yield from states.items()
            return
        seen = set()
        for tid in self._task_order:
            if tid in states:
                seen.add(tid)
                yield tid, states[tid]
        for tid in sorted(states):
            if tid not in seen:
                yield tid, states[tid]

    def on_start(self, states):
        for task_id, state in self._ordered_items(states):
            self._bar(task_id, int(state.total_block_count))
            self._last_desc[task_id] = self._desc(task_id, "▶", 0)

    def on_progress(self, states):
        for task_id, state in self._ordered_items(states):
            bar = self._bar(task_id, int(state.total_block_count))
            done = int(state.completed_count)
            delta = done - bar.n
            if delta > 0:
                bar.update(delta)
            self._maybe_update_desc(
                task_id, "▶", int(state.worker_restart_count),
            )
            bar.set_postfix({
                "⧗": int(state.pending_count),
                "▶": int(state.processing_count),
                "✔": int(state.completed_count),
                "✗": int(state.failed_count),
                "∅": int(state.orphaned_count),
            }, refresh=False)

    def on_finish(self, states):
        # Promote the trailing emoji to reflect final outcome and close.
        for task_id, state in self._ordered_items(states):
            bar = self._bar(task_id, int(state.total_block_count))
            failed = int(state.failed_count)
            orphaned = int(state.orphaned_count)
            if failed > 0:
                symbol = "✗"
            elif orphaned > 0:
                symbol = "∅"
            else:
                symbol = "✔"
            self._maybe_update_desc(
                task_id, symbol, int(state.worker_restart_count), refresh=True,
            )
            bar.close()


class JsonProgressObserver:
    """Streaming JSON observer for monitoring/dashboards.

    Emits one JSON object per task per state-changing event, with a
    monotonic timestamp and the full counter snapshot. Pipes cleanly
    into log aggregators (`fluentd`, `journald`), JSON-line tools
    (`jq -c`), or anything that reads line-delimited JSON.

    Usage::

        # to a file
        obs = JsonProgressObserver(path="/tmp/progress.jsonl")
        daisy.run_blockwise(tasks, progress=obs)

        # to stdout
        daisy.run_blockwise(tasks, progress=JsonProgressObserver())

        # to an existing file-like object
        with open("progress.jsonl", "w") as f:
            daisy.run_blockwise(tasks, progress=JsonProgressObserver(stream=f))

    One line per task per call. With three tasks and an `on_progress`
    rate of thousands per second, expect a high-volume sink — the
    observer is the right level of granularity for dashboards but not
    for `tail -f` on a slow run.

    Output schema (one line, pretty-printed for readability)::

        {
          "t": 1745781234.123,        # time.time() at emit
          "event": "progress",        # "start" | "progress" | "finish"
          "task": "extract",
          "total": 100000,
          "ready": 99,
          "processing": 4,
          "completed": 4321,
          "skipped": 0,
          "failed": 2,
          "orphaned": 0,
          "restarts": 1,              # worker_restart_count
          "failures": 1               # worker_failure_count (raw deaths)
        }
    """

    def __init__(self, path=None, stream=None):
        if path is not None and stream is not None:
            raise ValueError("specify path OR stream, not both")
        if path is not None:
            self._sink = open(path, "w", buffering=1)  # line-buffered
            self._owns_sink = True
        elif stream is not None:
            self._sink = stream
            self._owns_sink = False
        else:
            self._sink = sys.stdout
            self._owns_sink = False

    def _emit(self, event, states):
        t = time.time()
        for task_id, state in states.items():
            line = {
                "t": t,
                "event": event,
                "task": task_id,
                "total": int(state.total_block_count),
                "ready": int(state.ready_count),
                "processing": int(state.processing_count),
                "completed": int(state.completed_count),
                "skipped": int(state.skipped_count),
                "failed": int(state.failed_count),
                "orphaned": int(state.orphaned_count),
                "restarts": int(state.worker_restart_count),
                "failures": int(state.worker_failure_count),
            }
            self._sink.write(json.dumps(line) + "\n")
        try:
            self._sink.flush()
        except Exception:
            pass

    def on_start(self, states):
        self._emit("start", states)

    def on_progress(self, states):
        self._emit("progress", states)

    def on_finish(self, states):
        self._emit("finish", states)
        if self._owns_sink:
            try:
                self._sink.close()
            except Exception:
                pass


def _resolve_progress(progress, task_order=None):
    """Translate the user-facing `progress=` argument into a Rust-side
    observer object, or `None` if disabled.

    - `True` (default) → built-in `_TqdmObserver` seeded with
      `task_order` (topological + alphabetical, see `_topo_order`).
    - `False` / `None` → no observer (no progress bar).
    - any object exposing `on_start`/`on_progress`/`on_finish` → used directly.
    """
    if progress is False or progress is None:
        return None
    if progress is True:
        return _TqdmObserver(task_order=task_order)
    return progress


def _format_bytes(n):
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024.0:
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} PB"


def _print_resource_utilization(stats, task_order=None):
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
    for task_id in _ordered_states(per_task, task_order):
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
