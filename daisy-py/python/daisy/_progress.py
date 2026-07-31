"""Progress display: tqdm bars, JSON observer, post-run summary,
resource report, and the topological ordering that drives them.

Imports nothing from `_runner.py`; both `_runner.py` and the public
`__init__.py` import from here.
"""

import json
import logging
import sys
import time

from daisy import logging as _worker_log

logger = logging.getLogger(__name__)


def _log_resume_summary(states):
    """Emit one INFO record per task that skipped blocks via done markers.

    Skipping is easy to miss: a resumed run "succeeds" while executing a
    fraction (possibly none) of its blocks. This goes through the standard
    `daisy.*` logger — nothing is forced onto the terminal; users control
    visibility with ordinary logging configuration."""
    for task_id, state in states.items():
        skipped = state.skipped_count
        if skipped > 0:
            logger.info(
                "task %r: resumed — %d/%d blocks skipped via done markers "
                "(Task.reset() or done_marker_path=False reprocesses them)",
                task_id,
                skipped,
                state.total_block_count,
            )


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
    # the saved handle can be a test harness's (or host app's) capture
    # object that has since been closed — never let the summary crash a run
    if getattr(out, "closed", False):
        out = sys.__stdout__ or sys.stdout

    def p(s=""):
        try:
            print(s, file=out)
        except ValueError:  # closed file raced us
            pass

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
        (name_w + 2, f"{'task':<{name_w + 2}}"),  # name + status symbol
        (7, f"{'blocks':>7}"),
        (10, f"{'completed':>10}"),
        (7, f"{'skipped':>7}"),
        (6, f"{'failed':>6}"),
        (8, f"{'orphaned':>8}"),
    ]
    p()
    p("    " + "  ".join(text for _, text in cols))
    p("    " + "  ".join("─" * w for w, _ in cols))
    for task_id, status, total, completed, skipped, failed, orphaned in rows:
        first = f"{task_id} {status}".ljust(name_w + 2)
        p(
            f"    {first}  {total:>7}  {completed:>10}  "
            f"{skipped:>7}  {failed:>6}  {orphaned:>8}"
        )

    # Show the FIRST failure's traceback inline — one traceback, not N;
    # the root cause is almost always the first error, and the rest are
    # in the worker logs.
    for tid in failed_tasks:
        tb = getattr(states[tid], "first_worker_error", None)
        if tb:
            n_more = states[tid].failed_count - 1
            p()
            p(f"    First failure in task '{tid}':")
            for line in tb.splitlines():
                p(f"      {line}")
            if n_more > 0:
                p(f"      (+ {n_more} more failed blocks — see worker logs)")
            break

    # Timeout attribution: when failures include deadline reclaims, say so
    # and point at the knob — "my blocks are slow" and "my code crashes"
    # need different fixes.
    for tid, state in states.items():
        reclaims = getattr(state, "timeout_reclaim_count", 0)
        if not reclaims:
            continue
        t = getattr(state, "timeout_secs", None)
        from daisy._daisy import DEFAULT_BLOCK_TIMEOUT_SECS

        is_default = t is not None and t == DEFAULT_BLOCK_TIMEOUT_SECS
        shown = f"{t:g}s" if t is not None else "the configured timeout"
        p()
        p(
            f"    {reclaims} block attempt(s) in task '{tid}' exceeded the "
            f"block timeout ({shown}{' — the default' if is_default else ''}; "
            f"pass Task(timeout=...) to raise it for slow blocks)"
        )

    log_basedir = _worker_log.get_log_basedir()
    files_written = log_basedir is not None and _worker_log.get_log_mode() != "console"
    if failed_tasks and files_written:
        p()
        if len(failed_tasks) == 1:
            p(f"    See worker logs for details under {log_basedir / failed_tasks[0]}/")
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
                total=total,
                desc=self._desc(task_id, "▶", 0),
                unit="block",
                leave=True,
                dynamic_ncols=True,
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
                task_id,
                "▶",
                int(state.worker_restart_count),
            )
            bar.set_postfix(
                {
                    "⧗": int(state.pending_count),
                    "▶": int(state.processing_count),
                    "✔": int(state.completed_count),
                    "✗": int(state.failed_count),
                    "∅": int(state.orphaned_count),
                },
                refresh=False,
            )

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
                task_id,
                symbol,
                int(state.worker_restart_count),
                refresh=True,
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


def _format_bytes(n):
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024.0:
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} PB"


def _print_resource_utilization(stats, task_order=None):
    """Post-run resource report, agglomerated from per-block measurements.

    Every figure here is a fold over what the tracking layer wrote as
    blocks came back — `blocks` is the same count the scheduler completed,
    because it is the same counter. Tasks that did not opt into
    `resource_tracking` have no resource figures, so the panel is omitted
    rather than printing a table of zeros.
    """
    if stats is None:
        return
    import sys

    per_task = (stats.get("per_task") or {}) if hasattr(stats, "get") else {}
    measured = {k: v for k, v in per_task.items() if v.get("has_stats")}
    if not measured:
        return

    out = _worker_log._saved_stdout or sys.__stdout__ or sys.stdout
    # the saved handle can be a test harness's (or host app's) capture
    # object that has since been closed — never let the summary crash a run
    if getattr(out, "closed", False):
        out = sys.__stdout__ or sys.stdout

    def p(s=""):
        try:
            print(s, file=out)
        except ValueError:  # closed file raced us
            pass

    total_cpu = sum(float(t.get("total_cpu_secs", 0.0)) for t in measured.values())
    total_block = sum(float(t.get("total_block_secs", 0.0)) for t in measured.values())
    peak_rss = max(int(t.get("max_peak_rss_bytes", 0)) for t in measured.values())
    io_r = sum(int(t.get("io_read_bytes", 0)) for t in measured.values())
    io_w = sum(int(t.get("io_write_bytes", 0)) for t in measured.values())
    n_blocks = sum(int(t.get("blocks", 0)) for t in measured.values())

    p()
    p("Resource Utilization")
    p("--------------------")
    p()
    p("  Totals (summed over measured blocks):")
    p(f"    blocks measured : {n_blocks}")
    p(f"    CPU time        : {total_cpu:.2f} s")
    p(f"    in-block time   : {total_block:.2f} s")
    if total_block > 0:
        # How much CPU each second of block time consumed: ~1.0 means
        # CPU-bound, well under 1.0 means the blocks were waiting on IO.
        p(
            f"    CPU per block-s : {total_cpu / total_block:.2f}"
            "   (≈1.0 CPU-bound, «1.0 IO-bound)"
        )
    p(f"    peak RSS        : {_format_bytes(peak_rss)}   (largest single worker)")
    p(f"    IO read         : {_format_bytes(io_r)}")
    p(f"    IO write        : {_format_bytes(io_w)}")

    p()
    p("  Per-task:")
    p(
        f"    {'task':<14}{'blocks':>8}{'fails':>7}"
        f"    {'mean ms ∠ slope':<22}{'cpu s':>9}{'peak RSS':>11}"
    )
    p(f"    {'─' * 14}{'─' * 8}{'─' * 7}    {'─' * 22}{'─' * 9}{'─' * 11}")
    for task_id in _ordered_states(measured, task_order):
        t = measured[task_id]
        blocks = int(t.get("blocks", 0))
        fails = int(t.get("failures", 0))
        mean_ms = float(t.get("mean_block_ms", 0.0))
        slope = float(t.get("block_ms_slope", 0.0))
        cpu = float(t.get("total_cpu_secs", 0.0))
        rss = int(t.get("max_peak_rss_bytes", 0))
        trend = f"{mean_ms:6.2f} ∠ {slope:+.4f}"
        p(
            f"    {task_id:<14}{blocks:>8}{fails:>7}"
            f"    {trend:<22}{cpu:>8.2f}s{_format_bytes(rss):>11}"
        )
