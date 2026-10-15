"""The run report: per-block and per-worker figures for every task of a run.

Built from the JSON the Rust side emits (`daisy_core::run_report`) — the
same line it appends to each tracked task's ``runs.jsonl`` — so the object
you get back from a run and the record left on disk are one and the same
shape.

Primary interface::

    report = daisy.last_report()          # or daisy.Server().last_report
    t = report.tasks["segment"]
    t.blocks.wall                          # Stat: n, mean, std, min, max, total
    t.workers.startup                      # Stat over workers
    t.workers.idle                         # Stat over per-block gaps, all workers
    print(report)                          # the panel the run prints

Drill-downs::

    t.worker(1)                            # WorkerRecord
    t.block(block)                         # BlockRecord from the tracking store
    t.block((1, 2))                        # ...by grid coordinate
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

_last: list = [None]


def last_report():
    """The `RunReport` of the most recent run in this process (any entry
    point), or None before the first run finishes."""
    return _last[0]


def _set_last(report) -> None:
    _last[0] = report


# -- formatting ---------------------------------------------------------------


def _format_bytes(n) -> str:
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024.0:
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} PB"


def _secs_unit(x: float) -> tuple[float, str]:
    """Scale and unit that keep a duration readable."""
    x = abs(x)
    if x >= 1.0 or x == 0.0:
        return 1.0, "s"
    if x >= 1e-3:
        return 1e3, "ms"
    return 1e6, "µs"


def _format_secs(x: float | None) -> str:
    if x is None:
        return "—"
    scale, unit = _secs_unit(x)
    v = x * scale
    return f"{v:.2f} {unit}" if scale == 1.0 else f"{v:.1f} {unit}"


@dataclass(frozen=True)
class Stat:
    """A measured distribution: sample count, mean, sample standard
    deviation, min, max and total. Empty (``n == 0``) when nothing was
    measured — it then formats as ``—``."""

    n: int = 0
    mean: float = 0.0
    std: float = 0.0
    min: float = 0.0
    max: float = 0.0
    total: float = 0.0

    @classmethod
    def from_dict(cls, d) -> Stat:
        if not d:
            return cls()
        return cls(
            n=int(d.get("n", 0)),
            mean=float(d.get("mean", 0.0)),
            std=float(d.get("std", 0.0)),
            min=float(d.get("min", 0.0)),
            max=float(d.get("max", 0.0)),
            total=float(d.get("sum", d.get("total", 0.0))),
        )

    def to_dict(self) -> dict:
        return {
            "n": self.n,
            "mean": self.mean,
            "std": self.std,
            "min": self.min,
            "max": self.max,
            "sum": self.total,
        }

    def __bool__(self) -> bool:
        return self.n > 0

    def format(self, unit: str = "s") -> str:
        """``mean ± std`` in seconds (``unit="s"``) or bytes (``unit="B"``);
        just the mean when there is a single sample."""
        if self.n == 0:
            return "—"
        if unit == "B":
            m = _format_bytes(self.mean)
            return m if self.n < 2 else f"{m} ± {_format_bytes(self.std)}"
        scale, u = _secs_unit(self.mean)
        prec = 2 if scale == 1.0 else 1
        m = f"{self.mean * scale:.{prec}f}"
        if self.n < 2:
            return f"{m} {u}"
        return f"{m} {u} ± {self.std * scale:.{prec}f} {u}"

    def __str__(self) -> str:
        return self.format()


# -- sections -----------------------------------------------------------------


@dataclass(frozen=True)
class BlockSummary:
    """Per-block figures for one task over this run.

    ``round_trip`` (dispatch → return, measured by the scheduler) needs no
    tracking store and is there for every distributed task. The remaining
    ``Stat`` fields are measured inside the worker and are empty unless the
    task set ``resource_tracking=True``; ``overhead`` is the round trip
    minus the block's own wall time."""

    count: int = 0
    failures: int = 0
    has_stats: bool = False
    round_trip: Stat = field(default_factory=Stat)
    wall: Stat = field(default_factory=Stat)
    cpu: Stat = field(default_factory=Stat)
    peak_rss: Stat = field(default_factory=Stat)
    io_read: Stat = field(default_factory=Stat)
    io_write: Stat = field(default_factory=Stat)
    overhead: Stat = field(default_factory=Stat)
    trend_slope_ms: float = 0.0

    @classmethod
    def from_dict(cls, d) -> BlockSummary:
        d = d or {}
        return cls(
            count=int(d.get("count", 0)),
            failures=int(d.get("failures", 0)),
            has_stats=bool(d.get("has_stats", False)),
            round_trip=Stat.from_dict(d.get("round_trip")),
            wall=Stat.from_dict(d.get("wall")),
            cpu=Stat.from_dict(d.get("cpu")),
            peak_rss=Stat.from_dict(d.get("peak_rss")),
            io_read=Stat.from_dict(d.get("io_read")),
            io_write=Stat.from_dict(d.get("io_write")),
            overhead=Stat.from_dict(d.get("overhead")),
            trend_slope_ms=float(d.get("trend_slope_ms", 0.0)),
        )

    def to_dict(self) -> dict:
        return {
            "count": self.count,
            "failures": self.failures,
            "has_stats": self.has_stats,
            "round_trip": self.round_trip.to_dict(),
            "wall": self.wall.to_dict(),
            "cpu": self.cpu.to_dict(),
            "peak_rss": self.peak_rss.to_dict(),
            "io_read": self.io_read.to_dict(),
            "io_write": self.io_write.to_dict(),
            "overhead": self.overhead.to_dict(),
            "trend_slope_ms": self.trend_slope_ms,
        }


@dataclass(frozen=True)
class WorkerRecord:
    """One worker's lifecycle. Durations in seconds; ``None`` where the
    phase never happened (no ``startup`` for a worker that never connected,
    no ``teardown`` for one that crashed or was cut off)."""

    worker_id: int
    startup: float | None = None
    blocks_ok: int = 0
    blocks_failed: int = 0
    idle: Stat = field(default_factory=Stat)
    compute: Stat = field(default_factory=Stat)
    final_wait: float | None = None
    teardown: float | None = None
    #: ``"clean"``, ``"error"``, ``"timeout"``, ``"interrupted"`` or None
    #: if still running when the report was built.
    exit: str | None = None

    @classmethod
    def from_dict(cls, d) -> WorkerRecord:
        return cls(
            worker_id=int(d["worker_id"]),
            startup=d.get("startup"),
            blocks_ok=int(d.get("blocks_ok", 0)),
            blocks_failed=int(d.get("blocks_failed", 0)),
            idle=Stat.from_dict(d.get("idle")),
            compute=Stat.from_dict(d.get("compute")),
            final_wait=d.get("final_wait"),
            teardown=d.get("teardown"),
            exit=d.get("exit"),
        )

    def to_dict(self) -> dict:
        return {
            "worker_id": self.worker_id,
            "startup": self.startup,
            "blocks_ok": self.blocks_ok,
            "blocks_failed": self.blocks_failed,
            "idle": self.idle.to_dict(),
            "compute": self.compute.to_dict(),
            "final_wait": self.final_wait,
            "teardown": self.teardown,
            "exit": self.exit,
        }

    @property
    def blocks(self) -> int:
        return self.blocks_ok + self.blocks_failed


@dataclass(frozen=True)
class WorkerSummary:
    """The workers daisy spawned for one task. ``startup`` and ``teardown``
    have one sample per worker; ``idle`` and ``compute`` pool every
    worker's per-block samples."""

    count: int = 0
    unmanaged_clients: int = 0
    startup: Stat = field(default_factory=Stat)
    idle: Stat = field(default_factory=Stat)
    compute: Stat = field(default_factory=Stat)
    teardown: Stat = field(default_factory=Stat)
    exits: dict = field(default_factory=dict)
    by_id: dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d) -> WorkerSummary:
        d = d or {}
        rows = [WorkerRecord.from_dict(r) for r in d.get("by_id", [])]
        return cls(
            count=int(d.get("count", 0)),
            unmanaged_clients=int(d.get("unmanaged_clients", 0)),
            startup=Stat.from_dict(d.get("startup")),
            idle=Stat.from_dict(d.get("idle")),
            compute=Stat.from_dict(d.get("compute")),
            teardown=Stat.from_dict(d.get("teardown")),
            exits={k: int(v) for k, v in (d.get("exits") or {}).items()},
            by_id={r.worker_id: r for r in rows},
        )

    def to_dict(self) -> dict:
        return {
            "count": self.count,
            "unmanaged_clients": self.unmanaged_clients,
            "startup": self.startup.to_dict(),
            "idle": self.idle.to_dict(),
            "compute": self.compute.to_dict(),
            "teardown": self.teardown.to_dict(),
            "exits": dict(self.exits),
            "by_id": [r.to_dict() for r in self.by_id.values()],
        }


@dataclass(frozen=True)
class BlockRecord:
    """What the tracking store holds for one block."""

    block_id: tuple
    done: bool
    failures: int
    #: ``daisy.BlockStats`` or None (no resource tracking, or never ran)
    stats: object | None
    #: worker that completed it, or None (unmanaged client / never ran)
    worker_id: int | None
    #: seconds since the UNIX epoch on the scheduler's clock, or None
    sent_at: float | None
    returned_at: float | None

    @property
    def wall_seconds(self) -> float | None:
        return None if self.stats is None else self.stats.wall_seconds

    @property
    def peak_rss_bytes(self) -> int | None:
        return None if self.stats is None else self.stats.peak_rss_bytes


class TaskReport:
    """One task's report. ``blocks`` and ``workers`` are the summaries;
    ``worker(id)`` and ``block(...)`` drill down."""

    def __init__(
        self,
        task_id,
        blocks,
        workers,
        started_at=0.0,
        finished_at=0.0,
        tracked=False,
    ):
        self.task_id = task_id
        self.blocks = blocks
        self.workers = workers
        self.started_at = started_at
        self.finished_at = finished_at
        self.tracked = tracked
        self._task = None

    @classmethod
    def from_dict(cls, d) -> TaskReport:
        return cls(
            task_id=d["task_id"],
            blocks=BlockSummary.from_dict(d.get("blocks")),
            workers=WorkerSummary.from_dict(d.get("workers")),
            started_at=float(d.get("started_at", 0.0)),
            finished_at=float(d.get("finished_at", 0.0)),
            tracked=bool(d.get("tracked", False)),
        )

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "tracked": self.tracked,
            "blocks": self.blocks.to_dict(),
            "workers": self.workers.to_dict(),
        }

    @property
    def elapsed(self) -> float:
        """Run wall time in seconds, start to report."""
        return max(0.0, self.finished_at - self.started_at)

    def worker(self, worker_id) -> WorkerRecord:
        return self.workers.by_id[int(worker_id)]

    def block(self, block) -> BlockRecord | None:
        """Look a block up in the task's tracking store.

        ``block`` is a `daisy.Block`, a grid coordinate (tuple of ints,
        one per axis, anchored at ``total_roi``) or a spatial block id
        (int; found by scanning the grid, so prefer the other two forms on
        large tasks). None when the task has no store or the block is not
        in it. Available on reports returned by a run in this process;
        a report rebuilt from JSON has no task to read the store through.
        """
        task = self._task
        if task is None:
            raise RuntimeError(
                f"TaskReport({self.task_id!r}).block(): no task attached — "
                "per-block lookup reads the tracking store through the Task "
                "and is only available on the report a run returned"
            )
        native = _resolve_block(task, block)
        if native is None:
            return None
        rec = task.block_record(native)
        if rec is None:
            return None
        return BlockRecord(
            block_id=(task.task_id, native.block_id[1]),
            done=rec["done"],
            failures=rec["failures"],
            stats=rec["stats"],
            worker_id=rec["worker_id"],
            sent_at=rec["sent_at"],
            returned_at=rec["returned_at"],
        )

    def __repr__(self) -> str:
        return (
            f"TaskReport({self.task_id!r}, blocks={self.blocks.count}, "
            f"workers={self.workers.count})"
        )

    # -- rendering -------------------------------------------------------

    def render(self, max_workers: int = 10) -> str:
        """The task's section of the Resource Utilization panel."""
        b, w = self.blocks, self.workers
        lines = []
        head = f"  {self.task_id}   {b.count} blocks"
        if w.count:
            head += f", {w.count} worker{'s' if w.count != 1 else ''}"
        if self.elapsed > 0:
            head += f", {_format_secs(self.elapsed)} wall"
        lines.append(head)
        # Always available: the scheduler's own per-block round trip.
        label = "    blocks  "
        if b.round_trip:
            lines.append(f"{label} round trip {b.round_trip.format()}")
            label = "            "
        # Measured inside the worker: only with resource_tracking=True.
        if b.has_stats:
            lines.append(
                f"{label} wall {b.wall.format()}   cpu {b.cpu.format()}"
                f"   peak RSS {b.peak_rss.format('B')}"
            )
            second = (
                f"             io read {_format_bytes(b.io_read.total)}"
                f"  write {_format_bytes(b.io_write.total)}"
            )
            if b.overhead:
                second += f"   overhead {b.overhead.format()}"
            second += f"   trend ∠ {b.trend_slope_ms:+.4f} ms/block"
            lines.append(second)
        if b.failures:
            lines.append(f"             {b.failures} failed attempt(s)")
        if w.count:
            lines.append(
                f"    workers  startup {w.startup.format()}   idle {w.idle.format()}"
                f"   teardown {w.teardown.format()}"
            )
            exits = "  ".join(f"{n} {kind}" for kind, n in w.exits.items() if n)
            extra = f"             exits {exits or '—'}"
            if w.unmanaged_clients:
                extra += f"   (+ {w.unmanaged_clients} unmanaged client(s))"
            lines.append(extra)
            lines.extend(self._worker_table(max_workers))
        return "\n".join(lines)

    def _worker_table(self, max_workers: int) -> list[str]:
        rows = sorted(self.workers.by_id.values(), key=lambda r: r.worker_id)
        if not rows:
            return []
        cols = [
            ("id", 4),
            ("startup", 9),
            ("blocks", 6),
            ("idle", 9),
            ("idle / block", 19),
            ("compute", 9),
            ("compute / block", 19),
            ("teardown", 9),
        ]
        out = ["      " + "  ".join(f"{name:>{w}}" for name, w in cols) + "  exit"]
        for r in rows[:max_workers]:
            cells = [
                str(r.worker_id),
                _format_secs(r.startup),
                str(r.blocks),
                _format_secs(r.idle.total) if r.idle else "—",
                r.idle.format(),
                _format_secs(r.compute.total) if r.compute else "—",
                r.compute.format(),
                _format_secs(r.teardown),
            ]
            out.append(
                "      "
                + "  ".join(f"{c:>{w}}" for c, (_, w) in zip(cells, cols))
                + f"  {r.exit or 'running'}"
            )
        if len(rows) > max_workers:
            out.append(
                f"      … +{len(rows) - max_workers} more; see "
                f'report.tasks["{self.task_id}"].workers.by_id'
            )
        return out


class RunReport:
    """Reports for every task of one run, keyed by task id."""

    def __init__(self, tasks: dict):
        self.tasks = tasks

    @classmethod
    def from_dict(cls, d) -> RunReport:
        return cls({tid: TaskReport.from_dict(t) for tid, t in (d or {}).items()})

    @classmethod
    def from_json(cls, s: str) -> RunReport:
        return cls.from_dict(json.loads(s))

    def to_dict(self) -> dict:
        return {tid: t.to_dict() for tid, t in self.tasks.items()}

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    def __getitem__(self, task_id) -> TaskReport:
        return self.tasks[task_id]

    def __iter__(self):
        return iter(self.tasks)

    def __len__(self) -> int:
        return len(self.tasks)

    def __repr__(self) -> str:
        return f"RunReport({sorted(self.tasks)})"

    @property
    def measured(self) -> dict:
        """The tasks that measured their blocks (``resource_tracking=True``)."""
        return {tid: t for tid, t in self.tasks.items() if t.blocks.has_stats}

    @property
    def timed(self) -> dict:
        """The tasks with anything to show: a worker timeline, a per-block
        round trip, or resource measurements. A serial task without
        resource tracking has none of these."""
        return {
            tid: t
            for tid, t in self.tasks.items()
            if t.workers.count or t.blocks.round_trip or t.blocks.has_stats
        }

    def _attach(self, tasks) -> None:
        """Give each task report the `daisy.Task` it came from, for
        `TaskReport.block`."""
        for task in tasks:
            tid = getattr(task, "task_id", None)
            if tid in self.tasks:
                self.tasks[tid]._task = task

    def render(self, task_order=None, max_workers: int = 10) -> str:
        """The Resource Utilization panel, one section per task in
        ``task_order``. What needs no tracking store — the per-block round
        trip and the per-worker startup / idle / compute / teardown — is
        always shown; the in-worker measurements (wall, CPU, RSS, IO,
        overhead, trend) only for tasks with ``resource_tracking=True``.
        Empty when there is nothing at all to show (a serial run without
        resource tracking) — a table of zeros is worse than no table."""
        measured = self.timed
        if not measured:
            return ""
        if task_order is None:
            order = sorted(measured)
        else:
            seen = set(task_order)
            order = [t for t in task_order if t in measured] + sorted(
                t for t in measured if t not in seen
            )
        parts = ["", "Resource Utilization", "--------------------"]
        for tid in order:
            parts.append("")
            parts.append(measured[tid].render(max_workers=max_workers))
        return "\n".join(parts)

    def __str__(self) -> str:
        return self.render()


def _resolve_block(task, block):
    """A `Block` (or anything carrying a ``_native`` block), a grid
    coordinate tuple, or an int spatial id → the native block, or None."""
    from daisy.block import Block

    if isinstance(block, Block):
        return block._native
    native = getattr(block, "_native", None)
    if native is not None:
        return native
    if hasattr(block, "block_id") and hasattr(block, "write_roi"):
        return block  # already native
    from daisy._task import Roi

    total = task.total_roi
    write = task.write_roi
    if isinstance(block, tuple):
        if len(block) != total.dims:
            raise ValueError(
                f"grid coordinate {block} has {len(block)} entries; "
                f"task {task.task_id!r} has {total.dims} axes"
            )
        offset = tuple(
            int(o) + int(c) * int(s)
            for o, c, s in zip(total.offset, block, write.shape)
        )
        write_roi = Roi(offset, tuple(int(s) for s in write.shape))
        return Block(total, write_roi, write_roi, task_id=task.task_id)._native
    if isinstance(block, int):
        import itertools

        shape = task.block_grid_shape
        for coord in itertools.product(*(range(int(n)) for n in shape)):
            native = _resolve_block(task, tuple(coord))
            if native.block_id[1] == block:
                return native
        return None
    raise TypeError(
        f"block must be a daisy.Block, a grid coordinate tuple or an int "
        f"spatial id, not {type(block).__name__}"
    )
