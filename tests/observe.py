"""Helpers for observing worker behaviour across process boundaries.

Every distributed daisy worker runs in its own OS process, so a test cannot
watch one with an in-process counter — the counter would be incremented in
the child and the parent would read zero, which asserts nothing. These
helpers record what happened to files instead and reconstruct it afterwards.

Two rules of thumb for tests in this suite:

- To observe *concurrency*, record an interval per block with
  `record_interval` and sweep with `peak_concurrency`. Wall-clock
  timestamps come from `CLOCK_MONOTONIC`, which is system-wide on Linux
  and therefore comparable between processes (`time.perf_counter` is
  explicitly not).
- To observe *a value the block function computed*, write a file per block
  and read the directory afterwards.

Tests whose subject is mode-independent and which really want closures over
live objects should use `run_blockwise(..., multiprocessing=False)` instead:
serial mode runs the original function in-process.
"""

import os
import time
from pathlib import Path


def now_ns() -> int:
    """A timestamp comparable across processes."""
    return time.clock_gettime_ns(time.CLOCK_MONOTONIC)


def record_interval(outdir, task_id, block, start_ns, end_ns) -> None:
    """Append one block's execution interval to `outdir`."""
    Path(outdir).mkdir(parents=True, exist_ok=True)
    name = f"{task_id}-{block.block_id[1]}-{os.getpid()}-{start_ns}"
    Path(outdir, name).write_text(f"{task_id} {start_ns} {end_ns}")


def interval_recorder(outdir, task_id, hold_s):
    """Return a picklable 1-arg block function that holds for `hold_s` and
    records the interval it was running for."""
    outdir = str(outdir)

    def process(block):
        start = now_ns()
        time.sleep(hold_s)
        record_interval(outdir, task_id, block, start, now_ns())

    return process


def peak_concurrency(outdir, task_ids=None) -> int:
    """Maximum number of recorded intervals overlapping at any instant.

    With `task_ids`, only those tasks' intervals count — pass a set to get a
    combined peak across several tasks, which is what a shared resource
    budget bounds.
    """
    events: list[tuple[int, int]] = []
    outdir = Path(outdir)
    if not outdir.exists():
        return 0
    for path in outdir.iterdir():
        tid, start, end = path.read_text().split()
        if task_ids is not None and tid not in task_ids:
            continue
        events.append((int(start), 1))
        events.append((int(end), -1))
    # Ties resolve close-before-open (-1 sorts first), so an instantaneous
    # hand-off never reads as two concurrent workers.
    events.sort()
    alive = peak = 0
    for _, delta in events:
        alive += delta
        peak = max(peak, alive)
    return peak


def recorded_blocks(outdir, task_id=None) -> int:
    """How many blocks recorded an interval."""
    outdir = Path(outdir)
    if not outdir.exists():
        return 0
    return sum(
        1
        for p in outdir.iterdir()
        if task_id is None or p.read_text().split()[0] == task_id
    )
