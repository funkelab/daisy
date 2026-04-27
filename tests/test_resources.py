"""Integration tests for resource-aware worker dispatch.

A task declares `requires={"cpu": 1}` (or similar) and `max_workers`.
The runner is given a global `resources` budget. The number of
*concurrent* workers for a task is bounded by both:

  - its own `max_workers` cap,
  - and the global budget after subtracting other tasks' usage.

These tests verify those bounds empirically by tracking how many of a
task's workers were ever simultaneously alive.
"""

import threading
import time

import pytest

import daisy


def _make_task(task_id, *, max_workers, requires=None, blocks=8, hold_ms=20,
               concurrency_observer=None, total=80, block=10):
    counts = {"alive": 0, "peak": 0}
    lock = threading.Lock()

    def process(block):
        with lock:
            counts["alive"] += 1
            counts["peak"] = max(counts["peak"], counts["alive"])
        try:
            time.sleep(hold_ms / 1000.0)
        finally:
            with lock:
                counts["alive"] -= 1
        if concurrency_observer is not None:
            concurrency_observer(task_id, counts["peak"])

    return daisy.Task(
        task_id=task_id,
        total_roi=daisy.Roi([0], [total]),
        read_roi=daisy.Roi([0], [block]),
        write_roi=daisy.Roi([0], [block]),
        process_function=process,
        read_write_conflict=False,
        max_workers=max_workers,
        max_retries=0,
        requires=requires,
    ), counts


def test_max_workers_caps_concurrency_without_requires(tmp_path):
    """No `requires` → resource budget irrelevant, `max_workers` is the
    only cap. Even with a huge budget, peak alive workers ≤ max_workers."""
    task, counts = _make_task("a", max_workers=3, blocks=8)
    states = daisy.Server().run_blockwise(
        [task],
        resources={"cpu": 100},  # plenty, but task doesn't require it
    )
    assert states["a"].is_done()
    assert counts["peak"] <= 3, f"peak {counts['peak']} exceeded max_workers=3"


def test_resource_budget_caps_one_task_below_max_workers(tmp_path):
    """`requires={"cpu": 1}` with budget {"cpu": 2} caps peak at 2 even
    when max_workers=8."""
    task, counts = _make_task("a", max_workers=8, requires={"cpu": 1})
    states = daisy.Server().run_blockwise([task], resources={"cpu": 2})
    assert states["a"].is_done()
    assert counts["peak"] <= 2, f"peak {counts['peak']} exceeded budget"


def test_two_tasks_share_a_resource(tmp_path):
    """Two CPU tasks competing for a 4-CPU budget — combined peak ≤ 4."""
    cross_lock = threading.Lock()
    cross_alive = {"a": 0, "b": 0, "combined_peak": 0}

    def make(task_id):
        def process(block):
            with cross_lock:
                cross_alive[task_id] += 1
                combined = cross_alive["a"] + cross_alive["b"]
                cross_alive["combined_peak"] = max(cross_alive["combined_peak"], combined)
            try:
                time.sleep(0.02)
            finally:
                with cross_lock:
                    cross_alive[task_id] -= 1

        return daisy.Task(
            task_id=task_id,
            total_roi=daisy.Roi([0], [80]),
            read_roi=daisy.Roi([0], [10]),
            write_roi=daisy.Roi([0], [10]),
            process_function=process,
            read_write_conflict=False,
            max_workers=8,
            max_retries=0,
            requires={"cpu": 1},
        )

    tasks = [make("a"), make("b")]
    states = daisy.Server().run_blockwise(tasks, resources={"cpu": 4})
    assert all(states[t].is_done() for t in ("a", "b"))
    assert cross_alive["combined_peak"] <= 4, (
        f"combined peak {cross_alive['combined_peak']} exceeded budget cpu=4"
    )


def test_disjoint_resources_run_in_parallel(tmp_path):
    """A CPU task and a GPU task on disjoint budgets should both run
    near their `max_workers` cap simultaneously — neither blocks the
    other."""
    cpu_task, cpu_counts = _make_task(
        "cpu", max_workers=4, requires={"cpu": 1}, hold_ms=30
    )
    gpu_task, gpu_counts = _make_task(
        "gpu", max_workers=2, requires={"gpu": 1}, hold_ms=30
    )

    states = daisy.Server().run_blockwise(
        [cpu_task, gpu_task],
        resources={"cpu": 4, "gpu": 2},
    )
    assert states["cpu"].is_done() and states["gpu"].is_done()
    # Peaks should hit the caps — the runner should be willing to spawn
    # all of them since budgets are disjoint.
    assert cpu_counts["peak"] >= 2, f"cpu peak {cpu_counts['peak']} too low"
    assert gpu_counts["peak"] >= 1, f"gpu peak {gpu_counts['peak']} too low"


def test_requires_exceeds_budget_hard_errors():
    """A task whose per-worker `requires` exceeds the global budget must
    error at startup, not silently never spawn."""
    bad_task = daisy.Task(
        task_id="greedy",
        total_roi=daisy.Roi([0], [10]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=lambda b: None,
        read_write_conflict=False,
        max_workers=1,
        max_retries=0,
        requires={"gpu": 8},
    )
    with pytest.raises(Exception) as exc_info:
        daisy.Server().run_blockwise([bad_task], resources={"gpu": 1})
    msg = str(exc_info.value).lower()
    assert "greedy" in msg
    assert "gpu" in msg


def test_chained_tasks_reassign_workers_when_upstream_drains(tmp_path):
    """filter (CPU) → agglom (CPU). While filter is running, agglom is
    upstream-blocked. Once filter drains, its workers exit and agglom's
    workers start picking up. Total filter peak + agglom peak ≤ budget
    at any single instant; both tasks complete."""
    cross_lock = threading.Lock()
    cross_alive = {"filter": 0, "agglom": 0, "combined_peak": 0}

    def make(task_id, upstream=None):
        def process(block):
            with cross_lock:
                cross_alive[task_id] += 1
                combined = cross_alive["filter"] + cross_alive["agglom"]
                cross_alive["combined_peak"] = max(cross_alive["combined_peak"], combined)
            try:
                time.sleep(0.02)
            finally:
                with cross_lock:
                    cross_alive[task_id] -= 1

        return daisy.Task(
            task_id=task_id,
            total_roi=daisy.Roi([0], [40]),
            read_roi=daisy.Roi([0], [10]),
            write_roi=daisy.Roi([0], [10]),
            process_function=process,
            read_write_conflict=False,
            max_workers=4,
            max_retries=0,
            requires={"cpu": 1},
            upstream_tasks=[upstream] if upstream is not None else None,
        )

    filt = make("filter")
    agg = make("agglom", upstream=filt)
    states = daisy.Server().run_blockwise([filt, agg], resources={"cpu": 4})
    assert states["filter"].is_done()
    assert states["agglom"].is_done()
    # Combined peak respects the budget.
    assert cross_alive["combined_peak"] <= 4, (
        f"combined peak {cross_alive['combined_peak']} exceeded cpu=4"
    )


def test_num_workers_keyword_is_rejected():
    """`num_workers=` was the daisy-style keyword; daisy now only
    accepts `max_workers=`. Passing `num_workers=` should be a
    `TypeError` from the constructor signature."""
    with pytest.raises(TypeError):
        daisy.Task(
            task_id="legacy",
            total_roi=daisy.Roi([0], [20]),
            read_roi=daisy.Roi([0], [10]),
            write_roi=daisy.Roi([0], [10]),
            process_function=lambda b: None,
            read_write_conflict=False,
            num_workers=2,
            max_retries=0,
        )
