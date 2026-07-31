"""Integration tests for resource-aware worker dispatch.

A task declares `requires={"cpu": 1}` (or similar) and `max_workers`.
The runner is given a global `resources` budget. The number of
*concurrent* workers for a task is bounded by both:

  - its own `max_workers` cap,
  - and the global budget after subtracting other tasks' usage.

These tests verify those bounds empirically. Every worker is its own
process, so concurrency is measured the only way it can be: each block
records the interval it ran for, and the peak overlap is swept out of those
records afterwards (see `observe.py`). An earlier version of this file used
in-process closure counters and pinned thread workers to make them work.

`hold_s` has to comfortably exceed worker start-up (a fresh interpreter
importing daisy) or workers would drain the blocks in sequence and the
`>=` assertions would measure staggered starts rather than a concurrency
cap.
"""

import pytest
from observe import interval_recorder, peak_concurrency

import daisy

HOLD_S = 0.05


def _make_task(
    task_id,
    outdir,
    *,
    max_workers,
    requires=None,
    hold_s=HOLD_S,
    total=80,
    block=10,
):
    return daisy.Task(
        task_id=task_id,
        total_roi=daisy.Roi([0], [total]),
        read_roi=daisy.Roi([0], [block]),
        write_roi=daisy.Roi([0], [block]),
        process_function=interval_recorder(outdir, task_id, hold_s),
        read_write_conflict=False,
        max_workers=max_workers,
        max_retries=0,
        requires=requires,
    )


def test_max_workers_caps_concurrency_without_requires(tmp_path):
    """No `requires` → resource budget irrelevant, `max_workers` is the
    only cap. Even with a huge budget, peak alive workers ≤ max_workers."""
    outdir = tmp_path / "intervals"
    task = _make_task("a", outdir, max_workers=3)
    states = daisy.Server().run_blockwise(
        [task],
        resources={"cpu": 100},  # plenty, but task doesn't require it
    )
    assert states["a"].is_done()
    peak = peak_concurrency(outdir)
    assert peak <= 3, f"peak {peak} exceeded max_workers=3"


def test_resource_budget_caps_one_task_below_max_workers(tmp_path):
    """`requires={"cpu": 1}` with budget {"cpu": 2} caps peak at 2 even
    when max_workers=8."""
    outdir = tmp_path / "intervals"
    task = _make_task("a", outdir, max_workers=8, requires={"cpu": 1})
    states = daisy.Server().run_blockwise([task], resources={"cpu": 2})
    assert states["a"].is_done()
    peak = peak_concurrency(outdir)
    assert peak <= 2, f"peak {peak} exceeded budget"


def test_two_tasks_share_a_resource(tmp_path):
    """Two CPU tasks competing for a 4-CPU budget — combined peak ≤ 4."""
    outdir = tmp_path / "intervals"
    tasks = [
        _make_task(task_id, outdir, max_workers=8, requires={"cpu": 1})
        for task_id in ("a", "b")
    ]
    states = daisy.Server().run_blockwise(tasks, resources={"cpu": 4})
    assert all(states[t].is_done() for t in ("a", "b"))
    combined = peak_concurrency(outdir, task_ids={"a", "b"})
    assert combined <= 4, f"combined peak {combined} exceeded budget cpu=4"


def test_disjoint_resources_run_in_parallel(tmp_path):
    """A CPU task and a GPU task on disjoint budgets should both run
    near their `max_workers` cap simultaneously — neither blocks the
    other."""
    outdir = tmp_path / "intervals"
    cpu_task = _make_task(
        "cpu", outdir, max_workers=4, requires={"cpu": 1}, hold_s=0.15
    )
    gpu_task = _make_task(
        "gpu", outdir, max_workers=2, requires={"gpu": 1}, hold_s=0.15
    )

    states = daisy.Server().run_blockwise(
        [cpu_task, gpu_task],
        resources={"cpu": 4, "gpu": 2},
    )
    assert states["cpu"].is_done() and states["gpu"].is_done()
    # Peaks should hit the caps — the runner should be willing to spawn
    # all of them since budgets are disjoint.
    cpu_peak = peak_concurrency(outdir, task_ids={"cpu"})
    gpu_peak = peak_concurrency(outdir, task_ids={"gpu"})
    assert cpu_peak >= 2, f"cpu peak {cpu_peak} too low"
    assert gpu_peak >= 1, f"gpu peak {gpu_peak} too low"
    # Disjoint budgets mean the two tasks genuinely overlap in time.
    assert peak_concurrency(outdir) > max(cpu_peak, gpu_peak), (
        "cpu and gpu tasks never ran at the same time"
    )


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
    outdir = tmp_path / "intervals"

    def make(task_id, upstream=None):
        return daisy.Task(
            task_id=task_id,
            total_roi=daisy.Roi([0], [40]),
            read_roi=daisy.Roi([0], [10]),
            write_roi=daisy.Roi([0], [10]),
            process_function=interval_recorder(outdir, task_id, HOLD_S),
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
    combined = peak_concurrency(outdir, task_ids={"filter", "agglom"})
    assert combined <= 4, f"combined peak {combined} exceeded cpu=4"


def test_num_workers_keyword_is_rejected():
    """The v2-native `Task` (in `daisy.v2`) rejects the daisy 1.x
    `num_workers=` kwarg — only `max_workers=` is accepted. The
    v1.x-compat surface (`daisy.v1_compat`, also re-exported at
    top-level `daisy`) deliberately *does* accept `num_workers=`
    as an alias; that's covered by `tests/daisy_compat/`."""
    from daisy import v2

    with pytest.raises(TypeError):
        v2.Task(  # ty: ignore[unknown-argument]
            task_id="legacy",
            total_roi=v2.Roi([0], [20]),
            read_roi=v2.Roi([0], [10]),
            write_roi=v2.Roi([0], [10]),
            process_function=lambda b: None,
            read_write_conflict=False,
            num_workers=2,
            max_retries=0,
        )
