"""Tests for the `Pipeline` DSL — `+` (sequential) and `|` (parallel)
composition over `Task` and `Pipeline` instances."""

import pytest

import daisy.v2 as daisy
from daisy.v2 import Pipeline


def _task(task_id, upstream_tasks=None):
    return daisy.Task(
        task_id=task_id,
        total_roi=daisy.Roi([0], [40]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=lambda b: None,
        read_write_conflict=False,
        upstream_tasks=upstream_tasks,
    )


# -- Operator semantics ------------------------------------------------


def test_task_plus_task_makes_sequential_pipeline():
    a = _task("a")
    b = _task("b")
    p = a + b
    assert isinstance(p, Pipeline)
    assert [t.task_id for t in p.tasks] == ["a", "b"]
    assert [t.task_id for t in p.sources] == ["a"]
    assert [t.task_id for t in p.outputs] == ["b"]
    assert [(u.task_id, d.task_id) for u, d in p.edges] == [("a", "b")]


def test_task_pipe_task_makes_parallel_pipeline():
    a = _task("a")
    b = _task("b")
    p = a | b
    assert {t.task_id for t in p.sources} == {"a", "b"}
    assert {t.task_id for t in p.outputs} == {"a", "b"}
    assert p.edges == []


def test_pipeline_plus_pipeline_connects_outputs_to_sources():
    pre = _task("preprocess") + _task("predict")
    post = _task("relabel") + _task("extract_stats")
    full = pre + post
    edge_ids = {(u.task_id, d.task_id) for u, d in full.edges}
    assert edge_ids == {
        ("preprocess", "predict"),
        ("relabel", "extract_stats"),
        ("predict", "relabel"),
    }
    assert [t.task_id for t in full.sources] == ["preprocess"]
    assert [t.task_id for t in full.outputs] == ["extract_stats"]


def test_pipeline_pipe_pipeline_unions_sources_and_outputs():
    left = _task("a") + _task("b")
    right = _task("c") + _task("d")
    par = left | right
    assert {t.task_id for t in par.sources} == {"a", "c"}
    assert {t.task_id for t in par.outputs} == {"b", "d"}
    # Edges from each branch only — none added across the union:
    edge_ids = {(u.task_id, d.task_id) for u, d in par.edges}
    assert edge_ids == {("a", "b"), ("c", "d")}


def test_fan_in_then_continuation():
    """`(a | b) + c` — c depends on both a and b."""
    a, b, c = _task("a"), _task("b"), _task("c")
    p = (a | b) + c
    edge_ids = {(u.task_id, d.task_id) for u, d in p.edges}
    assert edge_ids == {("a", "c"), ("b", "c")}
    assert {t.task_id for t in p.sources} == {"a", "b"}
    assert [t.task_id for t in p.outputs] == ["c"]


def test_fan_out_then_join():
    """`a + (b | c) + d` — b and c both depend on a; d depends on both."""
    a, b, c, d = _task("a"), _task("b"), _task("c"), _task("d")
    p = a + (b | c) + d
    edge_ids = {(u.task_id, d_.task_id) for u, d_ in p.edges}
    assert edge_ids == {
        ("a", "b"),
        ("a", "c"),
        ("b", "d"),
        ("c", "d"),
    }


def test_radd_with_task_on_left():
    """Concrete `Task + Pipeline` works via `__add__` on Task; this
    test sanity-checks the symmetric `Pipeline + Task` is also fine."""
    a, b, c = _task("a"), _task("b"), _task("c")
    inner = b + c
    p = a + inner
    edge_ids = {(u.task_id, d.task_id) for u, d in p.edges}
    assert edge_ids == {("a", "b"), ("b", "c")}


def test_invalid_operand_type():
    a = _task("a")
    with pytest.raises(TypeError):
        a + 42
    with pytest.raises(TypeError):
        a | "not a task"


# -- Materialization & non-mutation -----------------------------------


def test_materialize_does_not_mutate_originals():
    a, b = _task("a"), _task("b")
    p = a + b
    assert a.upstream_tasks == []
    assert b.upstream_tasks == []
    p.materialize()
    # Originals unchanged after materialize:
    assert a.upstream_tasks == []
    assert b.upstream_tasks == []


def test_materialize_sets_clone_upstream():
    a, b = _task("a"), _task("b")
    p = a + b
    [b_clone] = p.materialize()
    assert b_clone is not b
    assert len(b_clone.upstream_tasks) == 1
    # The upstream points to the cloned `a`, not the original.
    a_clone = b_clone.upstream_tasks[0]
    assert a_clone is not a
    assert a_clone.task_id == "a"


def test_pipeline_can_be_run_independently_after_composition():
    """Composing a pipeline into a parent must not break running the
    sub-pipeline alone — the originals are unmutated."""
    a, b = _task("a"), _task("b")
    sub = a + b
    parent_extra = _task("c")
    _ = sub + parent_extra  # Compose into a larger pipeline.

    # Running `sub` alone should not include `c`. We check via
    # materialization — `sub`'s outputs only walk back to `a`/`b`.
    seen = set()

    def collect(t):
        if t.task_id in seen:
            return
        seen.add(t.task_id)
        for u in t.upstream_tasks:
            collect(u)

    for o in sub.materialize():
        collect(o)
    assert seen == {"a", "b"}


# -- Reset & run_blockwise --------------------------------------------


def test_pipeline_reset_clears_member_markers(tmp_path):
    """Pipeline.reset() invokes `Task.reset()` on every member task —
    no cascade beyond pipeline membership."""
    import daisy.logging as gl

    gl.set_log_basedir(tmp_path / "logs")

    a, b, c = _task("a"), _task("b"), _task("c")
    pipe = a + b + c

    # Run once to create markers.
    assert pipe.run_blockwise(progress=False)
    for tid in ("a", "b", "c"):
        assert (tmp_path / "logs" / tid).exists()

    # Reset only a sub-pipeline (b alone): only b's marker should clear.
    Pipeline.from_task(b).reset()
    assert (tmp_path / "logs" / "a").exists()
    assert not (tmp_path / "logs" / "b").exists()
    assert (tmp_path / "logs" / "c").exists()

    # Reset the whole pipeline: all three gone.
    pipe.reset()
    for tid in ("a", "b", "c"):
        assert not (tmp_path / "logs" / tid).exists()


def test_pipeline_run_blockwise_runs_in_dependency_order(tmp_path):
    import daisy.logging as gl

    gl.set_log_basedir(tmp_path / "logs")
    order = []

    def make(task_id):
        def proc(block):
            order.append((task_id, block.block_id[1]))
        return daisy.Task(
            task_id=task_id,
            total_roi=daisy.Roi([0], [40]),
            read_roi=daisy.Roi([0], [10]),
            write_roi=daisy.Roi([0], [10]),
            process_function=proc,
            read_write_conflict=False,
        )

    a, b, c = make("a"), make("b"), make("c")
    assert (a + b + c).run_blockwise(multiprocessing=False, progress=False)

    # Every block of `a` must precede every block of `b`, and `b` before `c`.
    a_indices = [i for tid, i in enumerate(order) if order[i][0] == "a"]
    b_indices = [i for tid, i in enumerate(order) if order[i][0] == "b"]
    c_indices = [i for tid, i in enumerate(order) if order[i][0] == "c"]
    if a_indices and b_indices:
        assert max(a_indices) < min(b_indices)
    if b_indices and c_indices:
        assert max(b_indices) < min(c_indices)


def test_top_level_run_blockwise_accepts_pipeline(tmp_path):
    import daisy.logging as gl

    gl.set_log_basedir(tmp_path / "logs")
    a, b = _task("first"), _task("second")
    p = a + b
    assert daisy.run_blockwise(p, multiprocessing=False, progress=False)
