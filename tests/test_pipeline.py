"""Tests for the Rust-backed Pipeline DSL — `+` (sequential) and `|`
(parallel) composition over `Task` and `Pipeline` instances."""

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


def _ids(tasks):
    return [t.task_id for t in tasks]


def _edge_ids(pipeline):
    return {(u.task_id, d.task_id) for u, d in pipeline.edges}


# -- Operator semantics ------------------------------------------------


def test_pipeline_class_lives_in_rust():
    """Pipeline is exposed as `_rs.Pipeline`, not a Python wrapper."""
    assert Pipeline.__module__ == "builtins"


def test_task_plus_task_makes_sequential_pipeline():
    a, b = _task("a"), _task("b")
    p = a + b
    assert isinstance(p, Pipeline)
    assert _ids(p.tasks) == ["a", "b"]
    assert _ids(p.sources) == ["a"]
    assert _ids(p.outputs) == ["b"]
    assert _edge_ids(p) == {("a", "b")}


def test_task_pipe_task_makes_parallel_pipeline():
    a, b = _task("a"), _task("b")
    p = a | b
    assert set(_ids(p.sources)) == {"a", "b"}
    assert set(_ids(p.outputs)) == {"a", "b"}
    assert p.edges == []


def test_pipeline_plus_pipeline_connects_outputs_to_sources():
    pre = _task("preprocess") + _task("predict")
    post = _task("relabel") + _task("extract_stats")
    full = pre + post
    assert _edge_ids(full) == {
        ("preprocess", "predict"),
        ("relabel", "extract_stats"),
        ("predict", "relabel"),
    }
    assert _ids(full.sources) == ["preprocess"]
    assert _ids(full.outputs) == ["extract_stats"]


def test_pipeline_pipe_pipeline_unions_sources_and_outputs():
    left = _task("a") + _task("b")
    right = _task("c") + _task("d")
    par = left | right
    assert set(_ids(par.sources)) == {"a", "c"}
    assert set(_ids(par.outputs)) == {"b", "d"}
    assert _edge_ids(par) == {("a", "b"), ("c", "d")}


def test_fan_in_then_continuation():
    """`(a | b) + c` — c depends on both a and b."""
    a, b, c = _task("a"), _task("b"), _task("c")
    p = (a | b) + c
    assert _edge_ids(p) == {("a", "c"), ("b", "c")}
    assert set(_ids(p.sources)) == {"a", "b"}
    assert _ids(p.outputs) == ["c"]


def test_fan_out_then_join():
    """`a + (b | c) + d` — b and c depend on a; d depends on both."""
    a, b, c, d = _task("a"), _task("b"), _task("c"), _task("d")
    p = a + (b | c) + d
    assert _edge_ids(p) == {
        ("a", "b"),
        ("a", "c"),
        ("b", "d"),
        ("c", "d"),
    }


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
    assert a.upstream_tasks == []
    assert b.upstream_tasks == []


def test_materialize_returns_clones_with_new_upstream():
    a, b = _task("a"), _task("b")
    p = a + b
    [b_clone] = p.materialize()
    assert b_clone is not b
    assert len(b_clone.upstream_tasks) == 1
    a_clone = b_clone.upstream_tasks[0]
    assert a_clone is not a
    assert a_clone.task_id == "a"


def test_pipeline_can_be_run_independently_after_composition():
    """Composing a sub-pipeline into a larger parent must not break
    running the sub-pipeline alone."""
    a, b = _task("a"), _task("b")
    sub = a + b
    parent_extra = _task("c")
    _ = sub + parent_extra

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
    import daisy.logging as gl

    gl.set_log_basedir(tmp_path / "logs")
    a, b, c = _task("a"), _task("b"), _task("c")
    pipe = a + b + c

    assert pipe.run_blockwise(progress=False)
    for tid in ("a", "b", "c"):
        assert (tmp_path / "logs" / tid).exists()

    # Reset only b: only b's marker disappears.
    Pipeline.from_task(b).reset()
    assert (tmp_path / "logs" / "a").exists()
    assert not (tmp_path / "logs" / "b").exists()
    assert (tmp_path / "logs" / "c").exists()

    # Reset the whole pipeline: all three gone.
    pipe.reset()
    for tid in ("a", "b", "c"):
        assert not (tmp_path / "logs" / tid).exists()


def test_pipeline_run_blockwise_respects_block_level_dependencies(tmp_path):
    """The scheduler streams downstream blocks as soon as their upstream
    block prerequisites complete (read_write_conflict=False, equal ROIs
    → block-i depends on block-i of the upstream task). What we assert
    here is that for every executed block of a downstream task, the
    matching block of its upstream ran first — strict whole-task
    ordering is *not* required."""
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

    # For each downstream block, its matching upstream block must
    # appear earlier in the execution order.
    pos = {entry: i for i, entry in enumerate(order)}
    for (tid_up, tid_down) in [("a", "b"), ("b", "c")]:
        for block_idx in {bi for tid, bi in order if tid == tid_down}:
            assert pos[(tid_up, block_idx)] < pos[(tid_down, block_idx)], (
                f"{tid_down}/{block_idx} ran before its upstream "
                f"{tid_up}/{block_idx}"
            )


def test_top_level_run_blockwise_accepts_pipeline(tmp_path):
    import daisy.logging as gl

    gl.set_log_basedir(tmp_path / "logs")
    a, b = _task("first"), _task("second")
    p = a + b
    assert daisy.run_blockwise(p, multiprocessing=False, progress=False)
