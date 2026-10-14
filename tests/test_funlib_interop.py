"""funlib.geometry is daisy's geometry on every surface.

- `daisy.Roi` / `daisy.Coordinate` (and the `daisy.v2` ones) are
  funlib.geometry's classes; every ROI daisy hands back is one.
- Every entry point accepts a funlib Roi (or anything with integer
  offset/shape) and any int sequence as a Coordinate; the private Rust
  carriers in `daisy._daisy` convert on the way in.
- `daisy.Block` is one Python class for every surface and every path a
  block takes to user code: process functions, check functions,
  `Client.acquire_block`, `Scheduler`, the dependency graphs, pickles.
"""

import pickle

import daisy
import daisy._daisy as _rs
import daisy.v2 as d2
import funlib.geometry as fg
import pytest


def test_public_geometry_is_funlib():
    assert d2.Roi is fg.Roi and d2.Coordinate is fg.Coordinate
    assert daisy.Roi is fg.Roi and daisy.Coordinate is fg.Coordinate
    assert d2.Block is daisy.Block is daisy.block.Block


def test_private_carriers_accept_funlib_and_sequences():
    """The Rust Roi/Coordinate never reach user code, but they take funlib
    values and plain sequences wherever the Python side hands them in."""
    r = _rs.Roi((0, 0), (10, 10))
    assert r == fg.Roi((0, 0), (10, 10)) and fg.Roi((0, 0), (10, 10)) == r
    assert r.intersect(fg.Roi((5, 5), (10, 10))) == fg.Roi((5, 5), (5, 5))
    assert r.contains(fg.Coordinate((1, 1))) and r.contains((1, 1)) and not r.contains([10, 0])
    assert r.contains(fg.Roi((1, 1), (2, 2)))
    assert _rs.Roi(fg.Coordinate((1, 2)), fg.Coordinate((3, 4))) == fg.Roi((1, 2), (3, 4))
    assert _rs.Coordinate((3, 4)) == fg.Coordinate((3, 4)) == (3, 4)
    assert _rs.Coordinate((3, 4)) != (3, 5) and r != "hello"
    with pytest.raises(TypeError, match="got str"):
        r.intersect("hello")
    with pytest.raises(TypeError, match="got str"):
        r.contains("hello")


@pytest.mark.parametrize("surface", [daisy, d2])
def test_positional_funlib_rois_and_funlib_blocks(surface):
    """The 1.x positional call form with funlib ROIs; the block the process
    function gets is a daisy.Block with funlib ROIs, on either surface."""
    seen = {}

    def process(block):
        seen["cls"] = type(block)
        seen["funlib"] = isinstance(block.read_roi, fg.Roi) and isinstance(block.write_roi, fg.Roi)
        seen["end"] = block.write_roi.offset + block.write_roi.shape  # funlib arithmetic

    task = surface.Task(
        "positional",
        fg.Roi((0,), (40,)),
        fg.Roi((0,), (10,)),
        fg.Roi((0,), (10,)),
        process,
        max_workers=1,
        done_marker_path=False,
    )
    assert isinstance(task.total_roi, fg.Roi) and task.total_roi == fg.Roi((0,), (40,))
    assert isinstance(task.read_roi, fg.Roi) and isinstance(task.write_roi, fg.Roi)
    assert task.total_roi.intersect(fg.Roi((30,), (40,))) == fg.Roi((30,), (10,))
    assert surface.run_blockwise([task], multiprocessing=False, progress=False)
    assert seen["cls"] is daisy.Block and seen["funlib"]
    assert isinstance(seen["end"], fg.Coordinate)


@pytest.mark.parametrize("style", ["kwargs", "positional"])
def test_check_fn_receives_funlib_rois_and_skips_done_blocks(tmp_path, style):
    """v1.x check functions do funlib arithmetic on the block's ROIs — the
    same idiom as process functions (volara's resume path). The Rust
    precheck swallows every exception into "not done", so a check that got
    the wrong block type would silently re-run every done block."""

    def marker(block):
        end = block.write_roi.offset + block.write_roi.shape
        return tmp_path / ("done_" + "_".join(str(c) for c in end))

    def process(block):
        marker(block).touch()

    def check(block):
        return marker(block).exists()

    def make_task():
        if style == "kwargs":
            return daisy.Task(
                "compat-check",
                total_roi=fg.Roi((0,), (40,)),
                read_roi=fg.Roi((0,), (10,)),
                write_roi=fg.Roi((0,), (10,)),
                process_function=process,
                check_function=check,
                num_workers=1,
                tracking_path=False,
            )
        return daisy.Task(
            "compat-check",
            fg.Roi((0,), (40,)),
            fg.Roi((0,), (10,)),
            fg.Roi((0,), (10,)),
            process,
            check,
            num_workers=1,
            tracking_path=False,
        )

    states = daisy.run_blockwise([make_task()], multiprocessing=False, progress=False, return_states=True)
    assert states["compat-check"].completed_count == 4
    assert states["compat-check"].skipped_count == 0
    states = daisy.run_blockwise([make_task()], multiprocessing=False, progress=False, return_states=True)
    assert states["compat-check"].skipped_count == 4


def test_scheduler_and_graphs_hand_out_blocks():
    task = d2.Task(
        "sched",
        fg.Roi((0,), (40,)),
        fg.Roi((0,), (10,)),
        fg.Roi((0,), (10,)),
        lambda block: None,
        max_workers=1,
        done_marker_path=False,
    )
    sched = d2.Scheduler(task)
    first = sched.acquire_block("sched")
    assert isinstance(first, daisy.Block) and isinstance(first.read_roi, fg.Roi)
    first.status = daisy.BlockStatus.SUCCESS
    sched.release_block(first)
    assert sched.task_states["sched"].completed_count == 1
    graph = sched.dependency_graph
    second = sched.acquire_block("sched")
    assert all(isinstance(b, daisy.Block) for b in graph.upstream(second) + graph.downstream(second))
    bwg = d2.BlockwiseDependencyGraph(
        "sched", fg.Roi((0,), (10,)), fg.Roi((0,), (10,)), False, "valid", total_read_roi=fg.Roi((0,), (40,))
    )
    assert bwg.num_blocks == 4
    assert all(isinstance(b, daisy.Block) for b in bwg.get_subgraph_blocks(fg.Roi((0,), (20,))))
    assert all(isinstance(b, daisy.Block) and isinstance(ds, list) for b, ds in bwg.enumerate_all_dependencies())


def test_block_pickles():
    b = daisy.Block(fg.Roi((0,), (40,)), fg.Roi((0,), (10,)), fg.Roi((2,), (6,)), task_id="p")
    b.status = daisy.BlockStatus.FAILED
    c = pickle.loads(pickle.dumps(b))
    assert c == b and hash(c) == hash(b)
    assert c.read_roi == b.read_roi and c.write_roi == b.write_roi
    assert c.block_id == b.block_id and c.status == daisy.BlockStatus.FAILED
