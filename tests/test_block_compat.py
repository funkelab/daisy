"""daisy.Block accepts funlib ROIs and presents funlib ROIs on every
surface — v1 parity for code that constructs blocks directly, e.g.
"process this one ROI without a blockwise job" helpers."""

import daisy
import funlib.geometry as fg


def test_block_constructs_from_funlib_rois():
    b = daisy.Block(fg.Roi((0,), (40,)), fg.Roi((0,), (10,)), fg.Roi((5,), (5,)))
    assert isinstance(b, daisy.Block)
    assert isinstance(b.read_roi, fg.Roi)
    assert isinstance(b.write_roi, fg.Roi)
    assert b.read_roi == fg.Roi((0,), (10,))
    assert b.write_roi == fg.Roi((5,), (5,))


def test_block_constructs_on_the_v2_surface_too():
    b = daisy.v2.Block(
        daisy.v2.Roi((0,), (40,)),
        daisy.v2.Roi((0,), (10,)),
        daisy.v2.Roi((0,), (10,)),
        task_id="t",
    )
    assert isinstance(b.read_roi, fg.Roi)
    assert b.block_id[0] == "t"


def test_process_roi_helper_pattern():
    """The downstream pattern that motivated this: build a block by hand,
    run the user's process function on it directly."""
    seen = {}

    def process_block(block):
        seen["is_funlib"] = isinstance(block.read_roi, fg.Roi)
        seen["grown"] = block.read_roi == block.write_roi.grow((2,), (2,))

    roi = fg.Roi((10,), (10,))
    context = fg.Coordinate((2,))
    block = daisy.Block(roi.grow(context, context), roi.grow(context, context), roi)
    process_block(block)
    assert seen == {"is_funlib": True, "grown": True}


def test_one_block_class_on_every_surface():
    assert daisy.v2.Block is daisy.Block is daisy.block.Block
    assert daisy.v2.Roi is daisy.Roi is fg.Roi
    assert daisy.v2.Coordinate is daisy.Coordinate is fg.Coordinate


def test_block_roundtrips_through_rust():
    """A Block carries a private Rust block; the Rust-backed APIs take the
    Python Block and hand Python Blocks back."""
    b = daisy.Block(fg.Roi((0,), (40,)), fg.Roi((0,), (10,)), fg.Roi((0,), (10,)), task_id="g")
    assert isinstance(b._native, daisy._daisy.Block)
    graph = daisy.BlockwiseDependencyGraph(
        "g", fg.Roi((0,), (10,)), fg.Roi((0,), (10,)), True, "valid", total_read_roi=fg.Roi((0,), (40,))
    )
    for neighbour in graph.upstream(b) + graph.downstream(b):
        assert isinstance(neighbour, daisy.Block) and isinstance(neighbour.read_roi, fg.Roi)
    assert graph.num_blocks == 4 and all(isinstance(r, daisy.Block) for r in graph.root_gen())


def test_acquired_and_manual_blocks_are_the_same_class():
    """Blocks daisy hands to process functions and blocks users construct
    are the SAME compat class: type() and isinstance agree everywhere."""
    seen = {}

    def probe(block):
        seen["type"] = type(block)
        seen["is_daisy_block"] = isinstance(block, daisy.Block)
        seen["roi_is_funlib"] = isinstance(block.read_roi, fg.Roi)

    task = daisy.Task(
        "same-class",
        total_roi=fg.Roi((0,), (10,)),
        read_roi=fg.Roi((0,), (10,)),
        write_roi=fg.Roi((0,), (10,)),
        process_function=probe,
        num_workers=1,
        done_marker_path=False,
    )
    # the `seen` dict needs in-process execution; the compat wrapping under
    # test happens at construction, before any worker is involved
    assert daisy.run_blockwise([task], multiprocessing=False, progress=False)
    assert seen["type"] is daisy.Block
    assert seen["is_daisy_block"] and seen["roi_is_funlib"]


def test_status_mutation_propagates_from_compat_block():
    """Setting FAILED on the compat block must reach daisy's bookkeeping
    (the compat view copies status back to the wire block)."""

    def fail_by_status(block):
        block.status = daisy.BlockStatus.FAILED

    # distributed workers: blocks flow through Client.acquire_block, where
    # v1's status semantics live.
    task = daisy.Task(
        "status-prop",
        total_roi=fg.Roi((0,), (20,)),
        read_roi=fg.Roi((0,), (10,)),
        write_roi=fg.Roi((0,), (10,)),
        process_function=fail_by_status,
        num_workers=1,
        max_retries=0,
        max_worker_restarts=0,
        done_marker_path=False,
    )
    states = daisy.Server().run_blockwise([task], progress=False)
    st = states["status-prop"]
    assert st.completed_count == 0
    assert st.failed_count + st.orphaned_count == 2
