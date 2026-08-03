"""daisy.Block (compat surface) accepts funlib ROIs and presents funlib
ROIs — v1 parity for code that constructs blocks directly, e.g.
"process this one ROI without a blockwise job" helpers."""

import daisy
import funlib.geometry as fg
import pytest


def test_block_constructs_from_funlib_rois():
    b = daisy.Block(fg.Roi((0,), (40,)), fg.Roi((0,), (10,)), fg.Roi((5,), (5,)))
    assert isinstance(b, daisy.Block)
    assert isinstance(b.read_roi, fg.Roi)
    assert isinstance(b.write_roi, fg.Roi)
    assert b.read_roi == fg.Roi((0,), (10,))
    assert b.write_roi == fg.Roi((5,), (5,))


def test_block_constructs_from_native_rois_too():
    b = daisy.Block(daisy.v2.Roi((0,), (40,)), daisy.v2.Roi((0,), (10,)),
                    daisy.v2.Roi((0,), (10,)), task_id="t")
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


def test_native_surface_block_stays_strict():
    with pytest.raises(TypeError):
        daisy.v2.Block(fg.Roi((0,), (10,)), fg.Roi((0,), (10,)), fg.Roi((0,), (10,)))


def test_compat_block_roundtrips_through_rust():
    """A compat Block is a real _rs.Block subclass: rust APIs accept it."""
    b = daisy.Block(fg.Roi((0,), (40,)), fg.Roi((0,), (10,)), fg.Roi((0,), (10,)))
    assert isinstance(b, daisy._daisy.Block)


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
        worker_processes=False,
        done_marker_path=False,
    )
    assert daisy.run_blockwise([task], progress=False)
    assert seen["type"] is daisy.Block
    assert seen["is_daisy_block"] and seen["roi_is_funlib"]


def test_status_mutation_propagates_from_compat_block():
    """Setting FAILED on the compat block must reach daisy's bookkeeping
    (the compat view copies status back to the wire block)."""

    def fail_by_status(block):
        block.status = daisy.BlockStatus.FAILED

    # subprocess workers (the default): blocks flow through Client.
    # acquire_block, where v1's status semantics live. (Thread mode —
    # worker_processes=False — decides success by exception only and has
    # never honored status mutations in v2.)
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
