"""daisy.Block (compat surface) accepts funlib ROIs and presents funlib
ROIs — v1 parity for code that constructs blocks directly, e.g.
"process this one ROI without a blockwise job" helpers."""

import funlib.geometry as fg
import pytest

import daisy


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
