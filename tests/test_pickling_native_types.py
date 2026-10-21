"""Native PyO3 classes must be locatable for pickling-by-reference.

Without `module = "daisy._daisy"` on the pyclass declarations they report
__module__ == "builtins", and anything that drags them into a pickle graph
(e.g. cloudpickle-serializing a v1-compat-wrapped process function for
subprocess workers) fails with "Can't pickle <class 'builtins.Roi'>".
"""

import pickle

import daisy
import pytest


@pytest.mark.parametrize(
    "cls_name",
    [
        "Roi",
        "Coordinate",
        "Block",
        "BlockStatus",
        "TaskState",
        "Task",
    ],
)
def test_native_classes_declare_their_module(cls_name):
    cls = getattr(daisy._daisy, cls_name)
    assert cls.__module__ == "daisy._daisy"


def test_native_classes_pickle_by_reference():
    for cls in (daisy._daisy.Roi, daisy._daisy.Coordinate, daisy._daisy.Block):
        assert pickle.loads(pickle.dumps(cls)) is cls


def test_block_using_fn_survives_cloudpickle():
    """Process functions that build or inspect daisy.Block are cloudpickled
    for subprocess workers; the Python Block and its funlib ROIs pickle."""
    cloudpickle = pytest.importorskip("cloudpickle")
    import funlib.geometry as fg

    def fn(block):
        return daisy.Block(
            block.read_roi, block.read_roi, block.write_roi, task_id=block.task_id
        )

    restored = cloudpickle.loads(cloudpickle.dumps(fn))
    b = daisy.Block(
        fg.Roi((0,), (10,)), fg.Roi((0,), (10,)), fg.Roi((0,), (10,)), task_id="t"
    )
    assert restored(b) == b
