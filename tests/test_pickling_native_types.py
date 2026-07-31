"""Native PyO3 classes must be locatable for pickling-by-reference.

Without `module = "daisy._daisy"` on the pyclass declarations they report
__module__ == "builtins", and anything that drags them into a pickle graph
(e.g. cloudpickle-serializing a v1-compat-wrapped process function for
subprocess workers) fails with "Can't pickle <class 'builtins.Roi'>".
"""

import pickle

import pytest

import daisy


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


def test_compat_wrapped_fn_survives_cloudpickle():
    cloudpickle = pytest.importorskip("cloudpickle")
    from daisy.v1_compat import _wrap_block_fn

    def fn(block):
        return block.read_roi

    wrapped = _wrap_block_fn(fn)
    assert cloudpickle.loads(cloudpickle.dumps(wrapped)) is not None
