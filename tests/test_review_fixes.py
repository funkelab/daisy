"""Regression tests for the fixes that came out of the 1.x vs 2.0 evaluation
review (small, certain items)."""

import pytest

import daisy


def _roi1d(n=40, block=10):
    return daisy.Roi([0], [n]), daisy.Roi([0], [block]), daisy.Roi([0], [block])


def _noop(block):
    return None


@pytest.mark.parametrize("bad", [0, -1])
def test_max_workers_below_one_is_rejected(bad):
    total, read, write = _roi1d()
    with pytest.raises(ValueError, match="max_workers must be at least 1"):
        daisy.Task("t", total, read, write, _noop, read_write_conflict=False, max_workers=bad)


def test_funlib_geometry_is_a_dependency():
    import funlib.geometry  # noqa: F401
    import cloudpickle  # noqa: F401


@pytest.mark.parametrize(
    "value",
    [
        1,
        pytest.param("npbool", id="numpy.bool_"),
        pytest.param("array1", id="one-element-array"),
    ],
)
def test_truthy_check_function_results_skip_blocks(value, tmp_path):
    """1.x evaluated `if check_function(block)`; 2.0 must accept any truthy
    object, not only a real bool."""
    np = pytest.importorskip("numpy")
    returns = {1: 1, "npbool": np.bool_(True), "array1": np.ones(1)}[value]
    seen = []

    def body(block):
        seen.append(block.block_id)

    total, read, write = _roi1d()
    task = daisy.Task(
        "t",
        total,
        read,
        write,
        body,
        check_function=lambda b: returns,
        read_write_conflict=False,
        max_workers=1,
        timeout=30,
    )
    states = daisy.run_blockwise([task], multiprocessing=False, return_states=True)
    assert seen == []
    assert states["t"].skipped_count == 4
    assert states["t"].completed_count == 4


def test_pipeline_run_blockwise_returns_states_when_asked(tmp_path):
    total, read, write = _roi1d()
    task = daisy.Task("t", total, read, write, _noop, read_write_conflict=False, max_workers=1, timeout=30)
    pipeline = daisy.Pipeline([task])
    result = pipeline.run_blockwise(multiprocessing=False, return_states=True)
    assert isinstance(result, dict)
    assert result["t"].completed_count == 4
    assert pipeline.run_blockwise(multiprocessing=False) is True


def test_serial_mode_refuses_worker_functions(tmp_path):
    """A 0-argument function is a worker loop; serial mode has no server for
    it to connect to and must refuse instead of marking every block done."""
    total, read, write = _roi1d()

    def worker_loop():
        raise AssertionError("must not be called")

    task = daisy.Task("t", total, read, write, worker_loop, read_write_conflict=False, max_workers=1, timeout=30)
    with pytest.raises(Exception, match="no 1-argument process_function"):
        daisy.run_blockwise([task], multiprocessing=False)
