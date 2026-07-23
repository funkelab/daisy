"""run_blockwise(return_states=True): TaskState access through the front
door, without touching the lower-level Server class."""

import pytest

import daisy


def _task(task_id, fn, total=40, **kw):
    kw.setdefault("max_workers", 2)
    return daisy.Task(
        task_id=task_id,
        total_roi=daisy.Roi([0], [total]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=fn,
        read_write_conflict=False,
        **kw,
    )


def test_clean_run_returns_states():
    states = daisy.run_blockwise(
        _task("clean", lambda b: None), progress=False, return_states=True
    )
    st = states["clean"]
    assert st.completed_count == st.total_block_count == 4
    assert st.failed_count == 0 and not st.abandoned


def test_default_still_returns_bool():
    ok = daisy.run_blockwise(_task("boolean", lambda b: None), progress=False)
    assert ok is True


def test_partial_failures_return_states_without_raising():
    def half_broken(block):
        if block.block_id[1] % 2:
            raise ValueError("corrupt chunk")

    states = daisy.run_blockwise(
        _task("flaky", half_broken, total=60, max_retries=0),
        progress=False,
        return_states=True,
    )
    st = states["flaky"]
    assert st.failed_count == 3
    assert st.completed_count == 3
    assert not st.abandoned
    # and the bool form agrees
    assert daisy.run_blockwise(
        _task("flaky2", half_broken, total=60, max_retries=0), progress=False
    ) is False


def test_abandonment_still_raises_with_return_states():
    def crashy():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="abandoned"):
        daisy.run_blockwise(
            _task("doomed", crashy, max_workers=1, max_worker_restarts=1),
            progress=False,
            return_states=True,
        )


def test_serial_mode_returns_states():
    states = daisy.run_blockwise(
        _task("serial", lambda b: None),
        multiprocessing=False,
        progress=False,
        return_states=True,
    )
    assert states["serial"].completed_count == 4


def test_v1_compat_surface():
    import daisy.v1_compat as compat

    task = compat.Task(
        "compat",
        total_roi=daisy.Roi([0], [40]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=lambda b: None,
        num_workers=2,
    )
    states = compat.run_blockwise([task], progress=False, return_states=True)
    assert states["compat"].completed_count == 4
