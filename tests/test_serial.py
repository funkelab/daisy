"""Serial-runner-specific behavior.

Serial mode is the in-process debugging path (`run_blockwise(...,
multiprocessing=False)`). The retry-on-failure behavior that the
multiprocessing path uses is deliberately disabled here: a process
function exception propagates immediately so the user gets the
original traceback at the call site instead of three buried retry
attempts in a post-run summary.
"""

import daisy
import pytest


def _task(task_id, process_function):
    return daisy.Task(
        task_id=task_id,
        total_roi=daisy.Roi([0], [40]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=process_function,
        read_write_conflict=False,
        max_retries=2,
    )


def test_serial_fails_fast_on_exception():
    """A process-function exception in serial mode propagates the
    *original* PyErr type (not a wrapped `RuntimeError`) on the first
    occurrence — no retries."""
    attempts = []

    def boom(block):
        attempts.append(block.block_id)
        raise ValueError("from inside process_function")

    with pytest.raises(ValueError, match="from inside process_function"):
        daisy.run_blockwise(
            [_task("boom", boom)],
            multiprocessing=False,
            progress=False,
        )

    # Fail-fast: only the very first block ever ran.
    assert len(attempts) == 1


def test_serial_clean_run_unchanged():
    """Sanity check: a clean process function still completes every
    block in serial mode."""
    seen = []

    def ok(block):
        seen.append(block.block_id)

    result = daisy.run_blockwise(
        [_task("clean", ok)],
        multiprocessing=False,
        progress=False,
    )
    assert result is True
    assert len(seen) == 4


def test_serial_traceback_preserved():
    """The original Python exception object surfaces — not just its
    string representation. The user can `except SpecificError:` it."""

    class CustomError(Exception):
        pass

    def boom(block):
        raise CustomError("custom error message")

    with pytest.raises(CustomError, match="custom error message"):
        daisy.run_blockwise(
            [_task("boom", boom)],
            multiprocessing=False,
            progress=False,
        )


@pytest.mark.parametrize(
    "skip_ids",
    [set(), {4}, {0, 4}, {2, 4}, {1, 2, 4}, {0, 1, 2, 4}],
    ids=lambda s: "-".join(map(str, sorted(s))) or "none",
)
def test_serial_runs_downstream_released_by_skipped_blocks(tmp_path, skip_ids):
    """Issue #83: upstream blocks skipped by `check_function` release
    downstream blocks inside the scheduler; serial mode must still hand
    those out instead of returning with the downstream task unfinished.
    Block ids of the 2x2 grid are 0, 1, 2, 4."""

    def mk(name, check):
        def proc(block):
            (tmp_path / f"{name}-{block.block_id[1]}").touch()

        wr = daisy.Roi((0, 0), (8, 8))
        return daisy.Task(
            name,
            daisy.Roi((0, 0), (16, 16)),
            wr,
            wr,
            process_function=proc,
            check_function=check,
            read_write_conflict=False,
            fit="shrink",
            max_workers=1,
            done_marker_path=False,
        )

    t1 = mk("t1", lambda b: b.block_id[1] in skip_ids)
    t2 = mk("t2", None)
    states = daisy.run_blockwise(
        t1 + t2,
        multiprocessing=False,
        return_states=True,
        summary=False,
        progress=False,
    )

    assert states["t1"].skipped_count == len(skip_ids)
    assert states["t2"].completed_count == 4
    assert states["t2"].is_done()
    assert len(list(tmp_path.glob("t2-*"))) == 4
