"""Serial-runner-specific behavior.

Serial mode is the in-process debugging path (`run_blockwise(...,
multiprocessing=False)`). The retry-on-failure behavior that the
multiprocessing path uses is deliberately disabled here: a process
function exception propagates immediately so the user gets the
original traceback at the call site instead of three buried retry
attempts in a post-run summary.
"""

import pytest

import daisy


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
            [_task("boom", boom)], multiprocessing=False, progress=False,
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
        [_task("clean", ok)], multiprocessing=False, progress=False,
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
            [_task("boom", boom)], multiprocessing=False, progress=False,
        )
