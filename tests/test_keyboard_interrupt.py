"""SIGINT / KeyboardInterrupt propagation through `run_blockwise`.

The run loop polls a Rust-side abort callback every 100ms; the
Python wrapper installs a callback that re-acquires the GIL and
calls `Python::check_signals()`. When SIGINT is delivered to the
process, the next poll trips the abort, the run loop exits cleanly,
and the wrapper re-raises the original `KeyboardInterrupt`.

Without this plumbing the run loop sits inside `tokio::block_on`
with the GIL released, the SIGINT flag is never observed, and the
process hangs until killed externally.
"""

import os
import signal
import threading
import time

import pytest

import daisy


def _slow_block(block):
    # Long enough that 100k blocks at 4 workers takes well over 1s,
    # so the SIGINT lands somewhere in the middle of the run.
    time.sleep(0.005)


def _fire_sigint_after(seconds):
    def fire():
        time.sleep(seconds)
        os.kill(os.getpid(), signal.SIGINT)
    threading.Thread(target=fire, daemon=True).start()


def test_sigint_raises_keyboardinterrupt_block_function_mode():
    """Block-function mode (1-arg `process(block)`)."""
    task = daisy.Task(
        task_id="sigint_test",
        total_roi=daisy.Roi([0], [100_000]),
        read_roi=daisy.Roi([0], [1]),
        write_roi=daisy.Roi([0], [1]),
        process_function=_slow_block,
        read_write_conflict=False,
        max_workers=4,
        max_retries=0,
    )

    _fire_sigint_after(0.5)

    server = daisy.Server()
    t0 = time.perf_counter()
    with pytest.raises(KeyboardInterrupt):
        server.run_blockwise([task], progress=False)
    elapsed = time.perf_counter() - t0

    # Should bail out within a couple of poll intervals after the
    # signal fires — well under 100k * 5ms / 4 workers = 125 s.
    assert elapsed < 5.0, f"run took {elapsed:.2f}s — expected prompt abort"


def _looping_worker():
    """0-arg worker — the path the stress example uses. Worker
    threads grab the GIL inside `Client()` setup and around each
    `acquire_block` / `release_block`, so signal-flag observation
    needs to be GIL-free for ctrl-C to feel responsive here."""
    client = daisy.Client()
    while True:
        with client.acquire_block() as block:
            if block is None:
                return
            time.sleep(0.005)


def test_sigint_raises_keyboardinterrupt_worker_function_mode():
    """Worker-function mode (0-arg, manages its own block loop)."""
    task = daisy.Task(
        task_id="sigint_worker_test",
        total_roi=daisy.Roi([0], [100_000]),
        read_roi=daisy.Roi([0], [1]),
        write_roi=daisy.Roi([0], [1]),
        process_function=_looping_worker,
        read_write_conflict=False,
        max_workers=4,
        max_retries=0,
    )

    _fire_sigint_after(0.5)

    server = daisy.Server()
    t0 = time.perf_counter()
    with pytest.raises(KeyboardInterrupt):
        server.run_blockwise([task], progress=False)
    elapsed = time.perf_counter() - t0

    assert elapsed < 5.0, f"run took {elapsed:.2f}s — expected prompt abort"
