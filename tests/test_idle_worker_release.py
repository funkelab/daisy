"""Tail-of-run worker economics (GitHub issue: idle workers never released
while the last blocks are in flight).

1. A worker whose acquire_block() finds every remaining block in flight
   (none ready, none pending) is told to shut down immediately, so the
   fleet shrinks to the in-flight count during the tail of a task.
2. That teardown is safe because `Task(timeout=...)` is enforced by a
   watchdog inside `daisy.Client` itself — every loop built on the client
   (including hand-written 0-arg worker loops) self-preempts, the server
   reclaims the block, and the rebalance loop spawns a replacement.
3. Spawn functions that return before their worker's lifetime ends
   (fire-and-forget `sbatch` without `--wait`) are a hard error: daisy
   reads worker liveness from the spawn call, so such a run's accounting
   is fiction.
"""

import os
import subprocess
import sys
import time

import pytest

import daisy


def _task(fn, **kw):
    kw.setdefault("total_roi", daisy.Roi([0], [20]))
    kw.setdefault("read_roi", daisy.Roi([0], [10]))
    kw.setdefault("write_roi", daisy.Roi([0], [10]))
    kw.setdefault("read_write_conflict", False)
    return daisy.Task(process_function=fn, **kw)


def test_idle_worker_released_while_last_block_in_flight(tmp_path):
    """Two blocks, two workers, one slow block: the worker that runs out
    of work must exit while the slow block is still processing — not park
    until the whole task completes."""
    events = tmp_path

    def worker(*, context=None):
        client = daisy.Client()
        while True:
            with client.acquire_block() as block:
                if block is None:
                    break
                if block.write_roi.begin[0] == 0:  # the slow block
                    time.sleep(5)
                    (events / "slow_done").write_text(str(time.time()))
        (events / f"exit-{client.worker_id}").write_text(str(time.time()))

    task = _task(worker, task_id="tail_release", max_workers=2)
    assert daisy.run_blockwise([task], progress=False)

    slow_done = float((events / "slow_done").read_text())
    exits = sorted(
        float(f.read_text()) for f in events.iterdir() if f.name.startswith("exit-")
    )
    # One worker held the slow block to the end; the other must have been
    # released well before the slow block finished.
    assert len(exits) == 2
    assert exits[0] < slow_done - 2.0, (
        f"idle worker exited only {slow_done - exits[0]:.1f}s before the "
        "slow block finished — it was parked instead of released"
    )


def test_custom_worker_loop_self_preempts_on_timeout(tmp_path):
    """The block watchdog lives in daisy.Client, so a hand-written 0-arg
    worker loop gets true timeout preemption with no extra code — the
    hung worker process is killed, not left running."""
    pids = tmp_path / "pids"
    pids.mkdir()

    def worker(*, context=None):
        client = daisy.Client()
        while True:
            with client.acquire_block() as block:
                if block is None:
                    return
                (pids / str(os.getpid())).touch()
                time.sleep(30)

    task = _task(
        worker,
        task_id="hung_custom_loop",
        total_roi=daisy.Roi([0], [10]),
        timeout=1,
        max_retries=0,
        max_workers=1,
    )
    t0 = time.monotonic()
    ok = daisy.run_blockwise([task], progress=False)
    wall = time.monotonic() - t0
    assert ok is False
    assert wall < 15, f"run should self-terminate quickly, took {wall:.1f}s"

    time.sleep(0.5)
    survivors = []
    for f in pids.iterdir():
        pid = int(f.name)
        try:
            os.kill(pid, 0)
            survivors.append(pid)
        except ProcessLookupError:
            pass
    for pid in survivors:  # cleanup before asserting
        os.kill(pid, 9)
    assert not survivors, f"hung worker processes outlived the run: {survivors}"


_GRANDCHILD = """
import time
import daisy

time.sleep(1.0)
client = daisy.Client()  # DAISY_CONTEXT inherited from the spawn
while True:
    with client.acquire_block() as block:
        if block is None:
            break
"""


def test_fire_and_forget_spawn_function_is_a_hard_error(tmp_path):
    """A spawn function that submits its worker and returns immediately
    (sbatch without --wait) breaks every liveness decision daisy makes.
    The moment the fired-off worker connects after its spawn call
    returned, the run fails with an error naming the problem."""
    script = tmp_path / "grandchild.py"
    script.write_text(_GRANDCHILD)

    def worker(*, context=None):
        # BROKEN ON PURPOSE: launches the real worker without blocking
        # on its lifetime.
        subprocess.Popen([sys.executable, str(script)])

    task = _task(
        worker,
        task_id="fire_and_forget",
        total_roi=daisy.Roi([0], [10]),
        max_workers=1,
        # Keep the respawn churn from exhausting the start budget (which
        # would abandon the task) before the grandchild connects.
        max_worker_restarts=1000,
    )
    with pytest.raises(RuntimeError, match="spawn function had already returned"):
        daisy.run_blockwise([task], progress=False)
