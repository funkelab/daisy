"""Spawn functions can receive their worker context as an argument.

The DAISY_CONTEXT env var is process-global: with concurrent spawns, a
spawn function that blocks before its child captures the environment (an
sbatch submission, a slow filesystem) can observe a LATER worker's value —
measured in the adversarial suite as 8 concurrent spawns all seeing
worker_id=7. Declaring a keyword-only `context` parameter opts into the
race-free path: `def start_worker(*, context): ...` receives this worker's
`daisy.Context` by value. Keyword-only params don't count toward positional
arity, so the 0-positional-args == spawn-function classification is
untouched.
"""

import os
import subprocess
import sys
import time

import pytest

import daisy


def _mk_task(spawn_fn, n_blocks=16, max_workers=8, **kw):
    return daisy.Task(
        task_id=kw.pop("task_id", "ctx_test"),
        total_roi=daisy.Roi([0], [n_blocks * 10]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=spawn_fn,
        read_write_conflict=False,
        max_workers=max_workers,
        max_retries=0,
        **kw,
    )


def test_slow_concurrent_spawns_get_distinct_contexts():
    """THE race test: 8 workers, each spawn sleeps 50ms before reading its
    identity. On the env-var path this collapses to one worker_id; on the
    argument path every spawn sees its own."""
    seen = []

    def slow_spawn(*, context):
        time.sleep(0.05)  # sbatch-submission stand-in
        seen.append(context["worker_id"])
        # no worker ever connects; the start budget ends the run

    task = _mk_task(slow_spawn, max_workers=8, max_worker_restarts=0)
    daisy.Server().run_blockwise([task], progress=False)

    assert len(seen) == 8, seen
    assert len(set(seen)) == 8, f"duplicate worker identities handed out: {seen}"


def test_env_var_still_set_for_context_spawns():
    """The env var keeps being set even on the argument path (children may
    inherit it), and matches the argument for non-overlapping spawns."""
    seen = {}

    def spawn(*, context):
        seen["arg"] = context["worker_id"]
        seen["env"] = daisy.Context.from_env()["worker_id"]

    task = _mk_task(spawn, max_workers=1, max_worker_restarts=0)
    daisy.Server().run_blockwise([task], progress=False)
    assert seen["arg"] == seen["env"]


def test_zero_arg_spawn_functions_unchanged():
    """Legacy 0-arg spawn functions keep working on the env-var path."""
    seen = []

    def legacy_spawn():
        seen.append(daisy.Context.from_env()["worker_id"])

    task = _mk_task(legacy_spawn, max_workers=1, max_worker_restarts=0)
    daisy.Server().run_blockwise([task], progress=False)
    assert len(seen) == 1


def test_shim_workers_get_distinct_identities():
    """Subprocess block-function workers (the default mode) inherit a
    deterministic DAISY_CONTEXT from the context argument."""
    import tempfile

    outdir = tempfile.mkdtemp()

    def record_identity(block):
        ctx = daisy.Context.from_env()
        open(os.path.join(outdir, f"w{ctx['worker_id']}_p{os.getpid()}"), "w").close()
        time.sleep(0.05)  # hold the block so all workers participate

    task = _mk_task(record_identity, n_blocks=32, max_workers=8,
                    task_id="shim_ctx")
    ok = daisy.run_blockwise([task], progress=False)
    assert ok
    worker_ids = {f.split("_")[0] for f in os.listdir(outdir)}
    assert len(worker_ids) == 8, sorted(worker_ids)


def test_context_round_trip():
    ctx = daisy.Context(hostname="node1", port=1234, task_id="t", worker_id=7)
    parsed = daisy.Context.from_env_string(ctx.to_env())
    assert parsed["hostname"] == "node1"
    assert parsed["port"] == "1234"
    assert parsed["worker_id"] == "7"


def test_legacy_zero_arg_spawn_warns_when_concurrent(recwarn):
    """0-arg spawn functions still work (env-var path) but warn about the
    DAISY_CONTEXT race when max_workers > 1."""
    import warnings as _w

    def legacy_spawn():
        pass

    def race_free(*, context):
        pass

    def block_fn(block):
        pass

    def mk(fn, workers):
        return daisy.Task(
            task_id="warncheck",
            total_roi=daisy.Roi([0], [20]),
            read_roi=daisy.Roi([0], [10]),
            write_roi=daisy.Roi([0], [10]),
            process_function=fn,
            read_write_conflict=False,
            max_workers=workers,
        )

    with pytest.warns(UserWarning, match="DAISY_CONTEXT"):
        mk(legacy_spawn, 2)

    # no warning: race-free signature, single worker, or block function
    for fn, workers in ((race_free, 8), (legacy_spawn, 1), (block_fn, 8)):
        with _w.catch_warnings():
            _w.simplefilter("error", UserWarning)
            mk(fn, workers)
