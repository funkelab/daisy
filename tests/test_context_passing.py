"""Every worker gets its own, correct context.

A worker learns who it is (`hostname`, `port`, `task_id`, `worker_id`) from a
`daisy.Context`, either as a keyword-only `context` argument or by reading
the `DAISY_CONTEXT` environment variable. Both are now race-free: each
worker runs in its own process, and the parent builds that process's
environment explicitly, so there is no shared mutable place for one worker's
identity to overwrite another's.

That used not to be true. Worker functions ran on server threads sharing one
process environment, so a spawn function that blocked before its child read
the environment (an sbatch submission, a slow filesystem) could observe a
LATER worker's value — measured in the adversarial suite as 8 concurrent
spawns all seeing worker_id=7. The keyword-only `context` parameter was
introduced as the opt-in fix and remains the recommended signature, since it
is explicit and does not depend on inheritance; it is no longer the
difference between correct and racy.

Identities are recorded to files: each worker is a separate process, so an
in-process list would be appended to in the child and read as empty here.
"""

import os
from pathlib import Path

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


def _ids_in(dirpath, prefix="w"):
    return [
        f.name.split("-")[1]
        for f in Path(dirpath).iterdir()
        if f.name.startswith(prefix + "-")
    ]


def test_slow_concurrent_context_arg_spawns_get_distinct_identities(tmp_path):
    """THE race test, argument form: 8 workers, each sleeping 50ms before
    reading its identity, must each see their own worker_id."""
    out = str(tmp_path / "ids")
    Path(out).mkdir()

    def slow_spawn(*, context):
        import time as _time
        from pathlib import Path as _Path

        _time.sleep(0.05)  # sbatch-submission stand-in
        _Path(out, f"w-{context['worker_id']}").touch()
        # no worker ever connects; the start budget ends the run

    task = _mk_task(slow_spawn, max_workers=8, max_worker_restarts=0)
    daisy.Server().run_blockwise([task], progress=False)

    seen = _ids_in(out)
    assert len(seen) == 8, seen
    assert len(set(seen)) == 8, f"duplicate worker identities handed out: {seen}"


def test_slow_concurrent_env_var_spawns_get_distinct_identities(tmp_path):
    """THE race test, environment form. This is the case that used to
    collapse to a single worker_id: reading DAISY_CONTEXT from a process
    shared with 7 other spawns. Each worker now has its own process and its
    own environment, so the bare 0-arg signature is race-free too."""
    out = str(tmp_path / "ids")
    Path(out).mkdir()

    def slow_spawn():
        import time as _time
        from pathlib import Path as _Path

        _time.sleep(0.05)
        _Path(out, f"w-{daisy.Context.from_env()['worker_id']}").touch()

    task = _mk_task(slow_spawn, max_workers=8, max_worker_restarts=0)
    daisy.Server().run_blockwise([task], progress=False)

    seen = _ids_in(out)
    assert len(seen) == 8, seen
    assert len(set(seen)) == 8, f"duplicate worker identities handed out: {seen}"


def test_context_argument_and_env_var_agree(tmp_path):
    """A worker's `context` argument and its inherited DAISY_CONTEXT
    describe the same worker — children it launches can rely on either."""
    out = str(tmp_path / "ids")
    Path(out).mkdir()

    def spawn(*, context):
        from pathlib import Path as _Path

        arg = context["worker_id"]
        env = daisy.Context.from_env()["worker_id"]
        _Path(out, f"pair-{arg}-{env}").touch()

    task = _mk_task(spawn, max_workers=1, max_worker_restarts=0)
    daisy.Server().run_blockwise([task], progress=False)

    pairs = [f.name.split("-")[1:] for f in Path(out).iterdir()]
    assert pairs, "spawn function never ran"
    for arg, env in pairs:
        assert arg == env, f"context argument {arg} != env var {env}"


def test_block_function_workers_get_distinct_identities(tmp_path):
    """Workers running a 1-arg block function get a deterministic
    DAISY_CONTEXT built for their process alone: worker ids and process
    ids map one-to-one. (The raced process-global DAISY_CONTEXT this
    guards against gave several processes the same worker_id.)

    Deliberately NOT asserted: that all 8 workers process a block. A
    worker that connects after the ready blocks drain is released
    immediately (tail teardown), so participation depends on machine
    load."""
    outdir = str(tmp_path / "ids")
    Path(outdir).mkdir()

    def record_identity(block):
        import os as _os
        import time as _time

        ctx = daisy.Context.from_env()
        open(os.path.join(outdir, f"w-{ctx['worker_id']}_p{_os.getpid()}"), "w").close()
        _time.sleep(0.1)  # hold the block so other workers participate

    task = _mk_task(record_identity, n_blocks=32, max_workers=8, task_id="shim_ctx")
    assert daisy.run_blockwise([task], progress=False)
    wid_to_pids: dict[str, set[str]] = {}
    pid_to_wids: dict[str, set[str]] = {}
    for f in Path(outdir).iterdir():
        wid, pid = f.name.split("_")
        wid_to_pids.setdefault(wid, set()).add(pid)
        pid_to_wids.setdefault(pid, set()).add(wid)
    assert all(len(pids) == 1 for pids in wid_to_pids.values()), wid_to_pids
    assert all(len(wids) == 1 for wids in pid_to_wids.values()), pid_to_wids
    # The blocks are held long enough that a lone worker can't drain the
    # task before at least one sibling joins.
    assert len(wid_to_pids) >= 2, sorted(wid_to_pids)


def test_context_round_trip():
    ctx = daisy.Context(hostname="node1", port=1234, task_id="t", worker_id=7)
    parsed = daisy.Context.from_env_string(ctx.to_env())
    assert parsed["hostname"] == "node1"
    assert parsed["port"] == "1234"
    assert parsed["worker_id"] == "7"


def test_both_spawn_signatures_are_accepted_without_warning(tmp_path):
    """Both the bare 0-arg and the keyword-only `context` signature are
    first-class. The 0-arg form used to warn about the DAISY_CONTEXT race
    when max_workers > 1; that warning is gone because the race is."""
    import warnings as _w

    def legacy_spawn():
        pass

    def race_free(*, context):
        pass

    def block_fn(block):
        pass

    for fn, workers in ((legacy_spawn, 8), (race_free, 8), (block_fn, 8)):
        with _w.catch_warnings():
            _w.simplefilter("error", UserWarning)
            daisy.Task(
                task_id="warncheck",
                total_roi=daisy.Roi([0], [20]),
                read_roi=daisy.Roi([0], [10]),
                write_roi=daisy.Roi([0], [10]),
                process_function=fn,
                read_write_conflict=False,
                max_workers=workers,
            )
