"""Entry point for daisy worker subprocesses.

Launched by ``daisy._worker_processes.make_spawn_function`` as
``python -m daisy._subprocess_worker``, one process per worker slot. Reads
the serialized payload from stdin (anonymous pipe — see
``_worker_processes`` for why not a temp file) and runs the user function
according to its arity:

- **arity 1** (a block function): connect back to the server via
  ``daisy.Client()`` — ``DAISY_CONTEXT`` comes from the environment the
  parent built for this process — and run the standard
  acquire/process/release loop.
- **arity 0** (a worker function): just call it. It owns its own worker
  loop, typically its own ``daisy.Client()``, and may hand off to a further
  process (``srun``, ``sbatch``, a container). Called with ``context=`` when
  it declares that keyword-only parameter.

Timeout preemption (arity 1): when the task has ``timeout=T``, a watchdog
timer is armed around every block. If the block is still running after
``T`` seconds the process prints one line to stderr and hard-exits with
``EXIT_BLOCK_TIMEOUT`` — killing stuck python or C code for real. The
parent spawn function maps that exit code to a dirty worker exit, and the
server's block bookkeeper — whose reclaim timer runs the same duration from
the same acquire — retries the block elsewhere; the two race within
milliseconds of each other, so a reclaimed block is never still executing
somewhere. An arity-0 worker owns its own process and must enforce its own
per-block deadline if it wants one; the server still reclaims and retries
the block either way.
"""

import inspect
import os
import sys
import threading


def _run_block_loop(process_function, timeout):
    from daisy._task import Client

    # A server that is already gone (end-of-run straggler race) leaves the
    # Client disconnected: acquire_block() yields None immediately and this
    # function returns normally — exit 0, so the straggler is not counted
    # against max_worker_restarts. Client itself logs the WARNING.
    client = Client()

    # NOTE on failure semantics: an exception from process_function
    # propagates out of acquire_block (which has already marked the block
    # failed and released it for retry) and crashes this worker process — a
    # dirty exit that counts against max_worker_restarts, so persistent bugs
    # drive restart-cap abandonment instead of silently slogging through
    # every block's retries.
    while True:
        with client.acquire_block() as block:
            if block is None:
                return
            watchdog = None
            if timeout is not None:
                from daisy._worker_processes import EXIT_BLOCK_TIMEOUT

                def _preempt(block_id=block.block_id):
                    print(
                        f"daisy worker {client.worker_id}: block "
                        f"{block_id} still running after "
                        f"timeout={timeout}s; killing worker process",
                        file=sys.stderr,
                        flush=True,
                    )
                    os._exit(EXIT_BLOCK_TIMEOUT)

                watchdog = threading.Timer(timeout, _preempt)
                watchdog.daemon = True
                watchdog.start()
            try:
                process_function(block)
            finally:
                if watchdog is not None:
                    watchdog.cancel()


def _run_worker_function(worker_function):
    """Call a 0-arg worker function, forwarding this process's context if it
    asks for one. Reading the context here is race-free: the parent gave
    this process its own environment."""
    wants_context = False
    try:
        wants_context = "context" in (
            inspect.getfullargspec(worker_function).kwonlyargs or []
        )
    except TypeError:
        pass
    if wants_context:
        from daisy._task import Context

        worker_function(context=Context.from_env())
    else:
        worker_function()


def _run():
    from daisy._worker_processes import read_payload

    payload = read_payload(sys.stdin.buffer)
    function = payload["function"]
    if payload["arity"] == 0:
        _run_worker_function(function)
    else:
        _run_block_loop(function, payload["timeout"])


if __name__ == "__main__":
    _run()
