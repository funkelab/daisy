"""Entry point for shim worker subprocesses (``Task(worker_processes=True)``).

Launched by ``daisy._worker_processes.make_spawn_function`` as
``python -m daisy._subprocess_worker``. Reads the serialized
``(process_function, timeout)`` payload from stdin (anonymous pipe — see
``_worker_processes`` for why not a temp file), connects back to the server
via ``daisy.Client()`` (``DAISY_CONTEXT`` comes from the inherited
environment), and runs the standard acquire/process/release loop.

Timeout preemption: when the task has ``timeout=T``, a watchdog timer is
armed around every block. If the block is still running after ``T``
seconds the process prints one line to stderr and hard-exits with
``EXIT_BLOCK_TIMEOUT`` — killing stuck python/C code for real, which a
thread-based worker can never do (threads: the reclaimed attempt keeps
running to completion and can double-apply its effects). The parent spawn
function maps that exit code to a dirty worker exit, and the server's
block bookkeeper — whose reclaim timer runs the same duration from the
same acquire — retries the block elsewhere. Reclaim and self-kill race
within milliseconds of each other, versus an unbounded overlap window in
thread mode.
"""

import os
import sys
import threading


def _run():
    from daisy._task import Client
    from daisy._worker_processes import read_payload

    process_function, timeout = read_payload(sys.stdin.buffer)

    try:
        client = Client()
    except (ConnectionRefusedError, RuntimeError) as e:
        if "Connection refused" in str(e):
            # end-of-run race: the server finished (all blocks done) and
            # shut down before this straggler worker came up. That's a
            # normal shutdown, not a worker failure — exit clean so it
            # isn't counted against max_worker_restarts.
            print(
                "daisy worker: server already gone at startup; exiting",
                file=sys.stderr,
            )
            return
        raise

    while True:
        with client.acquire_block() as block:
            if block is None:
                return
            watchdog = None
            if timeout is not None:
                from daisy._worker_processes import EXIT_BLOCK_TIMEOUT

                def _preempt(block_id=block.block_id):
                    print(
                        f"daisy worker {client.worker_id}: block {block_id} "
                        f"still running after timeout={timeout}s; killing "
                        "worker process",
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


if __name__ == "__main__":
    _run()
