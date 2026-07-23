"""Entry point for shim worker subprocesses (``Task(worker_processes=True)``).

Launched by ``daisy._worker_processes.make_spawn_function`` as
``python -m daisy._subprocess_worker``. Reads the serialized
``(process_function, timeout)`` payload from stdin (anonymous pipe — see
``_worker_processes`` for why not a temp file), connects back to the server
via ``daisy.Client()`` (``DAISY_CONTEXT`` comes from the inherited
environment), and runs the standard acquire/process/release loop.
"""

import sys


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
            process_function(block)


if __name__ == "__main__":
    _run()
