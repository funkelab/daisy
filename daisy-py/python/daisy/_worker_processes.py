"""Subprocess-worker shim: run a 1-arg ``process_function`` in real OS
processes instead of GIL-sharing threads (``Task(worker_processes=True)``).

daisy v2's default "multiprocessing" mode executes block functions on Rust
threads that each take the GIL per block — CPU-bound python work therefore
does not parallelize (and typically slows down) as ``max_workers`` grows.
This shim recovers daisy 1.x's headline convenience — a locally defined
lambda or closure saturating as many cores as you ask for — on top of the
v2 server: the function is serialized once at Task construction and each
worker slot launches ``python -m daisy._subprocess_worker``, which
deserializes it and runs the standard ``Client.acquire_block()`` loop.

Transport is an anonymous stdin pipe, deliberately not a temp file: the
payload never touches the filesystem, so there is nothing another process
can swap out or truncate between write and read, nothing to clean up, and
no path/size limits. The child reads stdin to EOF before any user code
runs.

Serialization prefers ``dill`` (lambdas, closures, interactively defined
functions) and falls back to stdlib ``pickle`` when dill is not installed
(module-level functions only). Install with ``pip install daisy[worker-processes]``
or ``pip install dill`` for full function support.
"""

import os
import pickle
import struct
import subprocess
import sys

#: exit code the worker child uses when it self-terminates because a block
#: exceeded ``Task(timeout=...)`` (see ``_subprocess_worker``).
EXIT_BLOCK_TIMEOUT = 87

# payload wire format: two length-prefixed frames.
#   frame 1 (stdlib pickle): {"sys_path": [...]} — the parent's sys.path,
#     prepended in the child so the function's module references resolve
#     exactly as they did in the parent. daisy 1.x got this for free by
#     forking; a spawned child re-imports, so it needs the parent's paths.
#   frame 2 (dill or pickle): (process_function, timeout)
_LEN = struct.Struct("<Q")


def _pack_frames(*frames: bytes) -> bytes:
    return b"".join(_LEN.pack(len(f)) + f for f in frames)


def _read_frame(stream) -> bytes:
    header = stream.read(_LEN.size)
    n = _LEN.unpack(header)[0]
    return stream.read(n)


def _serialize(obj) -> bytes:
    try:
        import dill
    except ImportError:
        try:
            body = pickle.dumps(obj)
        except Exception as e:
            raise RuntimeError(
                "daisy could not serialize this process_function for "
                "worker_processes=True: stdlib pickle supports module-level "
                "functions only. Install dill (`pip install dill`, or "
                "`pip install daisy[worker-processes]`) to use lambdas and "
                f"closures. Underlying error: {e!r}"
            ) from e
    else:
        body = dill.dumps(obj, recurse=True)
    header = pickle.dumps({"sys_path": list(sys.path)})
    return _pack_frames(header, body)


def read_payload(stream):
    """Child side: read (meta, body_bytes) from the stdin stream and apply
    the parent's sys.path before the function is deserialized."""
    meta = pickle.loads(_read_frame(stream))
    for p in reversed(meta.get("sys_path", [])):
        if p not in sys.path:
            sys.path.insert(0, p)
    body = _read_frame(stream)
    try:
        import dill as _pickle
    except ImportError:
        _pickle = pickle
    return _pickle.loads(body)


def make_spawn_function(process_function, timeout=None):
    """Wrap a 1-arg ``process_function`` into a 0-arg spawn function that
    runs it in a dedicated worker subprocess.

    The function (and the task's ``timeout``) are serialized eagerly, so an
    unserializable function fails at Task construction — not minutes later
    on a cluster. The returned spawn function raises on any non-zero child
    exit: a crashing worker is a *dirty* exit, which the server counts
    against ``max_worker_restarts`` instead of respawning it forever.
    """
    payload = _serialize((process_function, timeout))

    def _spawn_worker_process():
        # DAISY_CONTEXT is set (process-globally) by the server just before
        # this spawn function is called; snapshot the environment first
        # thing to minimize the window in which a concurrently spawning
        # worker could overwrite it.
        env = dict(os.environ)
        proc = subprocess.Popen(
            [sys.executable, "-m", "daisy._subprocess_worker"],
            stdin=subprocess.PIPE,
            env=env,
        )
        try:
            proc.stdin.write(payload)
            proc.stdin.close()
        except BrokenPipeError:
            # child died before reading the payload; the exit-code check
            # below turns that into a dirty worker exit
            pass
        returncode = proc.wait()
        if returncode != 0:
            raise RuntimeError(
                f"daisy worker subprocess exited with code {returncode}"
            )

    _spawn_worker_process.__name__ = "spawn_" + getattr(
        process_function, "__name__", "process_function"
    )
    return _spawn_worker_process
