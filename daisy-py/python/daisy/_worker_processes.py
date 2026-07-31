"""Subprocess workers: run a 1-arg ``process_function`` in real OS
processes. This is the DEFAULT execution mode for block functions on the
distributed run paths; ``Task(worker_processes=False)`` opts into
GIL-sharing thread workers instead.

Thread workers execute block functions on Rust threads that each take the
GIL per block — CPU-bound python work therefore does not parallelize (and
typically slows down) as ``max_workers`` grows. Benchmarks across workload
mixes show threads win only when the block function releases the GIL for
essentially its entire runtime (pure I/O waits, single-threaded C-library
calls), and then only by ~15%; with just 10% pure-python glue threads are
1.7x slower, at 30% python 8x slower, at 100% python 28x slower. Subprocess
workers are flat across every mix, hence the default. The function is
serialized once per run and each worker slot launches
``python -m daisy._subprocess_worker``, which deserializes it and runs the
standard ``Client.acquire_block()`` loop.

Transport is an anonymous stdin pipe, deliberately not a temp file: the
payload never touches the filesystem, so there is nothing another process
can swap out or truncate between write and read, nothing to clean up, and
no path/size limits. The child reads stdin to EOF before any user code
runs.

Serialization prefers ``cloudpickle`` (lambdas, closures, functions defined
in ``__main__`` or a notebook) and falls back to stdlib ``pickle`` when it
is not installed (module-level functions only). Install with
``pip install daisy[worker-processes]``.

Why cloudpickle and not dill: both serialize functions by value, but they
disagree about *modules* a function references as globals (``import mypkg``
then ``mypkg.helper()``). dill pickles any module outside
site-packages BY VALUE — the whole ``__dict__`` — so one unpicklable
member anywhere in your project package (a ``struct.Struct``, a
``threading.local``, a live DB connection) fails the payload even when the
block function never touches it. cloudpickle pickles importable modules by
reference and reserves by-value for ``__main__``, which is precisely the
split daisy wants: workers replicate the parent's ``sys.path`` (see
``read_payload``), and re-importing is also what genuinely remote cluster
workers do, so local subprocess runs behave like the real deployment.

The trade: cloudpickle refuses to serialize a few things dill accepts —
notably ``threading`` synchronization primitives and write-mode file
handles, including indirectly (a bound method whose ``self`` holds a
``Lock``). Those objects are meaningless across process boundaries
anyway, so failing loudly at submit time is the better outcome; the fix
is to create them inside the block function, or to use thread workers
(``worker_processes=False``), where a shared lock actually synchronizes.
"""

import os
import pickle
import struct
import subprocess
import sys
import tempfile

#: exit code the worker child uses when it self-terminates because a block
#: exceeded ``Task(timeout=...)`` (see ``_subprocess_worker``).
EXIT_BLOCK_TIMEOUT = 87

# payload wire format: two length-prefixed frames.
#   frame 1 (stdlib pickle): {"sys_path": [...]} — the parent's sys.path,
#     prepended in the child so the function's module references resolve
#     exactly as they did in the parent. daisy 1.x got this for free by
#     forking; a spawned child re-imports, so it needs the parent's paths.
#   frame 2 (cloudpickle or pickle): (process_function, timeout)
_LEN = struct.Struct("<Q")


def _pack_frames(*frames: bytes) -> bytes:
    return b"".join(_LEN.pack(len(f)) + f for f in frames)


def _read_frame(stream) -> bytes:
    header = stream.read(_LEN.size)
    n = _LEN.unpack(header)[0]
    return stream.read(n)


def _serialize(obj) -> bytes:
    try:
        import cloudpickle
    except ImportError:
        try:
            body = pickle.dumps(obj)
        except Exception as e:
            raise RuntimeError(
                "daisy could not serialize this process_function for "
                "subprocess workers (the default execution mode): stdlib "
                "pickle supports module-level functions only. Either install "
                "cloudpickle for lambda/closure support (`pip install "
                "cloudpickle`, or `pip install daisy[worker-processes]`), or "
                "opt into GIL-sharing thread workers with "
                "`Task(..., worker_processes=False)` (only sensible for "
                "block functions that release the GIL for essentially their "
                f"entire runtime). Underlying error: {e!r}"
            ) from e
    else:
        try:
            body = cloudpickle.dumps(obj)
        except Exception as e:
            raise RuntimeError(
                "daisy could not serialize this process_function for "
                "subprocess workers (the default execution mode). The "
                "function is shipped together with the objects it captures, "
                "and something it captures cannot be pickled — commonly a "
                "threading lock/condition, an open write handle, or a live "
                "connection, held either directly or by an object the "
                "function is bound to. Such objects do not carry meaning "
                "across process boundaries: create them inside the block "
                "function instead, or use `Task(..., worker_processes=False)` "
                "to run on threads, where shared locks and handles are "
                f"real. Underlying error: {e!r}"
            ) from e
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
        import cloudpickle as _pickle
    except ImportError:
        import pickle as _pickle
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

    def _spawn_worker_process(*, context):
        # The context arrives as an argument (race-free), so the child's
        # DAISY_CONTEXT can be set deterministically instead of hoping the
        # process-global env var wasn't overwritten by a concurrent spawn.
        env = dict(os.environ)
        env["DAISY_CONTEXT"] = context.to_env()
        # Capture the child's stderr (to a spooled file, not a pipe — no
        # reader-deadlock for chatty workers) so a crashing block function's
        # actual traceback can ride along in the raised error, where the
        # server records it as the task's last_worker_error. Everything
        # captured is re-emitted on our stderr so logs stay complete.
        with tempfile.SpooledTemporaryFile(max_size=1 << 20) as errbuf:
            proc = subprocess.Popen(
                [sys.executable, "-m", "daisy._subprocess_worker"],
                stdin=subprocess.PIPE,
                stderr=errbuf,
                env=env,
            )
            assert proc.stdin is not None  # stdin=PIPE above
            try:
                proc.stdin.write(payload)
                proc.stdin.close()
            except BrokenPipeError:
                # child died before reading the payload; the exit-code check
                # below turns that into a dirty worker exit
                pass
            returncode = proc.wait()
            errbuf.seek(0)
            err_text = errbuf.read().decode(errors="replace")
        if err_text:
            sys.stderr.write(err_text)
            sys.stderr.flush()
        tail = "\n".join(err_text.strip().splitlines()[-50:])
        if returncode == EXIT_BLOCK_TIMEOUT:
            raise RuntimeError(
                f"daisy worker subprocess killed after a block exceeded "
                f"timeout={timeout}s (true preemption; the server retries "
                "the block under max_retries)"
                + (f"\nworker stderr tail:\n{tail}" if tail else "")
            )
        if returncode != 0:
            raise RuntimeError(
                f"daisy worker subprocess exited with code {returncode}"
                + (f"\nworker stderr tail:\n{tail}" if tail else "")
            )

    _spawn_worker_process.__name__ = "spawn_" + getattr(
        process_function, "__name__", "process_function"
    )
    return _spawn_worker_process
