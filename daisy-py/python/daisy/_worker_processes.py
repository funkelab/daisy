"""Worker subprocesses: every distributed daisy worker is its own OS process.

There is exactly one execution model on the distributed run paths. Whatever
a task's ``process_function`` is, the runner turns it into a 0-arg *spawn
function* that launches ``python -m daisy._subprocess_worker`` and waits:

- a **1-arg block function** ``f(block)`` is run by the child in the standard
  ``Client.acquire_block()`` loop;
- a **0-arg worker function** ``f()`` (or ``f(*, context)``) is simply called
  by the child, which is free to open its own ``daisy.Client()``, or to
  ``srun``/``sbatch`` a further process.

Both kinds therefore get real parallelism (CPU-bound python scales with
``max_workers`` because there is no shared GIL), real timeout preemption
(a stuck block's process can be killed), and per-process resource figures
that mean what they say. daisy 1.x behaved the same way, via
``multiprocessing.Process``; v2 cannot fork — the Rust server runs a tokio
runtime per worker thread and forking a multithreaded process is unsafe —
so it spawns and serializes instead.

The cost of dropping in-process (thread) workers is small and bounded:
roughly 15% for block functions that release the GIL for essentially their
entire runtime (pure I/O waits, single-threaded C-library calls), which was
the only case where threads won at all, plus the loss of free shared
read-only memory — pass a path and ``numpy.memmap`` it in the child
instead. Anything with a meaningful pure-python fraction is dramatically
faster this way (measured: threads were 1.7x slower at 10% python glue, 28x
at 100%).

``run_blockwise(..., multiprocessing=False)`` is unaffected: it calls the
original function in-process, single-threaded, and is the mode to use for
pdb, closures over live objects, and in-process assertions in tests.

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
``Lock``). Those objects are meaningless across process boundaries anyway,
so failing loudly at submit time is the better outcome; create them inside
the function instead.
"""

import os
import pickle
import struct
import subprocess
import sys
import tempfile

#: exit code a worker process uses when its block watchdog kills it because
#: a block exceeded ``Task(timeout=...)``. The watchdog (and this value)
#: live in the Rust client — every ``daisy.Client`` loop is covered.
from daisy._daisy import EXIT_BLOCK_TIMEOUT as EXIT_BLOCK_TIMEOUT  # noqa: E402

# payload wire format: two length-prefixed frames.
#   frame 1 (stdlib pickle): the parent's process-global configuration that
#     a worker needs — `sys.path` so the function's module references
#     resolve as they did in the parent, and the `daisy.logging` settings so
#     worker output lands where the master said it should. daisy 1.x got all
#     of this for free by forking; a spawned child starts from defaults, so
#     anything process-global has to be carried explicitly.
#   frame 2 (cloudpickle or pickle): {"function":, "arity":, "timeout":}
#     ("timeout" is informational — the block watchdog is armed by
#     daisy.Client from the per-block value the server sends; the parent
#     keeps it only for the EXIT_BLOCK_TIMEOUT error message.)
_LEN = struct.Struct("<Q")


def _pack_frames(*frames: bytes) -> bytes:
    return b"".join(_LEN.pack(len(f)) + f for f in frames)


def _read_frame(stream) -> bytes:
    header = stream.read(_LEN.size)
    n = _LEN.unpack(header)[0]
    return stream.read(n)


def _capture_parent_config() -> dict:
    """Snapshot the parent's process-global state that a worker inherits.

    `daisy.logging`'s settings are module globals, so a spawned child starts
    with the defaults rather than whatever the master configured. Without
    this, `set_log_basedir(...)` would have no effect in any worker: per-
    worker log files would silently become console output, and workers that
    read `client.context["logdir"]` (the daisy 1.x idiom) would not find the
    key at all.
    """
    from daisy import logging as _worker_log

    basedir = _worker_log.get_log_basedir()
    return {
        "sys_path": list(sys.path),
        "log_basedir": None if basedir is None else str(basedir),
        "log_mode": _worker_log.get_log_mode(),
        "log_level": _worker_log.get_log_level(),
    }


def apply_parent_config(meta: dict) -> None:
    """Child side: restore the parent's globals before any user code runs."""
    for p in reversed(meta.get("sys_path", [])):
        if p not in sys.path:
            sys.path.insert(0, p)
    from daisy import logging as _worker_log

    if "log_basedir" in meta:
        _worker_log.set_log_basedir(meta["log_basedir"])
    if meta.get("log_mode"):
        _worker_log.set_log_mode(meta["log_mode"])
    if meta.get("log_level") is not None:
        _worker_log.set_log_level(meta["log_level"])


#: Shared tail for both serialization failure messages: what the caller can
#: actually do about it. Subprocess workers are the only distributed
#: execution model, so the escape hatch is serial mode, not another worker
#: mode.
_SERIALIZE_REMEDY = (
    "Fixes, in order of preference: define the function at module level; "
    "create unpicklable objects (locks, file handles, connections, "
    "database sessions) inside the function rather than capturing them; "
    "pass a path and reopen/memmap it in the worker. To run in-process "
    "instead — single-threaded, no workers, useful for debugging — use "
    "`run_blockwise(..., multiprocessing=False)`."
)


def _serialize(obj) -> bytes:
    try:
        import cloudpickle
    except ImportError:
        try:
            body = pickle.dumps(obj)
        except Exception as e:
            raise RuntimeError(
                "daisy could not serialize this function for its worker "
                "subprocess: stdlib pickle supports module-level functions "
                "only. Install cloudpickle for lambda/closure support "
                "(`pip install cloudpickle`, or `pip install "
                f"daisy[worker-processes]`). {_SERIALIZE_REMEDY} "
                f"Underlying error: {e!r}"
            ) from e
    else:
        try:
            body = cloudpickle.dumps(obj)
        except Exception as e:
            raise RuntimeError(
                "daisy could not serialize this function for its worker "
                "subprocess. Every daisy worker runs in its own process, so "
                "the function is shipped together with the objects it "
                "captures, and something it captures cannot be pickled — "
                "commonly a threading lock/condition, an open write handle, "
                "or a live connection, held either directly or by an object "
                f"the function is bound to. {_SERIALIZE_REMEDY} "
                f"Underlying error: {e!r}"
            ) from e
    header = pickle.dumps(_capture_parent_config())
    return _pack_frames(header, body)


def read_payload(stream):
    """Child side: read the payload dict from the stdin stream, restoring the
    parent's sys.path and logging configuration before the function is
    deserialized."""
    meta = pickle.loads(_read_frame(stream))
    apply_parent_config(meta)
    body = _read_frame(stream)
    try:
        import cloudpickle as _pickle
    except ImportError:
        import pickle as _pickle
    return _pickle.loads(body)


def make_spawn_function(process_function, arity, timeout=None):
    """Wrap a user function into a 0-arg spawn function that runs it in a
    dedicated worker subprocess.

    `arity` is 1 for a block function (the child drives the
    `Client.acquire_block()` loop and calls it per block) or 0 for a worker
    function (the child just calls it).

    The payload is serialized eagerly, so an unserializable function fails
    at run start — not minutes later on a cluster. The returned spawn
    function raises on any non-zero child exit: a crashing worker is a
    *dirty* exit, which the server counts against `max_worker_restarts`
    instead of respawning it forever.
    """
    payload = _serialize(
        {"function": process_function, "arity": arity, "timeout": timeout}
    )

    def _spawn_worker_process(*, context):
        # The context arrives as an argument (race-free), so the child's
        # DAISY_CONTEXT is set deterministically per process instead of
        # relying on the parent's process-global env var, which concurrent
        # spawns overwrite.
        env = dict(os.environ)
        # The context arrives complete: PySpawnWorker::spawn injects the
        # master's log directory before either spawn channel is written, so
        # there is nothing to add here — just re-encode it for the child.
        env["DAISY_CONTEXT"] = context.to_env()
        # Capture the child's output (to spooled files, not pipes — no
        # reader-deadlock for chatty workers) and re-emit it through our own
        # streams. The parent runs this inside `_WorkerLogContext`, whose
        # stream proxies are python-level and therefore not inherited by a
        # spawned child, so re-emitting here is what puts worker output in
        # the worker's log files. Keeping stderr separate also lets a
        # crashing function's real traceback ride along in the raised error,
        # where the server records it as the task's last_worker_error.
        with (
            tempfile.SpooledTemporaryFile(max_size=1 << 20) as outbuf,
            tempfile.SpooledTemporaryFile(max_size=1 << 20) as errbuf,
        ):
            proc = subprocess.Popen(
                [sys.executable, "-m", "daisy._subprocess_worker"],
                stdin=subprocess.PIPE,
                stdout=outbuf,
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
            outbuf.seek(0)
            out_text = outbuf.read().decode(errors="replace")
            errbuf.seek(0)
            err_text = errbuf.read().decode(errors="replace")
        for text, stream in ((out_text, sys.stdout), (err_text, sys.stderr)):
            if text:
                stream.write(text)
                stream.flush()
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
