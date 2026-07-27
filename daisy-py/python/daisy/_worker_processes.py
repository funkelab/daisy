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

Serialization prefers ``dill`` (lambdas, closures, interactively defined
functions) and falls back to stdlib ``pickle`` when dill is not installed
(module-level functions only). Install with ``pip install daisy[worker-processes]``
or ``pip install dill`` for full function support.
"""

import importlib
import io
import os
import pickle
import struct
import subprocess
import tempfile
import sys
import types

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


def _import_module(name):
    """Unpickle-side reconstructor for by-reference module pickles."""
    return importlib.import_module(name)


def _make_modules_by_ref_pickler(dill):
    """A ``dill.Pickler`` that pickles every imported module by reference.

    dill pickles a module it deems "not builtin" — anything whose file
    lives outside sys.prefix / site-packages, e.g. an editable install
    or a source-tree checkout — BY VALUE: its entire ``__dict__`` is
    serialized, so one unpicklable module-level object (a
    ``threading.local``, a ``struct.Struct``, an open file) anywhere in
    that namespace fails the whole payload. A block function only has to
    reference such a module as a global (``daisy.BlockStatus.SUCCESS``)
    to drag it in.

    Our child workers replicate the parent's ``sys.path`` before the
    function is deserialized (see ``read_payload``), so any module
    importable in the parent is importable in the child, and
    by-reference is both sufficient and robust. Only ``__main__`` (and
    its multiprocessing alias) genuinely needs dill's by-value path —
    the child's ``__main__`` is the worker shim, not the user's script.
    """

    class _ModulesByRefPickler(dill.Pickler):
        dispatch = dill.Pickler.dispatch.copy()

        def _save_module(self, obj):
            name = getattr(obj, "__name__", None)
            if (
                name
                and name not in ("__main__", "__mp_main__")
                and sys.modules.get(name) is obj
            ):
                self.save_reduce(_import_module, (name,), obj=obj)
            else:
                # __main__, or a synthetic/reloaded module the child could
                # not re-import — fall back to dill's own module handling.
                dill.Pickler.dispatch[types.ModuleType](self, obj)

        dispatch[types.ModuleType] = _save_module

    return _ModulesByRefPickler


def _serialize(obj) -> bytes:
    try:
        import dill
    except ImportError:
        try:
            body = pickle.dumps(obj)
        except Exception as e:
            raise RuntimeError(
                "daisy could not serialize this process_function for "
                "subprocess workers (the default execution mode): stdlib "
                "pickle supports module-level functions only. Either install "
                "dill for lambda/closure support (`pip install dill`, or "
                "`pip install daisy[worker-processes]`), or opt into "
                "GIL-sharing thread workers with "
                "`Task(..., worker_processes=False)` (only sensible for "
                "block functions that release the GIL for essentially their "
                f"entire runtime). Underlying error: {e!r}"
            ) from e
    else:
        try:
            # recurse=True is load-bearing: it ships the globals the
            # function actually references, so functions defined in
            # __main__ / notebooks resolve their names in the child.
            buf = io.BytesIO()
            _make_modules_by_ref_pickler(dill)(buf, recurse=True).dump(obj)
            body = buf.getvalue()
        except Exception as e:
            raise RuntimeError(
                "daisy could not serialize this process_function for "
                "subprocess workers (the default execution mode). dill "
                "serializes the function together with the globals it "
                "references — a referenced module or object that cannot "
                "be pickled fails the whole payload. Either restructure "
                "the function to import what it needs by name (e.g. "
                "`from mypkg import thing` instead of referencing the "
                "`mypkg` module as a global), or opt into GIL-sharing "
                "thread workers with `Task(..., worker_processes=False)` "
                "(only sensible for block functions that release the GIL "
                "for essentially their entire runtime). "
                f"Underlying error: {e!r}"
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
