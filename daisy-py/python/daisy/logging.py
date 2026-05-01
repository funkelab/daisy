"""Per-worker log file setup, modelled on daisy.logging.

While a worker's `process_function` runs, its stdout and stderr go
through a proxy whose destination is controlled by three knobs:

- `set_log_basedir(path)` — where per-worker log files live. Pass `None`
  to disable file logging entirely. Default: `./daisy_logs`.
- `set_log_mode(mode)` — where worker writes are routed while a block
  is being processed:
    - `"console"` — only the terminal (never touches a file).
    - `"file"`    — only the worker's `.out` / `.err` file.
    - `"both"`    — tee to both.
  Default: `"file"`, matching daisy's behaviour.
- `set_log_level(level)` — minimum level for the `"daisy"` logger,
  which daisy's own wrappers use to emit block-failure warnings. Takes
  the usual `logging.DEBUG` / `INFO` / `WARNING` / `ERROR` constants or
  the equivalent strings.

All three are process-global; change them before calling
`run_blockwise`.
"""

from __future__ import annotations

import typing
import logging as _py_logging
import re
import sys
import threading
import traceback as _py_traceback
from pathlib import Path


# --- Public knobs ------------------------------------------------------------

LOG_BASEDIR: Path | None = Path("./daisy_logs")

# "console" | "file" | "both". `get_log_mode()` resolves "file"/"both" to
# "console" when `LOG_BASEDIR is None`.
_LOG_MODE: str = "file"

_VALID_MODES = ("console", "file", "both")

class _DynamicStderrHandler(_py_logging.Handler):
    """StreamHandler equivalent that resolves `sys.stderr` on every emit
    instead of caching it at construction time, so it honours the
    per-thread proxy `_install_proxies` later sets up."""

    def emit(self, record):
        try:
            msg = self.format(record)
            sys.stderr.write(msg + "\n")
        except Exception:
            self.handleError(record)


logger = _py_logging.getLogger("daisy")
# A default handler so our warnings have somewhere to go even if the host
# application hasn't configured logging. Users can replace or remove it.
if not logger.handlers:
    _default_handler = _DynamicStderrHandler()
    _default_handler.setFormatter(_py_logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    ))
    logger.addHandler(_default_handler)
    logger.propagate = False
logger.setLevel(_py_logging.WARNING)


def set_log_basedir(path):
    """Set the base directory for per-worker log files. `None` disables
    file logging; regardless of mode, worker output will then fall
    through to the terminal."""
    global LOG_BASEDIR
    LOG_BASEDIR = Path(path) if path is not None else None


def get_log_basedir() -> Path | None:
    return LOG_BASEDIR


def set_log_mode(mode: str) -> None:
    """Choose where worker `process_function` stdout/stderr goes.

    `"console"` routes to the real terminal only; `"file"` routes to the
    worker's log file only; `"both"` tees to both. Invalid values raise
    `ValueError`.
    """
    global _LOG_MODE
    if mode not in _VALID_MODES:
        raise ValueError(
            f"mode must be one of {_VALID_MODES!r}, got {mode!r}"
        )
    _LOG_MODE = mode


def get_log_mode() -> str:
    """Effective mode. `"file"`/`"both"` degrade to `"console"` when
    `LOG_BASEDIR is None`."""
    if LOG_BASEDIR is None:
        return "console"
    return _LOG_MODE


def set_log_level(level) -> None:
    """Set the minimum level for the `"daisy"` logger. Accepts the
    usual `logging.DEBUG` / `INFO` / `WARNING` / `ERROR` constants or
    the equivalent strings."""
    logger.setLevel(level)


def get_log_level() -> int:
    return logger.level


def get_worker_log_basename(worker_id, task_id=None) -> Path | None:
    """`<basedir>/[task_id/]worker_<worker_id>` — without the .out/.err suffix."""
    base = get_log_basedir()
    if base is None:
        return None
    if task_id is not None:
        base = base / task_id
    return base / f"worker_{worker_id}"


# --- Per-thread redirection plumbing -----------------------------------------

_slot_lock = threading.Lock()
# (task_id, thread_id) -> worker slot index within the task.
_thread_slots: dict[tuple[str, int], int] = {}
# (task_id, slot) -> (out_file, err_file).
_slot_files: dict[tuple[str, int], tuple[typing.IO[str], typing.IO[str]]] = {}
# Per-thread: which (task_id, slot) pair is currently active. Missing = no
# worker slot bound; writes always go straight to the real stream.
_active = threading.local()


class _PerThreadStream:
    """sys.stdout/stderr proxy. While a worker slot is bound to the
    current thread, writes are routed according to the current log mode;
    otherwise they fall straight through to the original stream."""

    def __init__(self, kind: str, fallback):
        self._kind = kind  # "out" or "err"
        self._fallback = fallback

    def _current_file(self):
        key = getattr(_active, "key", None)
        if key is None:
            return None
        files = _slot_files.get(key)
        if files is None:
            return None
        return files[0] if self._kind == "out" else files[1]

    def write(self, s):
        f = self._current_file()
        if f is None:
            return self._fallback.write(s)
        mode = get_log_mode()
        written = 0
        if mode in ("file", "both"):
            written = f.write(s)
        if mode in ("console", "both"):
            self._fallback.write(s)
        return written or len(s)

    def flush(self):
        f = self._current_file()
        mode = get_log_mode()
        if f is not None and mode in ("file", "both"):
            try:
                f.flush()
            except Exception:
                pass
        if f is None or mode in ("console", "both"):
            try:
                self._fallback.flush()
            except Exception:
                pass

    def isatty(self):
        return False

    def __getattr__(self, name):
        return getattr(self._fallback, name)


_proxies_installed = False
_saved_stdout = None
_saved_stderr = None


def _install_proxies():
    global _proxies_installed, _saved_stdout, _saved_stderr
    if _proxies_installed:
        return
    _saved_stdout = sys.stdout
    _saved_stderr = sys.stderr
    sys.stdout = _PerThreadStream("out", sys.stdout)
    sys.stderr = _PerThreadStream("err", sys.stderr)
    _proxies_installed = True


def _assign_slot(task_id: str) -> int | None:
    base = get_log_basedir()
    if base is None:
        return None
    tid = threading.get_ident()
    key = (task_id, tid)
    with _slot_lock:
        if key in _thread_slots:
            return _thread_slots[key]
        slot = sum(1 for (t, _) in _thread_slots if t == task_id)
        _thread_slots[key] = slot
        # Reuse the already-narrowed `base` rather than re-querying (which
        # could race with a `set_log_basedir(None)` between the early
        # check above and now).
        basename = (base / task_id / f"worker_{slot}") if task_id else (
            base / f"worker_{slot}"
        )
        basename.parent.mkdir(parents=True, exist_ok=True)
        # Truncate (mode "w") so each `run_blockwise` invocation gets a
        # clean log instead of stacking old runs on top of new ones.
        out_f = open(basename.with_suffix(".out"), "w", buffering=1)
        err_f = open(basename.with_suffix(".err"), "w", buffering=1)
        _slot_files[(task_id, slot)] = (out_f, err_f)
        return slot


class _WorkerLogContext:
    """Context manager that binds a worker slot to the current thread
    for the duration of one `process_function` call."""

    def __init__(self, task_id: str):
        self._task_id = task_id
        self._prev_key = None
        self._slot = None

    def __enter__(self):
        # Install proxies even when file logging is off — they're no-op
        # fall-throughs in that case, and we want set_log_basedir(...)
        # to take effect on the next run_blockwise without a restart.
        _install_proxies()
        # "console" mode means never open any file; we also skip if the
        # base dir is unset.
        if get_log_basedir() is None or get_log_mode() == "console":
            return self
        self._slot = _assign_slot(self._task_id)
        if self._slot is not None:
            self._prev_key = getattr(_active, "key", None)
            _active.key = (self._task_id, self._slot)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._slot is None:
            return False
        try:
            files = _slot_files.get((self._task_id, self._slot))
            if files is not None:
                files[0].flush()
                files[1].flush()
        except Exception:
            pass
        _active.key = self._prev_key
        return False


_IPYTHON_CELL_RE = re.compile(r'<ipython-input-(\d+)-[a-f0-9]+>')

# Path of this package, used to strip daisy-internal frames from
# tracebacks before rendering.
import os as _os
_DAISY_PKG_DIR = _os.path.abspath(_os.path.dirname(__file__))


def _strip_daisy_frames(tb):
    """Walk the traceback chain past leading daisy-internal frames so
    the rendered traceback starts at the user's code. Frames in between
    user frames (e.g. the `yield` inside `Client.acquire_block`) are
    left alone — they're informative-ish and removing them would break
    the call-path narrative."""
    while tb is not None:
        fname = _os.path.abspath(tb.tb_frame.f_code.co_filename)
        if not fname.startswith(_DAISY_PKG_DIR):
            return tb
        tb = tb.tb_next
    return tb

# Traceback rendering knobs — see `set_traceback_style`.
_TRACEBACK_STYLE = "plain"          # "plain" | "rich"
_TRACEBACK_LOCALS = False
_TRACEBACK_WIDTH = 100
_TRACEBACK_EXTRA_LINES = 2
# Block-function tasks call `format_traceback` from N worker threads
# concurrently. Rich's `Traceback.from_exception` and our linecache
# mutations are not obviously thread-safe; serialise the whole render.
_TRACEBACK_LOCK = threading.Lock()


def set_traceback_style(style: str, *, show_locals: bool = False,
                        width: int = 100, extra_lines: int = 2) -> None:
    """Choose how block-failure tracebacks are formatted.

    - `"plain"` (default) — `traceback.format_exception` with IPython
      cell filenames cleaned up. No external deps. Good for `.err`
      files anyone can grep.
    - `"rich"` — `rich.traceback` with source context, optional locals,
      ANSI colour. Requires the `rich` package (`pip install rich`).
      Renders nicely in modern terminals and editors that support ANSI.

    `show_locals=True` (rich-only) shows local variables next to each
    frame; helpful but verbose. `width` and `extra_lines` are passed
    through to `rich.traceback.Traceback`.
    """
    global _TRACEBACK_STYLE, _TRACEBACK_LOCALS, _TRACEBACK_WIDTH, _TRACEBACK_EXTRA_LINES
    if style not in ("plain", "rich"):
        raise ValueError(
            f"style must be 'plain' or 'rich', got {style!r}"
        )
    if style == "rich":
        try:
            import rich.traceback  # noqa: F401
        except ImportError as e:
            raise ImportError(
                "set_traceback_style('rich') requires the `rich` package. "
                "Install it with `pip install rich`."
            ) from e
    _TRACEBACK_STYLE = style
    _TRACEBACK_LOCALS = bool(show_locals)
    _TRACEBACK_WIDTH = int(width)
    _TRACEBACK_EXTRA_LINES = int(extra_lines)


def get_traceback_style() -> str:
    return _TRACEBACK_STYLE


def _rewrite_rich_ipython_frames(tb_obj):
    """Rich skips any frame whose filename starts with `<` (no source
    rendering, no failing-line arrow). For IPython cells the source is
    available via `linecache` — we rewrite the per-frame filename to
    `[notebook cell N]` and seed `linecache` under that new key so
    rich's `linecache.getlines` lookup hits. Mutates `tb_obj` in
    place."""
    import linecache
    for stack in tb_obj.trace.stacks:
        for frame in stack.frames:
            m = _IPYTHON_CELL_RE.match(frame.filename)
            if m is None:
                continue
            new_name = f"[notebook cell {m.group(1)}]"
            entry = linecache.cache.get(frame.filename)
            if entry is not None and new_name not in linecache.cache:
                linecache.cache[new_name] = entry
            frame.filename = new_name


def _format_plain(exc_type, exc, tb) -> str:
    text = "".join(_py_traceback.format_exception(exc_type, exc, tb))
    return _IPYTHON_CELL_RE.sub(r"[notebook cell \1]", text)


def format_traceback(exc_type, exc, tb) -> str:
    """Render an exception's traceback for logging according to the
    current `set_traceback_style` configuration. Leading
    daisy-internal frames are stripped so the trace starts at the
    user's code, and IPython cell pseudo-filenames are rewritten to
    `[notebook cell N]`. Guaranteed not to raise — falls back to a
    bare repr of the exception if every formatter fails. Calls are
    serialised through `_TRACEBACK_LOCK` so concurrent renders from
    multiple worker threads don't corrupt rich's internal state or
    race on `linecache.cache` mutations."""
    with _TRACEBACK_LOCK:
        try:
            try:
                user_tb = _strip_daisy_frames(tb)
            except Exception:
                user_tb = tb  # frame walk failed; just use the original

            if _TRACEBACK_STYLE == "rich":
                try:
                    import io
                    from rich.console import Console
                    from rich.traceback import Traceback
                    tb_obj = Traceback.from_exception(
                        exc_type, exc, user_tb,
                        show_locals=_TRACEBACK_LOCALS,
                        extra_lines=_TRACEBACK_EXTRA_LINES,
                        locals_max_string=60,
                        locals_max_length=4,
                    )
                    try:
                        _rewrite_rich_ipython_frames(tb_obj)
                    except Exception:
                        pass  # not catastrophic
                    buf = io.StringIO()
                    Console(
                        file=buf, force_terminal=True,
                        # `force_jupyter=False` is critical: when
                        # rich's Console auto-detects a Jupyter kernel
                        # it routes `.print()` through
                        # `IPython.display.display(...)` instead of
                        # writing to the `file=` argument, so our buf
                        # comes back empty and we fall through to
                        # plain. Forcing it off makes Console behave
                        # the same in scripts and notebooks.
                        force_jupyter=False,
                        width=_TRACEBACK_WIDTH,
                        color_system="truecolor",
                    ).print(tb_obj)
                    rendered = buf.getvalue()
                    if rendered:
                        return rendered
                except Exception:
                    pass  # fall through to plain
            return _format_plain(exc_type, exc, user_tb)
        except Exception as e:
            return (
                f"{exc_type.__name__ if exc_type else 'Exception'}: {exc!r}\n"
                f"(daisy failed to format the traceback: {e!r})\n"
            )


def emit_failure(message: str) -> None:
    """Write a fully-formed failure message (e.g. `"block X failed:\\n<tb>"`)
    to whichever destinations the current `log_mode` selects. Bypasses
    `sys.stderr` entirely to keep the file and terminal byte-for-byte
    identical and immune to any host-environment hook that re-renders
    things written to `sys.stderr`.

    The current thread must be inside a `_WorkerLogContext` for the
    file half to fire."""
    if not message.endswith("\n"):
        message += "\n"
    mode = get_log_mode()
    key = getattr(_active, "key", None)
    file_obj = None
    if key is not None and mode in ("file", "both"):
        files = _slot_files.get(key)
        if files is not None:
            file_obj = files[1]   # .err
    if file_obj is not None:
        try:
            file_obj.write(message)
            file_obj.flush()
        except Exception:
            pass
    if mode in ("console", "both"):
        target = _saved_stderr or sys.__stderr__
        if target is not None:
            try:
                target.write(message)
                target.flush()
            except Exception:
                pass


def close_all_log_files():
    """Close every open worker log file. Called at the end of run_blockwise."""
    with _slot_lock:
        for out_f, err_f in _slot_files.values():
            try:
                out_f.close()
            except Exception:
                pass
            try:
                err_f.close()
            except Exception:
                pass
        _slot_files.clear()
        _thread_slots.clear()
