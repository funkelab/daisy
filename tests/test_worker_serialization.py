"""Subprocess-worker payload serialization (``daisy._worker_processes``).

The regression these tests guard: dill pickles any module it deems "not
builtin" — anything outside sys.prefix / site-packages, e.g. an editable
install or a source-tree checkout — BY VALUE, walking its entire
``__dict__``. One unpicklable module-level object (a ``threading.local``,
a ``struct.Struct``) anywhere in a referenced module's namespace then
fails the whole payload, and a block function only has to reference the
module as a global (``daisy.BlockStatus.SUCCESS``) to drag it in. daisy's
serializer now pickles every importable module by reference; the child
replicates the parent's ``sys.path`` before deserializing, so the import
always resolves.

These tests force the failure mode deterministically with a synthetic
module, so they protect the behaviour regardless of how daisy itself is
installed (the wheel installs used in CI never trip the by-value path
for daisy's own modules).
"""

import io
import pickle
import struct
import subprocess
import sys
import textwrap
import threading
import types

import pytest

import daisy

dill = pytest.importorskip(
    "dill", reason="subprocess-worker payloads use dill when available"
)

from daisy._worker_processes import _serialize, read_payload  # noqa: E402


def _roundtrip(obj):
    return read_payload(io.BytesIO(_serialize(obj)))


@pytest.fixture
def unpicklable_module(monkeypatch):
    """An importable module whose namespace contains the exact kinds of
    objects that broke daisy's own modules, injected into this test
    module's globals as ``unpicklable_mod``."""
    mod = types.ModuleType("_daisy_test_unpicklable_mod")
    mod.local_state = threading.local()  # broke daisy.logging
    mod.wire_format = struct.Struct("<Q")  # broke daisy._worker_processes
    mod.VALUE = 42
    monkeypatch.setitem(sys.modules, mod.__name__, mod)
    monkeypatch.setitem(globals(), "unpicklable_mod", mod)
    return mod


def test_module_with_unpicklable_attrs_ships_by_reference(unpicklable_module):
    def fn():
        return unpicklable_mod.VALUE  # noqa: F821 — injected by the fixture

    fn2, timeout = _roundtrip((fn, None))
    assert timeout is None
    assert fn2() == 42
    # By reference means the very same module object, not a copy.
    assert fn2.__globals__["unpicklable_mod"] is unpicklable_module


def test_function_referencing_daisy_module_global_serializes():
    """The user-facing shape of the regression: a block function that
    reaches through the ``daisy`` global. Must serialize regardless of
    whether daisy is a wheel or an editable/source-tree install."""

    def fn(block):
        block.status = daisy.BlockStatus.SUCCESS

    fn2, timeout = _roundtrip((fn, 1.0))
    assert timeout == 1.0
    assert fn2.__globals__["daisy"] is daisy


def test_unserializable_function_raises_guidance():
    """A payload that genuinely cannot be serialized (unpicklable object
    reachable outside any module namespace) fails eagerly with the
    escape hatches spelled out."""

    def fn(_state=threading.local()):
        return _state

    with pytest.raises(RuntimeError, match="worker_processes=False"):
        _serialize((fn, None))


def test_picklable_local_pickles_as_fresh_empty_local():
    from daisy.logging import _PicklableLocal

    loc = _PicklableLocal()
    loc.key = ("task", 3)
    clone = pickle.loads(pickle.dumps(loc))
    assert isinstance(clone, _PicklableLocal)
    # Per-thread binding state deliberately does not travel.
    assert not hasattr(clone, "key")


def test_main_defined_function_end_to_end(tmp_path):
    """Functions defined in ``__main__`` are the case the by-value path
    (dill's recurse=True globals shipping) exists for: the child's
    __main__ is the worker shim, so the script's globals must ride in
    the payload. Run a real script through subprocess workers."""
    script = tmp_path / "main_fn.py"
    script.write_text(
        textwrap.dedent(
            """
            import daisy

            FACTOR = 2  # __main__ global: must ship by value

            def process(block):
                assert FACTOR == 2
                block.status = daisy.BlockStatus.SUCCESS

            task = daisy.Task(
                task_id="main_fn_e2e",
                total_roi=daisy.Roi((0,), (20,)),
                read_roi=daisy.Roi((0,), (10,)),
                write_roi=daisy.Roi((0,), (10,)),
                process_function=process,
                read_write_conflict=False,
                max_workers=1,
            )
            assert daisy.run_blockwise(
                [task], multiprocessing=True, progress=False
            )
            print("MAIN-FN-E2E-OK")
            """
        )
    )
    res = subprocess.run(
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        timeout=120,
        cwd=tmp_path,
    )
    assert res.returncode == 0, res.stderr
    assert "MAIN-FN-E2E-OK" in res.stdout
