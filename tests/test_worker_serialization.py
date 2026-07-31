"""Serialization of block functions for subprocess workers (cloudpickle).

The contract: functions are shipped by value (lambdas, closures, and
functions defined in ``__main__`` all work), while modules the function
references as globals are shipped by *reference* — workers replicate the
parent's ``sys.path`` and re-import, exactly as a remote cluster worker
would. That split is what keeps an unpicklable object sitting in some
unrelated corner of your project package from failing the payload.
"""

import subprocess
import sys
import textwrap
import threading

import pytest

cloudpickle = pytest.importorskip("cloudpickle")

from daisy._worker_processes import _serialize, make_spawn_function  # noqa: E402


def _write_project_package(tmp_path):
    """A user project package holding an unpicklable module-level object —
    the shape that fails when a serializer ships modules by value."""
    pkg = tmp_path / "userpkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text(
        textwrap.dedent("""
        import struct, threading
        _CODEC = struct.Struct("<Q")      # unpicklable
        _TLS = threading.local()          # unpicklable
        SCALE = 3

        def helper(x):
            return x * SCALE
        """)
    )
    return pkg


def test_module_reference_does_not_drag_in_unpicklable_members(tmp_path, monkeypatch):
    """A block function referencing the module object (``userpkg.helper()``)
    must serialize even though the package holds unpicklable module-level
    objects it never touches."""
    _write_project_package(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    userpkg = pytest.importorskip("userpkg")

    def process(block):
        return userpkg.helper(1)

    payload = _serialize((process, None))
    assert payload  # by-reference module: no unpicklable member in the graph


def test_module_is_shipped_by_reference_not_by_value(tmp_path, monkeypatch):
    """Confirm the mechanism: the payload needs the module importable, i.e.
    it carries a reference rather than a copy of the namespace."""
    _write_project_package(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    userpkg = pytest.importorskip("userpkg")

    def process(block):
        return userpkg.helper(1)

    body = cloudpickle.dumps(process)
    code = (
        "import sys, cloudpickle; "
        f"sys.path = [p for p in sys.path if p != {str(tmp_path)!r}]; "
        f"cloudpickle.loads({body!r})"
    )
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert r.returncode != 0
    assert "ModuleNotFoundError" in r.stderr


def test_lambdas_and_closures_are_shipped_by_value():
    scale = 7
    payload = _serialize((lambda block: scale * 2, None))
    assert payload


def test_main_defined_function_round_trips(tmp_path):
    """Functions defined in a script's ``__main__`` must survive: they are
    exactly what cannot be shipped by reference."""
    script = tmp_path / "driver.py"
    script.write_text(
        textwrap.dedent("""
        from daisy._worker_processes import _serialize
        import io
        from daisy._worker_processes import read_payload

        def process(block):
            return "from __main__"

        payload = _serialize((process, None))
        fn, timeout = read_payload(io.BytesIO(payload))
        assert fn(None) == "from __main__", fn(None)
        print("OK")
        """)
    )
    r = subprocess.run(
        [sys.executable, str(script)], capture_output=True, text=True, cwd=tmp_path
    )
    assert r.returncode == 0, r.stderr
    assert "OK" in r.stdout


def test_captured_lock_fails_with_actionable_guidance():
    """Locks (and friends) cannot cross a process boundary. cloudpickle
    refuses them; daisy must explain the ways out rather than surface a bare
    pickling error. NOTE: dill would have accepted these silently, shipping
    a lock that synchronizes nothing."""
    lock = threading.Lock()

    def process(block):
        with lock:
            pass

    with pytest.raises(RuntimeError) as excinfo:
        _serialize((process, None))
    msg = str(excinfo.value)
    # the remedies, in the order a user should try them
    assert "module level" in msg
    assert "inside the function" in msg
    assert "multiprocessing=False" in msg


def test_bound_method_holding_a_lock_fails_with_guidance():
    """The same failure arrives indirectly when the block function is a
    bound method and ``self`` holds a lock — the realistic migration case."""

    class Writer:
        def __init__(self):
            self._lock = threading.Lock()

        def process_block(self, block):
            with self._lock:
                pass

    with pytest.raises(RuntimeError, match="could not serialize"):
        _serialize((Writer().process_block, None))


@pytest.mark.parametrize("arity", [0, 1], ids=["worker_function", "block_function"])
def test_spawn_function_serializes_eagerly(arity):
    """Unserializable functions must fail when the spawn function is built,
    not minutes later on a cluster node — for worker functions as well as
    block functions, since both are now shipped to a subprocess."""
    lock = threading.Lock()

    with pytest.raises(RuntimeError, match="cannot be pickled|could not serialize"):
        make_spawn_function(lambda block: lock, arity=arity, timeout=None)
