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

import daisy  # noqa: E402


def _task(task_id, process_function):
    return daisy.Task(
        task_id=task_id,
        total_roi=daisy.Roi([0], [10]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=process_function,
        read_write_conflict=False,
        max_workers=1,
    )


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
    runs in a worker even though the package holds unpicklable module-level
    objects it never touches."""
    _write_project_package(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    userpkg = pytest.importorskip("userpkg")
    result = tmp_path / "result"

    def process(block):
        result.write_text(str(userpkg.helper(1)))

    assert daisy.run_blockwise(_task("by-reference", process), progress=False)
    assert result.read_text() == "3"


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
    assert r.returncode == 1
    assert "ModuleNotFoundError" in r.stderr


def test_main_globals_reach_real_workers(tmp_path):
    """End to end, not just a round trip: a script whose block function reads a
    ``__main__`` module global, executed by actual subprocess workers.

    ``__main__`` is the one namespace that *must* travel by value — the child's
    ``__main__`` is the worker shim, so a reference would resolve to the wrong
    module. The in-process round-trip test above cannot catch a break here,
    because in that process ``__main__`` is still the script. Salvaged from
    funkelab/daisy#73.
    """
    script = tmp_path / "main_globals.py"
    script.write_text(
        textwrap.dedent("""
        import daisy

        FACTOR = 2  # a __main__ global the worker must see

        def process(block):
            # Fails the block (and so the run) if the global did not travel.
            assert FACTOR == 2, f"FACTOR came across as {FACTOR!r}"

        task = daisy.Task(
            task_id="main-globals",
            total_roi=daisy.Roi((0,), (20,)),
            read_roi=daisy.Roi((0,), (10,)),
            write_roi=daisy.Roi((0,), (10,)),
            process_function=process,
            read_write_conflict=False,
            max_workers=1,
        )
        assert daisy.run_blockwise(task, progress=False)
        print("MAIN-GLOBALS-OK")
        """)
    )
    r = subprocess.run(
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        timeout=120,
        cwd=tmp_path,
    )
    assert r.returncode == 0, r.stderr
    assert "MAIN-GLOBALS-OK" in r.stdout


def _closure_over_a_lock():
    lock = threading.Lock()

    def process(block):
        with lock:
            pass

    return process


def _bound_method_of_an_object_holding_a_lock():
    class Writer:
        def __init__(self):
            self._lock = threading.Lock()

        def process_block(self, block):
            with self._lock:
                pass

    return Writer().process_block


def _worker_function_closing_over_a_lock():
    lock = threading.Lock()

    def worker():
        with lock:
            pass

    return worker


@pytest.mark.parametrize(
    "make_function",
    [
        _closure_over_a_lock,
        _bound_method_of_an_object_holding_a_lock,
        _worker_function_closing_over_a_lock,
    ],
)
def test_unserializable_function_fails_the_run_with_guidance(make_function):
    """Locks (and friends) cannot cross a process boundary. cloudpickle
    refuses them; daisy explains the ways out rather than surfacing a bare
    pickling error, for block functions and worker functions alike."""
    with pytest.raises(RuntimeError, match="could not serialize") as excinfo:
        daisy.run_blockwise(_task("unpicklable", make_function()), progress=False)
    msg = str(excinfo.value)
    # the remedies, in the order a user should try them
    assert "module level" in msg
    assert "inside the function" in msg
    assert "multiprocessing=False" in msg
