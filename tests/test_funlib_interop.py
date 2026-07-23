"""funlib.geometry interop (f04).

Two guarantees:
1. Native Roi/Coordinate equality is duck-typed: a funlib.geometry Roi (or
   any object with offset/begin + shape), and any int sequence for
   Coordinate, compare by value instead of silently returning False.
   Unrelated types still compare unequal via NotImplemented, no exception.
2. On the v1-compat surface, 1-arg (block-taking) process functions receive
   `_BlockProxy`-wrapped blocks whose ROIs are funlib types — the same view
   spawn-mode workers get from the patched `Client.acquire_block`.

Cross-type *hash* equality is explicitly NOT guaranteed; mixed-type
dict/set keys remain unsupported.
"""

import pytest

fg = pytest.importorskip("funlib.geometry")

import daisy.v2 as d2


def test_native_roi_eq_funlib_roi():
    assert d2.Roi((0, 5), (10, 20)) == fg.Roi((0, 5), (10, 20))
    assert fg.Roi((0, 5), (10, 20)) == d2.Roi((0, 5), (10, 20))
    assert d2.Roi((0,), (10,)) != fg.Roi((0,), (11,))
    assert d2.Roi((0,), (10,)) != fg.Roi((1,), (10,))


def test_native_roi_eq_unrelated_types():
    r = d2.Roi((0,), (10,))
    assert r != "hello"
    assert r != 42
    assert r != (0, 10)


def test_native_native_eq_and_hash_unchanged():
    a, b = d2.Roi((0,), (10,)), d2.Roi((0,), (10,))
    assert a == b and hash(a) == hash(b)
    assert d2.Roi((0,), (10,)) != d2.Roi((0,), (11,))
    ca, cb = d2.Coordinate((1, 2)), d2.Coordinate((1, 2))
    assert ca == cb and hash(ca) == hash(cb)


def test_native_coordinate_duck_eq():
    c = d2.Coordinate((3, 4))
    assert c == fg.Coordinate((3, 4))
    assert fg.Coordinate((3, 4)) == c
    assert c == (3, 4)
    assert c == [3, 4]
    assert c != (3, 5)
    assert c != (3,)
    assert c != "xy"


def test_cross_type_intersect_error_names_types():
    with pytest.raises(TypeError, match="funlib.geometry"):
        d2.Roi((0,), (10,)).intersect(fg.Roi((0,), (5,)))


def test_compat_block_fn_receives_funlib_rois():
    import daisy.v1_compat as compat

    seen = {}

    def process(block):
        seen["read_is_funlib"] = isinstance(block.read_roi, fg.Roi)
        seen["write_is_funlib"] = isinstance(block.write_roi, fg.Roi)
        seen["eq"] = block.read_roi == fg.Roi((0,), (10,))

    task = compat.Task(
        "compat-proxy",
        total_roi=fg.Roi((0,), (10,)),  # single block: seen[] is unambiguous
        read_roi=fg.Roi((0,), (10,)),
        write_roi=fg.Roi((0,), (10,)),
        process_function=process,
        num_workers=1,
        done_marker_path=False,
        # the `seen` dict instrumentation needs in-process workers; the
        # proxy wrapping under test happens before worker-mode dispatch
        worker_processes=False,
    )
    assert compat.run_blockwise([task], progress=False)
    assert seen == {"read_is_funlib": True, "write_is_funlib": True, "eq": True}


def test_compat_spawn_fn_not_wrapped():
    """0-arg spawn functions must pass through unwrapped (arity preserved)."""
    from daisy.v1_compat import _wrap_block_fn

    def spawn():
        pass

    assert _wrap_block_fn(spawn) is spawn
    assert _wrap_block_fn(None) is None

    def block_fn(block):
        pass

    wrapped = _wrap_block_fn(block_fn)
    assert wrapped is not block_fn
    import inspect

    assert len(inspect.getfullargspec(wrapped).args) == 1
