"""funlib.geometry interop (f04).

Two guarantees:
1. Native Roi/Coordinate equality is duck-typed: a funlib.geometry Roi (or
   any object with offset/begin + shape), and any int sequence for
   Coordinate, compare by value instead of silently returning False.
   Unrelated types still compare unequal via NotImplemented, no exception.
2. On the v1-compat surface, 1-arg (block-taking) process functions receive
   compat `Block` views whose ROIs are funlib types — the same class
   spawn-mode workers get from the patched `Client.acquire_block`.

Cross-type *hash* equality is explicitly NOT guaranteed; mixed-type
dict/set keys remain unsupported.
"""

import pytest

fg = pytest.importorskip("funlib.geometry")

import daisy.v2 as d2  # noqa: E402 — importorskip must run first


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
    )
    # the `seen` dict needs in-process execution; the proxy wrapping under
    # test happens at construction, before any worker is involved
    assert compat.run_blockwise([task], multiprocessing=False, progress=False)
    assert seen == {"read_is_funlib": True, "write_is_funlib": True, "eq": True}


@pytest.mark.parametrize("style", ["kwargs", "positional"])
def test_compat_check_fn_receives_funlib_rois_and_skips_done_blocks(tmp_path, style):
    """v1.x check functions do funlib arithmetic on the block's ROIs — the
    same idiom as process functions (volara's resume path). They must get
    the same compat `Block` view: an unwrapped check raises TypeError on
    the native block, the Rust precheck swallows every exception into
    "not done", and every done block silently re-runs on resume."""
    import daisy.v1_compat as compat

    def marker(block):
        # funlib Coordinate arithmetic on the offset — raises TypeError on
        # a native block (daisy._daisy.Coordinate has no arithmetic dunders)
        end = block.write_roi.offset + block.write_roi.shape
        return tmp_path / ("done_" + "_".join(str(c) for c in end))

    def process(block):
        marker(block).touch()

    def check(block):
        return marker(block).exists()

    def make_task():
        if style == "kwargs":
            return compat.Task(
                "compat-check",
                total_roi=fg.Roi((0,), (40,)),
                read_roi=fg.Roi((0,), (10,)),
                write_roi=fg.Roi((0,), (10,)),
                process_function=process,
                check_function=check,
                num_workers=1,
                tracking_path=False,
            )
        # the v1 positional signature: (task_id, total_roi, read_roi,
        # write_roi, process_function, check_function, ...)
        return compat.Task(
            "compat-check",
            d2.Roi((0,), (40,)),
            d2.Roi((0,), (10,)),
            d2.Roi((0,), (10,)),
            process,
            check,
            num_workers=1,
            tracking_path=False,
        )

    # run 1: no markers yet — every block processes and writes its marker
    states = compat.run_blockwise(
        [make_task()], multiprocessing=False, progress=False, return_states=True
    )
    assert states["compat-check"].completed_count == 4
    assert states["compat-check"].skipped_count == 0

    # run 2 (the resume): the check sees every marker → all blocks skipped
    states = compat.run_blockwise(
        [make_task()], multiprocessing=False, progress=False, return_states=True
    )
    assert states["compat-check"].skipped_count == 4


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
