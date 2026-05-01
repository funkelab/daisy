"""Display ordering for tqdm bars and post-run reports.

Roots come first in alphabetical order. After a task is printed,
any of its children whose upstreams are now all printed become
candidates; the alphabetically smallest candidate is printed next.
This matches what a user reading top-to-bottom expects: a parent
is always above its descendants, but independent chains aren't
interleaved gratuitously.
"""

import daisy
from daisy._progress import _topo_order


def _task(task_id, upstream=None):
    return daisy.Task(
        task_id=task_id,
        total_roi=daisy.Roi([0], [10]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        read_write_conflict=False,
        upstream_tasks=upstream,
    )


def test_linear_chain():
    a = _task("A")
    b = _task("B", [a])
    c = _task("C", [b])
    assert _topo_order([a, b, c]) == ["A", "B", "C"]


def test_independent_roots_alphabetical():
    a = _task("A")
    c = _task("C")
    b = _task("B")
    assert _topo_order([c, a, b]) == ["A", "B", "C"]


def test_dfs_bias_keeps_chain_contiguous():
    """B is right after A even though C is also a root, because
    after printing A its only child B is alphabetically smaller
    than the next root C."""
    a = _task("A")
    b = _task("B", [a])
    c = _task("C")
    d = _task("D", [c])
    assert _topo_order([a, b, c, d]) == ["A", "B", "C", "D"]


def test_diamond_with_join():
    """The example from the user's spec:
    `(A → B), (C → D), ((B, D) → E)` should produce `A, B, C, D, E`.
    E doesn't get printed until *both* B and D are seen."""
    a = _task("A")
    b = _task("B", [a])
    c = _task("C")
    d = _task("D", [c])
    e = _task("E", [b, d])
    assert _topo_order([a, b, c, d, e]) == ["A", "B", "C", "D", "E"]


def test_walks_implicit_upstreams():
    """Only the leaf is passed to `_topo_order`; it must walk the
    upstream chain and surface ancestors too."""
    a = _task("A")
    b = _task("B", [a])
    c = _task("C", [b])
    assert _topo_order([c]) == ["A", "B", "C"]
