"""Display ordering for tqdm bars and post-run reports.

Roots come first in alphabetical order. After a task is printed,
any of its children whose upstreams are now all printed become
candidates; the alphabetically smallest candidate is printed next.
This matches what a user reading top-to-bottom expects: a parent
is always above its descendants, but independent chains aren't
interleaved gratuitously.
"""

import daisy
from daisy._daisy import _topo_order


def _task(task_id):
    return daisy.Task(
        task_id=task_id,
        total_roi=daisy.Roi([0], [10]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        read_write_conflict=False,
    )


def test_linear_chain():
    a, b, c = _task("A"), _task("B"), _task("C")
    assert _topo_order(a + b + c) == ["A", "B", "C"]


def test_independent_roots_alphabetical():
    a, b, c = _task("A"), _task("B"), _task("C")
    assert _topo_order(c | a | b) == ["A", "B", "C"]


def test_dfs_bias_keeps_chain_contiguous():
    """B is right after A even though C is also a root, because
    after printing A its only child B is alphabetically smaller
    than the next root C."""
    a, b, c, d = _task("A"), _task("B"), _task("C"), _task("D")
    assert _topo_order((a + b) | (c + d)) == ["A", "B", "C", "D"]


def test_diamond_with_join():
    """`(A → B), (C → D), ((B, D) → E)` should produce `A, B, C, D, E`.
    E doesn't get printed until *both* B and D are seen."""
    a, b, c, d, e = _task("A"), _task("B"), _task("C"), _task("D"), _task("E")
    assert _topo_order(((a + b) | (c + d)) + e) == ["A", "B", "C", "D", "E"]
