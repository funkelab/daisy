"""Display ordering for tqdm bars and post-run reports.

Roots come first in alphabetical order. After a task is printed,
any of its children whose upstreams are now all printed become
candidates; the alphabetically smallest candidate is printed next.
This matches what a user reading top-to-bottom expects: a parent
is always above its descendants, but independent chains aren't
interleaved gratuitously.
"""

import re

import daisy
import pytest
from conftest import linear_task


def _task(task_id):
    return linear_task(
        task_id,
        lambda block: None,
        1,
    )


@pytest.fixture
def summary_order(capsys):
    """Run a pipeline and return its task ids in the order the execution
    summary lists them."""

    def run(pipeline):
        assert daisy.run_blockwise(pipeline, multiprocessing=False, progress=False)
        out = capsys.readouterr().out
        return re.findall(r"^    (\w+) ✔", out, flags=re.MULTILINE)

    return run


def test_linear_chain(summary_order):
    a, b, c = _task("A"), _task("B"), _task("C")
    assert summary_order(a + b + c) == ["A", "B", "C"]


def test_independent_roots_alphabetical(summary_order):
    a, b, c = _task("A"), _task("B"), _task("C")
    assert summary_order(c | a | b) == ["A", "B", "C"]


def test_dfs_bias_keeps_chain_contiguous(summary_order):
    """B is right after A even though C is also a root, because
    after printing A its only child B is alphabetically smaller
    than the next root C."""
    a, b, c, d = _task("A"), _task("B"), _task("C"), _task("D")
    assert summary_order((a + b) | (c + d)) == ["A", "B", "C", "D"]


def test_diamond_with_join(summary_order):
    """`(A → B), (C → D), ((B, D) → E)` should produce `A, B, C, D, E`.
    E doesn't get printed until *both* B and D are seen."""
    a, b, c, d, e = _task("A"), _task("B"), _task("C"), _task("D"), _task("E")
    assert summary_order(((a + b) | (c + d)) + e) == ["A", "B", "C", "D", "E"]
