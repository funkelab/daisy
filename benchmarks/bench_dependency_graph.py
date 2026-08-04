"""Benchmark: dependency graph construction and full enumeration.

For a volume chunked into 125K-1M blocks, measures how long daisy takes to
build a `BlockwiseDependencyGraph` and then enumerate every block together
with its upstream dependencies.

This is a *scaling* benchmark of a single implementation, not a comparison.
It answers "how does graph cost grow with block count, conflict levels and
context", which is what decides whether a chunking is viable at all.
Construction is lazy, so `build_s` is expected to be ~0 and essentially all
the cost lands in `iter_s`.

Run from the repository root:

    python benchmarks/bench_dependency_graph.py

Writes `benchmarks/dep_graph_results.json` and
`benchmarks/dep_graph_benchmark.png`.
"""

import json
import time

from daisy import BlockwiseDependencyGraph, Roi

CONFIGS = [
    # (total_shape, block_shape, context, read_write_conflict, label)
    ((1000, 1000, 1000), (10, 10, 10), 0, False, "1M blocks, no conflict"),
    ((1000, 1000, 1000), (10, 10, 10), 2, True, "1M blocks, with conflict"),
    ((200, 200, 200), (4, 4, 4), 0, False, "125K blocks, small chunks"),
    ((500, 500, 500), (5, 5, 5), 1, True, "1M blocks, small context"),
]


def bench_dep_graph(total_shape, block_shape, context, read_write_conflict):
    total_roi = Roi([0, 0, 0], list(total_shape))
    write_roi = Roi([context] * len(block_shape), list(block_shape))
    read_shape = [b + 2 * context for b in block_shape]
    read_roi = Roi([0] * len(read_shape), read_shape)

    t0 = time.perf_counter()
    graph = BlockwiseDependencyGraph(
        "bench",
        read_roi,
        write_roi,
        read_write_conflict,
        "valid",
        total_read_roi=total_roi,
    )
    t_build = time.perf_counter() - t0

    t0 = time.perf_counter()
    deps = graph.enumerate_all_dependencies()
    count = len(deps)
    t_iter = time.perf_counter() - t0

    total_s = t_build + t_iter
    return {
        "blocks": count,
        "levels": graph.num_levels,
        "build_s": t_build,
        "iter_s": t_iter,
        "total_s": total_s,
        "blocks_per_s": count / total_s if total_s > 0 else float("inf"),
    }


def run_benchmarks():
    results = []
    for total_shape, block_shape, context, conflict, label in CONFIGS:
        print(f"\n{'=' * 60}")
        print(f"  {label}")
        print(
            f"  total={total_shape} block={block_shape} "
            f"context={context} conflict={conflict}"
        )
        print(f"{'=' * 60}")

        # Warmup, so the first configuration does not pay for one-off
        # allocator growth that the others don't.
        bench_dep_graph(total_shape, block_shape, context, conflict)

        r = bench_dep_graph(total_shape, block_shape, context, conflict)
        print(
            f"  {r['blocks']:>8} blocks, {r['levels']:>3} levels, "
            f"build={r['build_s']:.4f}s  iter={r['iter_s']:.4f}s  "
            f"total={r['total_s']:.4f}s  ({r['blocks_per_s']:,.0f} blocks/s)"
        )

        results.append({"label": label, **r})

    with open("benchmarks/dep_graph_results.json", "w") as f:
        json.dump(results, f, indent=2)

    return results


def plot_results(results):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    labels = [r["label"] for r in results]
    totals = [r["total_s"] for r in results]
    x = range(len(labels))

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(x, totals, 0.6, color="#4878CF", edgecolor="black")

    for bar, r in zip(bars, results):
        ax.text(
            bar.get_x() + bar.get_width() / 2.0,
            bar.get_height(),
            f"{r['total_s']:.3f}s\n{r['blocks_per_s'] / 1e6:.2f}M blocks/s",
            ha="center",
            va="bottom",
            fontsize=9,
        )

    ax.set_ylabel("Time (seconds)")
    ax.set_title("Dependency Graph: Build + Enumerate All Dependencies")
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, rotation=15, ha="right")
    ax.set_ylim(0, max(totals) * 1.25)

    plt.tight_layout()
    plt.savefig("benchmarks/dep_graph_benchmark.png", dpi=150)
    print("\nSaved benchmarks/dep_graph_benchmark.png")


if __name__ == "__main__":
    plot_results(run_benchmarks())
