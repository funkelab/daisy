"""Benchmark: dependency graph block iteration speed.

Measures time to iterate through all blocks in a dependency graph for a
volume chunked into ~1M blocks. Compares daisy (Python) vs gerbera (Rust).
"""

import time
import json

# --- Daisy ---
from daisy import BlockwiseDependencyGraph as DaisyGraph
from daisy import Roi as DaisyRoi

# --- Gerbera ---
from gerbera import BlockwiseDependencyGraph as GerberaGraph
from gerbera import Roi as GerberaRoi


def bench_daisy(total_shape, block_shape, context, read_write_conflict):
    total_roi = DaisyRoi((0, 0, 0), total_shape)
    write_roi = DaisyRoi((context, context, context), block_shape)
    read_shape = tuple(b + 2 * context for b in block_shape)
    read_roi = DaisyRoi((0, 0, 0), read_shape)

    t0 = time.perf_counter()
    graph = DaisyGraph(
        "bench", read_roi, write_roi, read_write_conflict, "valid",
        total_read_roi=total_roi,
    )
    t_build = time.perf_counter() - t0

    t0 = time.perf_counter()
    count = 0
    for block, upstream in graph.enumerate_all_dependencies():
        count += 1
    t_iter = time.perf_counter() - t0

    return {
        "blocks": count,
        "levels": graph.num_levels,
        "build_s": t_build,
        "iter_s": t_iter,
        "total_s": t_build + t_iter,
    }


def bench_gerbera(total_shape, block_shape, context, read_write_conflict):
    total_roi = GerberaRoi([0, 0, 0], list(total_shape))
    write_roi = GerberaRoi([context, context, context], list(block_shape))
    read_shape = [b + 2 * context for b in block_shape]
    read_roi = GerberaRoi([0, 0, 0], read_shape)

    t0 = time.perf_counter()
    graph = GerberaGraph(
        "bench", read_roi, write_roi, read_write_conflict, "valid",
        total_read_roi=total_roi,
    )
    t_build = time.perf_counter() - t0

    t0 = time.perf_counter()
    deps = graph.enumerate_all_dependencies()
    count = len(deps)
    t_iter = time.perf_counter() - t0

    return {
        "blocks": count,
        "levels": graph.num_levels,
        "build_s": t_build,
        "iter_s": t_iter,
        "total_s": t_build + t_iter,
    }


def run_benchmarks():
    configs = [
        # (total_shape, block_shape, context, conflict, label)
        ((1000, 1000, 1000), (10, 10, 10), 0, False, "1M blocks, no conflict"),
        ((1000, 1000, 1000), (10, 10, 10), 2, True,  "1M blocks, with conflict"),
        ((200, 200, 200),    (4, 4, 4),     0, False, "125K blocks, small chunks"),
        ((500, 500, 500),    (5, 5, 5),     1, True,  "1M blocks, small context"),
    ]

    results = []
    for total_shape, block_shape, context, conflict, label in configs:
        print(f"\n{'='*60}")
        print(f"  {label}")
        print(f"  total={total_shape} block={block_shape} context={context} conflict={conflict}")
        print(f"{'='*60}")

        # Warmup
        bench_gerbera(total_shape, block_shape, context, conflict)

        # Daisy
        d = bench_daisy(total_shape, block_shape, context, conflict)
        print(f"  daisy:   {d['blocks']:>8} blocks, {d['levels']:>3} levels, "
              f"build={d['build_s']:.4f}s  iter={d['iter_s']:.4f}s  total={d['total_s']:.4f}s")

        # Gerbera
        g = bench_gerbera(total_shape, block_shape, context, conflict)
        print(f"  gerbera: {g['blocks']:>8} blocks, {g['levels']:>3} levels, "
              f"build={g['build_s']:.4f}s  iter={g['iter_s']:.4f}s  total={g['total_s']:.4f}s")

        speedup = d['total_s'] / g['total_s'] if g['total_s'] > 0 else float('inf')
        print(f"  speedup: {speedup:.1f}x")

        results.append({
            "label": label,
            "daisy": d,
            "gerbera": g,
            "speedup": speedup,
        })

    # Save for plotting
    with open("benchmarks/dep_graph_results.json", "w") as f:
        json.dump(results, f, indent=2)

    return results


def plot_results(results):
    import matplotlib.pyplot as plt

    labels = [r["label"] for r in results]
    daisy_times = [r["daisy"]["total_s"] for r in results]
    gerbera_times = [r["gerbera"]["total_s"] for r in results]

    x = range(len(labels))
    width = 0.35

    fig, ax = plt.subplots(figsize=(12, 6))
    bars1 = ax.bar([i - width/2 for i in x], daisy_times, width, label="daisy (Python)", color="#4878CF")
    bars2 = ax.bar([i + width/2 for i in x], gerbera_times, width, label="gerbera (Rust)", color="#D65F5F")

    for bar, t in zip(bars1, daisy_times):
        ax.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 0.01,
                f'{t:.3f}s', ha='center', va='bottom', fontsize=9)
    for bar, t in zip(bars2, gerbera_times):
        ax.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 0.01,
                f'{t:.3f}s', ha='center', va='bottom', fontsize=9)

    ax.set_ylabel("Time (seconds)")
    ax.set_title("Dependency Graph: Build + Iterate All Blocks")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=15, ha='right')
    ax.legend()

    for i, r in enumerate(results):
        ax.text(i, max(daisy_times[i], gerbera_times[i]) * 1.15,
                f'{r["speedup"]:.1f}x', ha='center', fontsize=11, fontweight='bold')

    plt.tight_layout()
    plt.savefig("benchmarks/dep_graph_benchmark.png", dpi=150)
    print("\nSaved benchmarks/dep_graph_benchmark.png")


if __name__ == "__main__":
    results = run_benchmarks()
    plot_results(results)
