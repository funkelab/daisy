"""Benchmark: worker coordination scaling.

Measures total wall time to distribute and process blocks across varying
numbers of workers. The blocks do no work (the callback returns
immediately), so what is left is coordination overhead: server dispatch, TCP
round-trips, block lifecycle, and worker process startup.

Compares the two `process_function` modalities daisy v2 supports. Both run
in a dedicated worker subprocess (`python -m daisy._subprocess_worker`) --
that is the only execution model on the distributed path -- and differ only
in who drives the block loop:

    - block-fn:  `f(block)`, one arg. The subprocess runs daisy's own
                 `Client.acquire_block()` loop and calls `f` per block.
    - worker-fn: `f()`, no args. The subprocess just calls `f`, which opens
                 its own `daisy.Client()` and drives the loop in Python.

Both configurations use the distributed `Server` at every worker count,
including one, so the curve stays within a single execution model.
`progress=False` keeps tqdm rendering out of the timed region -- it is not
coordination overhead. Timings are machine-specific; compare shapes, not
absolute seconds.

Run from the repository root:

    python benchmarks/bench_worker_scaling.py

Writes `benchmarks/worker_scaling_results.json`,
`benchmarks/block_scaling_results.json` and
`benchmarks/worker_scaling_benchmark.png`.
"""

import json
import time

import daisy


def _task(num_blocks, num_workers, process_function):
    total_size = num_blocks * 10
    return daisy.Task(
        "bench",
        total_roi=daisy.Roi([0], [total_size]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=process_function,
        read_write_conflict=False,
        max_workers=num_workers,
        max_retries=0,
    )


def _run(task):
    """Time one distributed run and report whether every block actually
    succeeded. `TaskState.is_done()` only means the counters balance -- a run
    in which every block *failed* is also "done" -- so success is checked
    against `completed_count`, otherwise a broken run reports a fast time."""
    t0 = time.perf_counter()
    states = daisy.Server().run_blockwise([task], progress=False)
    elapsed = time.perf_counter() - t0

    state = states[task.task_id]
    completed, total = state.completed_count, state.total_block_count
    return elapsed, completed == total, completed


# --- block-function modality -------------------------------------------
# process_function takes a Block; the worker subprocess drives
# acquire/release over TCP and invokes this callback once per block.
def bench_block_fn(num_blocks, num_workers):
    def noop(block):
        pass

    return _run(_task(num_blocks, num_workers, noop))


# --- worker-function modality ------------------------------------------
# process_function takes no args; the worker subprocess calls it once and it
# opens a Client and runs the acquire/release loop itself.
def bench_worker_fn(num_blocks, num_workers):
    def worker():
        client = daisy.Client()
        while True:
            with client.acquire_block() as block:
                if block is None:
                    break

    return _run(_task(num_blocks, num_workers, worker))


def run_scaling():
    num_blocks = 10_000  # Reduced from 1M for reasonable runtime with TCP overhead
    worker_counts = [1, 2, 4, 8, 16, 32]

    print(f"Blocks: {num_blocks}")
    print(f"Worker counts: {worker_counts}")
    header = f"{'workers':>8} | {'block-fn':>10} | {'worker-fn':>11} | {'ratio':>8}"
    print(header)
    print("-" * len(header))

    results = []
    for nw in worker_counts:
        b_time, b_ok, b_count = bench_block_fn(num_blocks, nw)
        assert b_ok, f"block-fn incomplete: {b_count}/{num_blocks}"

        w_time, w_ok, w_count = bench_worker_fn(num_blocks, nw)
        assert w_ok, f"worker-fn incomplete: {w_count}/{num_blocks}"

        speedup = b_time / w_time if w_time > 0 else float("inf")
        print(f"{nw:>8} | {b_time:>9.3f}s | {w_time:>10.3f}s | {speedup:>7.2f}x")

        results.append(
            {
                "workers": nw,
                "blocks": num_blocks,
                "block_fn_s": b_time,
                "worker_fn_s": w_time,
                "worker_fn_speedup": speedup,
            }
        )

    with open("benchmarks/worker_scaling_results.json", "w") as f:
        json.dump(results, f, indent=2)

    return results


def run_block_scaling():
    """Scale number of blocks with fixed worker count."""
    block_counts = [100, 1_000, 10_000, 100_000]
    num_workers = 4

    print(f"\nBlock scaling (workers={num_workers})")
    header = f"{'blocks':>8} | {'block-fn':>10} | {'worker-fn':>11} | {'ratio':>8}"
    print(header)
    print("-" * len(header))

    results = []
    for nb in block_counts:
        b_time, b_ok, b_count = bench_block_fn(nb, num_workers)
        assert b_ok, f"block-fn incomplete: {b_count}/{nb}"

        w_time, w_ok, w_count = bench_worker_fn(nb, num_workers)
        assert w_ok, f"worker-fn incomplete: {w_count}/{nb}"

        speedup = b_time / w_time if w_time > 0 else float("inf")
        print(f"{nb:>8} | {b_time:>9.3f}s | {w_time:>10.3f}s | {speedup:>7.2f}x")

        results.append(
            {
                "blocks": nb,
                "workers": num_workers,
                "block_fn_s": b_time,
                "worker_fn_s": w_time,
                "worker_fn_speedup": speedup,
            }
        )

    with open("benchmarks/block_scaling_results.json", "w") as f:
        json.dump(results, f, indent=2)

    return results


def plot_results(worker_results, block_results):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(1, 3, figsize=(18, 6))

    BLOCK_FN_COLOR = "#D65F5F"
    WORKER_FN_COLOR = "#956CB4"

    # --- Plot 1: Worker scaling (absolute time) ---
    ax = axes[0]
    workers = [r["workers"] for r in worker_results]
    b_times = [r["block_fn_s"] for r in worker_results]
    w_times = [r["worker_fn_s"] for r in worker_results]

    ax.plot(workers, b_times, "s-", label="block-fn", color=BLOCK_FN_COLOR, linewidth=2)
    ax.plot(
        workers, w_times, "^-", label="worker-fn", color=WORKER_FN_COLOR, linewidth=2
    )
    ax.set_xlabel("Number of workers")
    ax.set_ylabel("Time (seconds)")
    ax.set_title(f"Worker Scaling ({worker_results[0]['blocks']} blocks)")
    ax.legend()
    ax.set_xscale("log", base=2)
    ax.set_xticks(workers)
    ax.set_xticklabels(workers)

    # --- Plot 2: worker-fn relative to block-fn ---
    ax = axes[1]
    speedups = [r["worker_fn_speedup"] for r in worker_results]
    x = range(len(workers))
    ax.bar(x, speedups, 0.5, color=WORKER_FN_COLOR, edgecolor="black")
    ax.set_xlabel("Number of workers")
    ax.set_ylabel("block-fn time / worker-fn time")
    ax.set_title("worker-fn relative to block-fn")
    ax.set_xticks(list(x))
    ax.set_xticklabels(workers)
    ax.axhline(y=1, color="gray", linestyle="--", alpha=0.5)
    for i, s in enumerate(speedups):
        ax.text(i, s, f"{s:.2f}x", ha="center", va="bottom", fontsize=9)

    # --- Plot 3: Block scaling ---
    ax = axes[2]
    blocks = [r["blocks"] for r in block_results]
    b_times = [r["block_fn_s"] for r in block_results]
    w_times = [r["worker_fn_s"] for r in block_results]

    ax.plot(blocks, b_times, "s-", label="block-fn", color=BLOCK_FN_COLOR, linewidth=2)
    ax.plot(
        blocks, w_times, "^-", label="worker-fn", color=WORKER_FN_COLOR, linewidth=2
    )
    ax.set_xlabel("Number of blocks")
    ax.set_ylabel("Time (seconds)")
    ax.set_title(f"Block Scaling ({block_results[0]['workers']} workers)")
    ax.legend()
    ax.set_xscale("log")
    ax.set_yscale("log")

    plt.tight_layout()
    plt.savefig("benchmarks/worker_scaling_benchmark.png", dpi=150)
    print("\nSaved benchmarks/worker_scaling_benchmark.png")


if __name__ == "__main__":
    worker_results = run_scaling()
    block_results = run_block_scaling()
    plot_results(worker_results, block_results)
