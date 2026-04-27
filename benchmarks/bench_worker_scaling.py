"""Benchmark: worker coordination scaling.

Measures total time to distribute and process blocks across varying numbers
of workers. Workers do trivial work (immediately return) to isolate the
coordination overhead: server dispatch, TCP round-trips, block lifecycle.

Compares three configurations:
    - daisy                (Python server + tornado TCP, block-function mode)
    - daisy block-fn     (Rust server + tokio TCP, Rust-driven worker loop)
    - daisy worker-fn    (Rust server + tokio TCP, Python-driven worker loop
                            via `daisy.Client`)
"""

import time
import json

# Ensure both packages are importable
import daisy
import daisy


# --- Daisy benchmark ---
def bench_daisy(num_blocks, num_workers):
    def noop(block):
        pass

    total_size = num_blocks * 10
    task = daisy.Task(
        "bench",
        total_roi=daisy.Roi((0,), (total_size,)),
        read_roi=daisy.Roi((0,), (10,)),
        write_roi=daisy.Roi((0,), (10,)),
        process_function=noop,
        read_write_conflict=False,
        num_workers=num_workers,
        max_retries=0,
    )

    server = daisy.SerialServer() if num_workers <= 1 else daisy.Server()

    t0 = time.perf_counter()
    states = server.run_blockwise([task])
    elapsed = time.perf_counter() - t0

    done = states[task.task_id].is_done()
    completed = states[task.task_id].completed_count
    return elapsed, done, completed


# --- Daisy block-function benchmark ---
# process_function takes a Block; the Rust worker loop drives acquire/release
# over TCP and only takes the GIL to invoke this callback.
def bench_daisy(num_blocks, num_workers):
    def noop(block):
        pass

    total_size = num_blocks * 10
    task = daisy.Task(
        "bench",
        total_roi=daisy.Roi([0], [total_size]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=noop,
        read_write_conflict=False,
        num_workers=num_workers,
        max_retries=0,
    )

    if num_workers <= 1:
        server = daisy.SerialServer()
    else:
        server = daisy.Server()

    t0 = time.perf_counter()
    states = server.run_blockwise([task])
    elapsed = time.perf_counter() - t0

    done = states[task.task_id].is_done()
    completed = states[task.task_id].completed_count
    return elapsed, done, completed


# --- Daisy worker-function benchmark ---
# process_function takes no args; each worker thread spawns a Client and
# runs the acquire/release loop in Python. SerialServer does not support
# this modality, so we always use the distributed Server even at
# num_workers=1.
def bench_daisy_worker_fn(num_blocks, num_workers):
    def worker():
        client = daisy.Client()
        while True:
            with client.acquire_block() as block:
                if block is None:
                    break

    total_size = num_blocks * 10
    task = daisy.Task(
        "bench",
        total_roi=daisy.Roi([0], [total_size]),
        read_roi=daisy.Roi([0], [10]),
        write_roi=daisy.Roi([0], [10]),
        process_function=worker,
        read_write_conflict=False,
        num_workers=num_workers,
        max_retries=0,
    )

    t0 = time.perf_counter()
    states = daisy.Server().run_blockwise([task])
    elapsed = time.perf_counter() - t0

    done = states[task.task_id].is_done()
    completed = states[task.task_id].completed_count
    return elapsed, done, completed


def run_scaling():
    num_blocks = 10_000  # Reduced from 1M for reasonable runtime with TCP overhead
    worker_counts = [1, 2, 4, 8, 16, 32]

    print(f"Blocks: {num_blocks}")
    print(f"Worker counts: {worker_counts}")
    header = (f"{'workers':>8} | {'daisy':>10} | {'daisy':>10} | "
              f"{'daisy-wf':>11} | {'speedup':>8}")
    print(header)
    print("-" * len(header))

    results = []
    for nw in worker_counts:
        g_time, g_done, g_count = bench_daisy(num_blocks, nw)
        assert g_done, f"daisy failed: {g_count}/{num_blocks}"

        w_time, w_done, w_count = bench_daisy_worker_fn(num_blocks, nw)
        assert w_done, f"daisy worker-fn failed: {w_count}/{num_blocks}"

        d_time, d_done, d_count = bench_daisy(num_blocks, nw)
        assert d_done, f"daisy failed: {d_count}/{num_blocks}"

        speedup = d_time / g_time if g_time > 0 else float('inf')
        print(f"{nw:>8} | {d_time:>9.3f}s | {g_time:>9.3f}s | "
              f"{w_time:>10.3f}s | {speedup:>7.1f}x")

        results.append({
            "workers": nw,
            "blocks": num_blocks,
            "daisy_s": d_time,
            "daisy_s": g_time,
            "daisy_worker_s": w_time,
            "speedup": speedup,
        })

    with open("benchmarks/worker_scaling_results.json", "w") as f:
        json.dump(results, f, indent=2)

    return results


def run_block_scaling():
    """Scale number of blocks with fixed worker count."""
    block_counts = [100, 1_000, 10_000, 100_000]
    num_workers = 4

    print(f"\nBlock scaling (workers={num_workers})")
    header = (f"{'blocks':>8} | {'daisy':>10} | {'daisy':>10} | "
              f"{'daisy-wf':>11} | {'speedup':>8}")
    print(header)
    print("-" * len(header))

    results = []
    for nb in block_counts:
        g_time, g_done, _ = bench_daisy(nb, num_workers)
        w_time, w_done, _ = bench_daisy_worker_fn(nb, num_workers)
        d_time, d_done, _ = bench_daisy(nb, num_workers)

        speedup = d_time / g_time if g_time > 0 else float('inf')
        print(f"{nb:>8} | {d_time:>9.3f}s | {g_time:>9.3f}s | "
              f"{w_time:>10.3f}s | {speedup:>7.1f}x")

        results.append({
            "blocks": nb,
            "workers": num_workers,
            "daisy_s": d_time,
            "daisy_s": g_time,
            "daisy_worker_s": w_time,
            "speedup": speedup,
        })

    with open("benchmarks/block_scaling_results.json", "w") as f:
        json.dump(results, f, indent=2)

    return results


def plot_results(worker_results, block_results):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(1, 3, figsize=(18, 6))

    DAISY_COLOR = "#4878CF"
    DAISY_BLOCK_COLOR = "#D65F5F"
    DAISY_WORKER_COLOR = "#956CB4"

    # --- Plot 1: Worker scaling (absolute time) ---
    ax = axes[0]
    workers = [r["workers"] for r in worker_results]
    d_times = [r["daisy_s"] for r in worker_results]
    g_times = [r["daisy_s"] for r in worker_results]
    w_times = [r["daisy_worker_s"] for r in worker_results]

    ax.plot(workers, d_times, 'o-', label="daisy", color=DAISY_COLOR, linewidth=2)
    ax.plot(workers, g_times, 's-', label="daisy block-fn",
            color=DAISY_BLOCK_COLOR, linewidth=2)
    ax.plot(workers, w_times, '^-', label="daisy worker-fn",
            color=DAISY_WORKER_COLOR, linewidth=2)
    ax.set_xlabel("Number of workers")
    ax.set_ylabel("Time (seconds)")
    ax.set_title(f"Worker Scaling ({worker_results[0]['blocks']} blocks)")
    ax.legend()
    ax.set_xscale('log', base=2)
    ax.set_xticks(workers)
    ax.set_xticklabels(workers)

    # --- Plot 2: Speedup vs workers (both daisy variants over daisy) ---
    ax = axes[1]
    g_speedups = [r["daisy_s"] / r["daisy_s"] if r["daisy_s"] > 0 else 0
                  for r in worker_results]
    w_speedups = [r["daisy_s"] / r["daisy_worker_s"] if r["daisy_worker_s"] > 0 else 0
                  for r in worker_results]
    x = range(len(workers))
    width = 0.4
    ax.bar([i - width / 2 for i in x], g_speedups, width,
           label="daisy block-fn", color=DAISY_BLOCK_COLOR, edgecolor="black")
    ax.bar([i + width / 2 for i in x], w_speedups, width,
           label="daisy worker-fn", color=DAISY_WORKER_COLOR, edgecolor="black")
    ax.set_xlabel("Number of workers")
    ax.set_ylabel("Speedup over daisy")
    ax.set_title("Daisy Speedup vs Daisy")
    ax.set_xticks(x)
    ax.set_xticklabels(workers)
    ax.axhline(y=1, color='gray', linestyle='--', alpha=0.5)
    ax.legend()
    for i, s in enumerate(g_speedups):
        ax.text(i - width / 2, s, f'{s:.1f}x', ha='center', va='bottom', fontsize=9)
    for i, s in enumerate(w_speedups):
        ax.text(i + width / 2, s, f'{s:.1f}x', ha='center', va='bottom', fontsize=9)

    # --- Plot 3: Block scaling ---
    ax = axes[2]
    blocks = [r["blocks"] for r in block_results]
    d_times = [r["daisy_s"] for r in block_results]
    g_times = [r["daisy_s"] for r in block_results]
    w_times = [r["daisy_worker_s"] for r in block_results]

    ax.plot(blocks, d_times, 'o-', label="daisy", color=DAISY_COLOR, linewidth=2)
    ax.plot(blocks, g_times, 's-', label="daisy block-fn",
            color=DAISY_BLOCK_COLOR, linewidth=2)
    ax.plot(blocks, w_times, '^-', label="daisy worker-fn",
            color=DAISY_WORKER_COLOR, linewidth=2)
    ax.set_xlabel("Number of blocks")
    ax.set_ylabel("Time (seconds)")
    ax.set_title(f"Block Scaling ({block_results[0]['workers']} workers)")
    ax.legend()
    ax.set_xscale('log')
    ax.set_yscale('log')

    plt.tight_layout()
    plt.savefig("benchmarks/worker_scaling_benchmark.png", dpi=150)
    print("\nSaved benchmarks/worker_scaling_benchmark.png")


if __name__ == "__main__":
    worker_results = run_scaling()
    block_results = run_block_scaling()
    plot_results(worker_results, block_results)
