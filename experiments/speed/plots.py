"""Draw the speed plots from the CSV files next to this script.

python plots.py   ->  throughput.png, resume.png, scheduler.png
"""

import csv
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))


def read(name):
    with open(os.path.join(HERE, name)) as f:
        return list(csv.DictReader(f))


def throughput():
    rows = read("throughput.csv")
    fig, axes = plt.subplots(1, 3, figsize=(12, 3.8), sharey=True)
    for ax, mode, title in zip(axes, "ABC", ("A: block function", "B: worker function", "C: external worker")):
        for version, style in (("1.x", "o--"), ("2.0", "s-")):
            pts = sorted((int(r["workers"]), float(r["blocks_per_second"])) for r in rows if r["mode"] == mode and r["version"] == version)
            if pts:
                ax.plot([p[0] for p in pts], [p[1] for p in pts], style, label=f"daisy {version}")
        ax.set_xscale("log", base=2)
        ax.set_yscale("log")
        ax.set_xlabel("workers")
        ax.set_title(title)
        ax.grid(True, which="both", alpha=0.3)
    axes[0].set_ylabel("no-op blocks per second")
    axes[0].legend()
    fig.suptitle("Throughput by worker mode (10,000 no-op blocks)")
    fig.tight_layout()
    fig.savefig(os.path.join(HERE, "throughput.png"), dpi=130)


def resume():
    rows = read("resume.csv")  # version,strategy,phase,blocks,seconds
    fig, ax = plt.subplots(figsize=(6.5, 3.8))
    labels, vals = [], []
    for r in rows:
        if r["phase"] == "resume":
            labels.append(f"{r['strategy']}\n(daisy {r['version']})")
            vals.append(float(r["seconds"]))
    bars = ax.bar(labels, vals, color=["#999999", "#999999", "#2f6fdb"][: len(vals)])
    for b, v in zip(bars, vals):
        ax.text(b.get_x() + b.get_width() / 2, v, f"{v:.2f} s", ha="center", va="bottom")
    ax.set_ylabel("seconds to resume (all blocks already done)")
    ax.set_title(f"Resume cost for {rows[0]['blocks']} blocks: check_function vs built-in tracking")
    fig.tight_layout()
    fig.savefig(os.path.join(HERE, "resume.png"), dpi=130)


def scheduler():
    rows = read("scheduler.csv")  # version,grid,blocks,seconds,blocks_per_second
    fig, ax = plt.subplots(figsize=(6.5, 3.8))
    grids = sorted({r["grid"] for r in rows})
    width = 0.38
    for i, (version, color) in enumerate((("1.x", "#999999"), ("2.0", "#2f6fdb"))):
        vals = [next((float(r["blocks_per_second"]) for r in rows if r["grid"] == g and r["version"] == version), 0) for g in grids]
        xs = [j + (i - 0.5) * width for j in range(len(grids))]
        bars = ax.bar(xs, vals, width, label=f"daisy {version}", color=color)
        for b, v in zip(bars, vals):
            ax.text(b.get_x() + b.get_width() / 2, v, f"{v:,.0f}", ha="center", va="bottom", fontsize=8)
    ax.set_xticks(range(len(grids)))
    ax.set_xticklabels(grids)
    ax.set_yscale("log")
    ax.set_ylabel("blocks scheduled per second (one thread, no workers)")
    ax.set_title("Scheduler drain: acquire + release, no message passing")
    ax.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(HERE, "scheduler.png"), dpi=130)


if __name__ == "__main__":
    throughput()
    resume()
    scheduler()
    print("wrote throughput.png, resume.png, scheduler.png")
