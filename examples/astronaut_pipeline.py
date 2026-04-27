# %% [markdown]
# # Multi-step pipeline with resource-aware dispatch
#
# A three-step image pipeline on the astronaut photo, set up so the
# resource-aware dispatcher actually has something to balance:
#
# 1. **edges** — per-block Sobel edge magnitude. CPU-bound.
# 2. **segment** — per-block watershed using local-minima markers.
#    Tagged as "GPU" (pseudo-resource — we don't actually use a GPU,
#    but treating the step as scarce demonstrates how `requires=` and
#    a global `resources=` budget interact).
# 3. **recolor** — for every segment found in step 2, replace its
#    pixels with the average colour from the *original* image. CPU.
#
# Tasks 1 and 3 share a CPU pool of 4. Task 2 is the bottleneck — only
# one "GPU" worker ever runs concurrently. Once the edges task drains,
# its CPU workers exit and the recolor task can grow into the freed
# budget. This is the cluster pattern: GPU-bound stage in the middle,
# CPU-bound stages on either side, sharing budgets across the
# pipeline.

# %%
import tempfile
import threading
import time
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from scipy.ndimage import label as scipy_label
from skimage.data import astronaut
from skimage.filters import sobel
from skimage.segmentation import watershed

import daisy
import daisy.logging as gl

_TMP = Path(tempfile.mkdtemp(prefix="daisy_astronaut_pipeline_"))
gl.set_log_basedir(_TMP / "logs")
print(f"output paths under: {_TMP}")

# %% [markdown]
# ## Load the astronaut

# %%
INPUT = astronaut()  # (512, 512, 3) uint8
H, W = INPUT.shape[:2]
BLOCK = 32
print(f"input: {INPUT.shape}, dtype={INPUT.dtype}, "
      f"block grid: {H // BLOCK}×{W // BLOCK} = {(H // BLOCK) * (W // BLOCK)} blocks")

plt.figure(figsize=(5, 5))
plt.imshow(INPUT, interpolation="nearest")
plt.title("input")
plt.axis("off")

# %% [markdown]
# ## Allocate the intermediate arrays
#
# Each step writes into its own shared array and reads from the
# previous step's output. Worker threads share the process so plain
# numpy arrays are fine.

# %%
EDGES   = np.zeros((H, W),    dtype=np.uint8)   # 0–255
LABELS  = np.zeros((H, W),    dtype=np.uint32)  # global per-block ids
OUTPUT  = np.zeros((H, W, 3), dtype=np.uint8)

# Per-task concurrency tracking, so we can plot how the dispatcher
# distributes workers across the pipeline.
_lock = threading.Lock()
_alive = {"edges": 0, "segment": 0, "recolor": 0}
_timeline: list[dict] = []


def _record(task_id, kind):
    with _lock:
        if kind == "enter":
            _alive[task_id] += 1
        elif kind == "exit":
            _alive[task_id] -= 1
        _timeline.append({
            "t": time.perf_counter(),
            "task": task_id,
            "kind": kind,
            "alive": dict(_alive),
        })


# %% [markdown]
# ## Step 1 — edges (CPU, max 4 workers, 1 CPU per worker)
#
# Read the input slice, convert to grayscale, take the Sobel magnitude,
# and write to `EDGES`. No halo — Sobel uses reflection padding at
# block boundaries, which is fine for this demo (the resulting block
# seams are visible in the output, on purpose).

# %%
def step_edges(block):
    _record("edges", "enter")
    try:
        time.sleep(0.002)  # simulate a more expensive operation
        off = block.write_roi.offset.to_list()
        shape = block.write_roi.shape.to_list()
        r0, c0 = off[0], off[1]
        rs, cs = shape[0], shape[1]

        gray = INPUT[r0 : r0 + rs, c0 : c0 + cs].mean(axis=-1) / 255.0
        EDGES[r0 : r0 + rs, c0 : c0 + cs] = (sobel(gray) * 255).clip(0, 255).astype(np.uint8)
    finally:
        _record("edges", "exit")


# %% [markdown]
# ## Step 2 — watershed (pseudo-"GPU", max 1 worker)
#
# Read the edge map, build markers from low-edge pixels (interior of
# objects), run a watershed, offset the local labels into a global
# range so blocks don't collide, and write to `LABELS`. We tag this as
# `requires={"gpu": 1}` to demonstrate the resource model — even
# though it's pure CPU work in reality, the dispatcher will only
# schedule one watershed worker at a time because the global budget
# only has `"gpu": 1`.

# %%
def step_segment(block):
    _record("segment", "enter")
    try:
        time.sleep(0.0025)  # simulate the expensive "GPU" step
        off = block.write_roi.offset.to_list()
        shape = block.write_roi.shape.to_list()
        r0, c0 = off[0], off[1]
        rs, cs = shape[0], shape[1]

        edges_tile = EDGES[r0 : r0 + rs, c0 : c0 + cs].astype(np.float32) / 255.0
        # Markers: where the edge response is low (object interiors).
        markers, _ = scipy_label(edges_tile < 0.04)
        seg = watershed(edges_tile, markers=markers).astype(np.uint32)

        # Make labels globally unique by offsetting per block.
        block_offset = block.block_id[1] * BLOCK * BLOCK
        LABELS[r0 : r0 + rs, c0 : c0 + cs] = np.where(seg > 0, seg + block_offset, 0)
    finally:
        _record("segment", "exit")


# %% [markdown]
# ## Step 3 — recolor (CPU, max 4 workers, 1 CPU per worker)
#
# For each segment in this block, replace its pixels with the average
# RGB colour of the *original* image over that segment's mask. Output
# is a low-poly-style abstraction of the astronaut.

# %%
def step_recolor(block):
    _record("recolor", "enter")
    try:
        time.sleep(0.004)
        off = block.write_roi.offset.to_list()
        shape = block.write_roi.shape.to_list()
        r0, c0 = off[0], off[1]
        rs, cs = shape[0], shape[1]

        labels_tile = LABELS[r0 : r0 + rs, c0 : c0 + cs]
        rgb_tile = INPUT[r0 : r0 + rs, c0 : c0 + cs]
        out_tile = np.zeros_like(rgb_tile)
        for label_id in np.unique(labels_tile):
            mask = labels_tile == label_id
            out_tile[mask] = rgb_tile[mask].mean(axis=0).astype(np.uint8)
        OUTPUT[r0 : r0 + rs, c0 : c0 + cs] = out_tile
    finally:
        _record("recolor", "exit")


# %% [markdown]
# ## Define the pipeline
#
# Three tasks, chained via `upstream_tasks=`. `requires` declares each
# task's per-worker resource cost; `max_workers` is the hard cap.
# `resources={"cpu": 4, "gpu": 1}` is the global budget — enough CPU
# headroom that the pipeline isn't CPU-starved, with the watershed
# stage gated by a single "GPU" slot.

# %%
edges_task = daisy.Task(
    task_id="edges",
    total_roi=daisy.Roi([0, 0], [H, W]),
    read_roi=daisy.Roi([0, 0], [BLOCK, BLOCK]),
    write_roi=daisy.Roi([0, 0], [BLOCK, BLOCK]),
    process_function=step_edges,
    read_write_conflict=False,
    max_workers=3,
    max_retries=0,
    requires={"cpu": 1},
)

segment_task = daisy.Task(
    task_id="segment",
    total_roi=daisy.Roi([0, 0], [H, W]),
    read_roi=daisy.Roi([0, 0], [BLOCK, BLOCK]),
    write_roi=daisy.Roi([0, 0], [BLOCK, BLOCK]),
    process_function=step_segment,
    read_write_conflict=False,
    max_workers=4,        # cap is 4, but global budget caps at 1
    max_retries=0,
    requires={"gpu": 1},
    upstream_tasks=[edges_task],
)

recolor_task = daisy.Task(
    task_id="recolor",
    total_roi=daisy.Roi([0, 0], [H, W]),
    read_roi=daisy.Roi([0, 0], [BLOCK, BLOCK]),
    write_roi=daisy.Roi([0, 0], [BLOCK, BLOCK]),
    process_function=step_recolor,
    read_write_conflict=False,
    max_workers=4,
    max_retries=0,
    requires={"cpu": 1},
    upstream_tasks=[segment_task],
)

# %% [markdown]
# ## Run the pipeline
#
# `run_blockwise` sees the dependency chain and dispatches each stage
# only after its upstream blocks complete. Block 0 of `recolor` can
# start as soon as block 0 of `segment` completes; it doesn't have to
# wait for `segment` to fully drain.

# %%
t_start = time.perf_counter()
ok = daisy.run_blockwise(
    [edges_task, segment_task, recolor_task],
    resources={"cpu": 4, "gpu": 1},
)
elapsed = time.perf_counter() - t_start
assert ok
print(f"\npipeline elapsed: {elapsed * 1e3:.0f} ms")

# %% [markdown]
# ## Visualise each stage of the pipeline

# %%
fig, axes = plt.subplots(1, 4, figsize=(16, 4))
axes[0].imshow(INPUT, interpolation="nearest")
axes[0].set_title("1. input")
axes[0].axis("off")
axes[1].imshow(EDGES, cmap="gray")
axes[1].set_title("2. Sobel edges")
axes[1].axis("off")

# Hash-coloured labels.
def label_to_rgb(seg, bg_colour=(0, 0, 0)):
    seg = np.asarray(seg).astype(np.int64)
    ids, inverse = np.unique(seg, return_inverse=True)
    rng = np.random.default_rng(42)
    colours = 0.2 + 0.8 * rng.random((ids.size, 3))
    for i, lid in enumerate(ids):
        if lid == 0:
            colours[i] = bg_colour
    return (colours[inverse].reshape(*seg.shape, 3) * 255).astype(np.uint8)


axes[2].imshow(label_to_rgb(LABELS), interpolation="nearest")
axes[2].set_title(f"3. watershed labels  ({len(np.unique(LABELS))} segments)")
axes[2].axis("off")

axes[3].imshow(OUTPUT, interpolation="nearest")
axes[3].set_title("4. recoloured")
axes[3].axis("off")
fig.tight_layout()

# %% [markdown]
# ## Concurrency timeline — was the budget honoured?
#
# Plot, for every event in the run, how many workers of each task were
# alive simultaneously. Two things to confirm visually:
#
# - `segment` (the "GPU" step) **never exceeds 1** alive worker — the
#   global budget caps it.
# - `edges` and `recolor` together **never exceed 4** alive workers
#   simultaneously, even though each task's own `max_workers=4` cap
#   would individually allow 4.
# - As `edges` drains, its alive count falls; `recolor`'s alive count
#   rises into the freed CPU budget — no respawning, just the
#   dispatcher spawning new workers for the next ready task.

# %%
t0 = _timeline[0]["t"]
times = [(e["t"] - t0) * 1e3 for e in _timeline]
edges_alive   = [e["alive"]["edges"]   for e in _timeline]
segment_alive = [e["alive"]["segment"] for e in _timeline]
recolor_alive = [e["alive"]["recolor"] for e in _timeline]

panels = [
    ("edges (CPU)",       edges_alive,   "#4878CF", 4,
     "max_workers=3 — shares cpu=4 with recolor"),
    ("segment ('GPU')",   segment_alive, "#956CB4", 1,
     "max_workers=4 — capped by gpu budget = 1"),
    ("recolor (CPU)",     recolor_alive, "#6ACC65", 4,
     "max_workers=4 — shares cpu=4 with edges"),
]
fig, axes = plt.subplots(3, 1, figsize=(11, 6.5), sharex=True)
for ax, (label, data, colour, cap, subtitle) in zip(axes, panels):
    ax.step(times, data, where="post", color=colour, linewidth=1.5)
    ax.fill_between(times, data, step="post", alpha=0.25, color=colour)
    ax.axhline(cap, color="black", linestyle=":", alpha=0.5, linewidth=0.9)
    ax.text(times[-1], cap, f" cap={cap}", va="center", fontsize=8, alpha=0.7)
    ax.set_ylabel("alive workers")
    ax.set_ylim(-0.2, max(cap, max(data)) + 0.6)
    ax.set_title(f"{label}  —  {subtitle}", fontsize=10, loc="left")
axes[-1].set_xlabel("time since pipeline start (ms)")
fig.tight_layout()

# Combined CPU concurrency check: edges + recolor.
combined = [e["alive"]["edges"] + e["alive"]["recolor"] for e in _timeline]
print(f"peak (edges + recolor) alive = {max(combined)}  (budget cpu=4)")
print(f"peak segment alive = {max(segment_alive)}  (budget gpu=1)")

plt.show()

# %%
