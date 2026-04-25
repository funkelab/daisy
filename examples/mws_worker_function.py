# %% [markdown]
# # Worker-function mode: each worker drives its own Client
#
# Same pipeline as `mws_block_function.py` — take a ground-truth 2D
# segmentation, generate affinities with `lsd_lite.get_affs`, then
# blockwise-agglomerate with `mwatershed.agglom` — but dispatched in
# **worker-function mode**.
#
# A worker function takes **zero arguments**. Gerbera spawns one copy per
# worker slot; each copy creates a `gerbera.Client` (configured via the
# `GERBERA_CONTEXT` environment variable the server injects) and loops
# over `client.acquire_block()` until it returns `None`.
#
# Use this mode when a worker needs **per-worker setup** you want to
# amortise across many blocks — loading a model, opening a database
# connection, memory-mapping a Zarr array. In block-function mode that
# setup would re-run on every single block.
#
# The serial runner only understands per-block callbacks, so
# worker-function mode always runs through the multiprocessing `Server`.

# %%
import threading
import time

import lsd_lite
import matplotlib.pyplot as plt
import mwatershed
import numpy as np

import gerbera

# %% [markdown]
# ## Build a dummy 2D segmentation
#
# 40 random discs painted onto a 512×512 label image. Label 0 is
# background; each disc gets its own positive integer id.

# %%
H, W = 512, 512
N_DISCS = 40
BLOCK = 128
NUM_WORKERS = 4


def label_to_rgb(seg, bg_colour=(0, 0, 0)):
    seg = np.asarray(seg).astype(np.int64)
    ids, inverse = np.unique(seg, return_inverse=True)
    rng = np.random.default_rng(42)
    colours = 0.2 + 0.8 * rng.random((ids.size, 3))
    for i, label in enumerate(ids):
        if label == 0:
            colours[i] = bg_colour
    return (colours[inverse].reshape(*seg.shape, 3) * 255).astype(np.uint8)


def random_disc_segmentation(height, width, n_discs, seed=0):
    rng = np.random.default_rng(seed)
    gt = np.zeros((height, width), dtype=np.uint32)
    y, x = np.ogrid[:height, :width]
    for label in range(1, n_discs + 1):
        cy = rng.integers(20, height - 20)
        cx = rng.integers(20, width - 20)
        r = rng.integers(15, 45)
        mask = (y - cy) ** 2 + (x - cx) ** 2 <= r * r
        gt[mask] = label
    return gt


GT = random_disc_segmentation(H, W, N_DISCS)
print(f"ground truth: {GT.shape}, {len(np.unique(GT)) - 1} foreground discs")

plt.figure(figsize=(5, 5))
plt.imshow(label_to_rgb(GT), interpolation="nearest")
plt.title("ground truth segmentation")
plt.axis("off")

# %% [markdown]
# ## Generate affinities
#
# Short- and long-range nearest-neighbour offsets `[dy, dx]`.
# `dist='equality'` produces 1 where `gt[y, x] == gt[y+dy, x+dx]` and 0
# otherwise — perfect boundary predictions derived from the GT.
# Re-centred to `[-0.5, +0.5]` for mutex watershed (positive merges,
# negative splits).

# %%
NEIGHBORHOOD = [
    [0, 1],   # right (short-range, attractive)
    [1, 0],   # down  (short-range, attractive)
    [0, 3],   # right-3 (long-range, repulsive)
    [3, 0],   # down-3  (long-range, repulsive)
]
AFFINITIES = lsd_lite.get_affs(GT, NEIGHBORHOOD, dist="equality").astype(np.float32) - 0.5
OUTPUT = np.zeros((H, W), dtype=np.uint32)

plt.figure(figsize=(5, 5))
plt.imshow(AFFINITIES[:3].transpose([1, 2, 0]) + 0.5, vmin=0, vmax=1)
plt.title("affinities (channels 0–2 as RGB)")
plt.axis("off")

# %% [markdown]
# ## Visualise the block tiling
#
# Gerbera tiles the image into `BLOCK × BLOCK` non-overlapping tiles
# with `read_roi == write_roi`. mutex watershed runs independently on
# each tile.

# %%
fig, ax = plt.subplots(figsize=(5, 5))
ax.imshow(label_to_rgb(GT), interpolation="nearest")
for r in range(BLOCK, H, BLOCK):
    ax.axhline(r - 0.5, color="yellow", linewidth=1.0)
for c in range(BLOCK, W, BLOCK):
    ax.axvline(c - 0.5, color="yellow", linewidth=1.0)
ax.set_title(f"{H // BLOCK} × {W // BLOCK} block grid, {BLOCK}×{BLOCK} each")
ax.axis("off")

# %% [markdown]
# ## Define the worker function
#
# The worker function takes **zero arguments**. That arity is how gerbera
# distinguishes it from a block-function: `inspect.getfullargspec` is
# called on your callable at task-build time.
#
# Inside the worker we:
# 1. run any one-time setup (here, stand-in `expensive_model_load`),
# 2. build a `Client()` — it reads `GERBERA_CONTEXT` from the env to
#    find the server's host/port/task_id/worker_id,
# 3. loop on `client.acquire_block()` until it returns `None`, running
#    `mwatershed.agglom` on each block's affinity tile.
#
# As in the block-function example, each block's labels are offset by
# `block_id × BLOCK²` so the global label space is disjoint and fully
# deterministic regardless of which worker picks which block up.
#
# We track, per worker, which blocks were handled and at what time, so
# we can visualise load balancing below.

# %%
_worker_events: list[dict] = []
_events_lock = threading.Lock()


def log(event):
    with _events_lock:
        _worker_events.append(event)


def expensive_model_load():
    # Stand-in for per-worker one-time setup (tokenizer, ONNX session, etc.).
    time.sleep(0.05)


def worker():
    tid = threading.get_ident()
    t_start = time.perf_counter()
    expensive_model_load()
    log({"tid": tid, "event": "ready", "t": time.perf_counter() - t_start})

    client = gerbera.Client()
    n_blocks = 0
    while True:
        with client.acquire_block() as block:
            if block is None:
                break
            off = block.write_roi.offset.to_list()
            shape = block.write_roi.shape.to_list()
            r0, c0 = off[0], off[1]
            rs, cs = shape[0], shape[1]

            tile = AFFINITIES[:, r0 : r0 + rs, c0 : c0 + cs].astype(np.float64)
            local = mwatershed.agglom(tile, offsets=NEIGHBORHOOD).astype(np.uint32)

            block_offset = block.block_id[1] * BLOCK * BLOCK
            OUTPUT[r0 : r0 + rs, c0 : c0 + cs] = np.where(
                local > 0, local + block_offset, 0
            )
            log({
                "tid": tid,
                "event": "block",
                "t": time.perf_counter() - t_start,
                "row": r0 // BLOCK,
                "col": c0 // BLOCK,
            })
            n_blocks += 1
    log({"tid": tid, "event": "done", "t": time.perf_counter() - t_start, "n": n_blocks})


# %% [markdown]
# ## Run the task
#
# Because `worker` takes zero positional arguments, gerbera registers it
# as a `spawn_function` instead of a per-block callback. `num_workers=4`
# means gerbera spawns four worker threads, each of which calls `worker()`
# exactly once and loops internally.

# %%
task = gerbera.Task(
    task_id="mws_worker",
    total_roi=gerbera.Roi([0, 0], [H, W]),
    read_roi=gerbera.Roi([0, 0], [BLOCK, BLOCK]),
    write_roi=gerbera.Roi([0, 0], [BLOCK, BLOCK]),
    process_function=worker,
    read_write_conflict=False,
    num_workers=NUM_WORKERS,
)

t0 = time.perf_counter()
ok = gerbera.run_blockwise([task], multiprocessing=True)
elapsed = time.perf_counter() - t0

assert ok, "some blocks failed"
print(f"elapsed={elapsed * 1e3:.1f} ms, events={len(_worker_events)}, "
      f"output segments={len(np.unique(OUTPUT)) - 1}")

# %% [markdown]
# ## Visualise the output
#
# GT vs worker-function output, coloured with the same hashed-label map
# as the block-function example.

# %%
fig, axes = plt.subplots(1, 2, figsize=(10, 5))
axes[0].imshow(label_to_rgb(GT), interpolation="nearest")
axes[0].set_title("ground truth")
axes[0].axis("off")
axes[1].imshow(label_to_rgb(OUTPUT), interpolation="nearest")
axes[1].set_title("output (worker-function mode)")
axes[1].axis("off")
fig.tight_layout()

# %% [markdown]
# ## How were the blocks distributed between workers?
#
# Each worker recorded which (row, col) block it processed and at what
# time since its own start. The grid below colours blocks by which of
# the four worker threads picked them up.

# %%
tids = sorted({e["tid"] for e in _worker_events if e["event"] == "block"})
tid_to_idx = {tid: i for i, tid in enumerate(tids)}

worker_map = np.full((H // BLOCK, W // BLOCK), -1, dtype=np.int16)
for e in _worker_events:
    if e["event"] == "block":
        worker_map[e["row"], e["col"]] = tid_to_idx[e["tid"]]

counts = np.bincount(
    [tid_to_idx[e["tid"]] for e in _worker_events if e["event"] == "block"],
    minlength=len(tids),
)

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4))
ax1.imshow(worker_map, cmap="tab10", vmin=0, vmax=9, interpolation="nearest")
ax1.set_title("block → worker index")
ax1.set_xlabel("block col"); ax1.set_ylabel("block row")

ax2.bar(range(len(tids)), counts, color=[plt.cm.tab10(i) for i in range(len(tids))])
ax2.set_xticks(range(len(tids)))
ax2.set_xticklabels([f"w{i}" for i in range(len(tids))])
ax2.set_ylabel("blocks processed")
ax2.set_title(f"per-worker block count (total={counts.sum()})")
fig.tight_layout()

# %% [markdown]
# ## Timeline
#
# For each worker we plot one marker per acquired block against
# wall-clock time (relative to the first event). Gaps in a row are TCP
# round-trips + any GIL waits between blocks.
#
# ### Performance note
#
# `gerbera.Client` is synchronous and wraps `tokio::runtime::block_on`.
# The GIL is released around every blocking call (`py.detach` in
# `py_sync_client.rs`) so the four workers really do overlap their TCP
# round-trips and `mwatershed.agglom` work — the timeline should show
# interleaved colours rather than one worker running to completion
# before the next starts.

# %%
t0_global = min(e["t"] for e in _worker_events if e["event"] == "block")
fig, ax = plt.subplots(figsize=(10, 3))
for e in _worker_events:
    if e["event"] == "block":
        idx = tid_to_idx[e["tid"]]
        ax.scatter(e["t"] - t0_global, idx, color=plt.cm.tab10(idx), s=18)
ax.set_yticks(range(len(tids)))
ax.set_yticklabels([f"w{i}" for i in range(len(tids))])
ax.set_xlabel("time since first block (s)")
ax.set_title("per-worker block timeline")
fig.tight_layout()

plt.show()
