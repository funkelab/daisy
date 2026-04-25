# %% [markdown]
# # Block-function mode: affinities → blockwise mutex watershed
#
# A connectomics-flavoured pipeline in two steps:
#
# 1. take a ground-truth 2D segmentation and generate short- and
#    long-range nearest-neighbour affinities with `lsd_lite.get_affs`
#    (in the real pipeline these would be the output of a UNet — we
#    derive them directly from the GT so the example is self-contained);
# 2. run `mwatershed.agglom` (mutex watershed) independently on each
#    tile of the affinity volume, driven by gerbera in block-function
#    mode.
#
# In **block-function mode** the Python function receives a single
# `Block` argument and gerbera drives the loop — for every block the
# scheduler hands out, gerbera calls `process_block(block)`. We run the
# task twice — once on the single-threaded `SerialServer`
# (`multiprocessing=False`) and once on the multi-worker `Server`
# (`multiprocessing=True`) — so you can see both code paths converge on
# the same result.

# %%
import tempfile
import threading
import time
from pathlib import Path

import lsd_lite
import matplotlib.pyplot as plt
import mwatershed
import numpy as np

import gerbera
import gerbera.logging as gl

# Write per-worker logs to a fresh temp dir so the example doesn't
# accumulate `gerbera_logs/` in the working directory across runs.
_TMP = Path(tempfile.mkdtemp(prefix="gerbera_block_function_"))
gl.set_log_basedir(_TMP / "logs")
print(f"output paths under: {_TMP}")

# %% [markdown]
# ## Build a dummy 2D segmentation
#
# 40 random discs painted onto a 512×512 label image. Label 0 is
# background; each disc gets its own positive integer id.

# %%
H, W = 512, 512
N_DISCS = 40


def label_to_rgb(seg, bg_colour=(0, 0, 0)):
    """Map each integer label to a distinct RGB colour by hashing the label
    through a fixed permutation. Label 0 is rendered as `bg_colour`. Works
    for arbitrary integer ranges — no tab20-style wrap-around collisions."""
    seg = np.asarray(seg).astype(np.int64)
    ids, inverse = np.unique(seg, return_inverse=True)
    rng = np.random.default_rng(42)
    colours = rng.random((ids.size, 3))
    # High minimum saturation: avoid near-black lookalikes colliding with
    # the background.
    colours = 0.2 + 0.8 * colours
    for i, label in enumerate(ids):
        if label == 0:
            colours[i] = bg_colour
    rgb = (colours[inverse].reshape(*seg.shape, 3) * 255).astype(np.uint8)
    return rgb


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
# A short-range + long-range nearest-neighbour `neighborhood` with
# offsets `[dy, dx]`. `dist='equality'` means each channel is 1 where
# `gt[y, x] == gt[y+dy, x+dx]` and 0 otherwise (perfect boundary
# predictions derived from the GT).
#
# mutex watershed expects affinities where **positive values merge** and
# **negative values split**, so we re-centre to `[-0.5, +0.5]`.

# %%
NEIGHBORHOOD = [
    [0, 1],   # right (short-range, attractive)
    [1, 0],   # down  (short-range, attractive)
    [0, 3],   # right-3 (long-range, repulsive)
    [3, 0],   # down-3  (long-range, repulsive)
]

affs_bool = lsd_lite.get_affs(GT, NEIGHBORHOOD, dist="equality")
AFFINITIES = affs_bool.astype(np.float32) - 0.5

print(f"affinities: {AFFINITIES.shape}, range "
      f"[{AFFINITIES.min():+.1f}, {AFFINITIES.max():+.1f}]")

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
BLOCK = 128

fig, ax = plt.subplots(figsize=(5, 5))
ax.imshow(label_to_rgb(GT), interpolation="nearest")
for r in range(BLOCK, H, BLOCK):
    ax.axhline(r - 0.5, color="yellow", linewidth=1.0)
for c in range(BLOCK, W, BLOCK):
    ax.axvline(c - 0.5, color="yellow", linewidth=1.0)
ax.set_title(f"{H // BLOCK} × {W // BLOCK} block grid, {BLOCK}×{BLOCK} each")
ax.axis("off")

# %% [markdown]
# ## Define the process function
#
# Each block runs `mwatershed.agglom` on its own tile of the affinity
# volume. Per-block label ids only have meaning locally — block A's
# "segment 3" is unrelated to block B's "segment 3" — so we shift each
# block's labels into a disjoint global range. In a real pipeline a
# second stitching pass would identify objects that straddle block
# boundaries and re-unify them; we skip that here and the output
# inherits a visible grid-aligned fragmentation.
#
# We also record, per block, which worker id touched it, so we can see
# how work was distributed after the run.

# %%
OUTPUT = np.zeros((H, W), dtype=np.uint32)
WORKER_MAP = np.full((H // BLOCK, W // BLOCK), -1, dtype=np.int16)

_worker_id_lock = threading.Lock()
_worker_ids: dict[int, int] = {}


def _worker_index():
    tid = threading.get_ident()
    with _worker_id_lock:
        if tid not in _worker_ids:
            _worker_ids[tid] = len(_worker_ids)
        return _worker_ids[tid]


def process_block(block):
    off = block.write_roi.offset.to_list()
    shape = block.write_roi.shape.to_list()
    r0, c0 = off[0], off[1]
    rs, cs = shape[0], shape[1]

    tile = AFFINITIES[:, r0 : r0 + rs, c0 : c0 + cs].astype(np.float64)
    local = mwatershed.agglom(tile, offsets=NEIGHBORHOOD).astype(np.uint32)

    # Each block owns a disjoint id range of size BLOCK**2, keyed on its
    # spatial block_id. Deterministic and worker-order-independent.
    block_offset = block.block_id[1] * BLOCK * BLOCK
    OUTPUT[r0 : r0 + rs, c0 : c0 + cs] = np.where(local > 0, local + block_offset, 0)
    WORKER_MAP[r0 // BLOCK, c0 // BLOCK] = _worker_index()


# %% [markdown]
# ## Run on the serial server
#
# `multiprocessing=False` dispatches to `SerialServer`: no TCP, no
# threads — the scheduler calls `process_block` directly on the main
# thread for every ready block. This is the debugging path.

# %%
def reset_buffers():
    OUTPUT.fill(0)
    WORKER_MAP.fill(-1)
    _worker_ids.clear()


mws_task = gerbera.Task(
        task_id="mws",
        total_roi=gerbera.Roi([0, 0], [H, W]),
        read_roi=gerbera.Roi([0, 0], [BLOCK, BLOCK]),
        write_roi=gerbera.Roi([0, 0], [BLOCK, BLOCK]),
        process_function=process_block,
        read_write_conflict=False,
        num_workers=4,
    )


reset_buffers()
t0 = time.perf_counter()
ok_serial = gerbera.run_blockwise([mws_task], multiprocessing=False)
serial_elapsed = time.perf_counter() - t0
serial_output = OUTPUT.copy()
serial_workers = WORKER_MAP.copy()
print(f"serial: ok={ok_serial}, elapsed={serial_elapsed * 1e3:.1f} ms, "
      f"unique workers={len(set(serial_workers.flatten())) - (1 if -1 in serial_workers else 0)}, "
      f"output segments={len(np.unique(serial_output)) - 1}")

# %% [markdown]
# ## Run on the multiprocessing server
#
# `multiprocessing=True` dispatches to the distributed `Server`: gerbera
# binds a TCP listener, spawns `num_workers` worker threads in Rust, and
# each worker pulls blocks from the scheduler over TCP. Worker threads
# take the GIL only for the `process_block` callback — the TCP round-trip
# and block lifecycle stay in Rust, so blocks process in parallel on
# numpy-heavy workloads like this one.

# %%
reset_buffers()
t0 = time.perf_counter()
ok_mp = gerbera.run_blockwise([mws_task], multiprocessing=True)
mp_elapsed = time.perf_counter() - t0
mp_output = OUTPUT.copy()
mp_workers = WORKER_MAP.copy()
print(f"mp:     ok={ok_mp}, elapsed={mp_elapsed * 1e3:.1f} ms, "
      f"unique workers={len(set(mp_workers.flatten())) - (1 if -1 in mp_workers else 0)}, "
      f"output segments={len(np.unique(mp_output)) - 1}")

# %% [markdown]
# ## Compare outputs
#
# Because each block's id-range is keyed on `block_id` rather than a
# shared counter, the output is fully deterministic — serial and mp
# produce literally identical arrays regardless of the order the blocks
# finish.

# %%
diff = (serial_output != mp_output).astype(np.uint8)

fig, axes = plt.subplots(1, 4, figsize=(16, 4))
axes[0].imshow(label_to_rgb(GT), interpolation="nearest")
axes[0].set_title("ground truth")
axes[0].axis("off")
axes[1].imshow(label_to_rgb(serial_output), interpolation="nearest")
axes[1].set_title("serial output")
axes[1].axis("off")
axes[2].imshow(label_to_rgb(mp_output), interpolation="nearest")
axes[2].set_title("mp output")
axes[2].axis("off")
axes[3].imshow(diff, cmap="inferno", vmin=0, vmax=1)
axes[3].set_title(f"|serial ≠ mp| ({diff.sum()} px)")
axes[3].axis("off")
fig.tight_layout()

assert np.array_equal(serial_output, mp_output), "serial and mp produced different outputs!"

# %% [markdown]
# ## Visualise worker assignment
#
# On the serial path every block was processed by the main thread (one
# "worker"). On the multiprocessing path blocks scatter across
# `num_workers` threads; colouring by worker id reveals how the
# scheduler handed work out.

# %%
fig, axes = plt.subplots(1, 2, figsize=(8, 4))
for ax, grid, title in zip(axes, [serial_workers, mp_workers], ["serial", "mp"]):
    im = ax.imshow(grid, cmap="tab10", vmin=0, vmax=9, interpolation="nearest")
    ax.set_title(f"{title}: blocks coloured by worker id")
    ax.set_xlabel("block col")
    ax.set_ylabel("block row")
fig.tight_layout()

plt.show()

# %%
