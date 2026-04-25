# %% [markdown]
# # Persistent done-marker: skip blocks across runs, overlay on results
#
# This example shows the **built-in done-marker**. When a task has
# `done_marker_path=...` set, gerbera persists per-block "done" state
# under that directory as a single-chunk Zarr v2 array. On re-run,
# already-done blocks are skipped before `process_function` is called —
# turning re-runs into instant resumes.
#
# We:
#
# 1. run the same MWS task as `mws_block_function.py` with a marker
#    path,
# 2. count how many times `process_block` actually fired,
# 3. re-run the same task and confirm `process_block` is called zero
#    times,
# 4. open the marker as a regular Zarr array and overlay it on the
#    output segmentation, so we can see exactly which tiles were done.
#
# The marker file lives at `gerbera-markers.zarr/<task_id>/`. It is a
# real Zarr v2 array (single chunk, `dtype="|u1"`, no compressor) — any
# tool that reads Zarr (napari, neuroglancer, `zarr.open(...)`) can
# inspect it without gerbera.

# %%
import shutil
from pathlib import Path

import lsd_lite
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import mwatershed
import numpy as np
import zarr

import gerbera

# %% [markdown]
# ## Build the same GT + affinities as the clean example

# %%
H, W = 512, 512
N_DISCS = 40
BLOCK = 128
MARKER_PATH = Path("gerbera-markers.zarr/mws_marker_demo")


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
        gt[(y - cy) ** 2 + (x - cx) ** 2 <= r * r] = label
    return gt


GT = random_disc_segmentation(H, W, N_DISCS)
NEIGHBORHOOD = [[0, 1], [1, 0], [0, 3], [3, 0]]
AFFINITIES = lsd_lite.get_affs(GT, NEIGHBORHOOD, dist="equality").astype(np.float32) - 0.5
OUTPUT = np.zeros((H, W), dtype=np.uint32)

# %% [markdown]
# ## Start fresh
#
# Delete any prior marker so this example demonstrates a clean first run
# followed by an instant-skip second run. In real usage you would *keep*
# the marker so re-runs resume.

# %%
if MARKER_PATH.exists():
    shutil.rmtree(MARKER_PATH)

# %% [markdown]
# ## Define the process function with a per-block call counter

# %%
_call_counts: list[int] = [0]


def process_block(block):
    _call_counts[0] += 1
    off = block.write_roi.offset.to_list()
    shape = block.write_roi.shape.to_list()
    r0, c0 = off[0], off[1]
    rs, cs = shape[0], shape[1]

    tile = AFFINITIES[:, r0 : r0 + rs, c0 : c0 + cs].astype(np.float64)
    local = mwatershed.agglom(tile, offsets=NEIGHBORHOOD).astype(np.uint32)
    block_offset = block.block_id[1] * BLOCK * BLOCK
    OUTPUT[r0 : r0 + rs, c0 : c0 + cs] = np.where(local > 0, local + block_offset, 0)


def make_task():
    return gerbera.Task(
        task_id="mws_marker_demo",
        total_roi=gerbera.Roi([0, 0], [H, W]),
        read_roi=gerbera.Roi([0, 0], [BLOCK, BLOCK]),
        write_roi=gerbera.Roi([0, 0], [BLOCK, BLOCK]),
        process_function=process_block,
        read_write_conflict=False,
        num_workers=4,
        # The whole point of this example:
        done_marker_path=str(MARKER_PATH),
    )


# %% [markdown]
# ## Run 1 — fresh task, marker is empty, all 16 blocks process

# %%
_call_counts[0] = 0
ok = gerbera.run_blockwise([make_task()], multiprocessing=True)
assert ok
print(f"run 1 process_block calls: {_call_counts[0]}  (expected 16)")

# %% [markdown]
# ## Inspect the marker between runs
#
# `gerbera-markers.zarr/<task_id>/` is a normal Zarr v2 array. We open
# it with `zarr.open` and pull the whole 4×4 grid into a numpy array —
# `1` means "this block is done", `0` means "still pending". On the
# happy path every cell is 1.

# %%
marker = zarr.open(str(MARKER_PATH), mode="r")
done_grid = np.asarray(marker[:])
print(f"marker shape: {done_grid.shape}, dtype: {done_grid.dtype}")
print(f"done blocks: {int(done_grid.sum())} / {done_grid.size}")
print(done_grid)

# %% [markdown]
# ## Run 2 — marker is populated, every block is skipped before
# `process_function` is called

# %%
_call_counts[0] = 0
ok = gerbera.run_blockwise([make_task()], multiprocessing=True)
assert ok
print(f"run 2 process_block calls: {_call_counts[0]}  (expected 0)")

# %% [markdown]
# ## Overlay the done-marker on the output
#
# Visualise the output segmentation with the done-grid overlaid as a
# yellow border per completed block. In a partial-progress scenario
# (e.g. you hit Ctrl-C halfway through) only the tiles that completed
# would have a border, giving you an immediate visual map of what's
# left to do.

# %%
fig, axes = plt.subplots(1, 3, figsize=(14, 4.5))

axes[0].imshow(label_to_rgb(GT), interpolation="nearest")
axes[0].set_title("ground truth")
axes[0].axis("off")

axes[1].imshow(label_to_rgb(OUTPUT), interpolation="nearest")
axes[1].set_title("output")
axes[1].axis("off")
# Outline every done tile.
for (r, c), v in np.ndenumerate(done_grid):
    if v:
        axes[1].add_patch(mpatches.Rectangle(
            (c * BLOCK - 0.5, r * BLOCK - 0.5), BLOCK, BLOCK,
            fill=False, edgecolor="yellow", linewidth=1.0,
        ))

im = axes[2].imshow(done_grid, cmap="Greens", vmin=0, vmax=1, interpolation="nearest")
axes[2].set_title(f"done-marker grid (sum={int(done_grid.sum())})")
axes[2].set_xlabel("block col"); axes[2].set_ylabel("block row")
for (r, c), v in np.ndenumerate(done_grid):
    axes[2].text(c, r, str(int(v)), ha="center", va="center",
                 color="black" if v else "lightgray")
fig.colorbar(im, ax=axes[2], fraction=0.046)
fig.tight_layout()

plt.show()
