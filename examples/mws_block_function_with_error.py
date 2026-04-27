# %% [markdown]
# # Block-function mode with a failing block
#
# Copy of `mws_block_function.py` that deliberately raises on one
# specific tile of the block grid. Everything else is identical:
# same GT, same affinities, same `NEIGHBORHOOD`, same `BLOCK=128`.
#
# What this example demonstrates:
#
# - `gerbera.run_blockwise` catches the exception in the Python wrapper
#   and still returns — the overall task is marked failed via the post-run
#   **Execution Summary** report, not via a raised exception from
#   `run_blockwise`.
# - The full traceback for each failing attempt is written to that
#   worker's `.err` file under `gerbera_logs/<task_id>/`.
# - Default `max_retries=2` means the scheduler retries the block twice
#   more before giving up, so you'll see three tracebacks per failing
#   block (possibly on different workers).
# - The failing tile in the output image shows up as a black square
#   (never written, stays at the `OUTPUT.fill(0)` value).
#
# Along the way we record per-attempt data (which tile, which worker,
# pass/fail, when) so we can visualise the retry behaviour.

# %%
import tempfile
import threading
import time
from pathlib import Path

import lsd_lite
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import mwatershed
import numpy as np
import zarr

import gerbera
import gerbera.logging as gl

# Write per-worker logs and the persistent done-marker to a fresh temp
# dir so the example doesn't accumulate `gerbera_logs/` and
# `gerbera-markers.zarr/` in the working directory across runs.
_TMP = Path(tempfile.mkdtemp(prefix="gerbera_block_function_with_error_"))
gl.set_log_basedir(_TMP / "logs")
print(f"output paths under: {_TMP}")

# %% [markdown]
# ## Debug configuration: tee worker output to terminal + file
#
# Gerbera's default log mode is `"file"` — worker-thread stdout and
# stderr during `process_block` land **only** in
# `gerbera_logs/<task_id>/worker_<slot>.{out,err}` and never reach the
# terminal. That matches daisy's behaviour and keeps a clean console on
# happy-path runs, but it's a bad fit when you're iterating on broken
# code.
#
# Switching to `"both"` tees every write so you see tracebacks inline
# in the notebook cell as the run happens, without giving up the log
# files. `"console"` would route to the terminal only and skip creating
# files altogether.

# %%
gl.set_log_mode("both")
# Structured warnings from gerbera's own logger — one per failed
# attempt — show up at WARNING. Raise to ERROR to silence them.
gl.set_log_level("WARNING")
# Render tracebacks with `rich`: source context per frame, ANSI colour,
# `show_locals=True` for variable values next to each frame. Falls back
# to plain when `rich` isn't installed.
gl.set_traceback_style("rich", show_locals=True)

# %% [markdown]
# ## Build a dummy 2D segmentation
#
# 40 random discs painted onto a 512×512 label image. Label 0 is
# background; each disc gets its own positive integer id.

# %%
H, W = 512, 512
N_DISCS = 40
BLOCK = 128
FAILING_TILE = (1, 2)  # (block_row, block_col) in the 4×4 grid
TASK_ID = "mws_with_error"
MARKER_PATH = _TMP / "markers.zarr" / TASK_ID


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

plt.figure(figsize=(5, 5))
plt.imshow(AFFINITIES[:3].transpose([1, 2, 0]) + 0.5, vmin=0, vmax=1)
plt.title("affinities (channels 0–2 as RGB)")
plt.axis("off")

# %% [markdown]
# ## Visualise the block tiling
#
# Gerbera tiles the image into `BLOCK × BLOCK` non-overlapping tiles
# with `read_roi == write_roi`. The tile we're about to deliberately
# fail is outlined in red so you can see exactly which chunk we're
# about to blow up.

# %%
fig, ax = plt.subplots(figsize=(5, 5))
ax.imshow(label_to_rgb(GT), interpolation="nearest")
for r in range(BLOCK, H, BLOCK):
    ax.axhline(r - 0.5, color="yellow", linewidth=1.0)
for c in range(BLOCK, W, BLOCK):
    ax.axvline(c - 0.5, color="yellow", linewidth=1.0)
fr, fc = FAILING_TILE
ax.add_patch(mpatches.Rectangle(
    (fc * BLOCK - 0.5, fr * BLOCK - 0.5), BLOCK, BLOCK,
    fill=False, edgecolor="red", linewidth=2.5, label=f"failing tile {FAILING_TILE}",
))
ax.legend(loc="upper right")
ax.set_title(f"{H // BLOCK} × {W // BLOCK} block grid, {BLOCK}×{BLOCK} each")
ax.axis("off")

# %% [markdown]
# ## `process_block` with a deliberate failure + per-attempt tracking
#
# Before doing any real work, check whether this block is the designated
# bad tile. If so, raise — simulating a corrupted chunk, a missing input
# file, a CUDA OOM, whatever. Gerbera's Python wrapper routes the
# traceback to stderr (which is redirected to this worker's `.err`) and
# re-raises so the Rust side marks the block failed.
#
# We also append one record per attempt to `ATTEMPTS` so we can plot
# retry behaviour after the run.

# %%
OUTPUT = np.zeros((H, W), dtype=np.uint32)

_worker_id_lock = threading.Lock()
_worker_ids: dict[int, int] = {}
_attempts_lock = threading.Lock()
ATTEMPTS: list[dict] = []  # one entry per process_block call


def _worker_index():
    tid = threading.get_ident()
    with _worker_id_lock:
        if tid not in _worker_ids:
            _worker_ids[tid] = len(_worker_ids)
        return _worker_ids[tid]


_t0 = time.perf_counter()


def _record(tile, worker, ok):
    with _attempts_lock:
        ATTEMPTS.append({
            "tile": tile,
            "worker": worker,
            "ok": ok,
            "t": time.perf_counter() - _t0,
        })


def process_block(block):
    off = block.write_roi.offset.to_list()
    shape = block.write_roi.shape.to_list()
    r0, c0 = off[0], off[1]
    rs, cs = shape[0], shape[1]
    tile_idx = (r0 // BLOCK, c0 // BLOCK)
    worker = _worker_index()

    if tile_idx == FAILING_TILE:
        _record(tile_idx, worker, ok=False)
        raise RuntimeError(
            f"simulated failure on tile {tile_idx} (block_id={block.block_id})"
        )

    tile = AFFINITIES[:, r0 : r0 + rs, c0 : c0 + cs].astype(np.float64)
    local = mwatershed.agglom(tile, offsets=NEIGHBORHOOD).astype(np.uint32)
    block_offset = block.block_id[1] * BLOCK * BLOCK
    OUTPUT[r0 : r0 + rs, c0 : c0 + cs] = np.where(local > 0, local + block_offset, 0)
    _record(tile_idx, worker, ok=True)


# %% [markdown]
# ## Run 1 — buggy `process_block`, one tile crashes
#
# `done_marker_path` points at a Zarr v3 array that gerbera maintains
# for us — every block that completes successfully is persisted there.
# The failing tile is *not* persisted, so on a later run gerbera will
# know exactly which blocks still need to execute.

# %%
def make_task(process_function):
    return gerbera.Task(
        task_id=TASK_ID,
        total_roi=gerbera.Roi([0, 0], [H, W]),
        read_roi=gerbera.Roi([0, 0], [BLOCK, BLOCK]),
        write_roi=gerbera.Roi([0, 0], [BLOCK, BLOCK]),
        process_function=process_function,
        read_write_conflict=False,
        max_workers=4,
        max_retries=2,
        done_marker_path=str(MARKER_PATH),
    )


OUTPUT.fill(0)
ATTEMPTS.clear()
_worker_ids.clear()
_t0 = time.perf_counter()

ok = gerbera.run_blockwise([make_task(process_block)], multiprocessing=True)
elapsed = time.perf_counter() - _t0
print(f"\nrun_blockwise returned ok={ok}")
print(f"elapsed={elapsed * 1e3:.1f} ms  |  attempts={len(ATTEMPTS)}  |  "
      f"output segments={len(np.unique(OUTPUT)) - 1}")

# %% [markdown]
# ## Debug from the worker log file
#
# Each worker's stdout/stderr during `process_block` is routed to
# `gerbera_logs/<task_id>/worker_<slot>.{out,err}`. The failing tile's
# traceback lands in whichever workers picked it up — across the three
# attempts that may be more than one.
#
# With `log_mode="both"` you should also have seen those same lines
# streamed into the notebook cell above when `run_blockwise` was
# executing — Jupyter captures `sys.stderr` writes. The log file is the
# durable record you can grep after the fact.

# %%
log_dir = _TMP / "logs" / TASK_ID

print("worker log files:")
for f in sorted(log_dir.glob("*.err")):
    print(f"  {f.name}: {f.stat().st_size} bytes")

# %% [markdown]
# Find the first worker that recorded a failure and dump its full
# `.err` file — this is the canonical "debug a failing block" workflow.

# %%
errfile = next(
    (f for f in sorted(log_dir.glob("*.err")) if f.stat().st_size > 0),
    None,
)
if errfile is None:
    print("(no failures recorded — nothing to debug)")
else:
    print(f"--- {errfile} ---")
    print(errfile.read_text())

# %% [markdown]
# ## What happened on each tile?
#
# Four views in one figure:
#
# 1. GT, for reference.
# 2. Output, with the failing tile outlined — its region is all zero
#    because no attempt ever wrote to it.
# 3. **Attempt grid** — number of times each tile was handed to
#    `process_block`. Successful tiles ran once; the failing tile ran
#    `max_retries + 1 = 3` times.
# 4. **Status grid** — final outcome per tile (green = success, red =
#    failed after retries).

# %%
attempt_counts = np.zeros((H // BLOCK, W // BLOCK), dtype=np.int16)
status_grid = np.zeros((H // BLOCK, W // BLOCK), dtype=np.int8)  # 0 unknown, 1 ok, 2 fail
for a in ATTEMPTS:
    r, c = a["tile"]
    attempt_counts[r, c] += 1
    # Last writer wins — a "fail then retry succeeds" tile would show as ok.
    status_grid[r, c] = 1 if a["ok"] else 2

from matplotlib.colors import ListedColormap
status_cmap = ListedColormap(["#222222", "#4caf50", "#e53935"])  # unknown / ok / fail

fig, axes = plt.subplots(1, 4, figsize=(18, 4.5))

axes[0].imshow(label_to_rgb(GT), interpolation="nearest")
axes[0].set_title("ground truth")
axes[0].axis("off")

axes[1].imshow(label_to_rgb(OUTPUT), interpolation="nearest")
axes[1].add_patch(mpatches.Rectangle(
    (fc * BLOCK - 0.5, fr * BLOCK - 0.5), BLOCK, BLOCK,
    fill=False, edgecolor="red", linewidth=2.0,
))
axes[1].set_title(f"output (failing tile {FAILING_TILE} outlined)")
axes[1].axis("off")

im2 = axes[2].imshow(attempt_counts, cmap="viridis", vmin=0, vmax=attempt_counts.max())
axes[2].set_title("attempts per tile")
for (r, c), v in np.ndenumerate(attempt_counts):
    axes[2].text(c, r, str(v), ha="center", va="center",
                 color="white" if v < attempt_counts.max() / 2 else "black")
axes[2].set_xlabel("block col"); axes[2].set_ylabel("block row")
plt.colorbar(im2, ax=axes[2], fraction=0.046)

axes[3].imshow(status_grid, cmap=status_cmap, vmin=0, vmax=2, interpolation="nearest")
axes[3].set_title("final status per tile")
axes[3].set_xlabel("block col"); axes[3].set_ylabel("block row")
legend_handles = [
    mpatches.Patch(color="#4caf50", label="ok"),
    mpatches.Patch(color="#e53935", label="failed"),
]
axes[3].legend(handles=legend_handles, loc="lower right", framealpha=0.9)

fig.tight_layout()

# %% [markdown]
# ## Timeline of every attempt
#
# Each marker is one `process_block` call: green circle = success, red
# × = failure. Rows are workers. You can see the retry attempts on the
# failing tile stretching out over time (the scheduler hands the failed
# block back out only after a recruit-workers tick, typically hundreds
# of ms later).

# %%
n_workers = max(a["worker"] for a in ATTEMPTS) + 1
fig, ax = plt.subplots(figsize=(11, 1.2 + 0.4 * n_workers))
for a in ATTEMPTS:
    marker = "o" if a["ok"] else "x"
    colour = "#4caf50" if a["ok"] else "#e53935"
    ax.scatter(a["t"], a["worker"], marker=marker, color=colour,
               s=80 if not a["ok"] else 40, linewidths=2 if not a["ok"] else 0)
ax.set_yticks(range(n_workers))
ax.set_yticklabels([f"w{i}" for i in range(n_workers)])
ax.set_xlabel("time since run_blockwise (s)")
ax.set_title(f"per-attempt timeline — {len(ATTEMPTS)} attempts, "
             f"{sum(1 for a in ATTEMPTS if not a['ok'])} failures")
ax.grid(axis="x", alpha=0.3)

failure_handle = plt.Line2D([], [], marker="x", color="#e53935", linestyle="",
                            markersize=10, markeredgewidth=2, label="failed")
success_handle = plt.Line2D([], [], marker="o", color="#4caf50", linestyle="",
                            markersize=7, label="ok")
ax.legend(handles=[success_handle, failure_handle], loc="upper right")
fig.tight_layout()

# %% [markdown]
# ## Inspect the persistent done-marker
#
# Every block that succeeded in run 1 was written to a Zarr v3 array at
# `gerbera-markers.zarr/<task_id>/`. We can `zarr.open` it like any
# other array — the cell at `(row, col)` is `1` if that tile is done.
# After run 1 we expect 15/16 tiles done, with the failing tile at 0.

# %%
done_after_run1 = np.asarray(zarr.open(str(MARKER_PATH), mode="r")[:])
print(f"marker after run 1: {int(done_after_run1.sum())} / "
      f"{done_after_run1.size} tiles done")
print(done_after_run1)

# %% [markdown]
# ## Fix the bug and rerun
#
# Pretend the traceback above pointed us at our bug, and we've patched
# `process_block`. Now we rerun the *same task* — gerbera consults the
# marker for every block before dispatching, so the 15 already-done
# tiles get skipped instantly and only the previously-failing tile
# actually executes. This is the production-grade resume-from-failure
# workflow: write your processor, run, fix what failed, re-run, only
# pay for what's actually new.

# %%
def process_block_fixed(block):
    """Fixed version — the buggy `if tile_idx == FAILING_TILE: raise`
    branch is gone."""
    off = block.write_roi.offset.to_list()
    shape = block.write_roi.shape.to_list()
    r0, c0 = off[0], off[1]
    rs, cs = shape[0], shape[1]
    tile_idx = (r0 // BLOCK, c0 // BLOCK)
    worker = _worker_index()

    tile = AFFINITIES[:, r0 : r0 + rs, c0 : c0 + cs].astype(np.float64)
    local = mwatershed.agglom(tile, offsets=NEIGHBORHOOD).astype(np.uint32)
    block_offset = block.block_id[1] * BLOCK * BLOCK
    OUTPUT[r0 : r0 + rs, c0 : c0 + cs] = np.where(local > 0, local + block_offset, 0)
    _record(tile_idx, worker, ok=True)


_run2_attempts_before = len(ATTEMPTS)
gerbera.run_blockwise([make_task(process_block_fixed)], multiprocessing=True)
run2_calls = len(ATTEMPTS) - _run2_attempts_before
print(f"\nrun 2 process_block calls: {run2_calls}  "
      f"(1 expected — only the previously-failing tile)")

# %% [markdown]
# ## Verify everything completed
#
# Marker is now 16/16. The output array has been filled in for the
# previously-blank tile too — no zeros where there used to be a hole.

# %%
done_after_run2 = np.asarray(zarr.open(str(MARKER_PATH), mode="r")[:])
print(f"marker after run 2: {int(done_after_run2.sum())} / "
      f"{done_after_run2.size} tiles done")

fig, axes = plt.subplots(1, 3, figsize=(14, 4.5))

axes[0].imshow(label_to_rgb(GT), interpolation="nearest")
axes[0].set_title("ground truth")
axes[0].axis("off")

axes[1].imshow(label_to_rgb(OUTPUT), interpolation="nearest")
axes[1].set_title(f"output after run 2  (segments={len(np.unique(OUTPUT)) - 1})")
axes[1].axis("off")

panel = np.concatenate([done_after_run1, done_after_run2], axis=1)
im = axes[2].imshow(panel, cmap="Greens", vmin=0, vmax=1, interpolation="nearest")
axes[2].axvline(done_after_run1.shape[1] - 0.5, color="black", linewidth=1.0)
axes[2].set_title(f"marker after run 1   |   after run 2")
axes[2].set_xticks([
    done_after_run1.shape[1] / 2 - 0.5,
    done_after_run1.shape[1] + done_after_run2.shape[1] / 2 - 0.5,
])
axes[2].set_xticklabels(["run 1", "run 2"])
axes[2].set_yticks(range(panel.shape[0]))
fig.colorbar(im, ax=axes[2], fraction=0.046)
fig.tight_layout()

plt.show()
