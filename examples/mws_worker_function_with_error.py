# %% [markdown]
# # Worker-function mode with a failing block
#
# Copy of `mws_worker_function.py` that deliberately raises on one
# specific tile of the block grid. Same GT, same affinities, same
# `NEIGHBORHOOD`, same `BLOCK=128`.
#
# What this example demonstrates:
#
# - In worker-function mode the exception happens **inside your own
#   `client.acquire_block()` loop**. `Client.acquire_block` (used as a
#   context manager) catches the exception, marks the block `FAILED`,
#   prints the traceback to stderr (→ this worker's `.err` file), and
#   re-raises. Our worker wraps the `with` block in a `try/except` so
#   the thread doesn't exit on a single bad block.
# - Default `max_retries=2` means the scheduler hands the bad block
#   back out up to two more times — possibly to a different worker each
#   time, which is visible on the plots below.
# - The failing tile's output region stays at its initial zero value.

# %%
import shutil
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
NUM_WORKERS = 4
FAILING_TILE = (1, 2)  # (block_row, block_col) in the 4×4 grid
TASK_ID = "mws_worker_with_error"
MARKER_PATH = Path("gerbera-markers.zarr") / TASK_ID
# Start from a clean marker so the example demonstrates the full
# fail → fix → resume cycle from scratch. In real usage you would
# *keep* the marker across runs.
if MARKER_PATH.exists():
    shutil.rmtree(MARKER_PATH)


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
# ## Worker function with a deliberate failure + per-attempt tracking
#
# The worker loops `acquire_block()`. Inside the `with` block we check
# the tile and raise for the designated bad one. Around the `with` we
# keep a `try/except` so the worker thread continues after a failed
# block rather than exiting.
#
# Every attempt (pass or fail) is appended to `ATTEMPTS` so we can plot
# retry behaviour and per-worker timelines after the run.

# %%
_worker_id_lock = threading.Lock()
_worker_slots: dict[int, int] = {}
_attempts_lock = threading.Lock()
ATTEMPTS: list[dict] = []

_t0 = time.perf_counter()


def _worker_slot():
    tid = threading.get_ident()
    with _worker_id_lock:
        if tid not in _worker_slots:
            _worker_slots[tid] = len(_worker_slots)
        return _worker_slots[tid]


def _record(tile, worker, ok):
    with _attempts_lock:
        ATTEMPTS.append({
            "tile": tile,
            "worker": worker,
            "ok": ok,
            "t": time.perf_counter() - _t0,
        })


def expensive_model_load():
    time.sleep(0.05)


def worker():
    slot = _worker_slot()
    t_start = time.perf_counter()
    expensive_model_load()

    client = gerbera.Client()
    while True:
        try:
            with client.acquire_block() as block:
                if block is None:
                    break
                off = block.write_roi.offset.to_list()
                shape = block.write_roi.shape.to_list()
                r0, c0 = off[0], off[1]
                rs, cs = shape[0], shape[1]
                tile_idx = (r0 // BLOCK, c0 // BLOCK)

                if tile_idx == FAILING_TILE:
                    _record(tile_idx, slot, ok=False)
                    raise RuntimeError(
                        f"simulated failure on tile {tile_idx} "
                        f"(block_id={block.block_id})"
                    )

                tile = AFFINITIES[:, r0 : r0 + rs, c0 : c0 + cs].astype(np.float64)
                local = mwatershed.agglom(tile, offsets=NEIGHBORHOOD).astype(np.uint32)
                block_offset = block.block_id[1] * BLOCK * BLOCK
                OUTPUT[r0 : r0 + rs, c0 : c0 + cs] = np.where(
                    local > 0, local + block_offset, 0
                )
                _record(tile_idx, slot, ok=True)
        except Exception:
            # `Client.acquire_block` already marked the block FAILED,
            # released it, and printed the traceback to this worker's
            # `.err`. We just swallow here so the worker thread keeps
            # acquiring more blocks.
            continue


# %% [markdown]
# ## Run 1 — buggy `worker`, one tile crashes
#
# `done_marker_path` points at a Zarr v3 array that gerbera maintains
# for us — every block that completes successfully is persisted there.
# The failing tile is *not* persisted, so on a later run gerbera will
# know exactly which blocks still need to execute.

# %%
def make_task(worker_fn):
    return gerbera.Task(
        task_id=TASK_ID,
        total_roi=gerbera.Roi([0, 0], [H, W]),
        read_roi=gerbera.Roi([0, 0], [BLOCK, BLOCK]),
        write_roi=gerbera.Roi([0, 0], [BLOCK, BLOCK]),
        process_function=worker_fn,
        read_write_conflict=False,
        num_workers=NUM_WORKERS,
        max_retries=2,
        done_marker_path=str(MARKER_PATH),
    )


OUTPUT.fill(0)
ATTEMPTS.clear()
_worker_slots.clear()
_t0 = time.perf_counter()

ok = gerbera.run_blockwise([make_task(worker)], multiprocessing=True)
elapsed = time.perf_counter() - _t0
print(f"\nrun_blockwise returned ok={ok}")
print(f"elapsed={elapsed * 1e3:.1f} ms  |  attempts={len(ATTEMPTS)}  |  "
      f"output segments={len(np.unique(OUTPUT)) - 1}")

# %% [markdown]
# ## Debug from the worker log file
#
# Each worker's stdout/stderr during the `worker()` callable is routed
# to `gerbera_logs/<task_id>/worker_<slot>.{out,err}`. Because this is
# worker-function mode, the failure happens inside our own
# `client.acquire_block()` loop; `Client.acquire_block` prints the
# traceback to stderr (→ the worker's `.err` file) before re-raising.
#
# With `log_mode="both"` the same tracebacks also streamed into the
# notebook cell above as `run_blockwise` executed.

# %%
log_dir = Path("gerbera_logs/mws_worker_with_error")

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
# Four views:
#
# 1. GT, for reference.
# 2. Output, with the failing tile outlined.
# 3. **Attempt grid** — number of times each tile passed through a
#    worker. Successful tiles count 1; the failing tile accumulates
#    `max_retries + 1 = 3` attempts.
# 4. **Status grid** — final outcome per tile (green = success, red =
#    failed after retries).

# %%
attempt_counts = np.zeros((H // BLOCK, W // BLOCK), dtype=np.int16)
status_grid = np.zeros((H // BLOCK, W // BLOCK), dtype=np.int8)
for a in ATTEMPTS:
    r, c = a["tile"]
    attempt_counts[r, c] += 1
    status_grid[r, c] = 1 if a["ok"] else 2

from matplotlib.colors import ListedColormap
status_cmap = ListedColormap(["#222222", "#4caf50", "#e53935"])

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
# ## Per-worker ok / fail counts
#
# Bar chart comparing successful and failed attempts per worker. Shows
# how the retries for the failing tile were (or weren't) spread across
# workers.

# %%
n_workers = max(a["worker"] for a in ATTEMPTS) + 1
ok_counts = np.zeros(n_workers, dtype=int)
fail_counts = np.zeros(n_workers, dtype=int)
for a in ATTEMPTS:
    if a["ok"]:
        ok_counts[a["worker"]] += 1
    else:
        fail_counts[a["worker"]] += 1

fig, ax = plt.subplots(figsize=(8, 3.5))
x = np.arange(n_workers)
width = 0.4
worker_colours = [plt.cm.tab10(i) for i in range(n_workers)]

# Same colour for both bars of a worker; failed bar is hatched + alpha'd
# so the ok/fail distinction shows without breaking the per-worker colour.
ax.bar(x - width / 2, ok_counts, width,
       color=worker_colours, edgecolor="black")
ax.bar(x + width / 2, fail_counts, width,
       color=worker_colours, edgecolor="black",
       hatch="////", alpha=0.55)
for i, (o, f) in enumerate(zip(ok_counts, fail_counts)):
    if o:
        ax.text(i - width / 2, o, str(o), ha="center", va="bottom", fontsize=9)
    if f:
        ax.text(i + width / 2, f, str(f), ha="center", va="bottom", fontsize=9)
ax.set_xticks(x)
ax.set_xticklabels([f"w{i}" for i in range(n_workers)])
ax.set_ylabel("blocks")
ax.set_title(f"per-worker ok/fail counts "
             f"(total ok={ok_counts.sum()}, fail={fail_counts.sum()})")

# Legend entries are colour-neutral so only the ok/fail meaning is conveyed.
ok_handle = mpatches.Patch(facecolor="lightgrey", edgecolor="black", label="ok")
fail_handle = mpatches.Patch(facecolor="lightgrey", edgecolor="black",
                             hatch="////", alpha=0.55, label="failed")
ax.legend(handles=[ok_handle, fail_handle])
fig.tight_layout()

# %% [markdown]
# ## Timeline of every attempt
#
# Each marker is one `process_block` call: green circle = success, red
# × = failure. Rows are workers. The retry gap on the failing tile is
# visible — after a failure the scheduler only reoffers the block on
# the next health-check tick (~500 ms), so you see the retries spaced
# out rather than back-to-back.

# %%
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
             f"{fail_counts.sum()} failures")
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
# `worker`. The fixed version drops the `if tile_idx == FAILING_TILE:
# raise` branch. When `run_blockwise` starts, gerbera consults the
# marker for every block before dispatching: the 15 already-done tiles
# are skipped — `acquire_block` returns the next *not-yet-done* block
# directly — and only tile `(1, 2)` actually executes. This is the
# production-grade resume-from-failure workflow: write your worker,
# run, fix what failed, re-run, only pay for what's actually new.

# %%
def worker_fixed():
    """Fixed version of the worker — no `if tile_idx == FAILING_TILE: raise`."""
    slot = _worker_slot()
    expensive_model_load()

    client = gerbera.Client()
    while True:
        try:
            with client.acquire_block() as block:
                if block is None:
                    break
                off = block.write_roi.offset.to_list()
                shape = block.write_roi.shape.to_list()
                r0, c0 = off[0], off[1]
                rs, cs = shape[0], shape[1]
                tile_idx = (r0 // BLOCK, c0 // BLOCK)

                tile = AFFINITIES[:, r0 : r0 + rs, c0 : c0 + cs].astype(np.float64)
                local = mwatershed.agglom(tile, offsets=NEIGHBORHOOD).astype(np.uint32)
                block_offset = block.block_id[1] * BLOCK * BLOCK
                OUTPUT[r0 : r0 + rs, c0 : c0 + cs] = np.where(
                    local > 0, local + block_offset, 0
                )
                _record(tile_idx, slot, ok=True)
        except Exception:
            continue


_run2_attempts_before = len(ATTEMPTS)
gerbera.run_blockwise([make_task(worker_fixed)], multiprocessing=True)
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
axes[2].set_title("marker after run 1   |   after run 2")
axes[2].set_xticks([
    done_after_run1.shape[1] / 2 - 0.5,
    done_after_run1.shape[1] + done_after_run2.shape[1] / 2 - 0.5,
])
axes[2].set_xticklabels(["run 1", "run 2"])
axes[2].set_yticks(range(panel.shape[0]))
fig.colorbar(im, ax=axes[2], fraction=0.046)
fig.tight_layout()

plt.show()
