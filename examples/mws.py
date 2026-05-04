# %% [markdown]
# # Mutex watershed pipeline (block- and worker-function modes)
#
# A connectomics-flavoured pipeline:
#
# 1. Generate short- and long-range nearest-neighbour affinities from a
#    ground-truth segmentation with `lsd_lite.get_affs` (in a real
#    pipeline these would come from a UNet — we derive them from GT so
#    the example is self-contained).
# 2. Run `mwatershed.agglom` blockwise, driven by daisy.
#
# The example walks through four runs that demonstrate daisy's two
# dispatch modes and its persistent done-marker behaviour:
#
# - **Run 1** — clean process function, every block runs, output matches
#   GT structure.
# - **Run 2** — same task again. Daisy consults the persistent
#   done-marker and skips every block.
# - **Run 3** — `task.reset()` clears the marker; a buggy process
#   function then fails on one tile. After `max_retries=2` retries the
#   block is permanently failed; the other tiles complete normally.
# - **Run 4** — fixed process function, no reset. Only the previously
#   failing tile re-executes; the 15 already-succeeded tiles are
#   skipped via the marker. This is the production resume-from-failure
#   workflow.
#
# ## Configuration
#
# Two flags select the dispatch mode and the runner. Override from the
# CLI with `--mode {block_function|worker_function}` and
# `--multiprocessing / --no-multiprocessing`. The defaults work
# unchanged in a notebook (`parse_known_args` ignores Jupyter's own
# argv noise).

# %%
import argparse

_parser = argparse.ArgumentParser(add_help=False)
_parser.add_argument(
    "--mode", choices=["block_function", "worker_function"], default=None,
)
_parser.add_argument(
    "--multiprocessing", action=argparse.BooleanOptionalAction, default=None,
)
_args, _ = _parser.parse_known_args()

MODE = _args.mode or "block_function"
MULTIPROCESSING = True if _args.multiprocessing is None else _args.multiprocessing

if MODE == "worker_function" and not MULTIPROCESSING:
    raise SystemExit(
        "MODE='worker_function' requires MULTIPROCESSING=True — the serial "
        "runner only understands per-block callbacks."
    )
print(f"MODE={MODE}  MULTIPROCESSING={MULTIPROCESSING}")

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

import daisy.v2 as daisy
import daisy.logging as gl

_TMP = Path(tempfile.mkdtemp(prefix="daisy_mws_"))
gl.set_log_basedir(_TMP / "logs")
# Tee worker stdout/stderr to both file and terminal so the run-3
# traceback is visible inline. Default `"file"` is the production-grade
# choice; `"both"` is the iteration-friendly one.
gl.set_log_mode("both")
print(f"output paths under: {_TMP}")

# %% [markdown]
# ## Build the input
#
# A 512×512 GT with 40 random discs, then short- and long-range
# nearest-neighbour affinities derived from it. mutex watershed wants
# affinities centred at zero (positive merges, negative splits), so we
# subtract 0.5.

# %%
H, W = 512, 512
N_DISCS = 40
BLOCK = 128
NUM_WORKERS = 4
TASK_ID = "mws"
MARKER_PATH = _TMP / "markers.zarr" / TASK_ID
FAILING_TILE = (1, 2)  # block-row, block-col in the 4×4 grid

NEIGHBORHOOD = [
    [0, 1],   # right (short-range, attractive)
    [1, 0],   # down  (short-range, attractive)
    [0, 3],   # right-3 (long-range, repulsive)
    [3, 0],   # down-3  (long-range, repulsive)
]


def label_to_rgb(seg, bg=(0, 0, 0)):
    seg = np.asarray(seg).astype(np.int64)
    ids, inverse = np.unique(seg, return_inverse=True)
    rng = np.random.default_rng(42)
    colours = 0.2 + 0.8 * rng.random((ids.size, 3))
    for i, label in enumerate(ids):
        if label == 0:
            colours[i] = bg
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
AFFINITIES = lsd_lite.get_affs(GT, NEIGHBORHOOD, dist="equality").astype(np.float32) - 0.5
OUTPUT = np.zeros((H, W), dtype=np.uint32)

fig, axes = plt.subplots(1, 3, figsize=(13, 4))
axes[0].imshow(label_to_rgb(GT), interpolation="nearest")
axes[0].set_title(f"GT — {N_DISCS} discs")
axes[0].axis("off")
axes[1].imshow(AFFINITIES[:3].transpose([1, 2, 0]) + 0.5, vmin=0, vmax=1)
axes[1].set_title("affinities (channels 0–2 as RGB)")
axes[1].axis("off")
axes[2].imshow(label_to_rgb(GT), interpolation="nearest")
for r in range(BLOCK, H, BLOCK):
    axes[2].axhline(r - 0.5, color="yellow", linewidth=1.0)
for c in range(BLOCK, W, BLOCK):
    axes[2].axvline(c - 0.5, color="yellow", linewidth=1.0)
fr, fc = FAILING_TILE
axes[2].add_patch(mpatches.Rectangle(
    (fc * BLOCK - 0.5, fr * BLOCK - 0.5), BLOCK, BLOCK,
    fill=False, edgecolor="red", linewidth=2.5,
))
axes[2].set_title(
    f"{H // BLOCK}×{W // BLOCK} block grid (failing tile in red)"
)
axes[2].axis("off")
fig.tight_layout()

# %% [markdown]
# ## Define `process_block` and the worker loop
#
# `process_block(block)` does the real work for one tile. The
# `MODE="worker_function"` path wraps it in a zero-arg `worker()` that
# loops on `client.acquire_block()` — that's how daisy distinguishes
# the two shapes (`inspect.getfullargspec` on the callable's arity).
#
# Per-block label ids only have local meaning, so we shift each
# block's labels into a disjoint global range keyed on `block_id` —
# fully deterministic regardless of which worker picks which block up.

# %%
def _tile_idx(block):
    off = block.write_roi.offset.to_list()
    return (off[0] // BLOCK, off[1] // BLOCK)


def process_block(block):
    off = block.write_roi.offset.to_list()
    shape = block.write_roi.shape.to_list()
    r0, c0 = off[0], off[1]
    rs, cs = shape[0], shape[1]
    tile = AFFINITIES[:, r0 : r0 + rs, c0 : c0 + cs].astype(np.float64)
    local = mwatershed.agglom(tile, offsets=NEIGHBORHOOD).astype(np.uint32)
    block_offset = block.block_id[1] * BLOCK * BLOCK
    OUTPUT[r0 : r0 + rs, c0 : c0 + cs] = np.where(local > 0, local + block_offset, 0)


def process_block_buggy(block):
    if _tile_idx(block) == FAILING_TILE:
        raise RuntimeError(
            f"simulated failure on tile {FAILING_TILE} (block_id={block.block_id})"
        )
    process_block(block)


def expensive_model_load():
    """Stand-in for per-worker one-time setup (model, mmap, DB handle)."""
    time.sleep(0.05)


def make_worker(process_fn):
    """Return a zero-arg worker that drives its own block loop. Lets us
    reuse `process_block` in worker-function mode."""
    def worker():
        expensive_model_load()
        client = daisy.Client()
        while True:
            try:
                with client.acquire_block() as block:
                    if block is None:
                        break
                    process_fn(block)
            except Exception:
                # Per-worker logs already captured the traceback;
                # don't let one bad block kill the whole worker.
                pass

    return worker


def make_task(process_fn):
    py_fn = process_fn if MODE == "block_function" else make_worker(process_fn)
    return daisy.Task(
        task_id=TASK_ID,
        total_roi=daisy.Roi([0, 0], [H, W]),
        read_roi=daisy.Roi([0, 0], [BLOCK, BLOCK]),
        write_roi=daisy.Roi([0, 0], [BLOCK, BLOCK]),
        process_function=py_fn,
        read_write_conflict=False,
        max_workers=NUM_WORKERS,
        max_retries=2,
        done_marker_path=str(MARKER_PATH),
    )


# %% [markdown]
# ## Run helpers
#
# `run` invokes `run_blockwise`, snapshots `OUTPUT`, and prints the
# per-task counters. `show_run` plots the output snapshot alongside
# the on-disk done-marker so you can see what each run actually did
# (or didn't do) to both the in-memory output and the persistent
# state.

# %%
RUNS: list[dict] = []


def show_run(run_info):
    """Two-panel plot: output snapshot left, marker grid right. The
    failing tile is outlined in red on the output panel for visual
    reference."""
    fig, axes = plt.subplots(1, 2, figsize=(9, 4.5))

    axes[0].imshow(label_to_rgb(run_info["snapshot"]), interpolation="nearest")
    axes[0].add_patch(mpatches.Rectangle(
        (fc * BLOCK - 0.5, fr * BLOCK - 0.5), BLOCK, BLOCK,
        fill=False, edgecolor="red", linewidth=2.0,
    ))
    axes[0].set_title(
        f"output after {run_info['label']}\n"
        f"executed={run_info['executed']} skipped={run_info['skipped']} "
        f"failed={run_info['failed']}"
    )
    axes[0].axis("off")

    marker = np.asarray(zarr.open(str(MARKER_PATH), mode="r")[:]).reshape(
        H // BLOCK, W // BLOCK,
    )
    axes[1].imshow(marker, cmap="Greens", vmin=0, vmax=1, interpolation="nearest")
    axes[1].set_title(f"done-marker  ({int(marker.sum())}/{marker.size} tiles done)")
    axes[1].set_xlabel("block col")
    axes[1].set_ylabel("block row")
    for (r, c), v in np.ndenumerate(marker):
        axes[1].text(
            c, r, str(int(v)), ha="center", va="center",
            color="white" if v else "black",
        )

    fig.tight_layout()


def run(label, task):
    OUTPUT.fill(0)
    t0 = time.perf_counter()
    if MULTIPROCESSING:
        # `Server` returns the per-task `TaskState` dict; the top-level
        # `daisy.run_blockwise` function returns just a bool.
        states = daisy.Server().run_blockwise([task], progress=False)
    else:
        from daisy._runner import _run_serial
        states = _run_serial([task])
    elapsed = time.perf_counter() - t0
    s = states[TASK_ID]
    snap = OUTPUT.copy()
    # `skipped_count` is a flag on top of `completed_count`: a block
    # that was pre-skipped via the marker counts toward both. We split
    # them here so `executed` is unambiguously "blocks that ran".
    executed = s.completed_count - s.skipped_count
    RUNS.append({
        "label": label,
        "elapsed_ms": elapsed * 1e3,
        "executed": executed,
        "skipped": s.skipped_count,
        "failed": s.failed_count,
        "orphaned": s.orphaned_count,
        "snapshot": snap,
    })
    print(
        f"{label:24s}  executed={executed:>2}  "
        f"skipped={s.skipped_count:>2}  failed={s.failed_count:>2}  "
        f"elapsed={elapsed * 1e3:7.1f} ms"
    )


# %% [markdown]
# ## Run 1 — clean run
#
# First invocation. Every block executes; the marker fills in for all
# 16 tiles.

# %%
clean_task = make_task(process_block)
run("1: clean", clean_task)
assert RUNS[-1]["executed"] == 16
assert RUNS[-1]["skipped"] == 0
show_run(RUNS[-1])

# %% [markdown]
# ## Run 2 — re-run without resetting
#
# Same task, run again. Daisy's `set_log_basedir(...)` doubles as the
# auto-resolved done-marker basedir for tasks with the default
# `done_marker_path=None`; here we set the path explicitly so we can
# inspect it below. Either way, the scheduler's pre-check sees every
# block already done and short-circuits without ever calling
# `process_block`. The post-run summary shows `skipped=16`.

# %%
run("2: re-run, no reset", clean_task)
assert RUNS[-1]["skipped"] == 16
assert RUNS[-1]["executed"] == 0
show_run(RUNS[-1])
# Output panel is all background colour: `OUTPUT.fill(0)` zeroed it
# before the run, and no `process_block` ran. Marker is unchanged
# from run 1 (still 16/16) — that's the whole point of the skip.

# %% [markdown]
# ## Run 3 — reset, then run a buggy process function
#
# `task.reset()` deletes the on-disk marker so the next invocation
# really executes every block. We then swap in `process_block_buggy`,
# which raises on `FAILING_TILE`.
#
# Behavior diverges by runner here:
#
# - **Multiprocessing**: with `max_retries=2` the scheduler hands the
#   failing block back out twice more before marking it permanently
#   failed; the other 15 tiles complete normally.
# - **Serial**: the in-process debugging path is fail-fast. The
#   original `RuntimeError` propagates out of `run_blockwise` on the
#   first attempt — no retries, no buried Failed status in a post-run
#   summary. Runs 3 and 4 below only make sense for the mp runner; in
#   serial mode we just demonstrate the fail-fast behavior.

# %%
clean_task.reset()
buggy_task = make_task(process_block_buggy)

if MULTIPROCESSING:
    run("3: buggy, after reset", buggy_task)
    assert RUNS[-1]["failed"] == 1
    assert RUNS[-1]["executed"] == 15
    show_run(RUNS[-1])
    # The output panel has 15 tiles populated and one black square at
    # the failing tile (red outline). The marker shows 15/16 done —
    # the failed tile is still 0 so a future run will retry it.
else:
    try:
        run("3: buggy, after reset", buggy_task)
    except RuntimeError as e:
        print(f"3: buggy, after reset    raised RuntimeError: {e}")

# %% [markdown]
# ## Run 4 — fix the bug, resume
#
# Pretend the traceback above pointed us at our bug and we've patched
# the function. Re-running the task without `reset()` consults the
# marker first: the 15 successfully-completed tiles are skipped, only
# the previously-failing tile re-executes. This is the production
# resume-from-failure workflow — write your processor, run, fix what
# failed, re-run, only pay for what's actually new.
#
# Skipped in serial mode: the buggy run above raised before any block
# was marked done, so there's nothing partial to resume from.

# %%
if MULTIPROCESSING:
    fixed_task = make_task(process_block)
    run("4: fixed, resume", fixed_task)
    assert RUNS[-1]["executed"] == 1
    assert RUNS[-1]["skipped"] == 15
    show_run(RUNS[-1])
    # The marker is now 16/16: the previously-failing tile got marked
    # done by run 4. The output panel, by contrast, only shows the one
    # newly-executed tile filled in — `OUTPUT` is an in-memory numpy
    # array that we zero before each run, and skipped blocks (by
    # definition) never re-run their `process_block`. In a real
    # pipeline the output is usually a persistent zarr array so the
    # 15 already-written tiles survive across runs and a resume
    # produces a complete result.

plt.show()
