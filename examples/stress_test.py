# %% [markdown]
# # Worker-restart stress test
#
# Three chained "do-nothing" tasks running 100 000 blocks each in
# **worker-function mode** (0-arg callable). Each worker connects to
# the server, loops acquiring blocks, and occasionally raises — which
# kills the worker thread. The runner respawns it (up to
# `max_worker_restarts`); the block that was in flight goes back to
# the ready queue and gets retried by another worker.
#
# Watch:
#
# 1. **`♻=N`** in each task's tqdm description — that's
#    `worker_failure_count`. It ticks up every time a worker dies
#    and gets respawned.
# 2. **`✔` filling**, with `✗` staying at zero. Worker death is a
#    transient failure for the in-flight block — `max_retries=2`
#    means a fresh worker re-runs it and it succeeds.
#
# Contrast with block-function mode (`def process(block): ...`),
# where raising marks just *that block* failed without touching the
# worker — `♻` would stay 0 and `✗` would tick up instead.

# %%
import random
import tempfile
import time
from pathlib import Path

import daisy.v2 as daisy
import daisy.logging as gl

_TMP = Path(tempfile.mkdtemp(prefix="daisy_stress_"))
gl.set_log_basedir(_TMP / "logs")
# Enable per-task done markers under _TMP/markers so partial progress
# survives across the retry loop below — abandoned runs leave behind a
# Zarr v3 array recording which blocks already completed, and the next
# call to `run_blockwise` skips those blocks via the pre-check.
daisy.set_done_marker_basedir(_TMP / "markers")
print(f"output paths under: {_TMP}")

# %% [markdown]
# ## Configuration

# %%
NUM_BLOCKS = 35_000
WORKER_DEATH_RATE = 1.0 / 3_000   # ~10 worker deaths per task
MAX_WORKERS = 4
MAX_WORKER_RESTARTS = 4         # unlikely to make it all the way through

# %% [markdown]
# ## The worker
#
# Connects via `DAISY_CONTEXT`, loops acquiring blocks, occasionally
# raises mid-loop. The `with client.acquire_block()` context manager
# marks the in-flight block FAILED on exit (so it's re-queued for
# retry), then re-raises — which propagates out of `worker()` and
# kills the thread. Rust spawns a new worker.

# %%
def worker():
    client = daisy.Client()
    while True:
        with client.acquire_block() as block:
            if block is None:
                return
            if random.random() < WORKER_DEATH_RATE:
                raise RuntimeError("Simulated worker crash")
            
def process(block):
    if random.random() < WORKER_DEATH_RATE:
        raise RuntimeError("Simulated block failure")

# %% [markdown]
# ## Three chained tasks
#
# `extract → predict → label`. Streaming dependency means downstream
# tasks start picking up blocks as soon as upstream produces them.

# %%
def make_task(task_id, upstream=None):
    return daisy.Task(
        task_id=task_id,
        total_roi=daisy.Roi([0], [NUM_BLOCKS]),
        read_roi=daisy.Roi([0], [1]),
        write_roi=daisy.Roi([0], [1]),
        process_function=process,
        read_write_conflict=False,
        max_workers=MAX_WORKERS,
        max_retries=2,
        max_worker_restarts=MAX_WORKER_RESTARTS,
        upstream_tasks=[upstream] if upstream is not None else None,
    )


extract = make_task("extract")
predict = make_task("predict", upstream=extract)
label   = make_task("label",   upstream=predict)

# %% [markdown]
# ## Run

# %%
completed = False
while not completed:
    t0 = time.perf_counter()
    completed = daisy.run_blockwise([extract, predict, label])
    elapsed = time.perf_counter() - t0
    print(f"\nrun_blockwise returned completed={completed}, total elapsed = {elapsed:.2f} s")
print(f"\nlogs and any failure tracebacks: {_TMP / 'logs'}")
