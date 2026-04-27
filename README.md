# daisy

Blockwise task scheduler for processing large volumetric data.

Daisy v2 is a Rust rewrite of the original [daisy](https://pypi.org/project/daisy/1.2.2/), with a focus on faster scheduling, deterministic block dependencies, and resumable runs via on-disk done markers. The Python API is built on a Rust core (`daisy-core`) exposed through PyO3 (`daisy._daisy`).

> **Upgrading from daisy 1.x?** v2 is a deliberate breaking release. Several 1.x APIs have moved or been removed (`daisy.persistence`, `daisy.Array`, `daisy.open_ds`, `daisy.prepare_ds`, the `funlib.geometry` Roi/Coordinate types). See [`CHANGELOG.md`](CHANGELOG.md) for the full list. If you depend on the 1.x surface, pin `daisy<2`.

## Install

```bash
pip install daisy
```

Requires Python ≥ 3.11. Prebuilt wheels are published for Linux, macOS, and Windows.

## Quick example

```python
import daisy

def process(block):
    # do work on block.read_roi, write to block.write_roi
    print(f"processing {block.block_id}: {block.write_roi}")

task = daisy.Task(
    task_id="example",
    total_roi=daisy.Roi((0,), (1000,)),
    read_roi=daisy.Roi((0,), (10,)),
    write_roi=daisy.Roi((0,), (10,)),
    process_function=process,
    read_write_conflict=False,
    max_workers=4,
)

daisy.run_blockwise([task])
```

For larger end-to-end examples (mutex watershed, multi-task pipelines, JSON progress observers, stress tests), see [`examples/`](examples/).

## Concepts

A **task** declares a `total_roi`, a `read_roi`, and a `write_roi`. Daisy tiles the total ROI into blocks at the stride of the write ROI, and dispatches blocks subject to the dependency constraints implied by overlap between read and write ROIs (within a task and across upstream/downstream tasks).

A **block function** receives one block at a time. A **worker function** takes zero arguments — daisy spawns one per worker slot, and the worker drives its own `daisy.Client` loop. Worker-function mode is the right choice when you have per-worker setup (model load, DB connection, mmap'd array) you want to amortise across many blocks.

## Documentation

Design and protocol documentation lives under [`docs/`](docs/):

- [`ARCHITECTURE.md`](docs/ARCHITECTURE.md) — system overview
- [`SCHEDULER.md`](docs/SCHEDULER.md) — dependency graph and ready surface
- [`PROTOCOL.md`](docs/PROTOCOL.md) — TCP wire format
- [`DONE_MARKERS.md`](docs/DONE_MARKERS.md) — on-disk persistence for resumable runs
- [`RUN_STATS.md`](docs/RUN_STATS.md) — per-worker and per-task statistics
- [`WORKER_POOL_COORDINATION.md`](docs/WORKER_POOL_COORDINATION.md) — resource budgets
- [`WORKER_SHUTDOWN_FLOWS.md`](docs/WORKER_SHUTDOWN_FLOWS.md) — shutdown sequencing
- [`ABANDONMENT.md`](docs/ABANDONMENT.md) — typestate model for task lifecycle

## License

MIT — see [`LICENSE`](LICENSE).
