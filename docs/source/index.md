# daisy

Blockwise task scheduler for processing large volumetric data.

Daisy v2 is a Rust rewrite of the original [daisy](https://pypi.org/project/daisy/1.2.2/), with a focus on faster scheduling, deterministic block dependencies, and resumable runs via on-disk done markers. The Python API is built on a Rust core (`daisy-core`) exposed through PyO3.

:::{admonition} Upgrading from daisy 1.x?
:class: tip
v2 is a deliberate breaking release. Several 1.x APIs have moved or been removed (`daisy.persistence`, `daisy.Array`, `daisy.open_ds`, `daisy.prepare_ds`, the `funlib.geometry` Roi/Coordinate types). See the [changelog](changelog.md) for the full list. If you depend on the 1.x surface, pin `daisy<2`.
:::

## Install

```bash
pip install daisy
```

Requires Python ≥ 3.11.

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

For full end-to-end pipelines (mutex watershed, multi-task chaining, JSON progress observers, fault-tolerance demos, stress tests), see the [tutorials](tutorials/index.md).

## Concepts

A **task** declares a `total_roi`, a `read_roi`, and a `write_roi`. Daisy tiles the total ROI into blocks at the stride of the write ROI, and dispatches blocks subject to the dependency constraints implied by overlap between read and write ROIs (within a task and across upstream/downstream tasks).

A **block function** receives one block at a time. A **worker function** takes zero arguments — daisy spawns one per worker slot, and the worker drives its own `daisy.Client` loop. Worker-function mode is the right choice when you have per-worker setup (model load, DB connection, mmap'd array) you want to amortise across many blocks.

:::{toctree}
:hidden:
:maxdepth: 2
:caption: User guide

tutorials/index
api
:::

:::{toctree}
:hidden:
:maxdepth: 1
:caption: Reference

design/index
changelog
:::
