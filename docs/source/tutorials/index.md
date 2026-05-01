# Tutorials

Each tutorial below is an executable Jupyter notebook generated from a `# %%`-cell Python file under [`examples/`](https://github.com/funkelab/daisy/tree/main/examples). They run end-to-end during the docs build, so the figures and counts you see are produced by the current daisy version on the build machine.

:::{admonition} Building tutorials locally
:class: note
After `uv sync --group docs`, run `uv run sphinx-build -b html docs/source docs/_build/html`. The first build executes every tutorial; subsequent builds reuse cached output via `jupyter-cache` and only re-run cells that changed.
:::

## Mutex watershed pipelines

The block-function vs. worker-function distinction comes up often, so we show the same pipeline in both modes plus their fault-tolerance counterparts.

- **{doc}`Block-function mode <mws_block_function>`** — one callable per block. Lowest overhead, no per-worker state.
- **{doc}`Worker-function mode <mws_worker_function>`** — zero-arg worker that drives its own `Client.acquire_block()` loop. Use when you have setup (model load, mmap, DB handle) to amortise across many blocks.
- **{doc}`Block-function mode with deliberate failure <mws_block_function_with_error>`** — same pipeline as block mode, with one block raising. Demonstrates retries, the post-run failure summary, and `.err` log files under `daisy_logs/`.
- **{doc}`Worker-function mode with deliberate failure <mws_worker_function_with_error>`** — same demo on the worker-function path.

## Multi-task pipelines

- **{doc}`Astronaut chaining example <astronaut_pipeline>`** — multi-task pipeline with explicit `upstream_tasks=[…]` dependencies, showing how downstream blocks wait on the upstream blocks whose write ROI they read.

## Stress / scaling

- **{doc}`Stress test <stress_test>`** *(rendered without execution)* — ~100k 1-D no-op blocks, used to measure scheduler throughput. Skipped during the docs build because it doesn't fit in the per-tutorial timeout.

:::{toctree}
:hidden:
:maxdepth: 1
:glob:

*
:::
