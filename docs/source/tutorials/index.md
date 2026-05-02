# Tutorials

Each tutorial below is an executable Jupyter notebook generated from a `# %%`-cell Python file under [`examples/`](https://github.com/funkelab/daisy/tree/main/examples). They run end-to-end during the docs build, so the figures and counts you see are produced by the current daisy version on the build machine.

:::{admonition} Building tutorials locally
:class: note
After `uv sync --group docs`, run `uv run sphinx-build -b html docs/source docs/_build/html`. The first build executes every tutorial; subsequent builds reuse cached output via `jupyter-cache` and only re-run cells that changed.
:::

## Mutex watershed pipeline

A single tour through daisy's two dispatch modes (block-function and worker-function), the multiprocessing vs. serial runners, and the persistent done-marker behaviour that resumes partial runs.

- **{doc}`Mutex watershed walkthrough <mws>`** — four runs of the same task: clean execution, no-op re-run via the done-marker, deliberate block failure with retries, and resume-after-fix where only the previously-failing tile re-executes. Top-of-file flags (or `--mode` / `--multiprocessing` from the CLI) select between block- and worker-function dispatch.

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
