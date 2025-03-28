![daisy](docs/source/_static/daisy.jpg)
[![tests](https://github.com/funkelab/daisy/actions/workflows/tests.yaml/badge.svg)](https://github.com/funkelab/daisy/actions/workflows/tests.yaml)
[![ruff](https://github.com/funkelab/daisy/actions/workflows/ruff.yaml/badge.svg)](https://github.com/funkelab/daisy/actions/workflows/ruff.yaml)
[![mypy](https://github.com/funkelab/daisy/actions/workflows/mypy.yaml/badge.svg)](https://github.com/funkelab/daisy/actions/workflows/mypy.yaml)
[![pypi](https://github.com/funkelab/daisy/actions/workflows/publish.yaml/badge.svg)](https://pypi.org/project/daisy/)

# Daisy: A Blockwise Task Scheduler

> Blockwise task scheduler for processing large volumetric data

## What is Daisy?

Daisy is a library framework that facilitates distributed processing of big nD datasets across clusters of computers.
It combines the best of MapReduce/Hadoop (the ability to map a process function across elements) and Luigi (the ability to chain dependent tasks together) together in one lightweight and efficient package with a focus on processing nD datasets.

Daisy documentations are at https://funkelab.github.io/daisy

## Updates

### Daisy v1.0 is now available on PyPI!

- Install it now with `pip install daisy`
- Besides quality-of-life improvements, we have also refactored I/O-related utilities to `funlib.persistence` to make code maintenance easier. This includes everything that was in `daisy.persistence` along with `daisy.Array` and helper functions such as `daisy.open_ds`, and `daisy.prepare_ds`. 
    - Just run `pip install funlib.persistence`.
    - These functions, which provide an easy to use interface to common formats such as zarr, n5 and any other dask friendly format for arrays and interfaces for storing large spatial graphs in SQLite, PostgreSQL, or Files remain the same.

## Overview

Developed by researchers at HHMI Janelia and Harvard, the intention behind Daisy was to develop a scalable and fast distributed block-wise scheduler for processing very large (TBs to PBs) 3D/4D bio image datasets.
We needed a fast and scalable scheduler but also resilient to failures and recoverable/resumable from hardware errors.
Daisy should also be generalizable enough to support efficient processing of different tasks with different computation and input/output modalities.


### Daisy is lightweight

* Daisy uses high performance TCP/IP libraries for communications between the scheduler and workers.

* It minimizes network overheads by sending only coordinates and status checks. Daisy does not enforce the exact method of data transfers to/between workers so that maximum performance is achieved for different tasks.


### Daisy's API is easy-to-use and extensible

* Built on Python, Daisy provides an easy-to-use native interface for Python scripts useful for both simple and complex use cases.

* Simplistically, Daisy is a framework for `map`ping a function across independent sub-blocks in the dataset.

* More complex usages include specifying inter-block dependencies, inter-task dependencies, using Daisy's array interface and geometric graph interface.


### Daisy chains complex pipelines of tasks

* Inspired by powerful workflow management frameworks like [Luigi](https://github.com/spotify/luigi) for automating long running tasks and decreasing overall processing time through task pipelining, Daisy allows user to specify dependency between tasks, allowing for task chaining and running multiple tasks in a pipeline with dynamic concurrent per-block execution.

* For instance, Daisy can chain a `map` task and a `reduce` task to implement a `map-reduce` task for nD datasets. Of course, any other combinations of `map` and `reduce` tasks are composable.

* By tracking dependencies at the `block` level, tasks can be executed concurrently to maximize pipelining parallelism.


### Daisy is tuned for processing datasets with real-world units

* Daisy has a native inferface to represent of regions in a volume in real world units, easily handling voxel sizes, with convenience functions for region manipulation (intersection, union, growing or shrinking, etc.)


## Citing Daisy

To cite this repository please use the following bibtex entry:

```
@software{daisy2022github,
  author = {Tri Nguyen and Caroline Malin-Mayor and William Patton and Jan Funke},
  title = {Daisy: block-wise task dependencies for luigi.},
  url = {https://github.com/funkelab/daisy},
  version = {1.0},
  year = {2022},
}
```

In the above bibtex entry, the version number is intended to be that from [daisy/setup.py](/setup.py), and the year corresponds to the project's 1.0 release.
