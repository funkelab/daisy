# Daisy: A Blockwise Task Scheduler

> Blockwise task scheduler for processing large volumetric data

## What is Daisy?

Daisy is a library framework that facilitates distributed processing of big nD datasets across clusters of computers.
It combines the best of MapReduce/Hadoop (the ability to map a process function across elements) and Luigi (the ability to chain dependent tasks together) together in one lightweight and efficient package with a focus on processing nD datasets.

Daisy documentations are at https://daisy-docs.readthedocs.io/

## Updates

### Daisy v1.0 is now available on PyPI!

- Install it now with `pip install -U daisy`
- Besides quality-of-life improvements, we have also refactored I/O-related utilities to `funlib.persistence` to make code maintenance easier. This includes everything that was in `daisy.persistence` along with `daisy.Array` and helper functions such as `daisy.open_ds`, and `daisy.prepare_ds`. 
    - Just run `pip install git+https://github.com/funkelab/funlib.persistence`.
    - These functions, which provide an easy to use interface to common formats such as zarr, n5, and hdf5 for arrays and interfaces for storing large spatial graphs in MongoDB or Files remain the same.

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


## Installation

```sh
pip install daisy
```

Alternatively, you can install from github for the latest development version:
```sh
pip install -e git+https://github.com/funkelab/daisy#egg=daisy
```

## Quickstart

See the following code in a [IPython notebook](/examples/daisy_quickstart.ipynb)!

### Map task

First, let's run a simple `map` task with Daisy. Supposed we have an array `a` that we want to compute the square for each element and store in `b`

```python
import numpy as np
shape = 4096000
a = np.arange(shape, dtype=np.int64)
b = np.empty_like(a, dtype=np.int64)
print(a)
# prints [0 1 2 ... 4095997 4095998 4095999]
```

We can use the following `process_fn`:

```python
def process_fn():
    # iterating and squaring each element in a and store to b
    with np.nditer([a, b],
                   op_flags=[['readonly'], ['readwrite']]) as it:
        with it:
           for x,y in it:
                y[...] = x**2
%timeit process_fn()  # 3.55 s ± 22.7 ms per loop
print(b)
# prints [0 1 4 ... 16777191424009 16777199616004 16777207808001]
```

Since `process_fn` linearly processes `a` in a single-thread, it is quite slow.
Let's use Daisy to break `a` into blocks and run `process_fn` in parallel.

First, we'll wrap `a` in a `daisy.Array` and make a `b` array based on `zarr` that multiple concurrent process can write to. We will also define `block_shape` - the granularity that each worker will be working at.

```python
import daisy
from funlib.persistence import Array
from funlib.geometry import Roi, Coordinate
import zarr
shape = 4096000
block_shape = 1024*16
# input array is wrapped in `Array` for easy of `Roi` indexing
a = Array(np.arange(shape, dtype=np.int64),
                roi=Roi((0,), shape),
                voxel_size=(1,))
# to parallelize across processes, we need persistent read/write arrays
# we'll use zarr here to do do that
b = zarr.open_array(zarr.TempStore(), 'w', (shape,),
                    chunks=(block_shape,),
                    dtype=np.int64)
# output array is wrapped in Array for easy of Roi indexing
b = Array(b,
                roi=Roi((0,), shape),
                voxel_size=(1,))
```

The `process_fn` is then modified slightly to take in a `block` object and perform read/write using the ROIs given by it.

```python
# same process function as previously, but with additional code
# to read and write data to persistent arrays
def process_fn_daisy(block):
    a_sub = a[block.read_roi].to_ndarray()
    b_sub = np.empty_like(a_sub)
    with np.nditer([a_sub, b_sub],
                   op_flags=[['readonly'], ['readwrite']],
                  ) as it:
        with it:
           for x,y in it:
                y[...] = x**2
    
    b[block.write_roi] = b_sub
```

Next, we define `total_roi` based on total amount of work (`shape`) and `block_roi` based on scheduling block size (`block_shape`). We then make a `daisy.Task` and run it.

```python
total_roi = Roi((0,), shape)  # total ROI to map process over
block_roi = Roi((0,), (block_shape,))  # block ROI for parallel processing
# creating a Daisy task, note that we do not specify how each
# worker should read/write to input/output arrays
task = daisy.Task(
    total_roi=total_roi,
    read_roi=block_roi,
    write_roi=block_roi,
    process_function=process_fn_daisy,
    num_workers=8,
    task_id='square',
)
daisy.run_blockwise([task])
'''
prints Execution Summary
-----------------
  Task square:
    num blocks : 250
    completed ✔: 250 (skipped 0)
    failed    ✗: 0
    orphaned  ∅: 0
    all blocks processed successfully
'''
# %timeit daisy.run_blockwise([task])  # 1.26 s ± 16.1 ms per loop
print(b.to_ndarray())
# prints [0 1 4 ... 16777191424009 16777199616004 16777207808001]
```

See that with just a minor modification, using Daisy to run multiple workers in parallel results in a 2.8176x speedups on a computer with 6 cores. For longer running tasks with larger block sizes (to minimize process spawning/joining overheads) the speedups should approach the # of threads/cores running in parallel more.

### Reduce task

Now we'll write and run a `reduce` task. This task performs a sum of blocks of shape `reduce_shape` from `b` and stores the results to `c`.

```python
import multiprocessing
reduce_shape = shape/16
# while using zarr with `Array` can be easier to understand and less error prone, it is not a requirement.
# Here we make a shared memory array for collecting results from different workers
c = multiprocessing.Array('Q', range(int(shape/reduce_shape)))
def process_fn_sum_reduce(block):
    b_sub = b[block.write_roi].to_ndarray()
    s = np.sum(b_sub)
    # compute c idx based on block offset and shape
    idx = (block.write_roi.offset / block.write_roi.shape)[0]
    c[idx] = s
total_roi = Roi((0,), shape)  # total ROI to map process over
block_roi = Roi((0,), reduce_shape)  # block ROI for parallel processing
task1 = daisy.Task(
    total_roi=total_roi,
    read_roi=block_roi,
    write_roi=block_roi,
    process_function=process_fn_sum_reduce,
    num_workers=8,
    task_id='sum_reduce',
)
daisy.run_blockwise([task1])
print(c[:])
```

This concludes our quickstart tutorial. For more examples/tutorials please see the [examples/](/examples) directory.


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
