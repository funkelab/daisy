# Daisy: A Blockwise Task Scheduler

> Blockwise task scheduler for processing large volumetric data

## What is Daisy?
Daisy is a lightweight blockwise task scheduler for processing n-D volumes in parallel.
Daisy is not a mapreduce framework. It reduces overhead by only sending coordinates and status checks between the scheduler and the worker. Each worker is responsible for reading from and writing to data stores in specified locations.

## Features of Daisy:
 - Representations of regions in a volume in real world units, easily handling voxel sizes, with convenience functions for region manipulation (intersection, union, growing or shrinking, etc.)
 - Daisy array interface that can wrap any object with a shape attribute and slicing operations (zarr, hdf5, numpy array)
 - Daisy geometric graph interface with support for file system and MongoDB storage (nodes are locations in space, and can be accessed by region)
 - Easy specification of inter-block dependencies (can ensure one worker won't read from a region while another worker is writing to it)
 - Worker initialization once per task, rather than once per block, to reduce overhead
 - Task chaining for running multiple steps in a pipeline

## API:
See docs at https://daisy-doc.readthedocs.io

## Examples:
See examples folder for some basic examples of using daisy for blockwise processing.
