# API reference

The `daisy` package re-exports the public surface from internal modules. All names below are accessible directly as `daisy.<name>`.

## Top-level helpers

```{eval-rst}
.. autofunction:: daisy.run_blockwise

.. autofunction:: daisy.set_done_marker_basedir

.. autofunction:: daisy.get_done_marker_basedir
```

## Tasks and blocks

```{eval-rst}
.. autoclass:: daisy.Task
   :members:

.. autoclass:: daisy.Block
   :members:

.. autoclass:: daisy.BlockStatus
   :members:

.. autoclass:: daisy.TaskState
   :members:
```

## Geometry

`daisy.Roi` and `daisy.Coordinate` are
[`funlib.geometry`](https://github.com/funkelab/funlib.geometry)'s classes,
as in 1.x. Every daisy entry point accepts them (or anything with integer
`offset` and `shape`), and every ROI daisy hands back is one.

## Scheduling

```{eval-rst}
.. autoclass:: daisy.Scheduler
   :members:

.. autoclass:: daisy.BlockwiseDependencyGraph
   :members:

.. autoclass:: daisy.DependencyGraph
   :members:
```

## Server / Client

```{eval-rst}
.. autoclass:: daisy.Server
   :members:

.. autoclass:: daisy.Client
   :members:

.. autoclass:: daisy.Context
   :members:
```

## Run report

```{eval-rst}
.. autoclass:: daisy.RunReport
   :members:

.. autoclass:: daisy.TaskReport
   :members:

.. autoclass:: daisy._report.BlockSummary
   :members:

.. autoclass:: daisy._report.WorkerSummary
   :members:

.. autoclass:: daisy._report.WorkerRecord
   :members:

.. autoclass:: daisy._report.BlockRecord
   :members:

.. autoclass:: daisy._report.Stat
   :members:
```

## Progress observers

```{eval-rst}
.. autoclass:: daisy.JsonProgressObserver
   :members:
```
