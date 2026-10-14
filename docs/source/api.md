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

On the default surface `daisy.Roi` and `daisy.Coordinate` are
[`funlib.geometry`](https://github.com/funkelab/funlib.geometry)'s classes,
as in 1.x. The Rust-backed types below accept them wherever a Roi or
Coordinate is expected.

```{eval-rst}
.. autoclass:: daisy.v2.Roi
   :members:

.. autoclass:: daisy.v2.Coordinate
   :members:
```

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

## Progress observers

```{eval-rst}
.. autoclass:: daisy.JsonProgressObserver
   :members:
```
