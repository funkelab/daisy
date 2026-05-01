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

```{eval-rst}
.. autoclass:: daisy.Roi
   :members:

.. autoclass:: daisy.Coordinate
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
