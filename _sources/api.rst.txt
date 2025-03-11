.. _sec_api:

API Reference
=============

.. automodule:: daisy

Convenience API
---------------

  .. autofunction:: run_blockwise

Block-wise Task Scheduling
--------------------------

  .. autoclass:: Block

  .. autoclass:: Scheduler

  .. autoclass:: Client

  .. autoclass:: Context

  .. autoclass:: Task

  .. autoclass:: DependencyGraph

Geometry
--------

Re-exported from `funlib.geometry`. We recommend importing directly from `funlib.geometry`
instead of using `daisy.Coordinate` and `daisy.Roi`.

Coordinate
^^^^^^^^^^
  .. autoclass:: Coordinate
    :members:

Roi
^^^
  .. autoclass:: Roi
    :members:


