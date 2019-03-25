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

Coordinate
^^^^^^^^^^
  .. autoclass:: Coordinate
    :members:

Roi
^^^
  .. autoclass:: Roi
    :members:

Arrays
------

  .. autoclass:: Array

  .. autofunction:: open_ds

  .. autofunction:: prepare_ds

Graphs
------

Graph
^^^^^

  .. autoclass:: Graph
    :members:

MongoDbGraphProvider
^^^^^^^^^^^^^^^^^^^^

  .. automodule:: daisy.persistence

  .. autoclass:: MongoDbGraphProvider
    :members:
