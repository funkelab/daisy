Release notes
=============

.. _release_v0.2:

Release v0.2
------------

Major changes
~~~~~~~~~~~~~

* Use Tornado for worker-scheduler communication.
  Communication between scheduler and workers is now using ``tornado`` instead of ``dask`` to be more lightweight and reliable. Furthermore, a worker client is persistent across blocks, allowing it to request and receive multiple blocks to process on. This change is heavily motivated by the long queuing delay of ``lsf``/``slurm`` and bring-up delay of ``tensorflow``.
  Furthermore, the user now has an API for acquiring and releasing block, allowing them to write their own Python module implementation of workers.
  By :user:`Tri Nguyen <trivoldus28>`, :user:`Jan Junke <funkey>`

* Introduce :class:`Task` and :class:`Parameter`, and :func:`daisy.distribute` to execute :class:`Task` chain.

* Changed :class:`SharedGraphProvider` and :class:`SharedSubGraph` APIs, and added many new features  
  including ability to have directed and undirected graphs. For the mongo backend, also added the ability to
  store node/edge features in separate collections, and filter by node/edge feature values.

Notable features
~~~~~~~~~~~~~~~~

* :class:`Task` sub-ROI request.
  A sub-region of a task's available ``total_roi`` can be restricted/requested
  explicitly using the :func:`daisy.distribute` interface.

  Example:

  ::

    task_spec = {'task': mytask, 'request': [daisy.Roi((3,), (2,))]}
    daisy.distribute([task_spec])

  The ``request`` will be expanded to align with ``write_roi``.

* Multiple :class:`Task` targets.
  A single :func:`daisy.distribute` can execute multiple target tasks simultaneous.

  Example:
  ::

    task_spec0 = {'task': mytask0}
    task_spec1 = {'task': mytask1}
    daisy.distribute([task_spec0, task_spec1])

  Tasks' dependencies (shared or not) will be processed correctly.

* Periodic status report.
  Daisy gives a status report of running/finished tasks, blocks running/finished
  status, and an ETA based on the completion rate of the last 2 minutes.

* Z-order block-wise scheduling.


Maintenance
~~~~~~~~~~~

* Drop support for Python 3.4.x and 3.5.x.
  We have moved to using Python's ``asyncio`` capability as the sole backend for Tornado. Python 3.4.x does not have asyncio. While Python 3.5.x does have asyncio built-in, its implementation is buggy.
