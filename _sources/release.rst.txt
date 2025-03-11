Release notes
=============

.. _release_v1.2.2:

Release v1.2.2
------------

* bugfix: the serial server had would not return task states in some cases.

Release v1.2.1
--------------

Fix small bugs:

* Server.run_blockwise now returns the task state dict mapping task names to task states. This is useful if you ever want to know how many blocks failed, skipped, were orphaned etc.

* bugfix: more reliable log directory setter. We now no longer produce a daisy_logs directory as an artifact when running tests
* bugfix: serial server doesn't mark all blocks as success. If block marked as failed it is released as a failed block


Release v1.2
------------

* add support for MacOS and Windows (via supporting multiprocessing start method "spawn")
* add a Serial Server. run_blockwise convenience function can be run with multiprocessing=False
  class methods can now be used as worker start functions and process block functions

* bugfix: no longer double counts orphans in some cases

Release v1.1.1
--------------

* Fixes a bug where daisy would hang indefinitely if the check_function skipped all blocks in all available tasks resulting in downstream tasks never having their workers recruited.

Release v1.1
------------

* increased max message size from 2**16 to 2**64
* check every second for whether all blocks have finished in the server event loop
* use context manager within run_blockwise to ensure process terminates
* remove deep copy in the calls to shrink. This was significantly slowing down the scheduler with excessive copying

Release v1.0
------------

* Overhauled the internals of daisy to clearly separate the "Scheduler",
  "Worker", "Client", "Server" and "Task" components. Much of the functionality
  remains the same, with the main entry point being the `run_blockwise` function
  that takes some `Task` instances. Some code related to IO has been moved to
  `funlib.persistence` which includes `Array` and `Graph` classes.

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
