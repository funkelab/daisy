from .parameter import Parameter, UNDEFINED_DAISY_PARAMETER


class Task():
    '''``daisy.Task`` takes inspiration from ``luigi.Task``, where
    users can define a task (or a stage in the pipeline) and chain
    multiple tasks together to create a bigger dependency graph for
    Daisy to execute block-wise.

    One key difference with the ``daisy.Task`` framework is that it
    allows current task's blocks to execute based on partial results
    of the previous tasks, unlike in ``luigi`` where each task must
    fully completed before the next can be run.

    Key methods that should be implemented in subclasses are:

        prepare():

            Here a task prepares and acquires all resources it would
            need to run, such as reading config, opening inputs and
            creating output folders.

            This function will be run for all tasks prior to making
            the dependency graph, in the order of dependency.

            Within this function, the task must call ``schedule(args)``
            to setup and schedule key block-wise processing parameters.
            See ``schedule()`` below for more details.

        requires():

            Override this to define dependencies of this task. Should
            return a list of ``Task``s that this one depends on.
    '''

    log_to_files = Parameter(default=False)
    log_to_stdout = Parameter(default=True)

    def inheritParameters(self, current_class):
        '''Recursively query and inherit `Parameter`s from base `Task`s.
        Parameters are copied to self.__daisy_params__ to be processed
        downstream.

        If duplicated, `Parameter`s of derived `Task`s will override ones
        from base `Task`s, even if it clears any default. This lets
        inherited `Task` to unset defaults and force user to input new
        values.'''
        for b in current_class.__bases__:
            self.inheritParameters(b)

        for param in current_class.__dict__:
            if isinstance(current_class.__dict__[param], Parameter):
                self.__daisy_params__[param] = current_class.__dict__[param]

    def __init__(self, task_id=None, global_config=None, **kwargs):
        '''Constructor for ``Task``. Should not be overridden by
        subclasses.

        Args:

            task_id(``string``, optional):

                Unique identifier of the task. Tasks that have the
                same ID are exactly the same task in the dependency
                graph.

            global_config (``dict``, optional):

                If given, parameters of this task will be initialized with
                values from the given dictionary. Named arguments will have
                precedence. The dictionary should map task IDs to dictionaries
                with the parameter names and values. Example::

                    class FooTask(daisy.Task):
                        a = daisy.Parameter()

                    f = FooTask(global_config={'FooTask': { 'a': 42 } })

                would be equivalent to::

                    f = FooTask(a=42)

            kwargs:

                Initializing ``Parameter``s of the task. These have
                the highest priority, namely higher than global
                configurations.

        '''
        if task_id:
            self.task_id = task_id
        else:
            # default task ID is the class name
            self.task_id = type(self).__name__

        self.global_config = global_config
        self.__init_parameters(**kwargs)

    def __init_parameters(self, **kwargs):

        self.__daisy_params__ = {}
        self.inheritParameters(self.__class__)

        # apply global configuration (if given)
        if self.global_config and self.task_id in self.global_config:
            config = self.global_config[self.task_id]
            for key in config:
                if key in self.__daisy_params__:
                    self.__daisy_params__[key].set(config[key])
                else:
                    raise RuntimeError(
                            "Key %s is not in the Parameter list for Task %s" %
                            (key, self.task_id))

        # applying user input parameters
        for key in kwargs:
            if key in self.__daisy_params__:
                self.__daisy_params__[key].set(kwargs[key])
            else:
                raise RuntimeError(
                        "Key %s not found in "
                        "Parameter list for Task %s" %
                        (key, self.task_id))

        # finalize parameters
        for param in self.__daisy_params__:
            val = self.__daisy_params__[param].val
            if val is UNDEFINED_DAISY_PARAMETER:
                raise RuntimeError(
                    "Parameter %s of %s is unassigned! You can probably fix "
                    "this by passing in `default` such as `None`" %
                    (param, self.task_id))
            setattr(self, param, val)

    def prepare(self):
        '''Subclasses override this to perform setup actions'''
        raise NotImplementedError("Client task needs to implement prepare()")

    def schedule(
            self,
            total_roi,
            read_roi,
            write_roi,
            process_function,
            check_function=None,
            read_write_conflict=True,
            num_workers=1,
            max_retries=2,
            fit='valid'
            ):
        '''Configure necessary parameters for the scheduler to run this
        task. The arguments are the same as those in
        ```scheduler.run_blockwise()```'''

        class Object(object):
            pass

        # avoid naming conflict with user
        self._daisy = Object()
        self._daisy.total_roi = total_roi
        self._daisy.orig_total_roi = total_roi
        self._daisy.read_roi = read_roi
        self._daisy.write_roi = write_roi
        self._daisy.process_function = process_function
        self._daisy.read_write_conflict = read_write_conflict
        self._daisy.fit = fit
        self._daisy.num_workers = num_workers
        self._daisy.max_retries = max_retries

        if check_function is not None:
            try:
                self._daisy.pre_check, self._daisy.post_check = check_function
            except TypeError:
                self._daisy.pre_check = check_function
                self._daisy.post_check = check_function

        else:
            self._daisy.pre_check = lambda _: False
            self._daisy.post_check = lambda _: True

    def cleanup(self):
        '''Override this to perform any post-task cleanup action'''
        pass

    def requires(self):
        '''Subclasses override this to specify its dependencies as a
        list of ``Task``s'''
        return []

    def _periodic_callback(self):
        '''Daisy calls this function periodically while checking for status.
        Override it to perform periodic bookkeeping.'''
        pass
