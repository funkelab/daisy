from .parameter import Parameter

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

    # Unique identifier for this task. By default it is set to the
    # (sub)class name, but can be overridden in the constructor
    task_id = Parameter()

    log_to_files = Parameter(default=False)
    log_to_stdout = Parameter(default=True)

    def __init__(self, task_id=None, **kwargs):
        '''Constructor for ``Task``. Should not be overridden by
        subclasses.

        Args:

            task_id(``string``, optional):

                Unique identifier of the task. Tasks that have the
                same ID are exactly the same task in the dependency
                graph.

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

        for key in kwargs:
            # TODO: we might want to check whether the class actually
            # specified these Parameters and not blindly setting them
            value = kwargs[key]
            setattr(self, key, value)

    def init_from_config(self, global_config):
        '''Daisy calls this function internally if a global config was
        passed in.

        These configurations have lower priority than the configs
        given in the constructor.
        '''

        if global_config == None:
            return
        if self.task_id not in global_config:
            return

        config = global_config[self.task_id]
        for key in config:
            if hasattr(self, key) and getattr(self, key) != None:
                # do not override config from __init__()
                continue
            value = config[key]
            setattr(self, key, value)

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

        self.total_roi = total_roi
        self.read_roi = read_roi
        self.write_roi = write_roi
        self.process_function = process_function
        self.read_write_conflict = read_write_conflict
        self.fit = fit
        self.num_workers = num_workers
        self.max_retries = max_retries

        if check_function is not None:
            try:
                self.pre_check, self.post_check = check_function
            except:
                self.pre_check = check_function
                self.post_check = check_function

        else:
            self.pre_check = lambda _: False
            self.post_check = lambda _: True

    def requires(self):
        ''' Subclasses override this to specify its dependencies as a
        list of ``Task``s'''
        return []
