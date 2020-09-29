from .parameter import Parameter, UNDEFINED_DAISY_PARAMETER
import copy


class Task:
    def __init__(
        self,
        task_id,
        total_roi,
        read_roi,
        write_roi,
        process_function=None,
        check_function=None,
        read_write_conflict=True,
        fit="valid",
        global_config=None,
        **kwargs,
    ):
        self.task_id = task_id

        class Object(object):
            pass

        # avoid naming conflict with user
        self._daisy = Object()
        self._daisy.total_roi = total_roi
        self._daisy.orig_total_roi = total_roi
        self._daisy.read_roi = read_roi
        self._daisy.write_roi = write_roi
        self._daisy.total_write_roi = self._daisy.total_roi.grow(
            -(write_roi.get_begin() - read_roi.get_begin()),
            -(read_roi.get_end() - write_roi.get_end()),
        )
        self._daisy.process_function = process_function
        self._daisy.read_write_conflict = read_write_conflict
        self._daisy.fit = fit

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
                        "Key %s is not in the Parameter list for Task %s"
                        % (key, self.task_id)
                    )

        # applying user input parameters
        for key in kwargs:
            if key in self.__daisy_params__:
                self.__daisy_params__[key].set(kwargs[key])
            else:
                raise RuntimeError(
                    "Key %s not found in "
                    "Parameter list for Task %s" % (key, self.task_id)
                )

        # finalize parameters
        for param in self.__daisy_params__:
            val = self.__daisy_params__[param].val
            if val is UNDEFINED_DAISY_PARAMETER:
                raise RuntimeError(
                    "Parameter %s of %s is unassigned! You can probably fix "
                    "this by passing in `default` such as `None`"
                    % (param, self.task_id)
                )
            setattr(self, param, val)

    def inheritParameters(self, current_class):
        """Recursively query and inherit `Parameter`s from base `Task`s.
        Parameters are copied to self.__daisy_params__ to be processed
        downstream.

        If duplicated, `Parameter`s of derived `Task`s will override ones
        from base `Task`s, even if it clears any default. This lets
        inherited `Task` to unset defaults and force user to input new
        values."""
        for b in current_class.__bases__:
            self.inheritParameters(b)

        for param in current_class.__dict__:
            if isinstance(current_class.__dict__[param], Parameter):
                if current_class.__dict__[param].val is not UNDEFINED_DAISY_PARAMETER:
                    self.__daisy_params__[param] = copy.deepcopy(
                        current_class.__dict__[param]
                    )
                else:
                    self.__daisy_params__[param] = current_class.__dict__[param]

    def requires(self):
        raise NotImplementedError()

    def prepare(self):
        raise NotImplementedError()
