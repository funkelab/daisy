

from .parameter import Parameter

class Task():

    task_id = Parameter()

    def __init__(self, task_id=None, **kwargs):
        if task_id:
            self.task_id = task_id
        else:
            self.task_id = type(self).__name__

        for key in kwargs:
            value = kwargs[key]
            setattr(self, key, value)

    def init_from_config(self, global_config):
        if global_config == None:
            return
        if self.task_id not in global_config:
            return

        config = global_config[self.task_id]
        for key in config:
            if hasattr(self, key) and getattr(self, key) != None:
                continue
            value = config[key]
            setattr(self, key, value)

    def prepare(self):
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
            max_retries=4,
            fit='valid'
            ):

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
        return []





