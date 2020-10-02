class Task:
    def __init__(
        self,
        total_roi,
        read_roi,
        write_roi,
        process_function,
        check_function=None,
        read_write_conflict=True,
        num_workers=1,
        max_retries=2,
        fit="valid",
        timeout=None,
        upstream_tasks=None,
    ):

        self.total_roi = total_roi
        self.orig_total_roi = total_roi
        self.read_roi = read_roi
        self.write_roi = write_roi
        self.total_write_roi = self.total_roi.grow(
            -(write_roi.get_begin() - read_roi.get_begin()),
            -(read_roi.get_end() - write_roi.get_end()),
        )
        self.process_function = process_function
        self.read_write_conflict = read_write_conflict
        self.fit = fit
        self.num_workers = num_workers
        self.max_retries = max_retries
        self.timeout = timeout

        self.upstream_tasks = []
        if upstream_tasks is not None:
            self.upstream_tasks.extend(upstream_tasks)

    def deps(self):
        return self.upstream_tasks
