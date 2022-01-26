class TaskState:
    def __init__(self):
        self.started = False
        self.total_block_count = 0

        # counts correspond with BlockStatus
        # self.pending_count = 0
        self.ready_count = 0
        self.processing_count = 0
        self.completed_count = 0
        self.skipped_count = 0
        self.failed_count = 0
        self.orphaned_count = 0

    @property
    def pending_count(self):
        # Pending count can include orphaned nodes
        return self.total_block_count - (
            self.ready_count
            + self.completed_count
            + self.failed_count
            + self.orphaned_count
            + self.processing_count
        )

    def is_done(self):
        return (
            self.total_block_count
            - self.completed_count
            - self.failed_count
            - self.orphaned_count
        ) == 0

    def __str__(self):
        return (
            f"Started: {self.started}\n"
            f"Total Blocks: {self.total_block_count}\n"
            f"Ready: {self.ready_count}\n"
            f"Processing: {self.processing_count}\n"
            f"Pending: {self.pending_count}\n"
            f"Completed: {self.completed_count}\n"
            f"Skipped: {self.skipped_count}\n"
            f"Failed: {self.failed_count}\n"
            f"Orphaned: {self.orphaned_count}\n"
        )

    def __repr__(self):
        return str(self)
