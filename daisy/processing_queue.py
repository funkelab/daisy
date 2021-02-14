import collections


class ProcessingQueue:
    """
    A helper class for the scheduler.

    Keeps track of:
    1) blocks that are ready to be scheduled
    2) blocks that are running
    3) how many times blocks have been retried
    """

    def __init__(self, num_roots=0, root_generator=None):
        self.ready_queue = collections.deque()
        self.processing_blocks = set()
        self.block_retries = collections.defaultdict(int)

        self.ready_roots = num_roots
        self.root_generator = root_generator

    @property
    def num_ready(self):
        return self.ready_roots + len(self.ready_queue)

    def get_next(self):
        if self.num_ready > 0:
            if self.ready_roots > 0:
                self.ready_roots -= 1
                return next(self.root_generator)
            else:
                return self.ready_queue.popleft()
        else:
            return None
