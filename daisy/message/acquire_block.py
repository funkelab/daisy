from .message import Message


class AcquireBlock(Message):
    def __init__(self, task_id):
        super().__init__()
        self.task_id = task_id
