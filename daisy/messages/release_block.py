from .message import Message


class ReleaseBlock(Message):
    def __init__(self, block):
        super().__init__()
        self.block = block
