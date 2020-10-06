from .message import Message


class SendBlock(Message):
    def __init__(self, block):
        super().__init__()
        self.block = block
