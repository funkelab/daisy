from .client_exception import ClientException


class BlockFailed(ClientException):

    def __init__(self, exception, block, context):

        super().__init__(exception, context)
        self.block = block
