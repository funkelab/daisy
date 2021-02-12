from .message import ExceptionMessage


class ClientException(ExceptionMessage):
    def __init__(self, exception, context):
        super().__init__(exception)
        self.context = context
