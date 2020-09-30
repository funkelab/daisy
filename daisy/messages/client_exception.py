from .message import ExceptionMessage


class ClientException(ExceptionMessage):
    def __init__(self, exception, client_id):
        super().__init__(exception)
        self.client_id = client_id
