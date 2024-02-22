from daisy.tcp import TCPMessage
import logging

logger = logging.getLogger(__name__)


class Message(TCPMessage):
    pass


class ExceptionMessage(Message):
    """A message representing an exception.

    Args:

        exception (:class:`Exception`):
            The exception to wrap into this message.
    """

    def __init__(self, exception):
        self.exception = exception
