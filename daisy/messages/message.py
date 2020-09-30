import logging
logger = logging.getLogger(__name__)


class Message:
    '''A message, to be sent between :class:`TCPServer` and :class:`TCPClient`.

    Args:

        payload (pickleable):
            The payload.

    Attributes:

        stream (:class:`TCPStream`):
            The stream the message was received from. Will be set by
            :class:`TCPStream` and is ``None`` for messages that have not been
            sent.
    '''
    def __init__(self, payload=None):
        self.payload = payload
        self.stream = None


class ExceptionMessage(Message):
    '''A message representing an exception.

    Args:

        exception (:class:`Exception`):
            The exception to wrap into this message.
    '''

    def __init__(self, exception):
        self.exception = exception
