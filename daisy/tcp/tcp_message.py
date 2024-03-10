class TCPMessage:
    """A message, to be sent between :class:`TCPServer` and :class:`TCPClient`.

    Args:

        payload (pickleable):
            The payload.

    Attributes:

        stream (:class:`TCPStream`):
            The stream the message was received from. Will be set by
            :class:`TCPStream` and is ``None`` for messages that have not been
            sent.
    """

    def __init__(self, payload=None):

        self.payload = payload
        self.stream = None
