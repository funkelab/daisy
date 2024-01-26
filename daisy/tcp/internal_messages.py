from .tcp_message import TCPMessage


class InternalTCPMessage(TCPMessage):
    """TCP messages used only between :class:`TCPServer` and :class:`TCPClient`
    for internal communication.
    """

    pass


class NotifyClientDisconnect(InternalTCPMessage):
    """Message to be sent from a :class:`TCPClient` to :class:`TCPServer` to
    initiate a disconnect.
    """

    pass


class AckClientDisconnect(InternalTCPMessage):
    """Message to be sent from a :class:`TCPServer` to :class:`TCPClient` to
    confirm a disconnect, i.e., the server will no longer listen to messages
    received from this client and the associated stream can be closed.
    """

    pass
