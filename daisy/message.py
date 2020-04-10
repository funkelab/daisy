import logging
logger = logging.getLogger(__name__)


class Message:
    # create subclasses of this class to send between
    # TCPClient and TCPServer. Must be picklable?
    def __init__(self, payload):
        self.payload = payload
