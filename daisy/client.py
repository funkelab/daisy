from contextlib import contextmanager
import logging

from daisy.tcp import TCPClient, StreamClosedError 

from .context import Context
from .messages import Message, ExceptionMessage

logger = logging.getLogger(__name__)


'''
Proposed API:

    External:
        A context for acquire_block/release_block. To be used like `with
        acquire_block() as b:`

    Internal:
        acquire_block()
        release_block()
        worker_id()
        task_id()

        TODO: should there be a __del__ function to close the stream?
        TODO: Should there be some sort of async thing reading messages and
        queueing up blocks as well as handling other messages like "close"?
        Or are we okay waiting to close until the user calls
        client.acquire_block()?
'''

class Client():
    '''Client code that runs on a remote worker providing task management
    API for user code. It communicates with the scheduler through TCP/IP.

    Scheduler IP address, port, and other configurations are typically
    passed to ``Client`` through an environment variable named
    'DAISY_CONTEXT'.

    Example usage:

        def blockwise_process(block):
            ...

        def main():
            client = Client()
            while True:
                with client.acquire_block() as block:
                    if block == None:
                        break
                    blockwise_process(block)
                    block.state = Done (or Failed)
                    
    '''

    def __init__(
            self,
            context=None):
        '''Initialize TCP connection with the scheduler.

        Args:

            context (`class:daisy.Context`, optional):

                If given, will be used to connect to the scheduler. If not
                given, the context will be read from environment variable
                ``DAISY_CONTEXT``.

        '''
        logger.debug("Client init")
        self.context = context
        if self.context is None:
            self.context = Context.from_env()
        
        self.host = self.context.hostname
        self.port = self.context.port
        self.worker_id = self.context.worker_id
        self.task_id = self.context.task_id
        
        # Make TCP Connection
        self.tcp_client = TCPClient(self.host, self.port)
        # send handshake
        # TODO: Send handshake!
        self.send(
            Message("handshake!",
                data=self.worker_id))
    
    @contextmanager
    def acquire_block(self):
        '''API for client to get a new block.'''
        self.tcp_client.send_message(Message()) #TODO: Fill in message
        message = self.tcp_client.get_message() #TODO: timeout?
        # TODO: Make sure message is a block and extract the block!
        block = message.block
        try:
            yield block
        finally:
            self.release_block(block)

    def release_block(self, block):
        logger.debug("Releasing block {}".format(block.block_id))
        # TODO: create message
        self.tcp_client.send_message(Message(block))
        # TODO: Make sure all the appropriate info is in the message
