from .block import BlockStatus
from .context import Context
from .messages import (
    AcquireBlock,
    ClientException,
    ReleaseBlock,
    RequestShutdown,
    SendBlock,
    UnexpectedMessage)
from contextlib import contextmanager
from daisy.tcp import TCPClient
import logging

logger = logging.getLogger(__name__)


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
                    if block is None:
                        break
                    blockwise_process(block)
                    block.state = BlockStatus.SUCCESS  # (or FAILED)
    '''

    def __init__(
            self,
            context=None):
        '''Initialize a client and connect to the server.

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
        logger.debug("Client context: %s", self.context)

        self.host = self.context['hostname']
        self.port = int(self.context['port'])
        self.worker_id = int(self.context['worker_id'])
        self.task_id = self.context['task_id']

        # Make TCP Connection
        self.tcp_client = TCPClient(self.host, self.port)

    @contextmanager
    def acquire_block(self):
        '''API for client to get a new block.'''
        self.tcp_client.send_message(AcquireBlock(self.task_id))
        message = self.tcp_client.get_message()
        if isinstance(message, SendBlock):
            logger.debug("Received block %s", message.block.block_id)
            try:
                block = message.block
                yield block
            except Exception as e:
                self.tcp_client.send_message(
                        ClientException(e, self.worker_id))
                block.status = BlockStatus.FAILED
                raise e
            finally:
                self.release_block(block)
        elif isinstance(message, RequestShutdown):
            logger.debug("No more blocks for this client, disconnecting")
            self.tcp_client.disconnect()
            yield
        else:
            raise UnexpectedMessage(message)

    def release_block(self, block):
        logger.debug("Releasing block %s", block.block_id)
        self.tcp_client.send_message(ReleaseBlock(block))
