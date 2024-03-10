import time


class BlockLog:

    def __init__(self, block, stream):
        self.block = block
        self.stream = stream
        self.time_sent = time.time()


class BlockBookkeeper:

    def __init__(self, processing_timeout=None):
        self.processing_timeout = processing_timeout
        self.sent_blocks = {}

    def notify_block_sent(self, block, stream):
        """Notify the bookkeeper that a block has been sent to a client (i.e.,
        a stream to the client)."""

        assert block.block_id not in self.sent_blocks, (
            f"Attempted to send block {block}, although it is already being "
            f"processed by {self.sent_blocks[block.block_id].stream}"
        )

        self.sent_blocks[block.block_id] = BlockLog(block, stream)

    def notify_block_returned(self, block, stream):
        """Notify the bookkeeper that a block was returned."""

        assert block.block_id in self.sent_blocks, (
            f"Block {block} was returned by {stream}, but is not in list "
            "of sent blocks"
        )

        log = self.sent_blocks[block.block_id]
        block.started_processing = log.time_sent
        block.stopped_processing = time.time()

        assert stream == log.stream, (
            f"Block {block} was returned by {stream}, but was sent to" f"{log.stream}"
        )

        del self.sent_blocks[block.block_id]

    def is_valid_return(self, block, stream):
        """Check whether the block from the given client (i.e., stream) is
        expected to be returned from this client. This is to avoid double
        returning blocks that have already been returned as lost blocks, but
        still come back from the client due to race conditions."""

        # block was never sent or already returned
        if block.block_id not in self.sent_blocks:
            return False

        # block is returned by different client than expected
        if self.sent_blocks[block.block_id].stream != stream:
            return False

        return True

    def get_lost_blocks(self):
        """Return a list of blocks that were sent and are lost, either because
        the stream to the client closed or the processing timed out. Those
        blocks are removed from the sent-list with the call of this
        function."""

        lost_block_ids = []
        for block_id, log in self.sent_blocks.items():

            # is the stream to the client still alive?
            if log.stream.closed():
                lost_block_ids.append(block_id)

            # did processing time out?
            if self.processing_timeout is not None:
                if time.time() - log.time_sent > self.processing_timeout:
                    lost_block_ids.append(block_id)

        lost_blocks = []
        for block_id in lost_block_ids:

            lost_block = self.sent_blocks[block_id].block
            lost_blocks.append(lost_block)
            del self.sent_blocks[block_id]

        return lost_blocks
