import tqdm
import logging
from .server_observer import ServerObserver


class TqdmLoggingHandler:
    '''A logging handler that uses ``tqdm.tqdm.write`` in ``emit()``, such that
    logging doesn't interfere with tqdm's progress bar.

    Heavily inspired by the fantastic
    https://github.com/EpicWink/tqdm-logging-wrapper/
    '''

    def __init__(self, handler):
        self.handler = handler

    def __getattr__(self, item):
        return getattr(self.handler, item)

    def emit(self, record):
        msg = self.handler.format(record)
        tqdm.tqdm.write(msg, file=self.handler.stream)

    handle = logging.Handler.handle


class CLMonitor(ServerObserver):

    def __init__(self, server):
        super().__init__(server)
        self.progresses = {}

        self._wrap_logging_handlers()

    def _wrap_logging_handlers(self):
        '''This adds a TqdmLoggingHandler around each logging handler that has
        a TTY stream attached to it, so that logging doesn't interfere with the
        progress bar.

        Heavily inspired by the fantastic
        https://github.com/EpicWink/tqdm-logging-wrapper/
        '''

        logger = logging.root
        for i in range(len(logger.handlers)):
            if self._is_tty_stream_handler(logger.handlers[i]):
                logger.handlers[i] = TqdmLoggingHandler(logger.handlers[i])

    def _is_tty_stream_handler(self, handler):

        return (
            hasattr(handler, "stream")
            and hasattr(handler.stream, "isatty")
            and handler.stream.isatty()
        )

    def on_acquire_block(self, task_id, task_state):
        self._update_state(task_id, task_state)

    def on_release_block(self, task_id, task_state):
        self._update_state(task_id, task_state)

    def on_task_done(self, task_id):
        if task_id in self.progresses:
            self.progresses[task_id].set_description(task_id + " ✔")

    def _update_state(self, task_id, task_state):

        if task_id not in self.progresses:
            total = task_state.total_block_count
            self.progresses[task_id] = tqdm.tqdm(
                total=total,
                desc=task_id + " ▶",
                unit='blocks',
                leave=True)

        completed = task_state.completed_count
        delta = completed - self.progresses[task_id].n
        if delta > 0:
            self.progresses[task_id].set_postfix({
                '⧗': task_state.pending_count,
                '▶': task_state.processing_count,
                '✔': task_state.completed_count,
                '✗': task_state.failed_count,
                '∅': task_state.orphaned_count
            })
            self.progresses[task_id].update(delta)
