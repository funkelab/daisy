from .server_observer import ServerObserver
from tqdm.auto import tqdm as tqdm_auto
import daisy.logging as daisy_logging
import logging
import tqdm


class TqdmLoggingHandler:
    """A logging handler that uses ``tqdm.tqdm.write`` in ``emit()``, such that
    logging doesn't interfere with tqdm's progress bar.

    Heavily inspired by the fantastic
    https://github.com/EpicWink/tqdm-logging-wrapper/
    """

    def __init__(self, handler):
        self.handler = handler

    def __getattr__(self, item):
        return getattr(self.handler, item)

    def emit(self, record):
        msg = self.handler.format(record)
        tqdm.tqdm.write(msg, file=self.handler.stream)

    handle = logging.Handler.handle


class TaskSummary:

    def __init__(self, state):

        self.block_failures = []
        self.state = state


class BlockFailure:

    def __init__(self, block, exception, worker_id):
        self.block = block
        self.exception = exception
        self.worker_id = worker_id

    def __repr__(self):
        return (
            f"block {self.block.block_id[1]} in worker "
            f"{self.worker_id} with exception {repr(self.exception)}"
        )


class CLMonitor(ServerObserver):

    def __init__(self, server):
        super().__init__(server)
        self.progresses = {}
        self.summaries = {}

        self._wrap_logging_handlers()

    def _wrap_logging_handlers(self):
        """This adds a TqdmLoggingHandler around each logging handler that has
        a TTY stream attached to it, so that logging doesn't interfere with the
        progress bar.

        Heavily inspired by the fantastic
        https://github.com/EpicWink/tqdm-logging-wrapper/
        """

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

    def on_block_failure(self, block, exception, context):

        task_id = block.block_id[0]
        self.summaries[task_id].block_failures.append(
            BlockFailure(block, exception, context["worker_id"])
        )

    def on_task_start(self, task_id, task_state):

        self.summaries[task_id] = TaskSummary(task_state)

    def on_task_done(self, task_id, task_state):

        if task_id not in self.summaries:
            self.summaries[task_id] = TaskSummary(task_state)
        else:
            self.summaries[task_id].state = task_state

        if task_id in self.progresses:
            if task_state.failed_count > 0:
                status_symbol = " ✗"
            elif task_state.orphaned_count > 0:
                status_symbol = " ∅"
            else:
                status_symbol = " ✔"
            self.progresses[task_id].set_description(task_id + status_symbol)
            self.progresses[task_id].close()

    def on_server_exit(self):

        for task_id, progress in self.progresses.items():
            progress.close()

        print()
        print("Execution Summary")
        print("-----------------")

        max_entries = 100

        for task_id, summary in self.summaries.items():

            num_block_failures = len(summary.block_failures)

            print()
            print(f"  Task {task_id}:")
            print()
            state = summary.state
            print(f"    num blocks : {state.total_block_count}")
            print(
                f"    completed ✔: {state.completed_count} "
                f"(skipped {state.skipped_count})"
            )
            print(f"    failed    ✗: {state.failed_count}")
            print(f"    orphaned  ∅: {state.orphaned_count}")
            print()

            if num_block_failures > 0:
                print("    Failed Blocks:")
                print()
                for block_failure in summary.block_failures[:max_entries]:
                    print(f"      {block_failure}")
                if num_block_failures > max_entries:
                    diff = len(summary.block_failures) - max_entries
                    print(f"      (and {diff} more)")

            if num_block_failures > 0:
                print()
                print("    See worker logs for details:")
                print()
                for block_failure in summary.block_failures[:10]:
                    log_basename = daisy_logging.get_worker_log_basename(
                        block_failure.worker_id, block_failure.block.block_id[0]
                    )
                    print(f"      {log_basename}.err / .out")
                if num_block_failures > 10:
                    print("      ...")

            if state.completed_count == state.total_block_count:
                print("    all blocks processed successfully")

    def _update_state(self, task_id, task_state):

        if task_id not in self.progresses:
            total = task_state.total_block_count
            self.progresses[task_id] = tqdm_auto(
                total=total, desc=task_id + " ▶", unit="blocks", leave=True
            )

        self.progresses[task_id].set_postfix(
            {
                "⧗": task_state.pending_count,
                "▶": task_state.processing_count,
                "✔": task_state.completed_count,
                "✗": task_state.failed_count,
                "∅": task_state.orphaned_count,
            }
        )

        completed = task_state.completed_count
        delta = completed - self.progresses[task_id].n
        self.progresses[task_id].update(delta)
