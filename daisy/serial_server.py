from .block import BlockStatus
from .scheduler import Scheduler
from .server_observer import ServerObservee
import logging

logger = logging.getLogger(__name__)


class SerialServer(ServerObservee):
    def __init__(self):
        super().__init__()

    def run_blockwise(self, tasks, scheduler=None):
        if scheduler is None:
            scheduler = Scheduler(tasks)
        else:
            scheduler = scheduler

        started_tasks = set()
        finished_tasks = set()
        all_tasks = set(task.task_id for task in tasks)
        process_funcs = {task.task_id: task.process_function for task in tasks}

        while True:
            ready_tasks = scheduler.get_ready_tasks()
            if finished_tasks == all_tasks:
                break
            else:
                block = None
                for ready_task in ready_tasks:
                    block = scheduler.acquire_block(ready_task.task_id)
                    if block is not None:
                        break
                if block is None:
                    break
                if block.task_id not in started_tasks:
                    self.notify_task_start(
                        block.task_id, scheduler.task_states[block.task_id]
                    )
                    started_tasks.add(block.task_id)
                self.notify_acquire_block(
                    block.task_id, scheduler.task_states[block.task_id]
                )
                process_funcs[block.task_id](block)
                block.status = BlockStatus.SUCCESS
                scheduler.release_block(block)
                self.notify_release_block(
                    block.task_id, scheduler.task_states[block.task_id]
                )

                if scheduler.task_states[block.task_id].is_done():
                    self.notify_task_done(
                        block.task_id, scheduler.task_states[block.task_id]
                    )
                    finished_tasks.add(block.task_id)
                    started_tasks.remove(block.task_id)
                    del process_funcs[block.task_id]

            if len(process_funcs) == 0:
                return True

        self.notify_server_exit()
