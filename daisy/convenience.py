from .cl_monitor import CLMonitor
from .server import Server
from .tcp import IOLooper
from multiprocessing.pool import ThreadPool
from multiprocessing import Event


def run_blockwise(tasks):
    """Schedule and run the given tasks.

    Args:
        list_of_tasks:
            The tasks to schedule over.

    Return:
        bool:
            `True` if all blocks in the given `tasks` were successfully
            run, else `False`
    """
    task_ids = set()
    all_tasks = []
    while len(tasks) > 0:
        task, tasks = tasks[0], tasks[1:]
        if task.task_id not in task_ids:
            task_ids.add(task.task_id)
            all_tasks.append(task)
        tasks.extend(task.upstream_tasks)

    tasks = all_tasks
    stop_event = Event()

    IOLooper.clear()
    with ThreadPool(processes=1) as pool:
        result = pool.apply_async(_run_blockwise, args=(tasks, stop_event))
        try:
            return result.get()
        except KeyboardInterrupt:
            stop_event.set()
            return result.get()


def _run_blockwise(tasks, stop_event):
    server = Server(stop_event=stop_event)
    cl_monitor = CLMonitor(server)  # noqa
    return server.run_blockwise(tasks)
