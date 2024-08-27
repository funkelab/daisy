from .cl_monitor import CLMonitor
from .server import Server
from .serial_server import SerialServer
from .tcp import IOLooper
from multiprocessing.pool import ThreadPool
from multiprocessing import Event


def run_blockwise(tasks, multiprocessing=True):
    """Schedule and run the given tasks.

    Args:
        list_of_tasks:
            The tasks to schedule over.
        multiprocessing (bool):
            If `False`, all multiprocessing is avoided and blocks are processed
            sequentially. This is useful for debugging. This will only work for
            tasks with a `process_function` that takes a single block as input
            since a worker process would not be able to start a client and hook
            up to the server.

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

    if not multiprocessing:
        server = SerialServer()
        cl_monitor = CLMonitor(server)
        return server.run_blockwise(tasks)

    else:
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
