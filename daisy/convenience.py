from .cl_monitor import CLMonitor
from .server import Server
from .tcp import IOLooper
from multiprocessing.pool import ThreadPool
from multiprocessing import Event


def run_blockwise(tasks):
    '''Schedule and run the given tasks.

        Args:
            list_of_tasks:
                The tasks to schedule over.

        Return:
            bool:
                `True` if all blocks in the given `tasks` were successfully
                run, else `False`
    '''

    stop_event = Event()

    IOLooper.clear()
    pool = ThreadPool(processes=1)
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
