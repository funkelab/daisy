from .cl_monitor import CLMonitor
from .server import Server
from .tcp import IOLooper
from threading import Thread, Event


def run_blockwise(tasks):

    stop_event = Event()

    IOLooper.clear()
    thread = Thread(target=_run_blockwise, args=(tasks, stop_event))
    thread.start()
    try:
        thread.join()
    except KeyboardInterrupt:
        stop_event.set()
        thread.join()


def _run_blockwise(tasks, stop_event):
    server = Server(stop_event=stop_event)
    cl_monitor = CLMonitor(server)  # noqa
    server.run_blockwise(tasks)
