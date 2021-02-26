from .cl_monitor import CLMonitor
from .server import Server
from .tcp import IOLooper
from threading import Thread


def run_blockwise(tasks):

    IOLooper.clear()
    thread = Thread(target=_run_blockwise, args=(tasks,))
    thread.start()
    thread.join()

def _run_blockwise(tasks):
    server = Server()
    cl_monitor = CLMonitor(server)
    server.run_blockwise(tasks)
