from .server import Server
from .cl_monitor import CLMonitor


def run_blockwise(tasks):
    server = Server()
    cl_monitor = CLMonitor(server)
    server.run_blockwise(tasks)
