from .server import Server


def run_blockwise(tasks):
    server = Server()
    server.run_blockwise(tasks)
