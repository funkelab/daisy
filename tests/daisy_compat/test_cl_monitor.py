import daisy
from daisy.cl_monitor import CLMonitor


def test_cl_monitor_keeps_the_server_it_was_given():
    """daisy 1.x callers construct a `CLMonitor(server)` for its side effect;
    in v2 it only holds the server (progress bars are built in)."""
    server = daisy.Server()
    assert CLMonitor(server).server is server
