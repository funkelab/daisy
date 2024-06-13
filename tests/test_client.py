import daisy
import unittest
import multiprocessing as mp
from daisy.messages import AcquireBlock, ReleaseBlock, SendBlock, ExceptionMessage
from daisy.tcp import TCPServer


def run_test_server(block, conn):
    server = TCPServer()
    conn.send(server.address)

    # handle first acquire_block message
    message = None
    for i in range(10):
        message = server.get_message(timeout=1)
        if message:
            break
    if not message:
        raise Exception("SERVER COULDN'T GET MESSAGE")
    try:
        assert isinstance(message, AcquireBlock)
        message.stream.send_message(SendBlock(block))
    except Exception as e:
        message.stream.send_message(ExceptionMessage(e))

    # handle return_block message
    message = server.get_message(timeout=1)
    try:
        assert isinstance(message, ReleaseBlock)
        assert message.block.status == daisy.BlockStatus.SUCCESS
    except Exception as e:
        message.stream.send_message(ExceptionMessage(e))
    conn.send(1)
    conn.close()


def test_basic():
    roi = daisy.Roi((0, 0, 0), (10, 10, 10))
    task_id = 1
    block = daisy.Block(roi, roi, roi, block_id=1, task_id=task_id)
    parent_conn, child_conn = mp.Pipe()
    server_process = mp.Process(target=run_test_server, args=(block, child_conn))
    server_process.start()
    host, port = parent_conn.recv()
    context = daisy.Context(hostname=host, port=port, task_id=task_id, worker_id=1)
    client = daisy.Client(context=context)
    with client.acquire_block() as block:
        block.status = daisy.BlockStatus.SUCCESS

    success = parent_conn.recv()
    server_process.join()
    assert success
