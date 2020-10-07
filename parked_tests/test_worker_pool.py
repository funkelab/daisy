from time import sleep
import daisy
import daisy.tcp
import unittest
import logging
import subprocess

logging.basicConfig(level=logging.DEBUG)


class Request(daisy.Message):
    def __init__(self, request_id):
        self.request_id = request_id


class Answer(daisy.Message):
    def __init__(self, answer_id):
        self.answer_id = answer_id


class UnluckyNumberException(Exception):
    pass


faulty_worker_id = None
early_exit_worker_id = None


def basic_worker():
    while True:
        sleep(1)


def context_worker():

    context = daisy.Context.from_env()

    logging.debug("started worker %s", context['worker_id'])

    while True:
        sleep(1)


def error_worker():

    context = daisy.Context.from_env()

    logging.debug("started worker %s", context['worker_id'])

    if context['worker_id'] == str(faulty_worker_id):
        raise UnluckyNumberException(
            "%d is an unlucky number!" %
            faulty_worker_id)

    while True:
        sleep(1)


def early_exit_worker():

    context = daisy.Context.from_env()

    logging.debug("started worker %s", context['worker_id'])

    if context['worker_id'] == str(early_exit_worker_id):
        return

    while True:
        sleep(1)


def command_worker():
    subprocess.check_call(['python', __file__])


def message_worker():

    context = daisy.Context.from_env()
    worker_id = int(context['worker_id'])
    server_name = context['server_name']
    server_port = int(context['server_port'])

    logging.debug("started worker %d", worker_id)
    logging.debug("connecting to %s:%d", server_name, server_port)

    client = daisy.tcp.TCPClient(server_name, server_port)

    logging.debug("sending request...")
    client.send_message(Request(worker_id))
    logging.debug("waiting for reply...")
    reply = client.get_message()
    logging.debug("got reply %s", reply)
    assert isinstance(reply, Answer)
    assert reply.answer_id == worker_id + 1


class TestWorkerPool(unittest.TestCase):

    def test_basic(self):

        pool = daisy.WorkerPool(basic_worker)

        pool.set_num_workers(10)
        sleep(1)
        pool.check_for_errors()
        pool.set_num_workers(5)
        sleep(1)
        pool.check_for_errors()
        pool.set_num_workers(20)
        sleep(1)
        pool.check_for_errors()
        pool.stop()

    def test_context(self):

        context = daisy.Context(task_id=0)

        pool = daisy.WorkerPool(context_worker, context)

        pool.set_num_workers(10)
        sleep(1)
        pool.check_for_errors()
        pool.set_num_workers(5)
        sleep(1)
        pool.check_for_errors()
        pool.set_num_workers(20)
        sleep(1)
        pool.check_for_errors()
        pool.stop()

    def test_error(self):

        global faulty_worker_id
        faulty_worker_id = daisy.Worker.get_next_id() + 13

        context = daisy.Context(task_id=0)

        pool = daisy.WorkerPool(error_worker, context)

        pool.set_num_workers(10)
        sleep(1)
        pool.check_for_errors()
        pool.set_num_workers(5)
        sleep(1)
        pool.check_for_errors()
        pool.set_num_workers(20)
        sleep(1)
        with self.assertRaises(UnluckyNumberException) as cm:
            pool.check_for_errors()
        self.assertTrue('unlucky' in str(cm.exception))
        pool.stop()

    def test_early_exit(self):

        global early_exit_worker_id
        early_exit_worker_id = daisy.Worker.get_next_id() + 13

        context = daisy.Context(task_id=0)

        pool = daisy.WorkerPool(early_exit_worker, context)

        pool.set_num_workers(10)
        sleep(1)
        pool.check_for_errors()
        pool.set_num_workers(5)
        sleep(1)
        pool.check_for_errors()
        pool.set_num_workers(20)
        sleep(1)
        pool.check_for_errors()
        pool.stop()

    def test_command(self):

        context = daisy.Context(task_id=0)
        pool = daisy.WorkerPool(command_worker, context)

        pool.set_num_workers(10)
        sleep(1)
        pool.check_for_errors()
        pool.set_num_workers(5)
        sleep(1)
        pool.check_for_errors()
        pool.set_num_workers(20)
        sleep(1)
        pool.check_for_errors()
        pool.stop()

    def test_tcp_server(self):

        server = daisy.tcp.TCPServer()
        context = daisy.Context(
            task_id=0,
            server_name=server.address[0],
            server_port=server.address[1])
        pool = daisy.WorkerPool(message_worker, context)

        pool.set_num_workers(10)

        for _ in range(10):
            message = server.get_message()
            if isinstance(message, Request):
                message.stream.send_message(Answer(message.request_id + 1))
            pool.check_for_errors()


if __name__ == "__main__":

    sleep(100)
