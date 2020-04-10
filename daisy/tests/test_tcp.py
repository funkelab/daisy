from daisy import TCPServer, TCPClient, Message
import unittest
import time


class TestTCPConnections(unittest.TestCase):
    def callback_func(self, server, message, stream, address):
        print("Got message %s from address %s:%d"
              % (message.payload, address[0], address[1]))
        self.sent = True

    def test_single_connection(self):
        self.sent = False
        server = TCPServer()
        server.register_message_callback(self.callback_func)
        host, port = server.get_address()
        tcp_client = TCPClient(host, port)
        tcp_client.send(Message("test worked"))

        while not self.sent:
            time.sleep(0.1)

        self.assertTrue(self.sent)
