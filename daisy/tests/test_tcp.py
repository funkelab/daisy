import time
import unittest

from daisy import Message
from daisy.tcp import TCPServer, TCPClient, StreamClosedError


class TestTCPConnections(unittest.TestCase):

    def callback_func(self, server, message, stream, address):
        print("Got message %s from address %s:%d"
              % (message.payload, address[0], address[1]))
        self.sent = True

        stream.send_message(Message("okay thanks bye"))

    def test_single_connection(self):
        self.sent = False
        server = TCPServer()
        server.register_message_callback(self.callback_func)
        host, port = server.address
        tcp_client = TCPClient(host, port)
        tcp_client.send_message(Message("test worked"))

        while not self.sent:
            time.sleep(0.1)

        self.assertTrue(self.sent)

        reply = tcp_client.get_message()
        self.assertTrue(isinstance(reply, Message))
        self.assertTrue("thanks" in reply.payload)

        reply = tcp_client.get_message(timeout=0)
        self.assertTrue(reply is None)

        # close the stream connecting server and client
        tcp_client.stream.stream.close()

        with self.assertRaises(StreamClosedError):
            reply = tcp_client.get_message()
