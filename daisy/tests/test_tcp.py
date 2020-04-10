import unittest

from daisy import Message
from daisy.tcp import TCPServer, TCPClient, StreamClosedError


class TestTCPConnections(unittest.TestCase):

    def test_single_connection(self):
        self.sent = False
        server = TCPServer()
        host, port = server.address
        client = TCPClient(host, port)
        client.send_message(Message("test worked"))

        message = server.get_message()
        self.assertTrue(isinstance(message, Message))
        self.assertTrue("test" in message.payload)
        message.stream.send_message(Message("okay thanks bye"))

        reply = client.get_message()
        self.assertTrue(isinstance(reply, Message))
        self.assertTrue("thanks" in reply.payload)

        message = server.get_message(timeout=0)
        self.assertTrue(message is None)

        reply = client.get_message(timeout=0)
        self.assertTrue(reply is None)

        # close the stream connecting server and client
        client.stream.stream.close()

        with self.assertRaises(StreamClosedError):
            reply = client.get_message()

        with self.assertRaises(StreamClosedError):
            message = server.get_message()
