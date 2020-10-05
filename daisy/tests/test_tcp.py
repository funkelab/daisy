import unittest
from daisy.tcp import TCPServer, TCPClient, TCPMessage
from daisy.tcp.exceptions import NotConnected


class TestTCPConnections(unittest.TestCase):

    def test_single_connection(self):

        # create a server
        server = TCPServer()
        host, port = server.address

        # create a client and connect to server
        client = TCPClient(host, port)

        # send a message to the server
        client.send_message(TCPMessage("test worked"))

        # receive a message at server
        message = server.get_message()
        self.assertTrue(isinstance(message, TCPMessage))
        self.assertTrue("test" in message.payload)

        # send a reply to client
        message.stream.send_message(TCPMessage("okay thanks bye"))

        # receive the reply
        reply = client.get_message()
        self.assertTrue(isinstance(reply, TCPMessage))
        self.assertTrue("thanks" in reply.payload)

        # try to receive another message at server
        message = server.get_message(timeout=0)
        self.assertTrue(message is None)

        # try to receive another message at client
        reply = client.get_message(timeout=0)
        self.assertTrue(reply is None)

        # disconnect from server
        client.disconnect()

        # assert that client can no longer send messages
        with self.assertRaises(NotConnected):
            client.send_message(TCPMessage("this message can not arrive"))

        # assert that client can no longer receive messages
        with self.assertRaises(NotConnected):
            reply = client.get_message()

        # try to receive a message at server and assert there is none
        message = server.get_message(timeout=0)
        self.assertTrue(message is None)
