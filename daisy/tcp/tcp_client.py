import tornado.tcpclient
import tornado.ioloop
from daisy.tcp_stream import TCPStream
from daisy import Context
import asyncio
import threading
import time


class TCPClient():
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.client = tornado.tcpclient.TCPClient()
        self.stream = None
        self.worker_id = None
        self.stream = None
        #self.context = Context.from_env()

        new_event_loop = asyncio.new_event_loop()
        asyncio._set_running_loop(new_event_loop)
        asyncio.set_event_loop(new_event_loop)
        self.ioloop = tornado.ioloop.IOLoop.current()
        t = threading.Thread(target=self.ioloop.start, daemon=True)
        t.start()

        self.ioloop.add_callback(self._connect)
        # wait until connected
        while not self.stream:
            time.sleep(.1)

    async def _connect(self):
        stream = await self.client.connect(self.host, self.port)
        self.stream = TCPStream(stream)

    def send(self, message):
        '''
        Args:
            message (Message):
                Message to send over the connection.

        Raises exception if there is no connection
        '''
        if not self.stream:
            raise Exception("No stream found. Did you call connect?")
        self.ioloop.add_callback(self.stream.send_message, message)
