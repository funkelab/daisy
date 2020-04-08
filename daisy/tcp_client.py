class TCPClient():
    def connect(host, port, worker_id):
        '''
        Args:
            host (str):
                The host to connect to.
            port(int):
                The port to connect to.
            worker_id(int):
                The id of the worker that the client is running on.
        '''
        pass

    def send(message):
        '''
        Args:
            message (Message):
                Message to send over the connection.

        Raises exception if there is no connection
        '''
        pass
