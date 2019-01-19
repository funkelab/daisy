class Worker():

    def __init__(self, worker_id, address, stream):
        self.worker_id = worker_id
        self.address = address
        self.stream = stream

    def __repr__(self):
        return "%d at %s:%d" % (
            self.worker_id,
            self.address[0],
            self.address[1])
