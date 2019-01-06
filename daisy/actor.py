class Actor():

    def __init__(self, actor_id, address, stream):
        self.actor_id = actor_id
        self.address = address
        self.stream = stream

    def __repr__(self):
        return "%d at %s:%d" % (
            self.actor_id,
            self.address[0],
            self.address[1])

