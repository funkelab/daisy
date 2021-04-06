class ServerObserver:

    def __init__(self, server):

        self.server = server
        server.register_observer(self)

    def on_task_start(self, task_id, task_state):
        pass

    def on_acquire_block(self, task_id, task_state):
        pass

    def on_release_block(self, task_id, task_state):
        pass

    def on_task_done(self, task_id, task_state):
        pass

    def on_block_failure(self, block, exception, context):
        pass

    def on_server_exit(self):
        pass


class ServerObservee:

    def __init__(self):
        self.observers = []

    def register_observer(self, observer):
        self.observers.append(observer)

    def unregister_observer(self, observer):
        self.observers.remove(observer)

    def notify_task_start(self, task_id, task_state):
        for observer in self.observers:
            observer.on_task_start(task_id, task_state)

    def notify_acquire_block(self, task_id, task_state):
        for observer in self.observers:
            observer.on_acquire_block(task_id, task_state)

    def notify_release_block(self, task_id, task_state):
        for observer in self.observers:
            observer.on_release_block(task_id, task_state)

    def notify_task_done(self, task_id, task_state):
        for observer in self.observers:
            observer.on_task_done(task_id, task_state)

    def notify_block_failure(self, block, exception, context):
        for observer in self.observers:
            observer.on_block_failure(block, exception, context)

    def notify_server_exit(self):
        for observer in self.observers:
            observer.on_server_exit()
