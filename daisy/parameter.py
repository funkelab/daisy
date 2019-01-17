UNDEFINED_DAISY_PARAMETER = object()


class Parameter():

    def __init__(self, default=UNDEFINED_DAISY_PARAMETER):
        self.val = default
        self.user_spec = False

    def set(self, val):
        self.val = val
        self.user_spec = True
