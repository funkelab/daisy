import copy
import logging
import os

logger = logging.getLogger(__name__)


class Context:

    ENV_VARIABLE = "DAISY_CONTEXT"

    def __init__(self, **kwargs):

        self.__dict = dict(**kwargs)

    def copy(self):

        return copy.deepcopy(self)

    def to_env(self):

        return ":".join("%s=%s" % (k, v) for k, v in self.__dict.items())

    def __setitem__(self, k, v):

        k = str(k)
        v = str(v)

        if "=" in k or ":" in k:
            raise RuntimeError("Context variables must not contain = or :.")
        if "=" in v or ":" in v:
            raise RuntimeError("Context values must not contain = or :.")

        self.__dict[k] = v

    def __getitem__(self, k):

        return self.__dict[k]

    def get(self, k, v=None):

        return self.__dict.get(k, v)

    def __repr__(self):

        return self.to_env()

    @staticmethod
    def from_env():

        try:

            tokens = os.environ[Context.ENV_VARIABLE].split(":")

        except KeyError:

            logger.error("%s environment variable not found!", Context.ENV_VARIABLE)
            raise

        context = Context()

        for token in tokens:
            k, v = token.split("=")
            context[k] = v

        return context
