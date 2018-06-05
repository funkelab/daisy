import luigi
import logging

logger = logging.getLogger(__name__)

class BlockDoneTarget(luigi.Target):

    def __init__(self, write_roi, check_function):
        self.write_roi = write_roi
        self.check_function = check_function

    def exists(self):
        return self.check_function(self.write_roi)
