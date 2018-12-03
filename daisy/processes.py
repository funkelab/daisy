from subprocess import check_call, CalledProcessError

from tornado.ioloop import IOLoop
from multiprocessing import Process

import sys
import logging



def call(command, log_out, log_err):
    '''Run ``command`` in a subprocess, log stdout and stderr to ``log_out``
    and ``log_err``'''

    try:
        with open(log_out, 'w') as stdout:
            with open(log_err, 'w') as stderr:
                check_call(
                    ' '.join(command),
                    shell=True,
                    stdout=stdout,
                    stderr=stderr)
    except CalledProcessError as exc:
        raise Exception("Calling %s failed with return code %s, stderr in %s"%(
            ' '.join(command),
            exc.returncode,
            stderr.name))
    except KeyboardInterrupt:
        raise Exception("Cancelled by SIGINT")


def call_function(function, args, log_out, log_err):

    try:
        with open(log_out, 'w') as sys.stdout:
            with open(log_err, 'w') as sys.stderr:
                logger = logging.getLogger()
                logger.handlers = []
                logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
                function(*args)
    except Exception as exc:
        raise Exception("Function {} failed with recode {}, stderr in {}".format(
            function,
            exc.returncode,
            sys.stderr.name))
    except KeyboardInterrupt:
        raise Exception("Cancelled by SIGINT")


def spawn_process(command, log_out, log_err):
    '''Run ``command`` in a simultaneous subprocess, log stdout and stderr to ``log_out``
    and ``log_err``'''

    Process(target = call, args=(command, log_out, log_err)).start()


def spawn_function(function, args, log_out, log_err):
    '''Run ``function(args)`` in a simultaneous subprocess, log stdout and stderr to ``log_out``
    and ``log_err``'''

    Process(target=call_function, args=(function, args, log_out, log_err)).start()

