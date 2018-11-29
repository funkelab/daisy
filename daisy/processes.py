from subprocess import check_call, CalledProcessError

from tornado.ioloop import IOLoop
from multiprocessing import Process

import dill
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


def call_pickled_function (payload, log_out, log_err):

    fun, args = dill.loads(payload)

    try:
        with open(log_out, 'w') as sys.stdout:
            with open(log_err, 'w') as sys.stderr:
                logger = logging.getLogger()
                logger.handlers = []
                logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
                fun(*args)
    except Exception as exc:
        raise Exception("Function {} failed with recode {}, stderr in {}".format(
            fun,
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

    # Process(target = function, args=(arg)).start()
    # print("function: {}".format(function))
    # print("arg: {}".format(arg))

    # dill is needed to run lambda defined functions
    payload = dill.dumps((function, args))
    Process(target = call_pickled_function, args=(payload, log_out, log_err)).start()

