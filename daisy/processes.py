
import logging
from multiprocessing import Process
import os
from subprocess import check_call, CalledProcessError
import sys

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
        raise Exception("Canceled by SIGINT")


def call_function(function, args, env log_out, log_err):
    '''Run ``function(args)`` in a subprocess, log stdout and stderr to
    ``log_out`` and ``log_err``'''

    # passing env to Daisy client
    for e in env:
        os.environ[e] = envs[e]

    try:
        with open(log_out, 'w') as sys.stdout:
            with open(log_err, 'w') as sys.stderr:
                logger = logging.getLogger()
                logging.basicConfig(
                    level=logging.INFO,
                    format='%(levelname)s:%(name)s:%(message)s')
                function(*args)
    except Exception as exc:
        raise Exception(
                "Function {} failed with return code {}, stderr in {}"
                    .format(function, exc.returncode, sys.stderr.name))
            # from exc
    except KeyboardInterrupt:
        raise Exception("Canceled by SIGINT")


# def spawn_call(command, log_out, log_err):
#     '''Spawn ``command`` in a simultaneous subprocess, log stdout and stderr to ``log_out`` and ``log_err``'''
#     Process(target = call, args=(command, log_out, log_err)).start()


def spawn_function(function, args, env, log_out, log_err):
    '''Spawn ``function(args)`` in a simultaneous subprocess, log stdout
    and stderr to ``log_out`` and ``log_err``'''
    Process(target=call_function, args=(function, args, env, log_out, log_err))
        .start()

