
import logging
from multiprocessing import Process
import os
from subprocess import check_call, CalledProcessError
import sys

def _freopen(filename, mode, fobj):
    """Redirects file descriptors
    Example: _freopen(log_out, 'w', sys.stdout)
    """
    new = open(filename, mode)
    newfd = new.fileno()
    targetfd = fobj.fileno()
    os.close(targetfd)
    os.dup2(newfd, targetfd)

def call(command, log_out, log_err):
    """Run ``command`` in a subprocess, log stdout and stderr to ``log_out``
    and ``log_err``"""
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


def call_function(function, args, env, log_out, log_err,
                  log_to_files, log_to_stdout):
    """Run ``function(args)`` in a subprocess, log stdout and stderr to
    ``log_out`` and ``log_in``"""

    # passing env to Daisy client
    for e in env:
        os.environ[e] = env[e]

    if log_to_files and not log_to_stdout:
        _freopen(log_out, 'w', sys.stdout)
        _freopen(log_err, 'w', sys.stderr)

    try:
        if log_to_files and log_to_stdout:
            # add redirection for logging
            logger = logging.getLogger()
            logger.addHandler(logging.FileHandler(log_out))
            # logger.handlers = []
            # logging.basicConfig(
            #     level=logging.INFO,
            #     filename=log_out,
            #     format='%(levelname)s:%(name)s:%(message)s')
        function(*args)
    except Exception as exc:
        raise Exception(
                "Function {} failed with return code {}, stderr in {}"
                    .format(function, exc.returncode, sys.stderr.name))
    except KeyboardInterrupt:
        raise Exception("Canceled by SIGINT")


def spawn_function(function, args, env, log_out, log_err,
    log_to_files, log_to_stdout):
    """Spawn ``function(args)`` in a simultaneous subprocess, log stdout
    and stderr to ``log_out`` and ``log_err``"""
    Process(
        target=call_function,
        args=(function, args, env, log_out, log_err,
              log_to_files, log_to_stdout)
    ).start()

