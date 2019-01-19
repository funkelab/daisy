from multiprocessing import Process
from subprocess import check_call, CalledProcessError
import logging
import os
import sys

logger = logging.getLogger(__name__)


def _freopen(filename, mode, fobj):
    """Redirect file descriptors
    Example usage: _freopen(log_out, 'w', sys.stdout)
    """
    new = open(filename, mode)
    newfd = new.fileno()
    targetfd = fobj.fileno()
    os.close(targetfd)
    os.dup2(newfd, targetfd)


def call(command, log_out, log_err):
    """Run ``command`` in a subprocess, log stdout and stderr to ``log_out``
    and ``log_err``"""

    logger.debug(
        "Running subprocess with:"
        "\n\tcommand %s"
        "\n\tlog_out %s"
        "\n\tlog_err %s",
        command, log_out, log_err)
    try:
        with open(log_out, 'w') as stdout:
            with open(log_err, 'w') as stderr:
                check_call(
                    ' '.join(command),
                    shell=True,
                    stdout=stdout,
                    stderr=stderr)
    except CalledProcessError as exc:
        raise Exception(
            "Calling %s failed with return code %s, stderr in %s" %
            (' '.join(command), exc.returncode, stderr.name))
    except KeyboardInterrupt:
        raise Exception("Canceled by SIGINT")


def call_wrapper(
        function,
        args,
        env,
        log_out=None,
        log_err=None,
        log_to_files=False,
        log_to_stdout=True):
    """Run ``function(args)`` with the given ``env`` as enironment and
    optionally log stdout and stderr to ``log_out`` and ``log_err``"""

    # set environment
    for e in env:
        os.environ[e] = env[e]

    if log_to_files and not log_to_stdout:
        # simply redirect outputs to files if we don't need to display
        # output to stdout/stderr
        _freopen(log_out, 'w', sys.stdout)
        _freopen(log_err, 'w', sys.stderr)

    if log_to_files and log_to_stdout:
        # if logging to both files and stdout, add file logging
        # option to logger
        logger = logging.getLogger()
        logger.addHandler(logging.FileHandler(log_out))

    function(*args)


def spawn_function(
        function,
        args,
        env,
        log_out,
        log_err,
        log_to_files,
        log_to_stdout):
    """Helper function to spawn ``function`` in a separate process with the
    given ``env`` added to the environment. Optionally logs the process' stdout
    and stderr in file ``log_out`` and ``log_err``.

    Returns started process so it can be terminated later"""

    proc = Process(
        target=call_wrapper,
        args=(function, args, env, log_out, log_err,
              log_to_files, log_to_stdout)
    )
    proc.start()
    return proc
