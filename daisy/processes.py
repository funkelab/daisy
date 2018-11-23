from subprocess import check_call, CalledProcessError

from tornado.ioloop import IOLoop
from multiprocessing import Process

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
        raise Exception("Calling %s failed with recode %s, stderr in %s"%(
            ' '.join(command),
            exc.returncode,
            stderr.name))
    except KeyboardInterrupt:
        raise Exception("Cancelled by SIGINT")

def call_async(command, log_out, log_err):
    '''Run ``command`` in a simultaneous subprocess, log stdout and stderr to ``log_out``
    and ``log_err``'''

    Process(target = call, args=(command, log_out, log_err)).start()

def call_function_async(function, arg):
    '''Run ``command`` in a simultaneous subprocess, log stdout and stderr to ``log_out``
    and ``log_err``'''

    Process(target = function, args=(arg)).start()

