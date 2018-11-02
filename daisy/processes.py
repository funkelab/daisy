from subprocess import check_call, CalledProcessError

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
