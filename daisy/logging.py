import os
import sys
from pathlib import Path


# default log dir
LOG_BASEDIR = Path("./daisy_logs")


def set_log_basedir(path):
    """Set the base directory for logging (indivudal worker logs and detailed
    task summaries). If set to ``None``, all logging will be shown on the
    command line (which can get very messy).

    Default is ``./daisy_logs``.
    """

    global LOG_BASEDIR

    if path is not None:
        LOG_BASEDIR = Path(path)
    else:
        LOG_BASEDIR = None


def get_worker_log_basename(worker_id, task_id=None):
    """Get the basename of log files for individual workers."""

    if LOG_BASEDIR is None:
        return None

    basename = LOG_BASEDIR
    if task_id is not None:
        basename /= task_id
    basename /= f"worker_{worker_id}"

    return basename


def redirect_stdouterr(basename, mode="w"):
    """Redirect stdout and stderr of the current process to files::

    <basename>.out
    <basename>.err
    """

    if basename is None:
        return

    basename = Path(basename)

    # ensure output directory exists
    logdir = basename.parent
    logdir.mkdir(parents=True, exist_ok=True)

    sys.stdout = _file_reopen(basename.with_suffix(".out"), mode, sys.__stdout__)
    sys.stderr = _file_reopen(basename.with_suffix(".err"), mode, sys.__stderr__)


def _file_reopen(filename, mode, file_obj):

    new = open(filename, mode)
    newfd = new.fileno()
    targetfd = file_obj.fileno()
    os.close(targetfd)
    os.dup2(newfd, targetfd)

    return new
