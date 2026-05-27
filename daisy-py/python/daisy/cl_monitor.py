"""daisy 1.x compatibility: ``from daisy.cl_monitor import CLMonitor``.

In daisy 1.x, ``CLMonitor`` was a ServerObserver subclass that printed a
tqdm progress bar from server events. daisy 2.x has its own default
``_TqdmObserver`` attached to ``Server``, so a no-op CLMonitor is enough
to satisfy callers that construct one for side effect alone.
"""


class CLMonitor:
    def __init__(self, server, *args, **kwargs):
        # No-op: daisy 2.x's default progress observer already handles
        # per-task tqdm output. Keeping the constructor signature so
        # daisy 1.x callers keep working without modification.
        self.server = server


__all__ = ["CLMonitor"]
