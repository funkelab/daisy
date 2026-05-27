"""daisy 1.x compatibility: ``from daisy.block import BlockStatus``.

The v1 submodule path is preserved for downstream callers that haven't
migrated to ``daisy.BlockStatus`` yet.
"""

from daisy._task import BlockStatus

__all__ = ["BlockStatus"]
