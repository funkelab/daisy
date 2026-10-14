"""The block handed to process and check functions.

`Block` is a plain Python object over the Rust carrier (`daisy._daisy.Block`):
its ROIs are `funlib.geometry.Roi`, its status is one of the `BlockStatus`
constants, and it pickles. The Rust types never reach user code; every
place daisy hands a block to Python (process and check functions,
`Client.acquire_block`, `Scheduler`, the dependency graphs,
`Task.failed_blocks`) goes through `Block._from_native`.

The 1.x import path `from daisy.block import Block, BlockStatus` still works.
"""

from __future__ import annotations

import daisy._daisy as _rs

BlockStatus = _rs.BlockStatus

__all__ = ["Block", "BlockStatus"]


class Block:
    """One unit of work: a read ROI, a write ROI, an id and a status.

    ``total_roi`` only feeds the derivation of the block id; pass
    ``block_id`` to override it (what daisy does for blocks received off
    the wire). ROIs may be `funlib.geometry.Roi` or anything with integer
    ``offset`` and ``shape``.
    """

    __slots__ = ("_native", "_read_roi", "_write_roi", "__weakref__")

    def __init__(self, total_roi, read_roi, write_roi, task_id=None, block_id=None):
        self._native = _rs.Block(
            total_roi, read_roi, write_roi, task_id=task_id, block_id=block_id
        )
        self._read_roi = None
        self._write_roi = None

    @classmethod
    def _from_native(cls, native):
        """View of an existing carrier: same identity, status and stats."""
        self = object.__new__(cls)
        self._native = native
        self._read_roi = None
        self._write_roi = None
        return self

    @property
    def read_roi(self):
        """`funlib.geometry.Roi` this block reads from."""
        roi = self._read_roi
        if roi is None:
            roi = self._read_roi = self._native.read_roi
        return roi

    @property
    def write_roi(self):
        """`funlib.geometry.Roi` this block writes to."""
        roi = self._write_roi
        if roi is None:
            roi = self._write_roi = self._native.write_roi
        return roi

    @property
    def block_id(self):
        """``(task_id, spatial_id)``, as in daisy 1.x."""
        return self._native.block_id

    @property
    def task_id(self):
        return self._native.task_id

    @property
    def status(self):
        """One of the `BlockStatus` constants; set it to FAILED to mark the
        block failed without raising."""
        return self._native.status

    @status.setter
    def status(self, value):
        self._native.status = value

    @property
    def stats(self):
        """Resource measurement attached by `profile_block`, or None."""
        return self._native.stats

    @stats.setter
    def stats(self, value):
        self._native.stats = value

    def __eq__(self, other):
        if isinstance(other, Block):
            return self._native.block_id == other._native.block_id
        return NotImplemented

    def __hash__(self):
        return hash(self._native.block_id)

    def __repr__(self):
        return repr(self._native)

    def __reduce__(self):
        return (
            _rebuild,
            (self.read_roi, self.write_roi, self.task_id, self.block_id[1], self.status),
        )


def _rebuild(read_roi, write_roi, task_id, spatial_id, status):
    block = Block(read_roi, read_roi, write_roi, task_id=task_id, block_id=spatial_id)
    block.status = status
    return block
