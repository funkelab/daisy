from __future__ import absolute_import
from .block import Block
from .coordinate import Coordinate
from .roi import Roi

import numpy as np

from itertools import product
import logging
import collections
from typing import List, Optional

logger = logging.getLogger(__name__)


class BlockwiseDependencyGraph:
    """Create a dependency graph as a list with elements::

        (block, [upstream_blocks])

    per block, where ``block`` is the block with exclusive write access to its
    ``write_roi`` (and save read access to its ``read_roi``), and
    ``upstream_blocks`` is a list of blocks that need to finish before this
    block can start (``[]`` if there are no upstream dependencies``).

    Args:

        total_roi (`class:daisy.Roi`):

            The region of interest (ROI) of the complete volume to process.

        block_read_roi (`class:daisy.Roi`):

            The ROI every block needs to read data from. Will be shifted over
            the ``total_roi``.

        block_write_roi (`class:daisy.Roi`):

            The ROI every block writes data from. Will be shifted over the
            ``total_roi`` in synchrony with ``block_read_roi``.

        read_write_conflict (``bool``, optional):

            Whether the read and write ROIs are conflicting, i.e., accessing
            the same resource. If set to ``False``, all blocks can run at the
            same time in parallel. In this case, providing a ``read_roi`` is
            simply a means of convenience to ensure no out-of-bound accesses
            and to avoid re-computation of it in each block.

        fit (``string``, optional):

            How to handle cases where shifting blocks by the size of
            ``block_write_roi`` does not tile the ``total_roi``. Possible
            options are:

            "valid": Skip blocks that would lie outside of ``total_roi``. This
            is the default::

                |---------------------------|     total ROI

                |rrrr|wwwwww|rrrr|                block 1
                       |rrrr|wwwwww|rrrr|         block 2
                                                no further block

            "overhang": Add all blocks that overlap with ``total_roi``, even if
            they leave it. Client code has to take care of save access beyond
            ``total_roi`` in this case.::

                |---------------------------|     total ROI

                |rrrr|wwwwww|rrrr|                block 1
                       |rrrr|wwwwww|rrrr|         block 2
                              |rrrr|wwwwww|rrrr|  block 3 (overhanging)

            "shrink": Like "overhang", but shrink the boundary blocks' read and
            write ROIs such that they are guaranteed to lie within
            ``total_roi``. The shrinking will preserve the context, i.e., the
            difference between the read ROI and write ROI stays the same.::

                |---------------------------|     total ROI

                |rrrr|wwwwww|rrrr|                block 1
                       |rrrr|wwwwww|rrrr|         block 2
                              |rrrr|www|rrrr|     block 3 (shrunk)
    """

    def __init__(
        self,
        task_id: str,
        block_read_roi: Roi,
        block_write_roi: Roi,
        read_write_conflict: bool,
        fit: str,
        total_read_roi: Optional[Roi] = None,
        total_write_roi: Optional[Roi] = None,
    ):
        self.block_read_roi = block_read_roi
        self.block_write_roi = block_write_roi
        self.read_write_context = (
            block_write_roi.begin - block_read_roi.begin,
            block_read_roi.end - block_write_roi.end,
        )

        if total_read_roi is not None and total_write_roi is not None:
            total_context = (
                total_write_roi.begin - total_read_roi.begin,
                total_read_roi.end - total_write_roi.end,
            )
            assert total_context == self.read_write_context, (
                "total_read_roi and total_write_roi have context "
                f"{total_context}, which is unequal to the block context: "
                f"{self.read_write_context}"
            )
        if total_read_roi is not None:
            self.total_read_roi = total_read_roi
            self.total_write_roi = self.total_read_roi.grow(
                -self.read_write_context[0], -self.read_write_context[1]
            )
        elif total_write_roi is not None:
            self.total_write_roi = total_write_roi
            self.total_read_roi = self.total_write_roi.grow(
                self.read_write_context[0], self.read_write_context[1]
            )
        else:
            raise ValueError(
                "Either total_read_roi or total_write_roi must be provided!"
            )

        self.task_id = task_id
        self.read_write_conflict = read_write_conflict
        self.fit = fit

        # when computing block offsets, make sure to include blocks
        # on the upper boundary with a rounding term
        if self.fit == "overhang" or self.fit == "shrink":
            # want to round up if there is any write roi left
            self.rounding_term = (1,) * self.block_write_roi.dims
        else:
            # want to round up only if there is a full write block left.
            self.rounding_term = self.block_write_roi.shape

        # computed values
        self._level_stride = self.compute_level_stride()
        self._level_offsets = self.compute_level_offsets()
        self._level_conflicts = self.compute_level_conflicts()

    @property
    def num_levels(self):
        return len(self._level_offsets)

    @property
    def num_blocks(self):
        num_blocks = 0
        for level in range(self.num_levels):
            num_blocks += self._num_level_blocks(level)
        return num_blocks

    @property
    def inclusion_criteria(self):
        # TODO: Can't we remove this entirely by pre computing the write_roi
        inclusion_criteria = {
            "valid": lambda b: self.total_write_roi.contains(b.write_roi),
            "overhang": lambda b: self.total_write_roi.contains(b.write_roi.begin),
            "shrink": lambda b: self.shrink_possible(b),
        }[self.fit]
        return inclusion_criteria

    @property
    def fit_block(self):
        # TODO: Can't we remove this by pre computing the write_roi and
        # intersecting edge blocks with the write roi while making them?
        fit_block = {
            "valid": lambda b: b,  # noop
            "overhang": lambda b: b,  # noop
            "shrink": lambda b: self.shrink(b),
        }[self.fit]
        return fit_block

    def num_roots(self):
        return self._num_level_blocks(0)

    def _num_level_blocks(self, level):
        level_offset = self._level_offsets[level]

        axis_blocks = [
            (e - lo + s - r) // s
            for lo, e, s, r in zip(
                level_offset,
                self.total_write_roi.shape,
                self._level_stride,
                self.rounding_term,
            )
        ]
        num_blocks = np.prod(axis_blocks)
        logger.debug(
            "number of blocks for write_roi: %s, level (%d), "
            "offset (%s), and stride (%s): %d (per dim: %s)",
            self.total_write_roi,
            level,
            level_offset,
            self._level_stride,
            num_blocks,
            axis_blocks,
        )

        return num_blocks

    def level_blocks(self, level):

        for block_offset in self._compute_level_block_offsets(level):
            block = Block(
                self.total_read_roi,
                self.block_read_roi + block_offset,
                self.block_write_roi + block_offset,
                task_id=self.task_id,
            )
            # TODO: We probably don't need to check every block for inclusion
            # and fit, but rather just the blocks on the total roi boundary.
            # can probably be handled when we calculate the block_dim_offsets
            if self.inclusion_criteria(block):
                yield self.fit_block(block)
            else:
                raise RuntimeError("Unreachable!")

    def root_gen(self):
        blocks = self.level_blocks(level=0)
        for block in blocks:
            yield self.fit_block(block)

    def _block_offset(self, block):
        # The block offset is the offset of the read roi relative to total roi
        block_offset = block.read_roi.offset - self.total_read_roi.offset
        return block_offset

    def _level(self, block):
        block_offset = self._block_offset(block)
        level_offset = block_offset % self._level_stride
        for i, offset in enumerate(self._level_offsets):
            if level_offset == offset:
                return i
        raise NotImplementedError(
            f"Should not be reachable! {level_offset} not in "
            f"{self._level_offsets}, stride: {self._level_stride}"
        )

    def downstream(self, block):
        """
        get all block_id's that are directly dependent on this block_id
        i.e. this block offset by all conflict offsets in the next level
        """
        level = self._level(block)
        next_level = level + 1
        if next_level >= self.num_levels:
            return []

        conflicts = []
        for conflict in self._level_conflicts[next_level]:
            conflict_block = Block(
                total_roi=self.total_read_roi,
                read_roi=Roi(
                    block.read_roi.offset - conflict,
                    self.block_read_roi.shape,
                ),
                write_roi=Roi(
                    block.write_roi.offset - conflict,
                    self.block_write_roi.shape,
                ),
                task_id=self.task_id,
            )
            # TODO: We probably don't need to check every block for inclusion
            # and fit, but rather just the blocks on the total roi boundary
            if self.inclusion_criteria(conflict_block):
                conflicts.append(self.fit_block(conflict_block))
        conflicts = list(set(conflicts))
        return conflicts

    def upstream(self, block):
        """
        get all upstream block id's for a given block_id
        i.e. this block offset by all conflict offsets in this level
        """
        level = self._level(block)

        conflicts = []
        for conflict in self._level_conflicts[level]:
            conflict_block = Block(
                total_roi=self.total_read_roi,
                read_roi=Roi(
                    block.read_roi.offset + conflict,
                    self.block_read_roi.shape,
                ),
                write_roi=Roi(
                    block.write_roi.offset + conflict,
                    self.block_write_roi.shape,
                ),
                task_id=self.task_id,
            )
            # TODO: We probably don't need to check every block for inclusion
            # and fit, but rather just the blocks on the total roi boundary
            if self.inclusion_criteria(conflict_block):
                conflicts.append(self.fit_block(conflict_block))
        conflicts = list(set(conflicts))
        return conflicts

    def enumerate_all_dependencies(self):

        self._level_block_offsets = self.compute_level_block_offsets()

        for level in range(self.num_levels):
            level_blocks = self.level_blocks(level)
            for block in level_blocks:
                yield (block, self.upstream(block))

    def compute_level_stride(self) -> Coordinate:
        """
        Get the stride that separates independent blocks in one level.
        """

        if not self.read_write_conflict:
            return self.block_write_roi.shape

        logger.debug(
            "Compute level stride for read ROI %s and write ROI %s.",
            self.block_read_roi,
            self.block_write_roi,
        )

        assert self.block_read_roi.contains(
            self.block_write_roi
        ), "Read ROI must contain write ROI."

        context_ul = self.block_write_roi.begin - self.block_read_roi.begin
        context_lr = self.block_read_roi.end - self.block_write_roi.end

        max_context = Coordinate(
            (max(ul, lr) for ul, lr in zip(context_ul, context_lr))
        )
        logger.debug("max context per dimension is %s", max_context)

        # this stride guarantees that blocks are independent, but not a
        # multiple of the write_roi shape. It would be impossible to tile
        # the output roi with blocks shifted by this min_level_stride
        min_level_stride = max_context + self.block_write_roi.shape

        logger.debug("min level stride is %s", min_level_stride)

        # to avoid overlapping write ROIs, increase the stride to the next
        # multiple of write shape
        write_shape = self.block_write_roi.shape
        level_stride = Coordinate(
            (
                ((level - 1) // w + 1) * w
                for level, w in zip(min_level_stride, write_shape)
            )
        )

        # Handle case where min_level_stride > total_write_roi.
        # This case leads to levels with no blocks in them. This makes
        # calculating dependencies on the fly significantly more difficult
        write_roi_shape = self.total_write_roi.shape
        if self.fit == "valid":
            # round down to nearest block size
            write_roi_shape -= write_roi_shape % self.block_write_roi.shape
        else:
            # round up to nearest block size
            write_roi_shape += (
                -write_roi_shape % self.block_write_roi.shape
            ) % self.block_write_roi.shape

        level_stride = Coordinate(
            (min(a, b) for a, b in zip(level_stride, write_roi_shape))
        )

        logger.debug("final level stride (multiples of write size) is %s", level_stride)

        return level_stride

    def compute_level_offsets(self) -> List[Coordinate]:
        """
        compute an offset for each level.
        """

        write_stride = self.block_write_roi.shape

        logger.debug(
            "Compute level offsets for level stride %s and write stride %s.",
            self._level_stride,
            write_stride,
        )

        dim_offsets = [
            range(0, e, step) for e, step in zip(self._level_stride, write_stride)
        ]

        level_offsets = list(reversed([Coordinate(o) for o in product(*dim_offsets)]))

        logger.debug("level offsets: %s", level_offsets)

        return level_offsets

    def compute_level_conflicts(self) -> List[List[Coordinate]]:
        """
        For each level, compute the set of conflicts from previous levels.
        """

        level_conflict_offsets = []
        prev_level_offset = None

        for level, level_offset in enumerate(self._level_offsets):

            # get conflicts to previous level
            if prev_level_offset is not None and self.read_write_conflict:
                conflict_offsets: List[Coordinate] = self.get_conflict_offsets(
                    level_offset, prev_level_offset, self._level_stride
                )
            else:
                conflict_offsets = []
            prev_level_offset = level_offset

            level_conflict_offsets.append(conflict_offsets)

        return level_conflict_offsets

    def _compute_level_block_offsets(self, level):
        level_offset = self._level_offsets[level]
        # all block offsets of the current level (relative to total ROI start)

        block_dim_offsets = [
            range(lo, e + 1 - r, s)
            for lo, e, s, r in zip(
                level_offset,
                self.total_write_roi.shape,
                self._level_stride,
                self.rounding_term,
            )
        ]
        for offset in product(*block_dim_offsets):
            # TODO: can we do this part lazily? This might be a lot of
            # Coordinates
            block_offset = Coordinate(offset)

            # convert to global coordinates
            block_offset += self.total_read_roi.begin - self.block_read_roi.begin
            yield block_offset

    def compute_level_block_offsets(self) -> List[List[Coordinate]]:
        """
        For each level, get the set of all offsets corresponding to blocks in
        this level.
        """
        level_block_offsets = []

        for level in range(self.num_levels):

            level_block_offsets.append(list(self._compute_level_block_offsets(level)))

        return level_block_offsets

    def get_conflict_offsets(self, level_offset, prev_level_offset, level_stride):
        """Get the offsets to all previous level blocks that are in conflict
        with the current level blocks."""

        offset_to_prev = prev_level_offset - level_offset
        logger.debug("offset to previous level: %s", offset_to_prev)

        def get_offsets(op, ls):
            if op < 0:
                return [op, op + ls]
            elif op == 0:
                return [op]
            else:
                return [op - ls, op]

        conflict_dim_offsets = [
            get_offsets(op, ls) for op, ls in zip(offset_to_prev, level_stride)
        ]

        conflict_offsets = [Coordinate(o) for o in product(*conflict_dim_offsets)]
        logger.debug("conflict offsets to previous level: %s", conflict_offsets)

        return conflict_offsets

    def shrink_possible(self, block):

        return self.total_write_roi.contains(block.write_roi.begin)

    def shrink(self, block):
        """Ensure that read and write ROI are within total ROI by shrinking
        both. Size of context will be preserved."""

        w = self.total_write_roi.intersect(block.write_roi)
        r = self.total_read_roi.intersect(block.read_roi)

        shrunk_block = block
        shrunk_block.read_roi = r
        shrunk_block.write_roi = w

        return shrunk_block

    def get_subgraph_blocks(self, sub_roi, read_roi=False):
        """Return ids of blocks, as instantiated in the full graph, such that
        their total write rois fully cover `sub_roi`.
        The function API assumes that `sub_roi` and `total_roi` use world
        coordinates and `self.block_read_roi` and `self.block_write_roi` use
        relative coordinates.
        """

        if read_roi:
            # if we want to get blocks whose read_roi overlaps with sub_roi
            # simply grow the sub_roi by the block context. That way we
            # only need to check if a blocks read_roi overlaps with sub_roi.
            # This is the same behavior as when we want write_roi overlap
            sub_roi = sub_roi.grow(
                self.read_write_context[0], self.read_write_context[1]
            )

        # TODO: handle unsatisfiable sub_rois
        # i.e. sub_roi is outside of *total_write_roi
        # after accounting for padding
        sub_roi = sub_roi.intersect(self.total_write_roi)

        # get sub_roi relative to the write roi
        begin = sub_roi.begin - self.total_write_roi.offset
        end = sub_roi.end - self.total_write_roi.offset

        # convert to block coordinates. Handle upper block based on fit
        aligned_subroi = (
            begin // self.block_write_roi.shape,  # `floordiv`
            -(-end // self.block_write_roi.shape),  # `ceildiv`
        )

        # generate relative offsets of relevant write blocks
        block_dim_offsets = [
            range(lo, e, s)
            for lo, e, s in zip(
                aligned_subroi[0] * self.block_write_roi.shape,
                aligned_subroi[1] * self.block_write_roi.shape,
                self.block_write_roi.shape,
            )
        ]

        # generate absolute offsets
        block_offsets = [
            Coordinate(o) + self.total_read_roi.offset
            for o in product(*block_dim_offsets)
        ]

        blocks = [
            self.fit_block(
                Block(
                    self.total_read_roi,
                    self.block_read_roi + offset - self.block_read_roi.offset,
                    self.block_write_roi + offset - self.block_read_roi.offset,
                    task_id=self.task_id,
                )
            )
            for offset in block_offsets
        ]
        return [block for block in blocks if self.inclusion_criteria(block)]


class DependencyGraph:
    def __init__(self, tasks):
        self.upstream_tasks = collections.defaultdict(set)
        self.downstream_tasks = collections.defaultdict(set)
        self.task_map = {}
        for task in tasks:
            self.__add_task(task)

        self.task_dependency_graphs = {}
        for task in self.task_map.values():
            self.__add_task_dependency_graph(task)

    @property
    def task_ids(self):
        return self.task_map.keys()

    def num_blocks(self, task_id):
        return self.task_dependency_graphs[task_id].num_blocks

    def upstream(self, block):
        upstream = self.task_dependency_graphs[block.task_id].upstream(block)
        for upstream_task in self.upstream_tasks[block.task_id]:
            upstream.extend(
                self.task_dependency_graphs[upstream_task].get_subgraph_blocks(
                    block.read_roi, read_roi=False
                )
            )
        return sorted(
            upstream,
            key=lambda b: b.block_id[1],
        )

    def downstream(self, block):
        dep_graphs = self.task_dependency_graphs
        downstream = dep_graphs[block.task_id].downstream(block)
        for downstream_task in self.downstream_tasks[block.task_id]:
            downstream.extend(
                dep_graphs[downstream_task].get_subgraph_blocks(
                    block.write_roi, read_roi=True
                )
            )
        return sorted(
            downstream,
            key=lambda b: b.block_id[1],
        )

    def root_tasks(self):
        return [
            task_id
            for task_id, upstream_tasks in self.upstream_tasks.items()
            if len(upstream_tasks) == 0
        ]

    def num_roots(self, task_id):
        return self.task_dependency_graphs[task_id].num_roots()

    def root_gen(self, task_id):
        return self.task_dependency_graphs[task_id].root_gen()

    def roots(self):
        root_tasks = self.root_tasks()
        return {
            task_id: (self.num_roots(task_id), self.root_gen(task_id))
            for task_id in root_tasks
        }

    def __add_task(self, task):
        if task.task_id not in self.task_map:
            self.task_map[task.task_id] = task
            self.upstream_tasks[task.task_id] = set()
            self.downstream_tasks[task.task_id] = set()
            for upstream_task in task.requires():
                self.__add_task(upstream_task)
                self.upstream_tasks[task.task_id].add(upstream_task.task_id)
                self.downstream_tasks[upstream_task.task_id].add(task.task_id)

    def __add_task_dependency_graph(self, task):
        """Create dependency graph a specific task"""

        # create intra task dependency graph
        self.task_dependency_graphs[task.task_id] = BlockwiseDependencyGraph(
            task.task_id,
            task.read_roi,
            task.write_roi,
            task.read_write_conflict,
            task.fit,
            total_read_roi=task.total_roi,
        )

    def __enumerate_all_dependencies(self):
        # enumerate all the blocks
        for task_id in self.task_ids:
            block_dependencies = self.task_dependency_graphs[
                task_id
            ].enumerate_all_dependencies()
            for block, upstream_blocks in block_dependencies:
                if block.block_id in self.blocks:
                    continue

                self.blocks[block.block_id] = block

                for upstream_block in upstream_blocks:
                    if upstream_block.block_id not in self.blocks:
                        raise RuntimeError(
                            "Block dependency %s is not found for task %s."
                            % (upstream_block.block_id, task_id)
                        )
                    self._downstream[upstream_block.block_id].add(block.block_id)
                    self._upstream[block.block_id].add(upstream_block.block_id)

        # enumerate all of the upstream / downstream dependencies
        for task_id in self.task_ids:
            # add inter-task read-write dependency
            if len(self.upstream_tasks[task_id]):
                for block in self.task_dependency_graphs[task_id].blocks:
                    roi = block.read_roi
                    upstream_blocks = []
                    for upstream_task_id in self.upstream_tasks[task_id]:
                        upstream_task_blocks = self.task_dependency_graphs[
                            upstream_task_id
                        ].get_subgraph_blocks(roi)
                        upstream_blocks.extend([upstream_task_blocks])

                    for upstream_block in upstream_blocks:
                        if upstream_block.block_id not in self.blocks:
                            raise RuntimeError(
                                "Block dependency %s is not found for task %s."
                                % (upstream_block.block_id, task_id)
                            )
                        self._downstream[upstream_block.block_id].add(block.block_id)
                        self._upstream[block.block_id].add(upstream_block.block_id)
