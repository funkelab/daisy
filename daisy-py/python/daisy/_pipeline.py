"""Pipeline DSL: compose tasks into DAGs with `+` and `|`.

A `Pipeline` is a DAG of `Task`s with explicit *source* tasks (no
incoming edges) and *output* tasks (no outgoing edges). Pipelines
compose via two operators:

- `a + b` — *sequential*: every output of `a` becomes a block-level
  upstream of every source of `b`. The result's sources are `a`'s,
  the result's outputs are `b`'s.
- `a | b` — *parallel*: union the two DAGs without adding edges.
  The result's sources/outputs are the union of both pipelines'.

Both operands can be a `Task` or a `Pipeline`; a `Task` is
implicitly promoted to a singleton pipeline (one source, one
output, one task, no edges). `__radd__` / `__ror__` make
`task + pipeline` and `task | pipeline` work too.

Pipelines do not mutate the underlying `Task` objects when
composed. At run time `Pipeline.run_blockwise()` materializes a
fresh tree of clones with `upstream_tasks` derived from the
pipeline's edge list. Per-task `Task(upstream_tasks=...)` set in
the constructor is honoured and merged with pipeline-derived
edges, deduplicated by object identity.

`Pipeline.reset()` clears every member task's done-marker (no
cascade — see `Task.reset()`).
"""

from __future__ import annotations

import copy
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from daisy._task import Task


class Pipeline:
    """A DAG of tasks with `+` (sequential) and `|` (parallel) composition.

    Build pipelines from `Task` instances via the operators rather
    than calling this constructor directly:

        preprocess = task_load + task_normalize
        postprocess = task_segment + task_export
        full = preprocess + postprocess
    """

    def __init__(
        self,
        tasks: list,
        edges: list,
        sources: list,
        outputs: list,
    ):
        # Internal constructor. Use `Pipeline.from_task` or the
        # operators on `Task` / `Pipeline` to build pipelines.
        self.tasks = list(tasks)
        self.edges = list(edges)
        self.sources = list(sources)
        self.outputs = list(outputs)

    @classmethod
    def from_task(cls, task) -> Pipeline:
        """Promote a single `Task` to a singleton `Pipeline`."""
        return cls(tasks=[task], edges=[], sources=[task], outputs=[task])

    def __add__(self, other) -> Pipeline:
        right = self._coerce(other)
        new_edges = [(out, src) for out in self.outputs for src in right.sources]
        return Pipeline(
            tasks=self._dedup(self.tasks + right.tasks),
            edges=self.edges + right.edges + new_edges,
            sources=self._dedup(self.sources),
            outputs=self._dedup(right.outputs),
        )

    def __or__(self, other) -> Pipeline:
        right = self._coerce(other)
        return Pipeline(
            tasks=self._dedup(self.tasks + right.tasks),
            edges=self.edges + right.edges,
            sources=self._dedup(self.sources + right.sources),
            outputs=self._dedup(self.outputs + right.outputs),
        )

    def __radd__(self, other) -> Pipeline:
        # task + pipeline path; symmetric.
        return self._coerce(other) + self

    def __ror__(self, other) -> Pipeline:
        return self._coerce(other) | self

    def __repr__(self) -> str:
        return (
            f"Pipeline(tasks={[t.task_id for t in self.tasks]}, "
            f"sources={[t.task_id for t in self.sources]}, "
            f"outputs={[t.task_id for t in self.outputs]}, "
            f"edges={[(u.task_id, d.task_id) for u, d in self.edges]})"
        )

    @staticmethod
    def _coerce(x) -> Pipeline:
        if isinstance(x, Pipeline):
            return x
        # Lazy import to avoid circular dependency.
        from daisy._task import Task

        if isinstance(x, Task):
            return Pipeline.from_task(x)
        raise TypeError(
            f"cannot compose Pipeline with {type(x).__name__}; "
            "expected Task or Pipeline"
        )

    @staticmethod
    def _dedup(items):
        seen = set()
        out = []
        for it in items:
            if id(it) not in seen:
                seen.add(id(it))
                out.append(it)
        return out

    def reset(self) -> None:
        """Clear every member task's done-marker. No-op for tasks
        without a configured marker. See `Task.reset()` for the
        per-task semantics — in particular, this does *not* cascade
        any further than the tasks in this pipeline."""
        for t in self.tasks:
            t.reset()

    def materialize(self) -> list:
        """Return a fresh tree of task clones whose `upstream_tasks`
        are set from this pipeline's edges (merged with each task's
        own constructor-set `upstream_tasks`, deduplicated by object
        identity). The output tasks are returned — passing them to
        `daisy.run_blockwise` walks the upstream chain to schedule
        the full DAG.

        This does *not* mutate the original Task instances, so a
        Pipeline can be composed into multiple parents and run
        independently in each."""
        # Map id(original) -> list of upstreams from pipeline edges.
        from_edges: dict[int, list] = {id(t): [] for t in self.tasks}
        for up, down in self.edges:
            from_edges[id(down)].append(up)

        # Clone each task once.
        clones: dict[int, object] = {}
        for t in self.tasks:
            clone = copy.copy(t)
            existing = list(t.upstream_tasks)
            seen = {id(u) for u in existing}
            for u in from_edges.get(id(t), ()):
                if id(u) not in seen:
                    existing.append(u)
                    seen.add(id(u))
            clone.upstream_tasks = existing
            clones[id(t)] = clone

        # Rewrite each clone's upstream_tasks so references point at
        # the corresponding clones (where they exist in this pipeline)
        # rather than the originals — keeps the cloned subgraph
        # internally consistent.
        for clone in clones.values():
            clone.upstream_tasks = [
                clones.get(id(u), u) for u in clone.upstream_tasks
            ]

        return [clones[id(o)] for o in self.outputs]

    def run_blockwise(self, **kwargs) -> bool:
        """Run this pipeline. Forwards `**kwargs` to
        `daisy.run_blockwise` — supports `multiprocessing`,
        `resources`, `progress`, and `block_tracking`."""
        from daisy._runner import run_blockwise as _run

        return _run(self.materialize(), **kwargs)
