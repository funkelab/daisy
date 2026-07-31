# When workers fail

What happens when your process function (or the node it runs on) is broken —
and what daisy tells you about it.

## A minimal failing task

```python
import daisy


def process(block):
    raise RuntimeError("broken import on this node")


task = daisy.Task(
    "my-step",
    total_roi=daisy.Roi((0,), (80,)),
    read_roi=daisy.Roi((0,), (10,)),
    write_roi=daisy.Roi((0,), (10,)),
    process_function=process,
    max_workers=4,
    max_worker_restarts=3,
)

daisy.run_blockwise([task])
```

## What daisy 1.x did

Daisy 1.x had no limit on worker restarts: a worker whose process died was
reaped and immediately respawned, forever
(`reap_dead_workers` + `inc_num_workers` in daisy 1.x's
`task_worker_pools.py`). For a worker that crashes on startup this meant an
**infinite respawn loop** — tens of new worker processes per second, zero
blocks processed, and a `run_blockwise` call that never returns. On a
cluster, that's requested (billed) nodes spinning until someone notices and
kills the job by hand.

## What daisy does now

The runner replaces a failed worker at most `max_worker_restarts` times
(default 10). If the cap is exhausted while blocks remain, the task is
**abandoned**: remaining blocks are accounted as `orphaned`, downstream tasks
that depended on them are abandoned transitively, and — most importantly —
the run *terminates and tells you why*:

```
RuntimeError: task 'my-step' was abandoned (worker restart cap exhausted):
workers failed 4 times, 3 restarts performed; 8 of 8 blocks were orphaned.
Last worker error: RuntimeError: broken import on this node
Fix the worker error or increase Task(max_worker_restarts=...); use
Server().run_blockwise(...) to inspect task states without raising.
```

The original worker exception is captured and carried into the error — you
don't have to dig through per-worker log files to learn what broke.

## Inspecting instead of raising

`run_blockwise` raising on abandonment is the right default for pipelines: a
run that produced no output should not look like a clean exit. If you want
to handle abandonment programmatically, use the server API, which returns
per-task states and never raises for abandonment:

```python
server = daisy.Server()
states = server.run_blockwise([task])

state = states["my-step"]
state.abandoned  # True
state.abandon_reason  # "worker restart cap exhausted"
state.last_worker_error  # "RuntimeError: broken import on this node"
state.orphaned_count  # 8
```

See {doc}`design/ABANDONMENT` for the mechanics (counter invariants, the
typestate lifecycle, and how in-flight messages from dying workers are
handled during the transition).
