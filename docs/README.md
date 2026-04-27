# gerbera architecture docs

Design documentation for the gerbera library. This directory holds the *why* and the *how* — for *what* and *how-to-use*, see the README and the `examples/` directory.

## Reading order

If you're new, start with the architecture overview and then dip into whichever component is relevant to what you're doing.

1. **[ARCHITECTURE.md](ARCHITECTURE.md)** — System-level overview. The runtime topology (Python entry → Rust runtime → workers), what each crate does, how a block flows from acquire to release.

2. **[SCHEDULER.md](SCHEDULER.md)** — The dependency graph, the ready surface, and how block-level dependencies become the ordering constraints that drive worker dispatch.

3. **[PROTOCOL.md](PROTOCOL.md)** — TCP wire format, message types, framing. Read this if you're writing a worker in another language or debugging connection issues.

4. **[DONE_MARKERS.md](DONE_MARKERS.md)** — On-disk persistence layer for resumable runs. Zarr v3 layout, hash-based layout-change detection, what survives across runs.

5. **[ABANDONMENT.md](ABANDONMENT.md)** — The typestate model that gates task counter mutations. Race windows that exist when workers die mid-block and how the lifecycle prevents them. (A copy of the design note; the source of truth.)

6. **[RUN_STATS.md](RUN_STATS.md)** — How per-worker, per-task, and process-wide statistics are collected and aggregated. The linear-regression slope for block-duration trend.

7. **[WORKER_POOL_COORDINATION.md](WORKER_POOL_COORDINATION.md)** — The resource-budget design (`requires` per task, global `resources` budget). How concurrent worker counts compose across tasks competing for the same resource.

8. **[WORKER_SHUTDOWN_FLOWS.md](WORKER_SHUTDOWN_FLOWS.md)** — Sequence diagrams for the three shutdown scenarios (normal exit, KeyboardInterrupt, dirty crash) compared between daisy and gerbera.

## Pointers to other docs

- **[../REFACTOR.md](../REFACTOR.md)** — Inventory of simplification opportunities and proposed features. Use this when planning what to work on next.
- **[../INTERNAL_DIFFERENCES.md](../INTERNAL_DIFFERENCES.md)** — Subsystem-by-subsystem comparison vs daisy. Useful for users migrating from daisy.
- **[../MIGRATION_REPORT.md](../MIGRATION_REPORT.md)** — Notes on the migration from daisy's API to gerbera's.

## Conventions

- Code references use `path:line` format (e.g. `gerbera-core/src/scheduler.rs:172`) so they're navigable in any editor that follows the convention.
- "daisy" without qualification means the upstream Python library that gerbera ports.
- Box-drawing characters (`└─`, `→`) appear in some diagrams. They render in any monospace font.
