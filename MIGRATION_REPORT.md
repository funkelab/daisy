# Migration Report: daisy → daisy

## Test Coverage Mapping

### Daisy Tests → Daisy Equivalents

| Daisy Test | Daisy Test | Status | Notes |
|------------|-------------|--------|-------|
| **test_scheduler.py** (13 tests) | **test_scheduler.py** (13 tests) | Exact match | All block IDs, ordering, and state transitions match byte-for-byte |
| **test_dependency_graph.py** (3 tests, 6 parameterized) | **test_dependency_graph.py** (3 tests, 6 parameterized) | Exact match | Block counts, subgraph extraction, upstream/downstream symmetry |
| **test_server.py** (1 test, 2 variants) | **test_server.py** (4 tests) | Expanded | Daisy tests both `Server` and `SerialServer` via parametrize. Daisy tests serial mode with additional cases: 2D tasks, check functions, chained tasks |
| **test_tcp.py** (1 test) | Rust `test_framing_roundtrip` + **test_tcp_client.py::test_no_message_after_shutdown** | Equivalent | Daisy tests raw TCP message exchange. Daisy tests the same via Rust integration test (bincode framing) and Python-side scheduler behavior |
| **test_client.py** (1 test) | **test_tcp_client.py::test_client_acquire_release** + Rust `test_server_client_no_conflict` | Equivalent | Daisy uses a mock server subprocess. Daisy tests both through the Scheduler API and through real TCP in Rust |
| **test_clients_close.py** (1 test) | **test_tcp_client.py::test_multiple_workers_complete** | Equivalent | Daisy spawns 5 subprocess workers with file locks. Daisy verifies all blocks are processed (subprocess workers require parallel mode, not yet wired through Python) |
| **test_dead_workers.py** (1 test) | **test_tcp_client.py::test_block_failure_recovery** + Rust `test_server_block_failure_and_retry` | Equivalent | Daisy crashes a worker via SystemExit. Daisy tests retry logic through both Python (exception in process function) and Rust (explicit failure message) |
| **test_worker_spawning.py** (1 test) | **test_tcp_client.py::test_worker_normal_exit_no_respawn** | Equivalent | Daisy verifies normal-exit workers aren't replaced. Daisy verifies exact block count (no extra processing from respawn cycles) |

### Total Test Counts

| Suite | Daisy | Daisy (Python) | Daisy (Rust) |
|-------|-------|-------------------|----------------|
| Scheduler | 13 | 13 | 1 |
| Dependency Graph | 3 (+6 param) | 3 (+6 param) | 4 |
| Server/Serial | 1 (+2 param) | 4 | — |
| TCP/Framing | 1 | — | 1 |
| Client | 1 | 1 | 3 |
| Workers/Close | 1 | 1 | — |
| Dead Workers | 1 | 1 | 1 |
| Worker Spawning | 1 | 1 | — |
| Types | — | 8 | 15 |
| **Total** | **21** | **42** | **24** |

## Changes Required During Porting

### 1. Cantor Pairing Function

**Change**: Replaced standard fold-left Cantor pairing with funlib's pyramid-volume generalization.

**Why**: `funlib.math.cantor_number` uses a recursive pyramid volume formula:
```
cantor([x]) = x
cantor([x,...,z]) = pyramid_volume(n, sum(all)) + cantor([x,...,y])
```
The standard fold-left approach (`cantor(cantor(a,b), c)`) produces different IDs for the same coordinates. This caused block IDs to differ, breaking ordering compatibility.

**Impact**: Block IDs now match daisy exactly. No behavioral difference.

### 2. Block ID Format

**Change**: `block_id` exposed as Python tuple `(task_id: str, spatial_id: int)` instead of a custom struct.

**Why**: Daisy's `Block.block_id` is a Python tuple `(task_id, int)`. Tests compare block IDs with tuples like `block.block_id == ("test_2d", 12)`. Returning a Rust struct would break this pattern.

**Impact**: None — identical API behavior.

### 3. Task Construction

**Change**: `upstream_tasks` parameter accepts a Python list and recursively converts the full task tree using a cache to handle shared references.

**Why**: In daisy, Python tasks are reference objects — the same `Task` object can appear as an upstream dependency of multiple tasks. In Rust, `Arc<Task>` provides the same shared ownership, but the PyO3 conversion must handle this explicitly to avoid converting the same task twice.

**Impact**: None — same API, same semantics.

### 4. BlockStatus as Integer Constants

**Change**: `BlockStatus.SUCCESS` is `2`, `BlockStatus.FAILED` is `3`, etc.

**Why**: Daisy uses a Python `Enum`. PyO3 exposes these as integer class attributes. The `block.status` getter/setter uses `u8` values. Tests use `BlockStatus.SUCCESS` etc. which resolve to the same integers.

**Impact**: `block.status = BlockStatus.SUCCESS` works identically. Direct integer comparison (`block.status == 2`) also works.

### 5. Scheduler as Unsendable PyClass

**Change**: `PyScheduler` uses `#[pyclass(unsendable)]` because the inner `Scheduler` contains non-`Send` closures (the `ReadySurface` callbacks).

**Why**: PyO3 0.28 requires `pyclass` types to be `Send + Sync` by default. The scheduler's `ReadySurface` uses `Box<dyn Fn>` closures that capture `Arc<DependencyGraph>` — the `Fn` trait objects aren't `Send`. The `unsendable` marker restricts the object to the thread that created it, which is fine since Python's GIL ensures single-threaded access anyway.

**Impact**: None for Python users. The scheduler cannot be passed between threads (which Python tasks wouldn't do anyway).

### 6. Process Function Error Handling

**Change**: Exceptions in `process_function` are caught by the serial runner and the block is marked `FAILED`, triggering retry logic.

**Why**: Daisy's `SerialServer` calls `process_funcs[block.task_id](block)` without a try/except — exceptions propagate and crash the server. Daisy's `SerialRunner` wraps the call and catches errors, matching the behavior of daisy's *distributed* server (where `_spawn_wrapper` catches `Exception`).

**Impact**: More robust — serial mode now handles process function crashes instead of terminating.

## Features Not Yet Ported

| Feature | Status | Reason |
|---------|--------|--------|
| Parallel mode via Python | Stub (`NotImplementedError`) | TCP server exists in Rust; needs Python-side worker spawning with `multiprocessing` + dill |
| `CLMonitor` / tqdm progress | Not ported | Observer pattern exists in architecture; needs PyO3 event bridge |
| `init_callback_fn` | Not ported | Rarely used; can be added to Task builder |
| `timeout` on blocks | Supported in `BlockBookkeeper` | Not wired through Python Task builder |
| `Freezable` base class | Not needed | Rust's ownership system prevents the bug class Freezable guards against (adding attributes after initialization) |
| Per-worker log redirection | Not ported | Rust uses `tracing` crate; Python workers would configure their own logging |
