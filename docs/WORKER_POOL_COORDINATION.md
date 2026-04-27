# Worker Pool Coordination Across Tasks

## The Problem

Currently, each task declares its own `num_workers`. When running a pipeline of tasks (A → B → C), the server spawns workers per-task:

```
Task A: 10 workers
Task B: 10 workers  
Task C: 10 workers
```

All 30 workers are spawned upfront. But if the machine only has 15 cores, or the user only wants to use 15 slots, there's no way to express that. And more importantly, the workers are static — if task A has no ready blocks (all its blocks are complete or waiting on processing), its 10 workers sit idle while task B has a backlog.

## What We Want

A global worker budget shared across tasks, with dynamic reallocation based on where the work actually is.

```python
pipeline = daisy.Pipeline(
    tasks=[task_a, task_b, task_c],
    total_workers=15,
)
pipeline.run()
```

The server should:
1. Start by giving all 15 workers to task A (the only task with ready blocks)
2. As task A blocks complete and task B blocks become ready, shift workers from A to B
3. If task A is done, give all 15 to B
4. Handle the case where A and B both have work — split based on ready block counts

## Current Architecture

The relevant pieces:

### Server event loop (`server.rs`)

Workers are spawned at startup, one thread per task per `num_workers`:

```rust
for task in tasks {
    for _ in 0..task.num_workers {
        let handle = Self::spawn_worker(&spec);
        workers.push((spec, WorkerThread::Running(handle)));
    }
}
```

Each worker thread is bound to a specific task — it connects via TCP and requests blocks only for its `task_id`. It runs until the server sends `RequestShutdown`.

### Scheduler state (`task_state.rs`)

The scheduler already tracks everything needed for allocation decisions:

```rust
pub struct TaskState {
    pub ready_count: i64,       // blocks available to dispatch
    pub processing_count: i64,  // blocks currently being worked on
    pub pending_count: i64,     // blocks waiting on dependencies
    pub completed_count: i64,
}
```

### Worker threads (`server.rs`)

Each worker is a `std::thread` with a TCP client that loops:

```
acquire_block(task_id) → process → release_block → repeat
```

The `task_id` is baked into the worker at spawn time.

## Design

### Core Idea: Task-Agnostic Workers

Instead of workers being bound to a single task, make them request "the next available block from any task." The server decides which task to assign based on the current state.

### Option A: Server-Side Assignment (Recommended)

Workers send a generic `AcquireBlock` without a task ID. The server picks the best task:

```rust
// New message variant:
AcquireBlockAny  // "give me any block from any task"

// Server picks task based on allocation policy:
fn pick_task(&self, scheduler: &Scheduler) -> Option<String> {
    // ... allocation logic ...
}
```

**Changes needed:**

1. **Protocol**: add `AcquireBlockAny` message variant alongside the existing `AcquireBlock { task_id }`. Existing workers that target a specific task still work.

2. **Server `handle_acquire`**: when receiving `AcquireBlockAny`, call an allocator to pick the task, then dispatch as normal through the single path.

3. **Worker threads**: change from `Client::connect(host, port, task_id)` to `Client::connect(host, port)` without a task binding. The worker loop becomes:
   ```rust
   loop {
       match client.acquire_block_any().await {
           Some(block) => { process(block); release(block); }
           None => break,
       }
   }
   ```

4. **Process function routing**: the block carries its `task_id`, so the worker looks up the correct process function for that task. This requires workers to have access to all tasks' process functions, not just one.

### Option B: Worker Rebalancing

Keep task-bound workers but dynamically adjust counts. A `WorkerAllocator` runs on the health tick and redistributes:

```rust
fn rebalance(
    task_states: &HashMap<String, TaskState>,
    current_allocation: &HashMap<String, usize>,
    total_budget: usize,
) -> HashMap<String, usize> {
    // ... new allocation ...
}
```

To shrink a task's workers: send `RequestShutdown` to excess workers.
To grow: spawn new worker threads for that task.

**Simpler to implement** (no protocol change), but has overhead from constantly spawning/killing threads and re-establishing TCP connections.

### Recommended: Option A

Option A is cleaner because workers are long-lived. They don't need to reconnect when work shifts between tasks. The server just routes different blocks to them.

## Allocation Policies

The allocator decides which task gets the next block when a worker asks. Several strategies:

### 1. Proportional to Ready Blocks

```rust
fn pick_task(states: &HashMap<String, TaskState>) -> Option<String> {
    states.iter()
        .filter(|(_, s)| s.ready_count > 0)
        .max_by_key(|(_, s)| s.ready_count)
        .map(|(id, _)| id.clone())
}
```

Give work to the task with the most ready blocks. Simple, avoids starvation, naturally balances.

### 2. Priority by Pipeline Position

Prefer downstream tasks (closer to final output). This minimizes intermediate data sitting in storage:

```rust
fn pick_task(
    states: &HashMap<String, TaskState>,
    topo_order: &[String],  // tasks in dependency order, downstream last
) -> Option<String> {
    // Prefer the latest task in the pipeline that has ready blocks.
    topo_order.iter().rev()
        .find(|id| states[*id].ready_count > 0)
        .cloned()
}
```

### 3. Drain-Then-Fill

Fully drain each task before moving to the next. Minimizes context switching and memory pressure from multiple tasks running simultaneously:

```rust
fn pick_task(states: &HashMap<String, TaskState>) -> Option<String> {
    // Pick the first task (in dependency order) that has ready blocks.
    // Only move to the next task when the current one is fully done
    // or blocked on dependencies.
}
```

### 4. User-Defined

Expose the allocator as a trait so users can implement custom policies:

```rust
pub trait WorkerAllocator: Send + Sync {
    fn pick_task(&self, states: &HashMap<String, TaskState>) -> Option<String>;
}
```

From Python:
```python
class MyAllocator:
    def pick_task(self, states):
        # Custom logic
        return task_id

pipeline = daisy.Pipeline(tasks, total_workers=15, allocator=MyAllocator())
```

## Implementation Steps

### Phase 1: Task-Agnostic Workers (Minimal)

1. Add `AcquireBlockAny` to the `Message` enum
2. Add `pick_task` method to server (default: proportional to ready blocks)
3. Change worker threads to not bind to a task — carry all tasks' process functions
4. When a block arrives, look up the process function by `block.task_id`
5. Add `total_workers` field to the `run_blockwise` API

**Estimated scope**: ~100 lines in `server.rs`, ~20 lines in `protocol.rs`, ~30 lines in `py_server.rs`.

### Phase 2: Allocation Policies

1. Define `WorkerAllocator` trait
2. Implement the four strategies above
3. Expose to Python via PyO3

**Estimated scope**: new `allocator.rs` module, ~150 lines.

### Phase 3: Observability

1. Log worker→task assignments per health tick
2. Expose current allocation via `Server.worker_allocation` property
3. Add metrics: blocks/sec per task, worker utilization, idle time

## What Doesn't Need to Change

- **Scheduler**: already tracks per-task state, no changes needed
- **Dependency graph**: task dependencies are already resolved correctly
- **Block bookkeeper**: tracks by client address, doesn't care about task assignment
- **Ready surface**: per-task, works as-is
- **Pending requests**: the single dispatch path handles any task's blocks
- **Python compat layer**: `Task.num_workers` becomes a hint/default; the global budget overrides it
