//! Resource budget for cluster-style worker dispatch.
//!
//! Each task declares `requires: HashMap<String, i64>` — what one of
//! *its* workers consumes (`{"cpu": 1}`, `{"gpu": 1, "cpu": 4}`, …).
//! The runner is given a global `budget` (e.g. `{"cpu": 32, "gpu": 8}`)
//! and uses this allocator to decide, at any moment, how many more
//! workers it can spawn for each ready task without overshooting.
//!
//! Workers are task-bound by design (worker-function workers are
//! stateful — a model loaded on startup), so this is *not* a pool of
//! fungible workers. It's a global budget that bounds concurrent
//! per-task spawn counts.
//!
//! See `Task::requires` for how tasks declare their per-worker cost.

use crate::error::DaisyError;
use crate::task::Task;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Debug, Default)]
pub struct ResourceBudget(HashMap<String, i64>);

impl ResourceBudget {
    pub fn new(budget: HashMap<String, i64>) -> Self {
        Self(budget)
    }

    pub fn empty() -> Self {
        Self(HashMap::new())
    }

    pub fn get(&self, key: &str) -> i64 {
        self.0.get(key).copied().unwrap_or(0)
    }

    pub fn as_map(&self) -> &HashMap<String, i64> {
        &self.0
    }
}

/// Live state: how much of each resource has already been consumed by
/// alive workers, and per-task counts.
pub struct ResourceAllocator {
    budget: ResourceBudget,
    used: HashMap<String, i64>,
    /// Number of workers currently alive for each task. Includes
    /// workers whose `spawn_function` is in flight.
    alive: HashMap<String, usize>,
}

impl ResourceAllocator {
    pub fn new(budget: ResourceBudget) -> Self {
        Self {
            used: HashMap::new(),
            alive: HashMap::new(),
            budget,
        }
    }

    /// Verify that *every* task's per-worker requirement fits in the
    /// global budget on its own. If not, no number of workers could
    /// ever run that task — we hard-error at startup so the user sees
    /// it immediately rather than mysteriously never spawning.
    pub fn validate(&self, tasks: &[Arc<Task>]) -> Result<(), DaisyError> {
        for task in tasks {
            for (k, v) in &task.requires {
                if *v <= 0 {
                    continue;
                }
                let cap = self.budget.get(k);
                if cap < *v {
                    return Err(DaisyError::InvalidConfig(format!(
                        "task {:?} requires {{{}: {}}} but the global budget \
                         only has {{{}: {}}}",
                        task.task_id, k, v, k, cap
                    )));
                }
            }
        }
        Ok(())
    }

    /// Number of workers currently alive for `task_id`.
    pub fn alive(&self, task_id: &str) -> usize {
        self.alive.get(task_id).copied().unwrap_or(0)
    }

    /// Try to consume one worker's worth of resources for `task`. Returns
    /// false if the budget can't fit it; in that case `alive` and `used`
    /// are unchanged.
    pub fn try_allocate(&mut self, task: &Task) -> bool {
        // No requires → unlimited from the budget's perspective. Just
        // bump alive and return.
        if task.requires.is_empty() {
            *self.alive.entry(task.task_id.clone()).or_insert(0) += 1;
            return true;
        }
        for (k, v) in &task.requires {
            if *v <= 0 {
                continue;
            }
            let used = self.used.get(k).copied().unwrap_or(0);
            if used + v > self.budget.get(k) {
                return false;
            }
        }
        for (k, v) in &task.requires {
            if *v <= 0 {
                continue;
            }
            *self.used.entry(k.clone()).or_insert(0) += v;
        }
        *self.alive.entry(task.task_id.clone()).or_insert(0) += 1;
        true
    }

    /// Release one worker's resources back to the budget. Symmetric to
    /// `try_allocate`. Idempotent at zero — calling on an already-empty
    /// task_id is a no-op (logged elsewhere).
    pub fn release(&mut self, task: &Task) {
        if let Some(count) = self.alive.get_mut(&task.task_id) {
            if *count > 0 {
                *count -= 1;
            }
        }
        if task.requires.is_empty() {
            return;
        }
        for (k, v) in &task.requires {
            if *v <= 0 {
                continue;
            }
            if let Some(used) = self.used.get_mut(k) {
                *used = (*used - v).max(0);
            }
        }
    }

    pub fn budget(&self) -> &ResourceBudget {
        &self.budget
    }

    pub fn used(&self) -> &HashMap<String, i64> {
        &self.used
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::roi::Roi;
    use crate::task::Task;

    fn task(id: &str, requires: &[(&str, i64)]) -> Arc<Task> {
        let r: HashMap<String, i64> = requires
            .iter()
            .map(|(k, v)| ((*k).to_string(), *v))
            .collect();
        Arc::new(
            Task::builder(id)
                .total_roi(Roi::from_slices(&[0], &[10]))
                .read_roi(Roi::from_slices(&[0], &[10]))
                .write_roi(Roi::from_slices(&[0], &[10]))
                .read_write_conflict(false)
                .requires(r)
                .build(),
        )
    }

    fn budget(b: &[(&str, i64)]) -> ResourceBudget {
        ResourceBudget::new(b.iter().map(|(k, v)| ((*k).to_string(), *v)).collect())
    }

    #[test]
    fn validate_fits() {
        let alloc = ResourceAllocator::new(budget(&[("cpu", 32), ("gpu", 8)]));
        let tasks = vec![
            task("a", &[("cpu", 1)]),
            task("b", &[("gpu", 1), ("cpu", 4)]),
        ];
        assert!(alloc.validate(&tasks).is_ok());
    }

    #[test]
    fn validate_fails_on_overshoot() {
        let alloc = ResourceAllocator::new(budget(&[("cpu", 4)]));
        let tasks = vec![task("greedy", &[("cpu", 8)])];
        let err = alloc.validate(&tasks).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("greedy"), "{msg}");
        assert!(msg.contains("cpu"), "{msg}");
    }

    #[test]
    fn allocate_release_round_trip() {
        let mut alloc = ResourceAllocator::new(budget(&[("cpu", 4)]));
        let t = task("t", &[("cpu", 1)]);

        assert!(alloc.try_allocate(&t)); // 1/4
        assert!(alloc.try_allocate(&t)); // 2/4
        assert!(alloc.try_allocate(&t)); // 3/4
        assert!(alloc.try_allocate(&t)); // 4/4
        assert!(!alloc.try_allocate(&t)); // 5/4 — over budget

        assert_eq!(alloc.alive("t"), 4);
        alloc.release(&t);
        assert_eq!(alloc.alive("t"), 3);
        assert!(alloc.try_allocate(&t)); // back to 4/4
    }

    #[test]
    fn empty_requires_is_unlimited_from_budget_perspective() {
        let mut alloc = ResourceAllocator::new(budget(&[("cpu", 1)]));
        let t = task("free", &[]);
        for _ in 0..100 {
            assert!(alloc.try_allocate(&t));
        }
        assert_eq!(alloc.alive("free"), 100);
        // Other tasks still constrained by the real budget.
        let bound = task("bound", &[("cpu", 1)]);
        assert!(alloc.try_allocate(&bound));
        assert!(!alloc.try_allocate(&bound));
    }

    #[test]
    fn shared_resource_split_across_tasks() {
        let mut alloc = ResourceAllocator::new(budget(&[("cpu", 4)]));
        let a = task("a", &[("cpu", 1)]);
        let b = task("b", &[("cpu", 1)]);

        // a takes 3, b takes 1 — total 4.
        assert!(alloc.try_allocate(&a));
        assert!(alloc.try_allocate(&a));
        assert!(alloc.try_allocate(&a));
        assert!(alloc.try_allocate(&b));
        assert!(!alloc.try_allocate(&a));
        assert!(!alloc.try_allocate(&b));

        // a finishes one — b can take it.
        alloc.release(&a);
        assert!(alloc.try_allocate(&b));
    }

    #[test]
    fn multi_resource_task_consumes_all_at_once() {
        let mut alloc = ResourceAllocator::new(budget(&[("cpu", 4), ("gpu", 1)]));
        let gpu = task("gpu", &[("gpu", 1), ("cpu", 4)]);

        assert!(alloc.try_allocate(&gpu));
        // Even though cpu would have room, gpu is exhausted.
        assert!(!alloc.try_allocate(&gpu));
    }
}
