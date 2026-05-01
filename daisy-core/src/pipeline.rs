//! `Pipeline` — the canonical task DAG carrier.
//!
//! Tasks are pure data; inter-task block dependencies live on the
//! Pipeline as `edges: Vec<(upstream_task_id, downstream_task_id)>`.
//! `Scheduler::new`, `Server::run_blockwise`, and `SerialRunner::run`
//! all consume a Pipeline; there is no "upstream" field on `Task`.

use crate::task::Task;
use std::collections::HashSet;
use std::sync::Arc;

/// A directed acyclic graph of tasks with explicit edges. Tasks
/// stored as `Arc<Task>` so the same task can appear in multiple
/// pipelines without cloning the underlying data.
#[derive(Clone)]
pub struct Pipeline {
    pub tasks: Vec<Arc<Task>>,
    pub edges: Vec<(String, String)>,
}

impl Pipeline {
    /// New pipeline from a flat task list and an explicit edge list.
    /// Validates that every edge endpoint is one of the listed
    /// task ids and that there are no duplicate task ids.
    pub fn new(tasks: Vec<Arc<Task>>, edges: Vec<(String, String)>) -> Result<Self, String> {
        let mut seen: HashSet<&str> = HashSet::new();
        for t in &tasks {
            if !seen.insert(t.task_id.as_str()) {
                return Err(format!("duplicate task_id in pipeline: {}", t.task_id));
            }
        }
        for (up, down) in &edges {
            if !seen.contains(up.as_str()) {
                return Err(format!("edge references unknown upstream task_id: {up}"));
            }
            if !seen.contains(down.as_str()) {
                return Err(format!("edge references unknown downstream task_id: {down}"));
            }
        }
        Ok(Self { tasks, edges })
    }

    /// Singleton pipeline — one task, no edges.
    pub fn from_task(task: Arc<Task>) -> Self {
        Self { tasks: vec![task], edges: Vec::new() }
    }

    /// Tasks with no incoming edges.
    pub fn sources(&self) -> Vec<&Arc<Task>> {
        let with_incoming: HashSet<&str> =
            self.edges.iter().map(|(_, d)| d.as_str()).collect();
        self.tasks
            .iter()
            .filter(|t| !with_incoming.contains(t.task_id.as_str()))
            .collect()
    }

    /// Tasks with no outgoing edges.
    pub fn outputs(&self) -> Vec<&Arc<Task>> {
        let with_outgoing: HashSet<&str> =
            self.edges.iter().map(|(u, _)| u.as_str()).collect();
        self.tasks
            .iter()
            .filter(|t| !with_outgoing.contains(t.task_id.as_str()))
            .collect()
    }
}
