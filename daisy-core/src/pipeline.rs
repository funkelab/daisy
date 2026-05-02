//! `Pipeline` — the canonical task DAG carrier.
//!
//! Tasks are pure data; inter-task block dependencies live on the
//! Pipeline as a directed acyclic graph (`petgraph::DiGraph`).
//! `Scheduler::new`, `Server::run_blockwise`, and `SerialRunner::run`
//! all consume a Pipeline; there is no "upstream" field on `Task`.

use crate::task::Task;
use petgraph::Direction;
use petgraph::algo::is_cyclic_directed;
use petgraph::graph::{DiGraph, NodeIndex};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// A directed acyclic graph of tasks. Tasks are stored as `Arc<Task>`
/// so the same task can appear in multiple pipelines without cloning
/// the underlying data; the DAG structure lives in a parallel
/// `DiGraph` whose `NodeIndex(i)` maps to `tasks[i]`.
pub struct Pipeline {
    pub tasks: Vec<Arc<Task>>,
    graph: DiGraph<(), ()>,
}

impl Pipeline {
    /// New pipeline from a flat task list and an explicit edge list.
    /// Validates that:
    ///   1. Task ids are unique.
    ///   2. Every edge endpoint is one of the listed task ids.
    ///   3. The resulting graph is acyclic.
    pub fn new(tasks: Vec<Arc<Task>>, edges: Vec<(String, String)>) -> Result<Self, String> {
        let mut seen: HashSet<&str> = HashSet::new();
        for t in &tasks {
            if !seen.insert(t.task_id.as_str()) {
                return Err(format!("duplicate task_id in pipeline: {}", t.task_id));
            }
        }
        let id_to_index: HashMap<&str, usize> = tasks
            .iter()
            .enumerate()
            .map(|(i, t)| (t.task_id.as_str(), i))
            .collect();

        let mut graph: DiGraph<(), ()> = DiGraph::with_capacity(tasks.len(), edges.len());
        for _ in 0..tasks.len() {
            graph.add_node(());
        }
        for (up, down) in &edges {
            let u = *id_to_index.get(up.as_str()).ok_or_else(|| {
                format!("edge references unknown upstream task_id: {up}")
            })?;
            let d = *id_to_index.get(down.as_str()).ok_or_else(|| {
                format!("edge references unknown downstream task_id: {down}")
            })?;
            graph.add_edge(NodeIndex::new(u), NodeIndex::new(d), ());
        }
        if is_cyclic_directed(&graph) {
            return Err("pipeline contains a cycle".into());
        }
        Ok(Self { tasks, graph })
    }

    /// Singleton pipeline — one task, no edges.
    pub fn from_task(task: Arc<Task>) -> Self {
        let mut graph: DiGraph<(), ()> = DiGraph::with_capacity(1, 0);
        graph.add_node(());
        Self { tasks: vec![task], graph }
    }

    /// Tasks with no incoming edges.
    pub fn sources(&self) -> Vec<&Arc<Task>> {
        self.graph
            .externals(Direction::Incoming)
            .map(|n| &self.tasks[n.index()])
            .collect()
    }

    /// Tasks with no outgoing edges.
    pub fn outputs(&self) -> Vec<&Arc<Task>> {
        self.graph
            .externals(Direction::Outgoing)
            .map(|n| &self.tasks[n.index()])
            .collect()
    }

    /// Iterator over `(upstream_task_id, downstream_task_id)` edges.
    pub fn edges(&self) -> impl Iterator<Item = (&str, &str)> + '_ {
        self.graph.edge_indices().map(move |e| {
            let (u, d) = self.graph.edge_endpoints(e).unwrap();
            (
                self.tasks[u.index()].task_id.as_str(),
                self.tasks[d.index()].task_id.as_str(),
            )
        })
    }
}
