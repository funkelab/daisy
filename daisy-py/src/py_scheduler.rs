use daisy_core::scheduler::Scheduler;
use daisy_core::task::Task;
use pyo3::prelude::*;
use pyo3::types::PyList;
use std::collections::HashMap;
use std::sync::Arc;

use crate::py_block::PyBlock;
use crate::py_dep_graph::PyDependencyGraph;
use crate::py_task::PyTask;
use crate::py_task_state::PyTaskState;

#[pyclass(name = "Scheduler", skip_from_py_object, unsendable)]
pub struct PyScheduler {
    inner: Scheduler,
}

#[pymethods]
impl PyScheduler {
    #[new]
    fn new(py: Python<'_>, tasks: Bound<'_, PyList>) -> PyResult<Self> {
        let mut cache: HashMap<String, Arc<Task>> = HashMap::new();
        let mut arc_tasks = Vec::new();
        for item in tasks.iter() {
            let bound_task: Bound<'_, PyTask> = item.cast()?.clone();
            let arc = PyTask::convert_task_tree(&bound_task, py, &mut cache)?;
            arc_tasks.push(arc);
        }
        let scheduler = Scheduler::new(&arc_tasks, true);
        Ok(Self { inner: scheduler })
    }

    fn acquire_block(&mut self, task_id: &str) -> Option<PyBlock> {
        self.inner
            .acquire_block(task_id)
            .map(PyBlock::from_core)
    }

    fn release_block(&mut self, block: &PyBlock) {
        self.inner.release_block(block.inner.clone());
    }

    #[getter]
    fn task_states(&self) -> HashMap<String, PyTaskState> {
        self.inner
            .task_states
            .iter()
            .map(|(k, v)| (k.clone(), PyTaskState { inner: v.counters() }))
            .collect()
    }

    #[getter]
    fn dependency_graph(&self) -> PyDependencyGraph {
        PyDependencyGraph::from_scheduler(&self.inner)
    }
}
