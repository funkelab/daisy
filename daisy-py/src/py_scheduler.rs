use daisy_core::scheduler::Scheduler;
use pyo3::prelude::*;

use crate::py_block::PyBlock;
use crate::py_dep_graph::PyDependencyGraph;
use crate::py_pipeline::PyPipeline;
use crate::py_task_state::PyTaskState;
use std::collections::HashMap;

#[pyclass(name = "Scheduler", skip_from_py_object, unsendable)]
pub struct PyScheduler {
    inner: Scheduler,
}

#[pymethods]
impl PyScheduler {
    /// Construct from a `Pipeline`, a single `Task`, or a list of
    /// tasks. List input goes through Python's `_task._build_pipeline_from_tasks`
    /// to honour v1.x-style `Task(upstream_tasks=[...])` declarations.
    /// Construct from a `Pipeline` or a `Task` (singleton-promoted).
    /// Lists of tasks are not accepted at this layer — v1_compat
    /// converts them to a Pipeline before reaching here.
    #[new]
    fn new(py: Python<'_>, input: &Bound<'_, PyAny>) -> PyResult<Self> {
        let pipeline = if let Ok(p) = input.downcast::<PyPipeline>() {
            p.clone().unbind()
        } else if input.downcast::<crate::py_task::PyTask>().is_ok() {
            Py::new(py, PyPipeline::from_task(input.clone().unbind()))?
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "Scheduler expects a Pipeline or a Task; got {}",
                input.get_type().name()?
            )));
        };
        let core_pipeline = pipeline.borrow(py).to_core(py)?;
        let scheduler = Scheduler::new(&core_pipeline, true);
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
