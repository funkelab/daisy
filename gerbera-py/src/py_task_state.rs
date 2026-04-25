use gerbera_core::task_state::TaskState;
use pyo3::prelude::*;

#[pyclass(name = "TaskState", skip_from_py_object)]
#[derive(Clone)]
pub struct PyTaskState {
    pub inner: TaskState,
}

#[pymethods]
impl PyTaskState {
    #[getter]
    fn started(&self) -> bool {
        self.inner.started
    }

    #[getter]
    fn total_block_count(&self) -> i64 {
        self.inner.total_block_count
    }

    #[getter]
    fn ready_count(&self) -> i64 {
        self.inner.ready_count
    }

    #[getter]
    fn processing_count(&self) -> i64 {
        self.inner.processing_count
    }

    #[getter]
    fn completed_count(&self) -> i64 {
        self.inner.completed_count
    }

    #[getter]
    fn skipped_count(&self) -> i64 {
        self.inner.skipped_count
    }

    #[getter]
    fn failed_count(&self) -> i64 {
        self.inner.failed_count
    }

    #[getter]
    fn orphaned_count(&self) -> i64 {
        self.inner.orphaned_count
    }

    #[getter]
    fn pending_count(&self) -> i64 {
        self.inner.pending_count()
    }

    fn is_done(&self) -> bool {
        self.inner.is_done()
    }

    fn __repr__(&self) -> String {
        format!("{}", self.inner)
    }
}
