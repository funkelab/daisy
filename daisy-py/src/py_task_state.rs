use daisy_core::task_state::TaskCounters;
use pyo3::prelude::*;

/// Python-facing wrapper around a `TaskCounters` snapshot. The Rust
/// scheduler internally tracks tasks as a typestate enum
/// (`Running`/`Done`/`Abandoned`), but observers and post-run
/// consumers only care about the counter values, so we hand them a
/// frozen snapshot at the FFI boundary.
#[pyclass(name = "TaskState", skip_from_py_object)]
#[derive(Clone)]
pub struct PyTaskState {
    pub inner: TaskCounters,
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

    #[getter]
    fn worker_failure_count(&self) -> u32 {
        self.inner.worker_failure_count
    }

    #[getter]
    fn worker_restart_count(&self) -> u32 {
        self.inner.worker_restart_count
    }

    fn is_done(&self) -> bool {
        // For a counter snapshot, "done" means the counters balance.
        // Frozen snapshots from terminal variants (Done/Abandoned)
        // always balance because the abandon transition orphans the
        // remainder before snapshotting.
        self.inner.balanced()
    }

    fn __repr__(&self) -> String {
        format!("{}", self.inner)
    }
}
