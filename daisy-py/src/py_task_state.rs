use daisy_core::task_state::TaskCounters;
use pyo3::prelude::*;

/// Python-facing wrapper around a `TaskCounters` snapshot. The Rust
/// scheduler internally tracks tasks as a typestate enum
/// (`Running`/`Done`/`Abandoned`), but observers and post-run
/// consumers only care about the counter values, so we hand them a
/// frozen snapshot at the FFI boundary.
#[pyclass(name = "TaskState", skip_from_py_object, module = "daisy._daisy")]
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

    /// True iff the task was abandoned (restart cap exhausted, or an
    /// upstream task was abandoned) rather than run to completion.
    #[getter]
    fn abandoned(&self) -> bool {
        self.inner.abandon_reason.is_some()
    }

    /// Human-readable abandonment reason, or None if not abandoned.
    #[getter]
    fn abandon_reason(&self) -> Option<String> {
        self.inner.abandon_reason.as_ref().map(|r| r.to_string())
    }

    /// The most recent worker error observed for this task (spawn
    /// function failure, dirty worker exit, or a reported block
    /// failure), or None if no worker ever errored.
    #[getter]
    fn last_worker_error(&self) -> Option<String> {
        self.inner.last_worker_error.clone()
    }

    /// The first worker error observed for this task (usually the
    /// root cause; later errors are often echoes), or None.
    #[getter]
    fn first_worker_error(&self) -> Option<String> {
        self.inner.first_worker_error.clone()
    }

    #[getter]
    fn worker_start_count(&self) -> u32 {
        self.inner.worker_start_count
    }

    /// Block attempts reclaimed for exceeding the block timeout.
    #[getter]
    fn timeout_reclaim_count(&self) -> u32 {
        self.inner.timeout_reclaim_count
    }

    /// The task's configured block timeout in seconds.
    #[getter]
    fn timeout_secs(&self) -> Option<f64> {
        self.inner.timeout_secs
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
