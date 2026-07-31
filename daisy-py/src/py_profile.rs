//! `daisy.profile_block(block)` — the user-facing seam for per-block
//! resource measurement.
//!
//! Measurement has to happen inside whoever runs the block: that is the only
//! vantage point that sees the same numbers regardless of whether the worker
//! is an in-process thread, a subprocess launched by daisy's shim, or a job
//! on another node. The measured payload is attached to the block and rides
//! home on the existing release message.
//!
//! daisy applies this automatically — thread mode measures in Rust around
//! the call, and every worker that goes through `daisy.Client.acquire_block`
//! (the shim workers and any hand-written cluster worker) is wrapped at that
//! boundary. It stays public because automatic scoping covers the whole
//! block body, and a user who wants to exclude their own setup or measure a
//! narrower region can say so:
//!
//! ```python
//! def process(block):
//!     data = expensive_setup()          # not measured
//!     with daisy.profile_block(block):  # measured
//!         compute(data, block)
//! ```
//!
//! Nesting is harmless: the first measurement to complete wins, so an inner
//! explicit `profile_block` takes precedence over the automatic outer one
//! rather than being overwritten by it.

use daisy_core::block_profile::BlockProfiler;
use pyo3::prelude::*;

use crate::py_block::{PyBlock, PyBlockStats};

/// Context manager returned by `daisy.profile_block(block)`.
#[pyclass(name = "BlockProfile", module = "daisy._daisy")]
pub struct PyBlockProfile {
    block: Py<PyBlock>,
    profiler: Option<BlockProfiler>,
}

#[pymethods]
impl PyBlockProfile {
    #[new]
    fn new(block: Py<PyBlock>) -> Self {
        Self {
            block,
            profiler: None,
        }
    }

    fn __enter__(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.profiler = Some(BlockProfiler::start());
        slf
    }

    /// Attach the measurement to the block.
    ///
    /// Returns `false` so an exception raised inside the block body keeps
    /// propagating — a failed block should still be reported as failed. The
    /// measurement is attached either way, so the tracking layer records
    /// what a failing block cost before it gave up.
    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__(
        &mut self,
        py: Python<'_>,
        _exc_type: Option<Py<PyAny>>,
        _exc_value: Option<Py<PyAny>>,
        _traceback: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        if let Some(profiler) = self.profiler.take() {
            let stats = profiler.finish();
            let mut block = self.block.borrow_mut(py);
            // Don't clobber a measurement already attached by an inner,
            // more precise `profile_block`.
            if block.inner.stats.is_none() {
                block.inner.stats = Some(stats);
            }
        }
        Ok(false)
    }
}

/// Measure the resources a block consumes.
///
/// Use as a context manager around the work for one block:
///
/// ```python
/// with daisy.profile_block(block):
///     ...
/// ```
///
/// daisy's own workers do this for you; call it explicitly only to narrow
/// what gets measured. Requires the task to have `resource_tracking=True`
/// for the measurement to be persisted — otherwise it is attached to the
/// block and ignored.
#[pyfunction]
pub fn profile_block(block: Py<PyBlock>) -> PyBlockProfile {
    PyBlockProfile::new(block)
}

/// Measure a plain callable's block, for callers that would rather not
/// manage a context manager. Exposed for the Python-side auto-wrapping.
#[pyfunction]
pub fn measure_block_stats(py: Python<'_>, block: Py<PyBlock>, f: Py<PyAny>) -> PyResult<Py<PyAny>> {
    let profiler = BlockProfiler::start();
    let result = f.call1(py, (block.clone_ref(py),));
    let stats = profiler.finish();
    {
        let mut b = block.borrow_mut(py);
        if b.inner.stats.is_none() {
            b.inner.stats = Some(stats);
        }
    }
    result
}

/// Build a `BlockStats` from an explicit measurement, for tests.
#[pyfunction]
pub fn _measure_now() -> PyBlockStats {
    PyBlockStats {
        inner: BlockProfiler::start().finish(),
    }
}
