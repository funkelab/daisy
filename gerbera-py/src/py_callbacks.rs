use gerbera_core::block::Block;
use gerbera_core::error::GerberaError;
use gerbera_core::task::{CheckBlock, ProcessBlock, SpawnWorker};
use pyo3::prelude::*;
use crate::py_block::PyBlock;

/// Wraps a Python callable as a `CheckBlock` implementation.
/// Acquires the GIL on each call to invoke the Python function.
pub struct PyCheckBlock {
    py_fn: Py<PyAny>,
}

impl PyCheckBlock {
    pub fn new(py_fn: Py<PyAny>) -> Self {
        Self { py_fn }
    }
}

impl CheckBlock for PyCheckBlock {
    fn check(&self, block: &Block) -> bool {
        Python::attach(|py| {
            let py_block = PyBlock::from_core(block.clone());
            self.py_fn
                .call1(py, (py_block,))
                .and_then(|r: Py<PyAny>| r.extract::<bool>(py))
                .unwrap_or(false)
        })
    }
}

/// Wraps a Python callable as a `ProcessBlock` implementation.
/// Acquires the GIL on each call to invoke the Python function.
pub struct PyProcessBlock {
    py_fn: Py<PyAny>,
}

impl PyProcessBlock {
    pub fn new(py_fn: Py<PyAny>) -> Self {
        Self { py_fn }
    }
}

// SAFETY: PyProcessBlock holds a Py<PyAny> which is Send. The GIL ensures
// only one thread calls into Python at a time.
unsafe impl Sync for PyProcessBlock {}

impl ProcessBlock for PyProcessBlock {
    fn process(&self, block: &mut Block) -> Result<(), GerberaError> {
        Python::attach(|py| {
            let py_block = PyBlock::from_core(block.clone());
            let result: PyResult<Py<PyAny>> = self.py_fn.call1(py, (py_block.clone(),));
            match result {
                Ok(_) => {
                    block.status = py_block.inner.status;
                    Ok(())
                }
                Err(e) => Err(GerberaError::ProcessFailed(format!("{e}"))),
            }
        })
    }
}

/// Wraps a Python 0-arg callable as a `SpawnWorker` implementation.
/// Acquires the GIL, sets the GERBERA_CONTEXT env var, and calls the function.
pub struct PySpawnWorker {
    py_fn: Py<PyAny>,
}

impl PySpawnWorker {
    pub fn new(py_fn: Py<PyAny>) -> Self {
        Self { py_fn }
    }
}

unsafe impl Sync for PySpawnWorker {}

impl SpawnWorker for PySpawnWorker {
    fn spawn(&self, env_context: &str) -> Result<(), GerberaError> {
        Python::attach(|py| {
            // Set the env var so Client() / subprocess workers can find the server.
            let os = py.import("os").map_err(|e| GerberaError::ProcessFailed(format!("{e}")))?;
            let environ = os.getattr("environ").map_err(|e| GerberaError::ProcessFailed(format!("{e}")))?;
            environ
                .set_item("GERBERA_CONTEXT", env_context)
                .map_err(|e| GerberaError::ProcessFailed(format!("{e}")))?;

            self.py_fn
                .call0(py)
                .map_err(|e| GerberaError::ProcessFailed(format!("{e}")))?;
            Ok(())
        })
    }
}
