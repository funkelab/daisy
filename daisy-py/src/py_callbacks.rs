use daisy_core::block::Block;
use daisy_core::error::DaisyError;
use daisy_core::server::ProgressObserver;
use daisy_core::task::{CheckBlock, ProcessBlock, SpawnWorker};
use daisy_core::task_state::TaskCounters;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::cell::RefCell;
use std::collections::HashMap;
use crate::py_block::PyBlock;
use crate::py_task_state::PyTaskState;

// Per-thread stash for the most recent Python exception raised by a
// `process_function`. Serial mode (`_run_serial`) clears this before
// the run starts and consults it on `Err` return so the original
// `PyErr` (with its full traceback chain) propagates back to the user
// instead of a string-formatted `RuntimeError` wrapper. Multiprocessing
// workers run on tokio threads, each with its own thread-local — they
// don't interfere with the main thread's stash.
thread_local! {
    static LAST_PROCESS_PYERR: RefCell<Option<PyErr>> = const { RefCell::new(None) };
}

pub fn clear_last_process_pyerr() {
    LAST_PROCESS_PYERR.with(|c| *c.borrow_mut() = None);
}

pub fn take_last_process_pyerr() -> Option<PyErr> {
    LAST_PROCESS_PYERR.with(|c| c.borrow_mut().take())
}

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
    fn process(&self, block: &mut Block) -> Result<(), DaisyError> {
        Python::attach(|py| {
            let py_block = PyBlock::from_core(block.clone());
            let result: PyResult<Py<PyAny>> = self.py_fn.call1(py, (py_block.clone(),));
            match result {
                Ok(_) => {
                    block.status = py_block.inner.status;
                    Ok(())
                }
                Err(e) => {
                    let formatted = format!("{e}");
                    // Stash the original PyErr so serial mode can
                    // re-raise it with its full traceback intact.
                    LAST_PROCESS_PYERR.with(|c| *c.borrow_mut() = Some(e));
                    Err(DaisyError::ProcessFailed(formatted))
                }
            }
        })
    }
}

/// Wraps a Python 0-arg callable as a `SpawnWorker` implementation.
/// Acquires the GIL, sets the DAISY_CONTEXT env var, and calls the function.
pub struct PySpawnWorker {
    py_fn: Py<PyAny>,
}

impl PySpawnWorker {
    pub fn new(py_fn: Py<PyAny>) -> Self {
        Self { py_fn }
    }
}

unsafe impl Sync for PySpawnWorker {}

/// Bridge between Rust's `ProgressObserver` and a Python observer that
/// implements `on_start(states_dict)`, `on_progress(states_dict)`, and
/// `on_finish(states_dict)`. Each callback acquires the GIL.
pub struct PyProgressObserver {
    py_obj: Py<PyAny>,
}

impl PyProgressObserver {
    pub fn new(py_obj: Py<PyAny>) -> Self {
        Self { py_obj }
    }

    fn call(&self, method: &str, states: &HashMap<String, TaskCounters>) {
        // Best-effort: a busted observer must not break the run loop.
        Python::attach(|py| -> PyResult<()> {
            let d = PyDict::new(py);
            for (k, v) in states {
                d.set_item(
                    k,
                    Py::new(py, PyTaskState { inner: v.clone() })?,
                )?;
            }
            self.py_obj.call_method1(py, method, (d,))?;
            Ok(())
        })
        .ok();
    }
}

unsafe impl Sync for PyProgressObserver {}

impl ProgressObserver for PyProgressObserver {
    fn on_progress(&self, states: &HashMap<String, TaskCounters>) {
        self.call("on_progress", states);
    }
    fn on_start(&self, states: &HashMap<String, TaskCounters>) {
        self.call("on_start", states);
    }
    fn on_finish(&self, states: &HashMap<String, TaskCounters>) {
        self.call("on_finish", states);
    }
}

impl SpawnWorker for PySpawnWorker {
    fn spawn(&self, env_context: &str) -> Result<(), DaisyError> {
        Python::attach(|py| {
            // Set the env var so Client() / subprocess workers can find the server.
            let os = py.import("os").map_err(|e| DaisyError::ProcessFailed(format!("{e}")))?;
            let environ = os.getattr("environ").map_err(|e| DaisyError::ProcessFailed(format!("{e}")))?;
            environ
                .set_item("DAISY_CONTEXT", env_context)
                .map_err(|e| DaisyError::ProcessFailed(format!("{e}")))?;

            self.py_fn
                .call0(py)
                .map_err(|e| DaisyError::ProcessFailed(format!("{e}")))?;
            Ok(())
        })
    }
}
