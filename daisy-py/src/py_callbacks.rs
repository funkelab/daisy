use daisy_core::block::Block;
use daisy_core::error::DaisyError;
use daisy_core::server::ProgressObserver;
use daisy_core::task::{CheckBlock, ProcessBlock, SpawnWorker};
use daisy_core::task_state::TaskCounters;
use pyo3::prelude::*;

/// Cap a (possibly multi-line) traceback string: keep the tail — the
/// raise site — and mark the cut. Mirrors the python-side cap in
/// `daisy._task._capped_traceback`.
pub(crate) fn cap_traceback(s: &str) -> String {
    const MAX_LINES: usize = 50;
    const MAX_BYTES: usize = 8192;
    let lines: Vec<&str> = s.lines().collect();
    let mut out = if lines.len() > MAX_LINES {
        let mut v = vec!["... (traceback truncated) ..."];
        v.extend(&lines[lines.len() - MAX_LINES..]);
        v.join("\n")
    } else {
        s.to_string()
    };
    if out.len() > MAX_BYTES {
        let cut = out.len() - MAX_BYTES;
        // find a char boundary at or after the cut point
        let mut idx = cut;
        while !out.is_char_boundary(idx) {
            idx += 1;
        }
        out = format!("... (traceback truncated) ...\n{}", &out[idx..]);
    }
    out
}
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
    /// Whether to measure each block. Mirrors the task's
    /// `resource_tracking`, so an unmeasured run costs nothing.
    resource_tracking: bool,
}

impl PyProcessBlock {
    pub fn new(py_fn: Py<PyAny>) -> Self {
        Self {
            py_fn,
            resource_tracking: false,
        }
    }

    /// Enable per-block measurement for this callback.
    pub fn with_resource_tracking(py_fn: Py<PyAny>, resource_tracking: bool) -> Self {
        Self {
            py_fn,
            resource_tracking,
        }
    }
}

// SAFETY: PyProcessBlock holds a Py<PyAny> which is Send. The GIL ensures
// only one thread calls into Python at a time.
unsafe impl Sync for PyProcessBlock {}

impl ProcessBlock for PyProcessBlock {
    fn process(&self, block: &mut Block) -> Result<(), DaisyError> {
        Python::attach(|py| {
            let py_block = PyBlock::from_core(block.clone());
            // Thread mode measures here rather than through the Python
            // context manager: same measurement code, no GIL round trip,
            // and it covers the whole call including anything the user
            // does before their own `profile_block` (if they add one).
            let profiler = self
                .resource_tracking
                .then(daisy_core::block_profile::BlockProfiler::start);
            let result: PyResult<Py<PyAny>> = self.py_fn.call1(py, (py_block.clone(),));
            // Carry back whatever the function attached; fall back to our
            // own measurement. Done for failures too, so a failing block's
            // cost is still recorded.
            block.stats = py_block.inner.stats.or_else(|| profiler.map(|p| p.finish()));
            match result {
                Ok(_) => {
                    block.status = py_block.inner.status;
                    Ok(())
                }
                Err(e) => {
                    // Include the formatted python traceback (capped)
                    // so the failure cause survives to the run summary
                    // and abandonment error, not just the worker log.
                    let tb = e
                        .traceback(py)
                        .and_then(|t| t.format().ok())
                        .unwrap_or_default();
                    let formatted = cap_traceback(&format!("{tb}{e}"));
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

impl PySpawnWorker {
    /// True when the spawn function declares a keyword-only `context`
    /// parameter (`def start_worker(*, context): ...`). Keyword-only
    /// params don't count toward positional arity, so this composes
    /// with the 0-positional-args == spawn-function classification.
    fn wants_context(&self, py: Python<'_>) -> bool {
        (|| -> PyResult<bool> {
            let inspect = py.import("inspect")?;
            let argspec = inspect.call_method1(
                "getfullargspec",
                (self.py_fn.clone_ref(py),),
            )?;
            let kwonly: Vec<String> = argspec.getattr("kwonlyargs")?.extract()?;
            Ok(kwonly.iter().any(|a| a == "context"))
        })()
        .unwrap_or(false)
    }
}

impl SpawnWorker for PySpawnWorker {
    fn spawn(&self, env_context: &str) -> Result<(), DaisyError> {
        Python::attach(|py| {
            // Still set the env var: 0-arg spawn functions and child
            // processes that read DAISY_CONTEXT keep working. NOTE this
            // variable is process-global — with concurrent spawns, a slow
            // spawn function can observe a later worker's value. Spawn
            // functions that need a reliable identity must take the
            // keyword-only `context` argument below.
            let os = py.import("os").map_err(|e| DaisyError::ProcessFailed(format!("{e}")))?;
            let environ = os.getattr("environ").map_err(|e| DaisyError::ProcessFailed(format!("{e}")))?;
            environ
                .set_item("DAISY_CONTEXT", env_context)
                .map_err(|e| DaisyError::ProcessFailed(format!("{e}")))?;

let wrap_err = |e: pyo3::PyErr| {
                // include the formatted python traceback (capped), as
                // for block functions — see PyProcessBlock::process
                Python::attach(|py| {
                    let tb = e
                        .traceback(py)
                        .and_then(|t| t.format().ok())
                        .unwrap_or_default();
                    DaisyError::ProcessFailed(cap_traceback(&format!("{tb}{e}")))
                })
            };
            if self.wants_context(py) {
                // Race-free path: this worker's context, passed by value.
                let ctx = crate::py_context::PyContext::from_encoded(env_context)
                    .map_err(|e| DaisyError::ProcessFailed(format!("{e}")))?;
                let kwargs = PyDict::new(py);
                kwargs
                    .set_item("context", ctx.into_pyobject(py).map_err(|e| {
                        DaisyError::ProcessFailed(format!("{e}"))
                    })?)
                    .map_err(|e| DaisyError::ProcessFailed(format!("{e}")))?;
                self.py_fn.call(py, (), Some(&kwargs)).map_err(wrap_err)?;
            } else {
                self.py_fn.call0(py).map_err(wrap_err)?;
            }
            Ok(())
        })
    }
}
