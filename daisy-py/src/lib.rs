use pyo3::prelude::*;
use pyo3::types::PyList;
use std::collections::HashMap;
use std::sync::Arc;

mod py_block;
mod py_callbacks;
mod py_dep_graph;
mod py_roi;
mod py_scheduler;
mod py_server;
mod py_sync_client;
mod py_task;
mod py_task_state;

use py_block::{PyBlock, PyBlockStatus};
use py_dep_graph::{PyBlockwiseDepGraph, PyDependencyGraph};
use py_roi::{PyCoordinate, PyRoi};
use py_scheduler::PyScheduler;
use py_sync_client::PySyncClient;
use py_task::PyTask;
use py_task_state::PyTaskState;

/// Run tasks to completion in serial mode (single-threaded, no TCP).
#[pyfunction]
#[pyo3(signature = (tasks, multiprocessing=true))]
fn run_blockwise(py: Python<'_>, tasks: Bound<'_, PyList>, multiprocessing: bool) -> PyResult<bool> {
    if multiprocessing {
        return Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
            "use daisy.Server().run_blockwise() for distributed mode",
        ));
    }

    let mut cache: HashMap<String, Arc<daisy_core::Task>> = HashMap::new();
    let mut arc_tasks = Vec::new();
    for item in tasks.iter() {
        let bound_task: Bound<'_, PyTask> = item.cast()?.clone();
        let arc = PyTask::convert_task_tree(&bound_task, py, &mut cache)?;
        arc_tasks.push(arc);
    }

    let states = daisy_core::SerialRunner::run(&arc_tasks)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    Ok(states.values().all(|s| s.balanced()))
}

#[pymodule]
fn _daisy(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyCoordinate>()?;
    m.add_class::<PyRoi>()?;
    m.add_class::<PyBlock>()?;
    m.add_class::<PyBlockStatus>()?;
    m.add_class::<PyTask>()?;
    m.add_class::<PyTaskState>()?;
    m.add_class::<PyScheduler>()?;
    m.add_class::<PyBlockwiseDepGraph>()?;
    m.add_class::<PyDependencyGraph>()?;
    m.add_class::<PySyncClient>()?;
    m.add_function(wrap_pyfunction!(run_blockwise, m)?)?;
    m.add_function(wrap_pyfunction!(py_server::_run_serial, m)?)?;
    m.add_function(wrap_pyfunction!(py_server::_run_distributed_server, m)?)?;
    Ok(())
}
