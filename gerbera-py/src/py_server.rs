use gerbera_core::serial::SerialRunner;
use gerbera_core::server::Server;
use gerbera_core::task::Task;
use gerbera_core::worker_pool::WorkerPool;
use pyo3::prelude::*;
use pyo3::types::PyList;
use std::collections::HashMap;
use std::sync::Arc;

use crate::py_task::PyTask;
use crate::py_task_state::PyTaskState;

fn rt_err(e: impl std::fmt::Display) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e}"))
}

/// Run tasks serially (single-threaded, no TCP).
#[pyfunction]
pub fn _run_serial(
    py: Python<'_>,
    tasks: Bound<'_, PyList>,
) -> PyResult<HashMap<String, PyTaskState>> {
    let mut cache: HashMap<String, Arc<Task>> = HashMap::new();
    let mut arc_tasks: Vec<Arc<Task>> = Vec::new();
    for item in tasks.iter() {
        let bound_task: Bound<'_, PyTask> = item.cast()?.clone();
        let arc = PyTask::convert_task_tree(&bound_task, py, &mut cache)?;
        arc_tasks.push(arc);
    }

    let states = SerialRunner::run(&arc_tasks).map_err(rt_err)?;

    Ok(states
        .into_iter()
        .map(|(k, v)| (k, PyTaskState { inner: v }))
        .collect())
}

/// Run the distributed server with Rust-managed worker threads.
/// Worker threads call back into Python via the GIL to execute
/// process_function / spawn_function.
#[pyfunction]
#[pyo3(signature = (tasks, host="127.0.0.1"))]
pub fn _run_distributed_server(
    py: Python<'_>,
    tasks: Bound<'_, PyList>,
    host: &str,
) -> PyResult<HashMap<String, PyTaskState>> {
    let mut cache: HashMap<String, Arc<Task>> = HashMap::new();
    let mut arc_tasks: Vec<Arc<Task>> = Vec::new();
    for item in tasks.iter() {
        let bound_task: Bound<'_, PyTask> = item.cast()?.clone();
        let arc = PyTask::convert_task_tree(&bound_task, py, &mut cache)?;
        arc_tasks.push(arc);
    }

    let rt = tokio::runtime::Runtime::new().map_err(rt_err)?;
    let (server, listener) = rt.block_on(Server::bind(host)).map_err(rt_err)?;

    // Release GIL and run the event loop. Worker threads are spawned by
    // the Rust server and call back into Python via Python::attach when
    // they need to execute the process function.
    let mut worker_pools: HashMap<String, WorkerPool> = HashMap::new();
    let tasks_clone = arc_tasks.clone();
    let states = py.detach(move || {
        rt.block_on(server.run_blockwise(listener, &tasks_clone, &mut worker_pools))
    })
    .map_err(rt_err)?;

    Ok(states
        .into_iter()
        .map(|(k, v)| (k, PyTaskState { inner: v }))
        .collect())
}
