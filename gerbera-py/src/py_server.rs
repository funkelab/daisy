use gerbera_core::resource_allocator::ResourceBudget;
use gerbera_core::serial::SerialRunner;
use gerbera_core::server::Server;
use gerbera_core::task::Task;
use gerbera_core::worker_pool::WorkerPool;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
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
///
/// Returns a 2-tuple `(task_states, run_stats)` where `run_stats` is a
/// nested dict matching `gerbera_core::run_stats::RunStats`.
#[pyfunction]
#[pyo3(signature = (tasks, resources=None, host="127.0.0.1"))]
pub fn _run_distributed_server(
    py: Python<'_>,
    tasks: Bound<'_, PyList>,
    resources: Option<Bound<'_, PyDict>>,
    host: &str,
) -> PyResult<Py<pyo3::types::PyTuple>> {
    let mut cache: HashMap<String, Arc<Task>> = HashMap::new();
    let mut arc_tasks: Vec<Arc<Task>> = Vec::new();
    for item in tasks.iter() {
        let bound_task: Bound<'_, PyTask> = item.cast()?.clone();
        let arc = PyTask::convert_task_tree(&bound_task, py, &mut cache)?;
        arc_tasks.push(arc);
    }

    let budget = if let Some(d) = resources {
        let mut m = HashMap::new();
        for (k, v) in d.iter() {
            let key: String = k.extract()?;
            let val: i64 = v.extract()?;
            m.insert(key, val);
        }
        ResourceBudget::new(m)
    } else {
        ResourceBudget::empty()
    };

    let rt = tokio::runtime::Runtime::new().map_err(rt_err)?;
    let (server, listener) = rt.block_on(Server::bind(host)).map_err(rt_err)?;

    // Release GIL and run the event loop. Worker threads are spawned by
    // the Rust server and call back into Python via Python::attach when
    // they need to execute the process function.
    let mut worker_pools: HashMap<String, WorkerPool> = HashMap::new();
    let tasks_clone = arc_tasks.clone();
    let (states, run_stats) = py.detach(move || {
        rt.block_on(server.run_blockwise(listener, &tasks_clone, &mut worker_pools, budget))
    })
    .map_err(rt_err)?;

    let states_py: HashMap<String, PyTaskState> = states
        .into_iter()
        .map(|(k, v)| (k, PyTaskState { inner: v }))
        .collect();
    let stats_py = run_stats_to_py(py, &run_stats)?;

    let result = pyo3::types::PyTuple::new(py, &[states_py.into_pyobject(py)?.into_any(), stats_py])?;
    Ok(result.unbind())
}

fn run_stats_to_py<'py>(
    py: Python<'py>,
    s: &gerbera_core::run_stats::RunStats,
) -> PyResult<Bound<'py, PyAny>> {
    let process = PyDict::new(py);
    process.set_item("wall_time_secs", s.process.wall_time.as_secs_f64())?;
    process.set_item("peak_rss_bytes", s.process.peak_rss_bytes)?;
    process.set_item("peak_virt_bytes", s.process.peak_virt_bytes)?;
    process.set_item("total_cpu_time_secs", s.process.total_cpu_time.as_secs_f64())?;
    process.set_item("disk_read_bytes", s.process.disk_read_bytes)?;
    process.set_item("disk_write_bytes", s.process.disk_write_bytes)?;
    process.set_item("unavailable", s.process.unavailable)?;

    let per_task = PyDict::new(py);
    for (task_id, t) in &s.per_task {
        let d = PyDict::new(py);
        d.set_item("blocks_processed", t.blocks_processed)?;
        d.set_item("max_concurrent_workers", t.max_concurrent_workers)?;
        d.set_item("total_block_time_secs", t.total_block_time.as_secs_f64())?;
        d.set_item("total_wall_time_secs", t.total_wall_time.as_secs_f64())?;
        match t.total_cpu_time {
            Some(c) => d.set_item("total_cpu_time_secs", c.as_secs_f64())?,
            None => d.set_item("total_cpu_time_secs", py.None())?,
        }
        d.set_item("mean_block_ms", t.mean_block_ms)?;
        d.set_item("block_ms_slope", t.block_ms_slope)?;
        per_task.set_item(task_id, d)?;
    }

    let per_worker = PyList::empty(py);
    for w in &s.per_worker {
        let d = PyDict::new(py);
        d.set_item("task_id", &w.task_id)?;
        d.set_item("worker_id", w.worker_id)?;
        d.set_item("wall_time_secs", w.wall_time.as_secs_f64())?;
        match w.cpu_time {
            Some(c) => d.set_item("cpu_time_secs", c.as_secs_f64())?,
            None => d.set_item("cpu_time_secs", py.None())?,
        }
        d.set_item("blocks_processed", w.blocks_processed)?;
        per_worker.append(d)?;
    }

    let out = PyDict::new(py);
    out.set_item("process", process)?;
    out.set_item("per_task", per_task)?;
    out.set_item("per_worker", per_worker)?;
    Ok(out.into_any())
}
