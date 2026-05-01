use daisy_core::resource_allocator::ResourceBudget;
use daisy_core::serial::SerialRunner;
use daisy_core::server::{ProgressObserver, Server};
use daisy_core::task::Task;
use daisy_core::worker_pool::WorkerPool;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Set by our raw SIGINT handler. Read by the abort-check callback
/// the run loop polls every 100ms.
///
/// We bypass `PyErr_CheckSignals` because CPython only processes
/// signals on the main thread, and tokio's multi-threaded runtime
/// polls our abort arm on whichever worker thread is available —
/// almost never the main thread. A raw POSIX handler sets this
/// flag the moment SIGINT is delivered to the process, regardless
/// of which thread the kernel routes it to, and the flag read is
/// GIL-free.
static SIGINT_FLAG: AtomicBool = AtomicBool::new(false);

/// Stash of the SIGINT handler installed before we took over, so we
/// can restore it after the run completes (even on panic / error).
/// `0` means "no previous handler captured" — at process start the
/// default is `SIG_DFL` (`0`), which is what Python overrides on
/// import.
static PREV_SIGINT_HANDLER: AtomicUsize = AtomicUsize::new(0);

extern "C" fn handle_sigint(_signum: libc::c_int) {
    SIGINT_FLAG.store(true, Ordering::SeqCst);
}

use crate::py_callbacks::PyProgressObserver;

use crate::py_task::PyTask;
use crate::py_task_state::PyTaskState;

/// Topological order of `tasks` with alphabetical tiebreaker on the
/// ready set. Roots first; a task becomes a candidate once every one
/// of its upstream dependencies has been emitted; from the candidate
/// set we always pick the alphabetically smallest. This is the order
/// used to render the post-run execution summary.
#[pyfunction]
pub fn _topo_order(tasks: Bound<'_, PyList>) -> PyResult<Vec<String>> {
    use std::collections::{BinaryHeap, HashMap, HashSet};

    let mut all_tasks: HashSet<String> = HashSet::new();
    let mut upstream_map: HashMap<String, HashSet<String>> = HashMap::new();
    let mut downstream_map: HashMap<String, Vec<String>> = HashMap::new();

    fn visit(
        t: &Bound<'_, PyAny>,
        all_tasks: &mut HashSet<String>,
        upstream_map: &mut HashMap<String, HashSet<String>>,
        downstream_map: &mut HashMap<String, Vec<String>>,
    ) -> PyResult<()> {
        let tid: String = t.getattr("task_id")?.extract()?;
        if all_tasks.contains(&tid) {
            return Ok(());
        }
        all_tasks.insert(tid.clone());
        let ups_obj = t.getattr("upstream_tasks")?;
        let ups: Vec<Bound<'_, PyAny>> = ups_obj.try_iter()?.collect::<PyResult<_>>()?;
        let mut up_ids: HashSet<String> = HashSet::new();
        for u in &ups {
            let uid: String = u.getattr("task_id")?.extract()?;
            up_ids.insert(uid.clone());
            downstream_map.entry(uid).or_default().push(tid.clone());
        }
        upstream_map.insert(tid, up_ids);
        for u in &ups {
            visit(u, all_tasks, upstream_map, downstream_map)?;
        }
        Ok(())
    }

    for t in tasks.iter() {
        visit(&t, &mut all_tasks, &mut upstream_map, &mut downstream_map)?;
    }

    // Min-heap by alphabetical order: BinaryHeap is a max-heap, so we
    // wrap with `std::cmp::Reverse`.
    use std::cmp::Reverse;
    let mut ready: BinaryHeap<Reverse<String>> = BinaryHeap::new();
    for (tid, ups) in &upstream_map {
        if ups.is_empty() {
            ready.push(Reverse(tid.clone()));
        }
    }

    let mut visited: HashSet<String> = HashSet::new();
    let mut order: Vec<String> = Vec::new();
    while let Some(Reverse(tid)) = ready.pop() {
        if visited.contains(&tid) {
            continue;
        }
        visited.insert(tid.clone());
        order.push(tid.clone());
        if let Some(children) = downstream_map.get(&tid).cloned() {
            for child in children {
                if visited.contains(&child) {
                    continue;
                }
                let child_ups = &upstream_map[&child];
                if child_ups.iter().all(|u| visited.contains(u)) {
                    ready.push(Reverse(child));
                }
            }
        }
    }

    Ok(order)
}

/// Top-level orchestrator. Receives already-converted Rust tasks
/// (`_rs.Task` instances), computes the topological display order in
/// Rust, dispatches to the serial or distributed runner, and calls
/// back into Python for the execution summary printing (which lives
/// in Python because it shares the per-worker logging / stdout
/// machinery — see the user's "logging" carve-out for the all-Rust
/// rewrite). Returns `True` only if every block of every task either
/// completed successfully or was skipped from a prior run.
#[pyfunction]
#[pyo3(signature = (
    tasks,
    multiprocessing = true,
    resources = None,
    progress = None,
    block_tracking = true,
))]
pub fn _run_blockwise_orchestrator(
    py: Python<'_>,
    tasks: Bound<'_, PyList>,
    multiprocessing: bool,
    resources: Option<Bound<'_, PyDict>>,
    progress: Option<Py<PyAny>>,
    block_tracking: bool,
) -> PyResult<bool> {
    // Compute the display topological order.
    let order = _topo_order(tasks.clone())?;
    let order_py = pyo3::types::PyList::new(py, &order)?;

    // Resolve progress argument:
    //   - Python True (or omitted in distributed mode) → _TqdmObserver(task_order)
    //   - Python False / None → no observer
    //   - object → use as-is
    // Always disabled in serial mode regardless of arg.
    let progress_obj: Option<Py<PyAny>> = if multiprocessing {
        match progress {
            None => None,
            Some(p) => {
                let bound = p.bind(py);
                if bound.is_none() {
                    None
                } else if let Ok(b) = bound.extract::<bool>() {
                    if b {
                        let progress_mod = py.import("daisy._progress")?;
                        let tqdm_class = progress_mod.getattr("_TqdmObserver")?;
                        Some(tqdm_class.call1((order_py.clone(),))?.unbind())
                    } else {
                        None
                    }
                } else {
                    Some(p)
                }
            }
        }
    } else {
        None
    };

    // Dispatch to the appropriate runner. Both paths return a Python
    // dict of `task_id → PyTaskState` plus optional run stats.
    let (states_obj, run_stats_obj): (Py<PyAny>, Option<Py<PyAny>>) = if multiprocessing {
        let result = _run_distributed_server(
            py,
            tasks,
            resources,
            progress_obj,
            "127.0.0.1",
            block_tracking,
        )?;
        let tup = result.bind(py);
        let states: Py<PyAny> = tup.get_item(0)?.into_pyobject(py)?.into_any().unbind();
        let stats: Py<PyAny> = tup.get_item(1)?.into_pyobject(py)?.into_any().unbind();
        (states, Some(stats))
    } else {
        let states_map = _run_serial(py, tasks, block_tracking)?;
        let dict = pyo3::types::PyDict::new(py);
        for (k, v) in states_map {
            dict.set_item(k, v.into_pyobject(py)?)?;
        }
        (dict.into_any().unbind(), None)
    };

    // Call back into Python for the formatted post-run report.
    // Printing lives in Python because the per-worker stdout proxy is
    // Python-implemented and the formatting is share-printed cleanly.
    let progress_mod = py.import("daisy._progress")?;
    progress_mod.call_method1(
        "_print_execution_summary",
        (&states_obj, &order_py),
    )?;
    if let Some(stats) = &run_stats_obj {
        progress_mod.call_method1(
            "_print_resource_utilization",
            (stats, &order_py),
        )?;
    } else {
        progress_mod.call_method1(
            "_print_resource_utilization",
            (py.None(), &order_py),
        )?;
    }

    // Bool result: True iff every block of every task was completed
    // (skipped blocks are folded into completed_count by the
    // scheduler).
    let states_dict = states_obj.bind(py);
    let mut all_succeeded = true;
    for (_k, v) in states_dict.try_iter()?.zip(states_dict.call_method0("values")?.try_iter()?) {
        let _ = _k?;
        let state = v?;
        let completed: i64 = state.getattr("completed_count")?.extract()?;
        let total: i64 = state.getattr("total_block_count")?.extract()?;
        if completed != total {
            all_succeeded = false;
            break;
        }
    }
    Ok(all_succeeded)
}

fn rt_err(e: impl std::fmt::Display) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e}"))
}

/// Run tasks serially (single-threaded, no TCP).
#[pyfunction]
#[pyo3(signature = (tasks, block_tracking=true))]
pub fn _run_serial(
    py: Python<'_>,
    tasks: Bound<'_, PyList>,
    block_tracking: bool,
) -> PyResult<HashMap<String, PyTaskState>> {
    let mut cache: HashMap<String, Arc<Task>> = HashMap::new();
    let mut arc_tasks: Vec<Arc<Task>> = Vec::new();
    for item in tasks.iter() {
        let bound_task: Bound<'_, PyTask> = item.cast()?.clone();
        let arc = PyTask::convert_task_tree(&bound_task, py, &mut cache)?;
        arc_tasks.push(arc);
    }

    let states = SerialRunner::run(&arc_tasks, block_tracking).map_err(rt_err)?;

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
/// nested dict matching `daisy_core::run_stats::RunStats`.
///
/// `progress_observer`, if provided, must be a Python object exposing
/// `on_start(states)`, `on_progress(states)`, and `on_finish(states)`
/// — see `daisy/_compat.py:_TqdmObserver` for a tqdm-backed example.
#[pyfunction]
#[pyo3(signature = (tasks, resources=None, progress_observer=None, host="127.0.0.1", block_tracking=true))]
pub fn _run_distributed_server(
    py: Python<'_>,
    tasks: Bound<'_, PyList>,
    resources: Option<Bound<'_, PyDict>>,
    progress_observer: Option<Py<PyAny>>,
    host: &str,
    block_tracking: bool,
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

    let progress: Option<Arc<dyn ProgressObserver>> = progress_observer
        .map(|obj| Arc::new(PyProgressObserver::new(obj)) as Arc<dyn ProgressObserver>);

    let rt = tokio::runtime::Runtime::new().map_err(rt_err)?;
    let (server, listener) = rt.block_on(Server::bind(host)).map_err(rt_err)?;

    // Install a raw SIGINT handler that sets `SIGINT_FLAG`. The run
    // loop's abort callback reads the flag every 100ms and exits
    // cleanly when set. We restore the previous handler after the
    // run regardless of outcome, so Python's normal KeyboardInterrupt
    // machinery resumes for any code that runs after `run_blockwise`.
    SIGINT_FLAG.store(false, Ordering::SeqCst);
    let prev = unsafe {
        libc::signal(libc::SIGINT, handle_sigint as *const () as libc::sighandler_t)
    };
    PREV_SIGINT_HANDLER.store(prev as usize, Ordering::SeqCst);

    let abort_check: Arc<dyn Fn() -> bool + Send + Sync> =
        Arc::new(|| SIGINT_FLAG.load(Ordering::Relaxed));

    // Release GIL and run the event loop. Worker threads are spawned by
    // the Rust server and call back into Python via Python::attach when
    // they need to execute the process function.
    let mut worker_pools: HashMap<String, WorkerPool> = HashMap::new();
    let tasks_clone = arc_tasks.clone();
    let result = py.detach(move || {
        rt.block_on(server.run_blockwise(
            listener,
            &tasks_clone,
            &mut worker_pools,
            budget,
            progress,
            Some(abort_check),
            block_tracking,
        ))
    });

    // Always restore the previous handler before any early return,
    // so a partial / failed run doesn't leave the process with a
    // crippled SIGINT handler.
    let prev = PREV_SIGINT_HANDLER.load(Ordering::SeqCst);
    unsafe {
        libc::signal(libc::SIGINT, prev as libc::sighandler_t);
    }

    let (states, run_stats) = match result {
        Ok(v) => v,
        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {
            return Err(PyErr::new::<pyo3::exceptions::PyKeyboardInterrupt, _>(
                "run aborted by SIGINT",
            ));
        }
        Err(e) => return Err(rt_err(e)),
    };

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
    s: &daisy_core::run_stats::RunStats,
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
