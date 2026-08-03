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
use crate::py_pipeline::PyPipeline;

use crate::py_task::PyTask;
use crate::py_task_state::PyTaskState;

/// Coerce a Pipeline-or-Task input into a `PyPipeline`. A Task is
/// promoted to a singleton pipeline so the rest of the runtime only
/// sees one shape.
fn coerce_pipeline_or_task(
    py: Python<'_>,
    input: &Bound<'_, PyAny>,
) -> PyResult<Py<PyPipeline>> {
    if let Ok(p) = input.downcast::<PyPipeline>() {
        return Ok(p.clone().unbind());
    }
    if input.downcast::<PyTask>().is_ok() {
        let task_obj: Py<PyAny> = input.clone().unbind();
        return Py::new(py, PyPipeline::from_task(task_obj));
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
        "expected a Pipeline or a Task; got {}",
        input.get_type().name()?
    )))
}

/// Topological order of a `Pipeline` (or singleton-promoted `Task`)
/// with alphabetical tiebreaker on the ready set. Roots first; a
/// task becomes a candidate once every one of its upstream
/// dependencies has been emitted; from the candidate set we always
/// pick the alphabetically smallest. This is the order used to
/// render the post-run execution summary.
#[pyfunction]
pub fn _topo_order(input: &Bound<'_, PyAny>, py: Python<'_>) -> PyResult<Vec<String>> {
    let pipeline_obj = coerce_pipeline_or_task(py, input)?;
    let pipeline = pipeline_obj.borrow(py);

    use petgraph::Direction;
    use petgraph::graph::{DiGraph, NodeIndex};
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    // Pull task ids in pipeline order.
    let task_ids: Vec<String> = pipeline
        .tasks
        .iter()
        .map(|t| t.getattr(py, "task_id")?.extract::<String>(py))
        .collect::<PyResult<_>>()?;

    // Build a DiGraph whose NodeIndex matches the pipeline's task order.
    let mut graph: DiGraph<(), ()> =
        DiGraph::with_capacity(task_ids.len(), pipeline.edges.len());
    for _ in 0..task_ids.len() {
        graph.add_node(());
    }
    for &(u_idx, d_idx) in &pipeline.edges {
        graph.add_edge(NodeIndex::new(u_idx), NodeIndex::new(d_idx), ());
    }

    // Kahn's algorithm with alphabetical tiebreaker on the ready set.
    let mut in_degree: Vec<usize> = (0..task_ids.len())
        .map(|i| {
            graph
                .neighbors_directed(NodeIndex::new(i), Direction::Incoming)
                .count()
        })
        .collect();
    let mut ready: BinaryHeap<Reverse<(String, NodeIndex)>> = graph
        .externals(Direction::Incoming)
        .map(|n| Reverse((task_ids[n.index()].clone(), n)))
        .collect();
    let mut order: Vec<String> = Vec::with_capacity(task_ids.len());
    while let Some(Reverse((tid, n))) = ready.pop() {
        order.push(tid);
        for child in graph.neighbors_directed(n, Direction::Outgoing) {
            in_degree[child.index()] -= 1;
            if in_degree[child.index()] == 0 {
                ready.push(Reverse((task_ids[child.index()].clone(), child)));
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
/// rewrite). Returns the `task_id -> TaskState` dict; the public
/// `daisy.run_blockwise` computes its bool result from it (or hands
/// it to the caller verbatim under `return_states=True`).
#[pyfunction]
#[pyo3(signature = (
    input,
    multiprocessing = true,
    resources = None,
    progress = None,
    block_tracking = true,
    host = None,
))]
pub fn _run_blockwise_orchestrator(
    py: Python<'_>,
    input: &Bound<'_, PyAny>,
    multiprocessing: bool,
    resources: Option<Bound<'_, PyDict>>,
    progress: Option<Py<PyAny>>,
    block_tracking: bool,
    host: Option<&str>,
) -> PyResult<Py<PyAny>> {
    let pipeline = coerce_pipeline_or_task(py, input)?;
    let pipeline_any = pipeline.clone_ref(py).into_any();
    // Compute the display topological order.
    let order = _topo_order(pipeline_any.bind(py), py)?;
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

    // Dispatch to the appropriate runner. Both paths return a
    // `(task_id → PyTaskState, tracking summary)` tuple — serial mode
    // measures blocks the same way, so it reports the same summary.
    let result: Py<PyAny> = if multiprocessing {
        _run_distributed_server(
            py,
            pipeline.bind(py).as_any(),
            resources,
            progress_obj,
            host,
            block_tracking,
        )?
        .into_any()
    } else {
        _run_serial(py, pipeline.bind(py).as_any(), block_tracking)?
    };
    let tup = result.bind(py);
    let states_obj: Py<PyAny> = tup.get_item(0)?.into_pyobject(py)?.into_any().unbind();
    let run_stats_obj: Py<PyAny> = tup.get_item(1)?.into_pyobject(py)?.into_any().unbind();

    // Call back into Python for the formatted post-run report.
    // Printing lives in Python because the per-worker stdout proxy is
    // Python-implemented and the formatting is share-printed cleanly.
    let progress_mod = py.import("daisy._progress")?;
    progress_mod.call_method1(
        "_print_execution_summary",
        (&states_obj, &order_py),
    )?;
    // Resume visibility: INFO log per task that skipped blocks via
    // done markers (python logging — user-configurable, never forced).
    progress_mod.call_method1("_log_resume_summary", (&states_obj,))?;
    progress_mod.call_method1(
        "_print_resource_utilization",
        (&run_stats_obj, &order_py),
    )?;

    // Abandoned tasks fail loudly: silently returning False loses the
    // reason a run produced no output (the classic v1 failure mode was
    // the opposite — respawning crashed workers forever). Raise after
    // the summary has printed so users still see the table.
    let states_dict = states_obj.bind(py);
    let mut abandoned_msgs: Vec<String> = Vec::new();
    for kv in states_dict.call_method0("items")?.try_iter()? {
        let kv = kv?;
        let task_id: String = kv.get_item(0)?.extract()?;
        let state = kv.get_item(1)?;
        let is_abandoned: bool = state.getattr("abandoned")?.extract()?;
        if !is_abandoned {
            continue;
        }
        let reason: Option<String> = state.getattr("abandon_reason")?.extract()?;
        let failures: u32 = state.getattr("worker_failure_count")?.extract()?;
        let restarts: u32 = state.getattr("worker_restart_count")?.extract()?;
        let orphaned: i64 = state.getattr("orphaned_count")?.extract()?;
        let total: i64 = state.getattr("total_block_count")?.extract()?;
        let last_error: Option<String> = state.getattr("last_worker_error")?.extract()?;
        let mut msg = format!(
            "task '{}' was abandoned ({}): workers failed {} times, {} restarts performed; \
             {} of {} blocks were orphaned.",
            task_id,
            reason.as_deref().unwrap_or("unknown reason"),
            failures,
            restarts,
            orphaned,
            total,
        );
        let reclaims: u32 = state
            .getattr("timeout_reclaim_count")
            .and_then(|v| v.extract())
            .unwrap_or(0);
        if reclaims > 0 {
            let t: Option<f64> = state
                .getattr("timeout_secs")
                .and_then(|v| v.extract())
                .unwrap_or(None);
            let is_default =
                t == Some(crate::py_task::DEFAULT_BLOCK_TIMEOUT_SECS);
            let shown = t
                .map(|v| format!("{v}s"))
                .unwrap_or_else(|| "the configured timeout".to_string());
            msg.push_str(&format!(
                " {} block attempt(s) exceeded the block timeout ({}{}; \
                 pass Task(timeout=...) to raise it for slow blocks).",
                reclaims,
                shown,
                if is_default { " — the default" } else { "" },
            ));
        }
        match last_error {
            Some(err) if err.contains('\n') => {
                // multi-line = a formatted traceback; set it off as an
                // indented block so the summary line stays scannable
                let indented = err
                    .lines()
                    .map(|l| format!("    {l}"))
                    .collect::<Vec<_>>()
                    .join("\n");
                msg.push_str(&format!(" Last worker error:\n{indented}"));
            }
            Some(err) => msg.push_str(&format!(" Last worker error: {err}")),
            None => msg.push_str(" No worker error was captured."),
        }
        abandoned_msgs.push(msg);
    }
    if !abandoned_msgs.is_empty() {
        abandoned_msgs.push(
            "Fix the worker error or increase Task(max_worker_restarts=...); \
             use run_blockwise(..., return_states=True) to inspect task states on non-abandonment failures, or Server().run_blockwise(...) to never raise."
                .to_string(),
        );
        return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            abandoned_msgs.join("\n"),
        ));
    }

    Ok(states_obj)
}

fn rt_err(e: impl std::fmt::Display) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e}"))
}

/// Run a pipeline (or singleton-promoted task) serially. Serial mode
/// is for debugging — block-processing exceptions are not caught and
/// retried; the original Python exception is re-raised immediately
/// with its full traceback intact.
#[pyfunction]
#[pyo3(signature = (input, block_tracking=true))]
pub fn _run_serial(
    py: Python<'_>,
    input: &Bound<'_, PyAny>,
    block_tracking: bool,
) -> PyResult<Py<PyAny>> {
    let pipeline = coerce_pipeline_or_task(py, input)?;
    let core_pipeline = pipeline.borrow(py).to_core(py)?;
    crate::py_callbacks::clear_last_process_pyerr();
    let result = SerialRunner::run(&core_pipeline, block_tracking);
    let states = match result {
        Ok(s) => s,
        Err(e) => {
            // If a Python exception was stashed by `PyProcessBlock`,
            // re-raise the original PyErr (preserving its type and
            // traceback) instead of the string-wrapped version.
            return match crate::py_callbacks::take_last_process_pyerr() {
                Some(pyerr) => Err(pyerr),
                None => Err(rt_err(e)),
            };
        }
    };
    let (states, summaries) = states;
    let states_py: HashMap<String, PyTaskState> = states
        .into_iter()
        .map(|(k, v)| (k, PyTaskState { inner: v }))
        .collect();
    let summary_py = tracking_summary_to_py(py, &summaries)?;
    let out = pyo3::types::PyTuple::new(
        py,
        &[states_py.into_pyobject(py)?.into_any(), summary_py],
    )?;
    Ok(out.into_any().unbind())
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
#[pyo3(signature = (input, resources=None, progress_observer=None, host=None, block_tracking=true))]
pub fn _run_distributed_server(
    py: Python<'_>,
    input: &Bound<'_, PyAny>,
    resources: Option<Bound<'_, PyDict>>,
    progress_observer: Option<Py<PyAny>>,
    host: Option<&str>,
    block_tracking: bool,
) -> PyResult<Py<pyo3::types::PyTuple>> {
    let pipeline = coerce_pipeline_or_task(py, input)?;
    let core_pipeline = pipeline.borrow(py).to_core(py)?;

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
    let result = py.detach(move || {
        rt.block_on(server.run_blockwise(
            listener,
            &core_pipeline,
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

    let (states, summaries) = match result {
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
    let stats_py = tracking_summary_to_py(py, &summaries)?;

    let result = pyo3::types::PyTuple::new(py, &[states_py.into_pyobject(py)?.into_any(), stats_py])?;
    Ok(result.unbind())
}

/// Convert this run's per-task tracking aggregates into the dict the
/// Python summary renderer consumes.
///
/// Everything here was measured per block inside the workers and folded by
/// `block_tracking::TaskTracking` as blocks were recorded — there is no
/// separate accumulator, so `blocks` cannot disagree with what the
/// scheduler saw. Tasks without tracking configured simply don't appear,
/// which is how the renderer knows to omit the resource panel.
fn tracking_summary_to_py<'py>(
    py: Python<'py>,
    summaries: &HashMap<String, daisy_core::block_tracking::TaskSummary>,
) -> PyResult<Bound<'py, PyAny>> {
    let per_task = PyDict::new(py);
    for (task_id, s) in summaries {
        let (mean_ms, slope_ms) = s.block_ms_trend();
        let d = PyDict::new(py);
        d.set_item("blocks", s.blocks_recorded)?;
        d.set_item("failures", s.failures_recorded)?;
        d.set_item("has_stats", s.has_stats)?;
        d.set_item("total_cpu_secs", s.total_cpu_seconds)?;
        d.set_item("total_block_secs", s.total_wall_seconds)?;
        d.set_item("max_peak_rss_bytes", s.max_peak_rss_bytes)?;
        d.set_item("io_read_bytes", s.total_io_read_bytes)?;
        d.set_item("io_write_bytes", s.total_io_write_bytes)?;
        d.set_item("mean_block_ms", mean_ms)?;
        d.set_item("block_ms_slope", slope_ms)?;
        per_task.set_item(task_id, d)?;
    }
    let out = PyDict::new(py);
    out.set_item("per_task", per_task)?;
    Ok(out.into_any())
}
