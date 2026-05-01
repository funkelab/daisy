use pyo3::prelude::*;

mod py_block;
mod py_callbacks;
mod py_context;
mod py_dep_graph;
mod py_pipeline;
mod py_roi;
mod py_scheduler;
mod py_server;
mod py_sync_client;
mod py_task;
mod py_task_state;

use py_block::{PyBlock, PyBlockStatus};
use py_context::PyContext;
use py_dep_graph::{PyBlockwiseDepGraph, PyDependencyGraph};
use py_pipeline::PyPipeline;
use py_roi::{PyCoordinate, PyRoi};
use py_scheduler::PyScheduler;
use py_sync_client::PySyncClient;
use py_task::PyTask;
use py_task_state::PyTaskState;

// (Legacy `_daisy.run_blockwise` removed — Python uses
// `_run_blockwise_orchestrator`, which takes a Pipeline.)

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
    m.add_class::<PyPipeline>()?;
    m.add_class::<PyContext>()?;
    m.add_function(wrap_pyfunction!(py_task::set_done_marker_basedir, m)?)?;
    m.add_function(wrap_pyfunction!(py_task::get_done_marker_basedir, m)?)?;
    m.add_function(wrap_pyfunction!(py_server::_run_serial, m)?)?;
    m.add_function(wrap_pyfunction!(py_server::_run_distributed_server, m)?)?;
    m.add_function(wrap_pyfunction!(py_server::_run_blockwise_orchestrator, m)?)?;
    m.add_function(wrap_pyfunction!(py_server::_topo_order, m)?)?;
    Ok(())
}
