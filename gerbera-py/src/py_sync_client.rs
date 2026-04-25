use gerbera_core::block::BlockStatus;
use gerbera_core::client::Client;
use pyo3::prelude::*;

use crate::py_block::PyBlock;

/// Synchronous Python-facing client. Creates its own tokio runtime
/// so worker processes can use it without an async event loop.
#[pyclass(name = "SyncClient", unsendable)]
pub struct PySyncClient {
    rt: tokio::runtime::Runtime,
    client: Option<Client>,
}

fn io_err(e: std::io::Error) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("{e}"))
}

#[pymethods]
impl PySyncClient {
    #[new]
    fn new(py: Python<'_>, host: &str, port: u16, task_id: &str) -> PyResult<Self> {
        let rt = tokio::runtime::Runtime::new().map_err(io_err)?;
        let client = py
            .detach(|| rt.block_on(Client::connect(host, port, task_id)))
            .map_err(io_err)?;
        Ok(Self {
            rt,
            client: Some(client),
        })
    }

    /// Acquire a block. Returns None when the server signals no more work.
    fn acquire_block(&mut self, py: Python<'_>) -> PyResult<Option<PyBlock>> {
        let client = self.client.as_mut().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("client disconnected")
        })?;
        let block = py
            .detach(|| self.rt.block_on(client.acquire_block()))
            .map_err(io_err)?;
        Ok(block.map(|mut b| {
            b.status = BlockStatus::Processing;
            PyBlock::from_core(b)
        }))
    }

    /// Release a processed block. Called automatically by __exit__.
    fn release_block(&mut self, py: Python<'_>, block: &PyBlock) -> PyResult<()> {
        let client = self.client.as_mut().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("client disconnected")
        })?;
        py.detach(|| self.rt.block_on(client.release_block(block.inner.clone())))
            .map_err(io_err)
    }

    fn report_failure(&mut self, py: Python<'_>, block: &PyBlock, error: &str) -> PyResult<()> {
        let client = self.client.as_mut().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("client disconnected")
        })?;
        py.detach(|| {
            self.rt
                .block_on(client.report_failure(block.inner.clone(), error.to_string()))
        })
        .map_err(io_err)
    }

    fn disconnect(&mut self, py: Python<'_>) -> PyResult<()> {
        if let Some(mut client) = self.client.take() {
            py.detach(|| {
                let _ = self.rt.block_on(client.disconnect());
            });
        }
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.client.as_ref().is_some_and(|c| c.is_connected())
    }
}
