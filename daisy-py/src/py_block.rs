use daisy_core::block::{Block, BlockId, BlockStatus};
use pyo3::prelude::*;

use crate::py_roi::PyRoi;

#[pyclass(name = "BlockStatus", skip_from_py_object)]
#[derive(Clone)]
pub struct PyBlockStatus {
    pub inner: BlockStatus,
}

#[pymethods]
impl PyBlockStatus {
    #[classattr]
    const CREATED: u8 = 0;
    #[classattr]
    const PROCESSING: u8 = 1;
    #[classattr]
    const SUCCESS: u8 = 2;
    #[classattr]
    const FAILED: u8 = 3;

    fn __repr__(&self) -> String {
        format!("{:?}", self.inner)
    }
}

/// Block pyclass. Not frozen (status is mutable), so eq/hash are manual.
#[pyclass(name = "Block", from_py_object)]
#[derive(Clone)]
pub struct PyBlock {
    pub inner: Block,
}

#[pymethods]
impl PyBlock {
    #[new]
    #[pyo3(signature = (total_roi, read_roi, write_roi, task_id=None, block_id=None))]
    fn new(
        total_roi: PyRoi,
        read_roi: PyRoi,
        write_roi: PyRoi,
        task_id: Option<String>,
        block_id: Option<u64>,
    ) -> Self {
        let tid = task_id.unwrap_or_default();
        let mut block = Block::new(&total_roi.inner, read_roi.inner, write_roi.inner, &tid);
        if let Some(bid) = block_id {
            block.block_id = BlockId {
                task_id: tid,
                spatial_id: bid,
            };
        }
        Self { inner: block }
    }

    #[getter]
    fn read_roi(&self) -> PyRoi {
        PyRoi {
            inner: self.inner.read_roi.clone(),
        }
    }

    #[getter]
    fn write_roi(&self) -> PyRoi {
        PyRoi {
            inner: self.inner.write_roi.clone(),
        }
    }

    /// Return block_id as (task_id, spatial_id) tuple, matching daisy format.
    #[getter]
    fn block_id(&self) -> (String, u64) {
        (
            self.inner.block_id.task_id.clone(),
            self.inner.block_id.spatial_id,
        )
    }

    #[getter]
    fn task_id(&self) -> String {
        self.inner.block_id.task_id.clone()
    }

    #[getter]
    fn status(&self) -> u8 {
        match self.inner.status {
            BlockStatus::Created => 0,
            BlockStatus::Processing => 1,
            BlockStatus::Success => 2,
            BlockStatus::Failed => 3,
        }
    }

    #[setter]
    fn set_status(&mut self, value: u8) -> PyResult<()> {
        self.inner.status = match value {
            0 => BlockStatus::Created,
            1 => BlockStatus::Processing,
            2 => BlockStatus::Success,
            3 => BlockStatus::Failed,
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "invalid status value",
                ))
            }
        };
        Ok(())
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.inner.block_id == other.inner.block_id
    }

    fn __hash__(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.inner.block_id.hash(&mut hasher);
        hasher.finish()
    }

    fn __repr__(&self) -> String {
        format!("{}", self.inner)
    }
}

impl PyBlock {
    pub fn from_core(block: Block) -> Self {
        Self { inner: block }
    }
}
