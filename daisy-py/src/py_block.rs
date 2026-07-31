use daisy_core::block::{Block, BlockId, BlockStatus};
use daisy_core::block_profile::BlockStats;
use pyo3::prelude::*;

use crate::py_roi::PyRoi;

#[pyclass(name = "BlockStatus", skip_from_py_object, module = "daisy._daisy")]
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

/// Resources one block consumed, as measured by whoever ran it.
///
/// Attached to a block by `daisy.profile_block(block)` (or automatically by
/// daisy's own workers) and carried home on `Block.stats`, where the server
/// persists it into the task's tracking group.
#[pyclass(name = "BlockStats", module = "daisy._daisy")]
#[derive(Clone)]
pub struct PyBlockStats {
    pub inner: BlockStats,
}

#[pymethods]
impl PyBlockStats {
    #[new]
    #[pyo3(signature = (
        wall_seconds=0.0,
        cpu_seconds=0.0,
        peak_rss_bytes=0,
        io_read_bytes=0,
        io_write_bytes=0,
        gpu_util_pct=f32::NAN,
        gpu_mem_bytes=0,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        wall_seconds: f64,
        cpu_seconds: f64,
        peak_rss_bytes: u64,
        io_read_bytes: u64,
        io_write_bytes: u64,
        gpu_util_pct: f32,
        gpu_mem_bytes: u64,
    ) -> Self {
        Self {
            inner: BlockStats {
                wall_seconds,
                cpu_seconds,
                peak_rss_bytes,
                io_read_bytes,
                io_write_bytes,
                gpu_util_pct,
                gpu_mem_bytes,
            },
        }
    }

    #[getter]
    fn wall_seconds(&self) -> f64 {
        self.inner.wall_seconds
    }

    #[getter]
    fn cpu_seconds(&self) -> f64 {
        self.inner.cpu_seconds
    }

    #[getter]
    fn peak_rss_bytes(&self) -> u64 {
        self.inner.peak_rss_bytes
    }

    #[getter]
    fn io_read_bytes(&self) -> u64 {
        self.inner.io_read_bytes
    }

    #[getter]
    fn io_write_bytes(&self) -> u64 {
        self.inner.io_write_bytes
    }

    /// Reserved for NVML; NaN until GPU tracking is implemented.
    #[getter]
    fn gpu_util_pct(&self) -> f32 {
        self.inner.gpu_util_pct
    }

    /// Reserved for NVML; 0 until GPU tracking is implemented.
    #[getter]
    fn gpu_mem_bytes(&self) -> u64 {
        self.inner.gpu_mem_bytes
    }

    fn __repr__(&self) -> String {
        format!(
            "BlockStats(wall_seconds={:.6}, cpu_seconds={:.6}, peak_rss_bytes={}, \
             io_read_bytes={}, io_write_bytes={})",
            self.inner.wall_seconds,
            self.inner.cpu_seconds,
            self.inner.peak_rss_bytes,
            self.inner.io_read_bytes,
            self.inner.io_write_bytes,
        )
    }
}

/// Block pyclass. Not frozen (status is mutable), so eq/hash are manual.
#[pyclass(name = "Block", from_py_object, subclass, module = "daisy._daisy")]
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

    /// Resource measurements for this block, or None if it hasn't been
    /// measured. Set by `daisy.profile_block(block)`; the server reads it
    /// when the block is released.
    #[getter]
    fn stats(&self) -> Option<PyBlockStats> {
        self.inner.stats.map(|inner| PyBlockStats { inner })
    }

    #[setter]
    fn set_stats(&mut self, value: Option<PyBlockStats>) {
        self.inner.stats = value.map(|s| s.inner);
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
