use daisy_core::dependency_graph::{BlockwiseDependencyGraph, DependencyGraph};
use daisy_core::scheduler::Scheduler;
use daisy_core::task::Fit;
use pyo3::prelude::*;
use std::sync::Arc;

use crate::py_block::PyBlock;
use crate::py_roi::PyRoi;

#[pyclass(name = "BlockwiseDependencyGraph", skip_from_py_object)]
pub struct PyBlockwiseDepGraph {
    inner: BlockwiseDependencyGraph,
}

#[pymethods]
impl PyBlockwiseDepGraph {
    #[new]
    #[pyo3(signature = (task_id, read_roi, write_roi, read_write_conflict, fit, total_read_roi=None))]
    fn new(
        task_id: &str,
        read_roi: PyRoi,
        write_roi: PyRoi,
        read_write_conflict: bool,
        fit: &str,
        total_read_roi: Option<PyRoi>,
    ) -> Self {
        let graph = BlockwiseDependencyGraph::new(
            task_id,
            read_roi.inner,
            write_roi.inner,
            read_write_conflict,
            Fit::from_str(fit),
            total_read_roi.map(|r| r.inner),
            None,
        );
        Self { inner: graph }
    }

    #[getter]
    fn num_blocks(&self) -> i64 {
        self.inner.num_blocks()
    }

    #[getter]
    fn num_levels(&self) -> usize {
        self.inner.num_levels()
    }

    fn num_roots(&self) -> i64 {
        self.inner.num_roots()
    }

    fn enumerate_all_dependencies(&self) -> Vec<(PyBlock, Vec<PyBlock>)> {
        let mut result = Vec::new();
        for level in 0..self.inner.num_levels() {
            for block in self.inner.level_blocks(level) {
                let upstream: Vec<PyBlock> = self
                    .inner
                    .upstream(&block)
                    .into_iter()
                    .map(PyBlock::from_core)
                    .collect();
                result.push((PyBlock::from_core(block), upstream));
            }
        }
        result
    }

    #[pyo3(signature = (sub_roi, read_roi=false))]
    fn get_subgraph_blocks(&self, sub_roi: PyRoi, read_roi: bool) -> Vec<PyBlock> {
        self.inner
            .get_subgraph_blocks(&sub_roi.inner, read_roi)
            .into_iter()
            .map(PyBlock::from_core)
            .collect()
    }

    fn upstream(&self, block: &PyBlock) -> Vec<PyBlock> {
        self.inner
            .upstream(&block.inner)
            .into_iter()
            .map(PyBlock::from_core)
            .collect()
    }

    fn downstream(&self, block: &PyBlock) -> Vec<PyBlock> {
        self.inner
            .downstream(&block.inner)
            .into_iter()
            .map(PyBlock::from_core)
            .collect()
    }

    fn root_gen(&self) -> Vec<PyBlock> {
        self.inner.root_gen().map(PyBlock::from_core).collect()
    }
}

#[pyclass(name = "DependencyGraph", skip_from_py_object)]
pub struct PyDependencyGraph {
    inner: DependencyGraph,
}

impl PyDependencyGraph {
    pub fn from_scheduler(scheduler: &Scheduler) -> Self {
        // Build a fresh DependencyGraph from the scheduler's task_map +
        // its already-resolved upstream/downstream maps. We can't clone
        // the scheduler's existing graph (no Clone impl), so we
        // reconstruct via `Pipeline::new`.
        use daisy_core::pipeline::Pipeline;
        let tasks: Vec<_> = scheduler.task_map.values().cloned().collect();
        let mut edges: Vec<(String, String)> = Vec::new();
        for (down, ups) in scheduler.dependency_graph().upstream_tasks.iter() {
            for up in ups {
                edges.push((up.clone(), down.clone()));
            }
        }
        let pipeline = Pipeline::new(tasks, edges).expect("scheduler invariant");
        Self {
            inner: DependencyGraph::from_pipeline(&pipeline),
        }
    }
}

#[pymethods]
impl PyDependencyGraph {
    fn upstream(&self, block: &PyBlock) -> Vec<PyBlock> {
        self.inner
            .upstream(&block.inner)
            .into_iter()
            .map(PyBlock::from_core)
            .collect()
    }

    fn downstream(&self, block: &PyBlock) -> Vec<PyBlock> {
        self.inner
            .downstream(&block.inner)
            .into_iter()
            .map(PyBlock::from_core)
            .collect()
    }
}
