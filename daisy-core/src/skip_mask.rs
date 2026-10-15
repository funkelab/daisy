//! A caller-provided mask over a task's block grid: one byte per block,
//! nonzero means "skip this block".
//!
//! The mask is an *input to a run*, not persisted state. It is checked by
//! the scheduler's precheck next to the done array and a `check_function`:
//! a masked block is reported as skipped, its downstream dependencies are
//! released as if it had completed, and nothing is written to the
//! tracking store for it. A later run with a narrower mask (or none)
//! therefore runs those blocks; a block that was *done* stays done.
//!
//! ## Indexing
//!
//! The mask is indexed by block position, in C order. The cell of a block
//! is
//!
//! ```text
//! (block.write_roi.offset - total_write_roi.offset) / write_roi.shape
//! ```
//!
//! where `total_write_roi` is the task's total ROI shrunk by the
//! read/write context. Cell `(0, 0, ...)` is therefore the first block
//! regardless of context, and the grid shape is the number of blocks per
//! axis (`Task::block_grid_shape`). This deliberately differs from the
//! tracking store's internal grid, which is anchored at `total_roi` and
//! shifts by `context / block_size` whenever the context is at least one
//! block wide.

use crate::block::Block;
use crate::roi::Roi;
use crate::task::Fit;
use std::sync::Arc;

/// Number of blocks per axis for a task layout: how many write ROIs of
/// shape `write_roi.shape` fit along each axis of `total_write_roi`.
/// `Fit::Valid` drops a partial trailing block; `Overhang` and `Shrink`
/// keep it.
pub fn block_grid_shape(total_write_roi: &Roi, write_roi: &Roi, fit: &Fit) -> Vec<usize> {
    total_write_roi
        .shape()
        .as_slice()
        .iter()
        .zip(write_roi.shape().as_slice().iter())
        .map(|(t, w)| {
            if *w <= 0 || *t <= 0 {
                return 0;
            }
            let n = match fit {
                Fit::Valid => t / w,
                Fit::Overhang | Fit::Shrink => (t + w - 1) / w,
            };
            n.max(0) as usize
        })
        .collect()
}

/// A block-grid mask. Cheap to clone (the bytes are shared).
#[derive(Clone, Debug)]
pub struct SkipMask {
    shape: Vec<usize>,
    strides: Vec<usize>,
    data: Arc<[u8]>,
}

impl SkipMask {
    /// Build a mask from C-ordered bytes at `shape`. The byte count must
    /// equal the product of `shape`.
    pub fn new(shape: Vec<usize>, data: impl Into<Arc<[u8]>>) -> Result<Self, String> {
        let data: Arc<[u8]> = data.into();
        let n: usize = shape.iter().product();
        if data.len() != n {
            return Err(format!(
                "skip mask has {} bytes but shape {:?} needs {}",
                data.len(),
                shape,
                n
            ));
        }
        let strides = c_strides(&shape);
        Ok(Self {
            shape,
            strides,
            data,
        })
    }

    pub fn shape(&self) -> &[usize] {
        &self.shape
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Number of masked (nonzero) cells.
    pub fn count(&self) -> usize {
        self.data.iter().filter(|&&b| b != 0).count()
    }

    /// Whether `block` falls on a masked cell. Blocks outside the grid
    /// (rank mismatch, negative or out-of-range coordinate) are never
    /// masked.
    pub fn contains(&self, block: &Block, total_write_roi: &Roi, write_roi: &Roi) -> bool {
        let off = block.write_roi.offset().as_slice();
        let origin = total_write_roi.offset().as_slice();
        let step = write_roi.shape().as_slice();
        if off.len() != self.shape.len() || origin.len() != off.len() || step.len() != off.len() {
            return false;
        }
        let mut idx = 0usize;
        for i in 0..off.len() {
            if step[i] <= 0 {
                return false;
            }
            let c = (off[i] - origin[i]) / step[i];
            if c < 0 || (c as usize) >= self.shape[i] {
                return false;
            }
            idx += (c as usize) * self.strides[i];
        }
        self.data[idx] != 0
    }
}

fn c_strides(shape: &[usize]) -> Vec<usize> {
    let n = shape.len();
    let mut strides = vec![1usize; n];
    for i in (0..n.saturating_sub(1)).rev() {
        strides[i] = strides[i + 1] * shape[i + 1];
    }
    strides
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::Block;
    use crate::dependency_graph::BlockwiseDependencyGraph;
    use crate::task::Task;

    fn roi(offset: &[i64], shape: &[i64]) -> Roi {
        Roi::from_slices(offset, shape)
    }

    fn all_blocks(task: &Task) -> Vec<Block> {
        let graph = BlockwiseDependencyGraph::from_task(task);
        (0..graph.num_levels())
            .flat_map(|l| graph.level_blocks(l))
            .collect()
    }

    fn task_with(total: Roi, read: Roi, write: Roi, fit: Fit) -> Task {
        Task::builder("t")
            .total_roi(total)
            .read_roi(read)
            .write_roi(write)
            .fit(fit)
            .build()
    }

    #[test]
    fn grid_shape_matches_dependency_graph_block_count() {
        for fit in [Fit::Valid, Fit::Overhang, Fit::Shrink] {
            for total_shape in [[400i64, 300], [410, 300], [90, 310]] {
                let task = task_with(
                    roi(&[10, -20], &total_shape),
                    roi(&[0, 0], &[100, 100]),
                    roi(&[0, 0], &[100, 100]),
                    fit.clone(),
                );
                let shape = task.block_grid_shape();
                let n: usize = shape.iter().product();
                assert_eq!(
                    n as i64,
                    BlockwiseDependencyGraph::from_task(&task).num_blocks(),
                    "fit {fit:?} total {total_shape:?} grid {shape:?}"
                );
            }
        }
    }

    #[test]
    fn cell_zero_is_the_first_block_even_with_a_block_wide_context() {
        // Context (100) equals the block size: under the tracking store's
        // anchoring the first block would land at cell 1; under the
        // mask's anchoring it is cell 0.
        let task = task_with(
            roi(&[-100, -100], &[600, 600]),
            roi(&[-100, -100], &[300, 300]),
            roi(&[0, 0], &[100, 100]),
            Fit::Valid,
        );
        assert_eq!(task.block_grid_shape(), vec![4, 4]);
        let mut data = vec![0u8; 16];
        data[0] = 1; // cell (0, 0)
        data[1 * 4 + 2] = 1; // cell (1, 2)
        let mask = SkipMask::new(vec![4, 4], data).unwrap();
        let blocks = all_blocks(&task);
        assert_eq!(blocks.len(), 16);
        let masked: Vec<Vec<i64>> = blocks
            .iter()
            .filter(|b| mask.contains(b, &task.total_write_roi, &task.write_roi))
            .map(|b| b.write_roi.offset().as_slice().to_vec())
            .collect();
        assert_eq!(masked.len(), 2);
        assert!(masked.contains(&vec![0, 0]));
        assert!(masked.contains(&vec![100, 200]));
    }

    #[test]
    fn shrink_keeps_the_trailing_block_addressable() {
        let task = task_with(
            roi(&[0, 0], &[250, 100]),
            roi(&[0, 0], &[100, 100]),
            roi(&[0, 0], &[100, 100]),
            Fit::Shrink,
        );
        assert_eq!(task.block_grid_shape(), vec![3, 1]);
        let mask = SkipMask::new(vec![3, 1], vec![0, 0, 1]).unwrap();
        let blocks = all_blocks(&task);
        let last = blocks
            .iter()
            .find(|b| b.write_roi.offset().as_slice() == [200, 0])
            .expect("shrunk trailing block");
        assert_eq!(last.write_roi.shape().as_slice(), [50, 100]);
        assert!(mask.contains(last, &task.total_write_roi, &task.write_roi));
    }

    #[test]
    fn wrong_byte_count_is_rejected() {
        let err = SkipMask::new(vec![2, 2], vec![1u8; 3]).unwrap_err();
        assert!(err.contains("3 bytes"), "{err}");
    }
}
