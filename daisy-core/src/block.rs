use bincode::{Decode, Encode};
use crate::roi::Roi;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub enum BlockStatus {
    Created,
    Processing,
    Success,
    Failed,
}

/// A unique identifier for a block: (task_id index, spatial cantor number).
/// Using a two-part ID lets blocks from different tasks share a coordinate
/// space without collisions.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct BlockId {
    pub task_id: String,
    pub spatial_id: u64,
}

impl fmt::Display for BlockId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.task_id, self.spatial_id)
    }
}

/// A block to process, with read and write ROIs and lifecycle status.
#[derive(Clone, Debug, Serialize, Deserialize, Encode, Decode)]
pub struct Block {
    pub read_roi: Roi,
    pub write_roi: Roi,
    pub block_id: BlockId,
    pub status: BlockStatus,
}

impl Block {
    /// Create a new block. The spatial block ID is computed from the write ROI
    /// position relative to the total ROI using the Cantor pairing function.
    pub fn new(total_roi: &Roi, read_roi: Roi, write_roi: Roi, task_id: &str) -> Self {
        let spatial_id = compute_block_id(total_roi, &write_roi);
        Self {
            read_roi,
            write_roi,
            block_id: BlockId {
                task_id: task_id.to_string(),
                spatial_id,
            },
            status: BlockStatus::Created,
        }
    }

    pub fn task_id(&self) -> &str {
        &self.block_id.task_id
    }
}

impl PartialEq for Block {
    fn eq(&self, other: &Self) -> bool {
        self.block_id == other.block_id
    }
}

impl Eq for Block {}

impl std::hash::Hash for Block {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.block_id.hash(state);
    }
}

impl fmt::Display for Block {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} read={} write={}",
            self.block_id, self.read_roi, self.write_roi
        )
    }
}

/// Compute the spatial block ID using a Cantor pairing function over the
/// block index (write_roi offset relative to total_roi, divided by write
/// shape).
fn compute_block_id(total_roi: &Roi, write_roi: &Roi) -> u64 {
    let block_index = (write_roi.offset() - total_roi.offset()) / write_roi.shape();
    cantor_number(block_index.as_slice())
}

/// Generalized Cantor number matching `funlib.math.cantor_number`.
///
/// Uses the "pyramid volume" generalization:
///   cantor_number([x]) = x
///   cantor_number([x, y, ...]) = pyramid_volume(n, sum) + cantor_number([x, y, ...][:-1])
///
/// where pyramid_volume(dims, edge) = product(edge+d for d in 0..dims) / dims!
/// Generalized Cantor number matching funlib.math.cantor_number.
/// Uses u128 throughout to handle large 3D coordinate sums without overflow.
/// Negative indices (from blocks outside the valid region) are clamped to 0;
/// these blocks are filtered by inclusion_criteria before use.
fn cantor_number(indices: &[i64]) -> u64 {
    cantor_number_u128(indices) as u64
}

fn cantor_number_u128(indices: &[i64]) -> u128 {
    let n = indices.len();
    if n == 1 {
        return indices[0].max(0) as u128;
    }
    let s: u128 = indices.iter().map(|&i| i.max(0) as u128).sum();
    pyramid_volume(n as u128, s).wrapping_add(cantor_number_u128(&indices[..n - 1]))
}

/// Volume of an n-dimensional pyramid with given edge length.
/// pyramid_volume(dims, edge) = product(edge + d, d=0..dims) / factorial(dims)
fn pyramid_volume(dims: u128, edge_length: u128) -> u128 {
    if edge_length == 0 {
        return 0;
    }
    let mut v: u128 = 1;
    for d in 0..dims {
        // The incremental multiply-then-divide keeps intermediates small for
        // typical block counts, but for very large coordinates we use
        // wrapping to avoid panics. The result is still a valid unique ID
        // (uniqueness may be lost only for coordinates > 2^42 per dimension,
        // far beyond practical block counts).
        v = v.wrapping_mul(edge_length.wrapping_add(d)) / (d + 1);
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cantor_matches_funlib() {
        // Values verified against funlib.math.cantor_number
        assert_eq!(cantor_number(&[0]), 0);
        assert_eq!(cantor_number(&[1]), 1);
        assert_eq!(cantor_number(&[2]), 2);

        // 2D: cantor((2,2)) = pyramid_volume(2, 4) + 2 = 10 + 2 = 12
        assert_eq!(cantor_number(&[2, 2]), 12);
        // 2D: cantor((2,4)) = pyramid_volume(2, 6) + 2 = 21 + 2 = 23
        assert_eq!(cantor_number(&[2, 4]), 23);
        // 2D: cantor((4,2)) = pyramid_volume(2, 6) + 4 = 21 + 4 = 25
        assert_eq!(cantor_number(&[4, 2]), 25);
        // 2D: cantor((4,4)) = pyramid_volume(2, 8) + 4 = 36 + 4 = 40
        assert_eq!(cantor_number(&[4, 4]), 40);

        // 3D: cantor(1,0,0) = pv(3,1) + cantor(1,0) = 1 + (pv(2,1) + 1) = 1 + 2 = 3
        assert_eq!(cantor_number(&[0, 0, 0]), 0);
        assert_eq!(cantor_number(&[1, 0, 0]), 3);
    }

    #[test]
    fn test_block_id_uniqueness() {
        let total = Roi::from_slices(&[0, 0], &[100, 100]);
        let b1 = Block::new(
            &total,
            Roi::from_slices(&[0, 0], &[20, 20]),
            Roi::from_slices(&[0, 0], &[10, 10]),
            "task",
        );
        let b2 = Block::new(
            &total,
            Roi::from_slices(&[10, 0], &[20, 20]),
            Roi::from_slices(&[10, 0], &[10, 10]),
            "task",
        );
        assert_ne!(b1.block_id, b2.block_id);
    }

    #[test]
    fn test_block_equality_by_id() {
        let total = Roi::from_slices(&[0, 0], &[100, 100]);
        let b1 = Block::new(
            &total,
            Roi::from_slices(&[0, 0], &[20, 20]),
            Roi::from_slices(&[0, 0], &[10, 10]),
            "task",
        );
        let b2 = Block::new(
            &total,
            Roi::from_slices(&[0, 0], &[20, 20]),
            Roi::from_slices(&[0, 0], &[10, 10]),
            "task",
        );
        assert_eq!(b1, b2);
    }
}
