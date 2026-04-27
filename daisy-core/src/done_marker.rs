//! Persistent per-block "done" tracker, used to skip already-completed
//! blocks across runs. Layout on disk is a single-chunk **Zarr v3**
//! array of shape = block-grid shape, dtype `uint8`, with only the
//! `bytes` codec (no compression) — i.e. a raw `prod(grid_shape)`-byte
//! chunk file we can mmap directly. One byte per block, `1` = done.
//!
//! The on-disk layout matches what `zarr.create(..., codecs=[BytesCodec()])`
//! emits in zarr-python 3, so the marker can be opened by any zarr v3
//! reader (napari, neuroglancer, `zarr.open(...)`) for inspection.
//!
//! All writes go through the central scheduler / server task, so no
//! atomicity is needed: a single-threaded writer with concurrent
//! many-thread readers is the worst-case access pattern, and a single
//! `u8` store is naturally torn-free.
//!
//! On open, we verify a `gerbera_task_hash` entry in the array's
//! `attributes` matching the task's `(total_roi, read_roi, write_roi,
//! fit)`. A mismatch means the stored marker was written for a
//! *different* task layout and would produce wrong skip decisions; we
//! refuse to load it and return a `LayoutMismatch` error instructing
//! the user to delete it.

use crate::block::Block;
use crate::roi::Roi;
use crate::task::Fit;
use memmap2::MmapMut;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum DoneMarkerError {
    #[error("io error opening done-marker at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error(
        "done-marker at {path} was written for a different task layout \
         (stored hash = {stored_hash}, expected = {expected_hash}). \
         Delete it to start fresh:\n    rm -rf {path}"
    )]
    LayoutMismatch {
        path: PathBuf,
        stored_hash: String,
        expected_hash: String,
    },

    #[error("done-marker metadata at {path} is invalid: {reason}")]
    InvalidMetadata { path: PathBuf, reason: String },
}

impl DoneMarkerError {
    fn io(path: impl Into<PathBuf>, source: io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }
}

/// Hash a task's layout into a stable hex string. Anything that changes
/// the block grid (total/read/write ROIs, fit mode) must change this.
pub fn compute_task_hash(
    total_roi: &Roi,
    read_roi: &Roi,
    write_roi: &Roi,
    fit: &Fit,
) -> String {
    let mut h = Sha256::new();
    h.update(b"gerbera-done-marker:v1\n");
    for (label, roi) in [("total", total_roi), ("read", read_roi), ("write", write_roi)] {
        h.update(label.as_bytes());
        h.update(b":offset=");
        for v in roi.offset().as_slice() {
            h.update(v.to_le_bytes());
            h.update(b",");
        }
        h.update(b";shape=");
        for v in roi.shape().as_slice() {
            h.update(v.to_le_bytes());
            h.update(b",");
        }
        h.update(b";");
    }
    h.update(b"fit=");
    h.update(format!("{fit:?}").as_bytes());
    let digest = h.finalize();
    let mut s = String::with_capacity(digest.len() * 2);
    for b in digest {
        use std::fmt::Write as _;
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Compute the block grid shape: how many blocks fit along each axis of
/// `total_roi` when stepping by `write_roi.shape()`. All entries are
/// guaranteed positive (we treat zero or negative shapes as 1 to avoid
/// a zero-sized marker file; in practice the scheduler rejects those
/// upstream).
pub fn compute_grid_shape(total_roi: &Roi, write_roi: &Roi) -> Vec<usize> {
    total_roi
        .shape()
        .as_slice()
        .iter()
        .zip(write_roi.shape().as_slice().iter())
        .map(|(t, w)| {
            if *w <= 0 {
                1
            } else {
                let q = (*t as f64 / *w as f64).ceil() as i64;
                q.max(1) as usize
            }
        })
        .collect()
}

/// Compute the grid coordinate of a block: `(block.write_roi.offset -
/// total_roi.offset) / write_roi.shape`, element-wise. Negative
/// components (boundary blocks under `Fit::Overhang`/`Shrink`) are
/// clamped to 0 — the marker simply ignores those, since they aren't
/// addressable.
fn grid_coord(block: &Block, total_offset: &[i64], block_shape: &[i64]) -> Option<Vec<usize>> {
    let off = block.write_roi.offset().as_slice();
    if off.len() != total_offset.len() || off.len() != block_shape.len() {
        return None;
    }
    let mut coord = Vec::with_capacity(off.len());
    for ((o, t), s) in off.iter().zip(total_offset.iter()).zip(block_shape.iter()) {
        if *s <= 0 {
            return None;
        }
        let raw = (*o - *t) / *s;
        if raw < 0 {
            return None;
        }
        coord.push(raw as usize);
    }
    Some(coord)
}

fn linear_index(coord: &[usize], strides: &[usize]) -> usize {
    coord
        .iter()
        .zip(strides.iter())
        .map(|(c, s)| c * s)
        .sum()
}

/// Compute C-order (row-major) strides for a given shape.
fn c_strides(shape: &[usize]) -> Vec<usize> {
    let n = shape.len();
    let mut strides = vec![1usize; n];
    for i in (0..n.saturating_sub(1)).rev() {
        strides[i] = strides[i + 1] * shape[i + 1];
    }
    strides
}

/// Persistent done-marker for one task. Owns the mmap'd chunk file.
pub struct DoneMarker {
    path: PathBuf,
    grid_shape: Vec<usize>,
    strides: Vec<usize>,
    /// Cached so we don't subtract every check.
    total_offset: Vec<i64>,
    /// `write_roi.shape()` — used to convert block.write_roi.offset to a
    /// grid coordinate. Stored as a copy (i64) to avoid borrowing the
    /// task across the marker.
    block_shape: Vec<i64>,
    mmap: MmapMut,
}

impl DoneMarker {
    /// Open or create the done-marker for a task. Verifies the stored
    /// task hash matches; returns `LayoutMismatch` on conflict.
    pub fn open_or_create(
        path: &Path,
        total_roi: &Roi,
        read_roi: &Roi,
        write_roi: &Roi,
        fit: &Fit,
    ) -> Result<Self, DoneMarkerError> {
        std::fs::create_dir_all(path).map_err(|e| DoneMarkerError::io(path, e))?;

        let task_hash = compute_task_hash(total_roi, read_roi, write_roi, fit);
        let grid_shape = compute_grid_shape(total_roi, write_roi);
        let n_bytes: usize = grid_shape.iter().product();

        let zarr_json_path = path.join("zarr.json");
        // Zarr v3 chunk for the single chunk at the all-zero coordinate
        // under the default encoding (separator "/"): `c/0/0/...`.
        let mut chunk_path = path.join("c");
        for _ in 0..grid_shape.len().max(1) {
            chunk_path.push("0");
        }

        let shape_json: Vec<u64> = grid_shape.iter().map(|&v| v as u64).collect();
        let layout = json!({
            "zarr_format": 3,
            "node_type": "array",
            "shape": shape_json.clone(),
            "data_type": "uint8",
            "chunk_grid": {
                "name": "regular",
                "configuration": { "chunk_shape": shape_json }
            },
            "chunk_key_encoding": {
                "name": "default",
                "configuration": { "separator": "/" }
            },
            "fill_value": 0,
            "codecs": [ { "name": "bytes" } ],
            "attributes": { "gerbera_task_hash": task_hash },
            "storage_transformers": []
        });

        if zarr_json_path.exists() {
            // Reuse: verify hash, ensure chunk file exists at right size.
            verify_existing(&zarr_json_path, &grid_shape, &task_hash, path)?;
            ensure_chunk_file(&chunk_path, n_bytes, path)?;
        } else {
            // Fresh create.
            write_json(&zarr_json_path, &layout)
                .map_err(|e| DoneMarkerError::io(&zarr_json_path, e))?;
            ensure_chunk_file(&chunk_path, n_bytes, path)?;
        }

        let mmap = mmap_chunk(&chunk_path, n_bytes)?;
        let strides = c_strides(&grid_shape);
        Ok(Self {
            path: path.to_path_buf(),
            grid_shape,
            strides,
            total_offset: total_roi.offset().as_slice().to_vec(),
            block_shape: write_roi.shape().as_slice().to_vec(),
            mmap,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn grid_shape(&self) -> &[usize] {
        &self.grid_shape
    }

    /// Total number of cells in the marker (i.e. addressable blocks).
    pub fn capacity(&self) -> usize {
        self.mmap.len()
    }

    /// Number of currently-marked blocks. O(N) — for diagnostics, not
    /// the hot path.
    pub fn count_done(&self) -> usize {
        self.mmap.iter().filter(|&&b| b != 0).count()
    }

    /// Has this block already been processed?
    pub fn is_done(&self, block: &Block) -> bool {
        let Some(coord) = grid_coord(block, &self.total_offset, &self.block_shape) else {
            return false;
        };
        for (c, dim) in coord.iter().zip(self.grid_shape.iter()) {
            if c >= dim {
                return false;
            }
        }
        let idx = linear_index(&coord, &self.strides);
        idx < self.mmap.len() && self.mmap[idx] != 0
    }

    /// Mark this block successful. No-op if the coordinate is out of
    /// range (e.g. boundary blocks under `Fit::Overhang`).
    pub fn mark_success(&mut self, block: &Block) {
        let Some(coord) = grid_coord(block, &self.total_offset, &self.block_shape) else {
            return;
        };
        for (c, dim) in coord.iter().zip(self.grid_shape.iter()) {
            if c >= dim {
                return;
            }
        }
        let idx = linear_index(&coord, &self.strides);
        if idx < self.mmap.len() {
            self.mmap[idx] = 1;
        }
    }

    /// msync the mmap to disk. Called on Drop too, but explicit flush
    /// is useful at task-completion checkpoints.
    pub fn flush(&self) -> io::Result<()> {
        self.mmap.flush()
    }
}

impl Drop for DoneMarker {
    fn drop(&mut self) {
        let _ = self.mmap.flush();
    }
}

fn write_json(path: &Path, value: &serde_json::Value) -> io::Result<()> {
    let mut f = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    serde_json::to_writer_pretty(&mut f, value)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    f.write_all(b"\n")?;
    Ok(())
}

fn ensure_chunk_file(path: &Path, n_bytes: usize, root: &Path) -> Result<(), DoneMarkerError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| DoneMarkerError::io(root, e))?;
    }
    let f = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)
        .map_err(|e| DoneMarkerError::io(root, e))?;
    let len = f
        .metadata()
        .map_err(|e| DoneMarkerError::io(root, e))?
        .len() as usize;
    if len != n_bytes {
        f.set_len(n_bytes as u64)
            .map_err(|e| DoneMarkerError::io(root, e))?;
    }
    Ok(())
}

fn mmap_chunk(path: &Path, expected_bytes: usize) -> Result<MmapMut, DoneMarkerError> {
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|e| DoneMarkerError::io(path, e))?;
    let len = f
        .metadata()
        .map_err(|e| DoneMarkerError::io(path, e))?
        .len() as usize;
    if len != expected_bytes {
        return Err(DoneMarkerError::InvalidMetadata {
            path: path.to_path_buf(),
            reason: format!("chunk file is {len} bytes, expected {expected_bytes}"),
        });
    }
    // SAFETY: We own the file, no one else mutates it concurrently while
    // we hold this DoneMarker. The mmap stays valid until Drop.
    unsafe { MmapMut::map_mut(&f) }.map_err(|e| DoneMarkerError::io(path, e))
}

fn verify_existing(
    zarr_json_path: &Path,
    expected_grid_shape: &[usize],
    expected_hash: &str,
    root: &Path,
) -> Result<(), DoneMarkerError> {
    let metadata: serde_json::Value = read_json(zarr_json_path)?;

    // First the hash check, since it gives the user the most actionable
    // message ("rm -rf …") and almost always also catches shape changes.
    let stored_hash = metadata
        .get("attributes")
        .and_then(|a| a.get("gerbera_task_hash"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let stored_hash = stored_hash.ok_or_else(|| DoneMarkerError::LayoutMismatch {
        path: root.to_path_buf(),
        stored_hash: "<missing zarr.json/attributes.gerbera_task_hash>".to_string(),
        expected_hash: expected_hash.to_string(),
    })?;
    if stored_hash != expected_hash {
        return Err(DoneMarkerError::LayoutMismatch {
            path: root.to_path_buf(),
            stored_hash,
            expected_hash: expected_hash.to_string(),
        });
    }

    // Hash matched but the on-disk shape disagrees — that's a corrupted
    // marker rather than a layout mismatch. Surface as InvalidMetadata.
    let stored_shape = metadata.get("shape").and_then(|v| v.as_array());
    let shape_matches = stored_shape
        .map(|arr| {
            arr.len() == expected_grid_shape.len()
                && arr
                    .iter()
                    .zip(expected_grid_shape.iter())
                    .all(|(v, &expected)| v.as_u64() == Some(expected as u64))
        })
        .unwrap_or(false);
    if !shape_matches {
        return Err(DoneMarkerError::InvalidMetadata {
            path: root.to_path_buf(),
            reason: format!(
                "zarr.json shape inconsistent with task layout despite matching hash \
                 (stored = {:?}, expected = {expected_grid_shape:?})",
                stored_shape
            ),
        });
    }
    Ok(())
}

fn read_json(path: &Path) -> Result<serde_json::Value, DoneMarkerError> {
    let bytes = std::fs::read(path).map_err(|e| DoneMarkerError::io(path, e))?;
    serde_json::from_slice(&bytes).map_err(|e| DoneMarkerError::InvalidMetadata {
        path: path.to_path_buf(),
        reason: e.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::Block;
    use crate::coordinate::Coordinate;

    fn make_roi(offset: &[i64], shape: &[i64]) -> Roi {
        Roi::new(Coordinate::from(offset), Coordinate::from(shape))
    }

    fn block_at(total: &Roi, write_offset: &[i64], block_shape: &[i64]) -> Block {
        Block::new(
            total,
            make_roi(write_offset, block_shape),
            make_roi(write_offset, block_shape),
            "t",
        )
    }

    #[test]
    fn round_trip() {
        let dir = tempdir();
        let total = make_roi(&[0, 0], &[400, 400]);
        let block_shape = make_roi(&[0, 0], &[100, 100]);
        let mut m = DoneMarker::open_or_create(
            &dir,
            &total,
            &block_shape,
            &block_shape,
            &Fit::Valid,
        )
        .unwrap();
        assert_eq!(m.grid_shape(), &[4, 4]);
        assert_eq!(m.capacity(), 16);

        let b = block_at(&total, &[100, 200], &[100, 100]); // grid (1, 2)
        assert!(!m.is_done(&b));
        m.mark_success(&b);
        assert!(m.is_done(&b));
        assert_eq!(m.count_done(), 1);

        // Reopen and confirm persistence.
        drop(m);
        let m2 = DoneMarker::open_or_create(
            &dir,
            &total,
            &block_shape,
            &block_shape,
            &Fit::Valid,
        )
        .unwrap();
        assert!(m2.is_done(&b));
    }

    #[test]
    fn layout_mismatch_refuses_to_open() {
        let dir = tempdir();
        let total = make_roi(&[0, 0], &[400, 400]);
        let block_shape = make_roi(&[0, 0], &[100, 100]);
        let _ = DoneMarker::open_or_create(
            &dir,
            &total,
            &block_shape,
            &block_shape,
            &Fit::Valid,
        )
        .unwrap();

        // Different block shape → different hash → must refuse.
        let other = make_roi(&[0, 0], &[50, 50]);
        let result = DoneMarker::open_or_create(&dir, &total, &other, &other, &Fit::Valid);
        let err = match result {
            Ok(_) => panic!("expected LayoutMismatch / InvalidMetadata, got Ok"),
            Err(e) => e,
        };
        assert!(matches!(
            err,
            DoneMarkerError::LayoutMismatch { .. } | DoneMarkerError::InvalidMetadata { .. }
        ));
    }

    #[test]
    fn capacity_for_billion_blocks_is_addressable() {
        // Sanity: 1000 × 1000 × 1000 grid → 1 GB. We don't actually open
        // it (would be slow), just verify the math.
        let total = make_roi(&[0, 0, 0], &[1_000, 1_000, 1_000]);
        let block_shape = make_roi(&[0, 0, 0], &[1, 1, 1]);
        let shape = compute_grid_shape(&total, &block_shape);
        assert_eq!(shape, vec![1000, 1000, 1000]);
        let n: usize = shape.iter().product();
        assert_eq!(n, 1_000_000_000);
    }

    fn tempdir() -> PathBuf {
        let n = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let p = std::env::temp_dir().join(format!("gerbera-done-marker-test-{n}"));
        let _ = std::fs::remove_dir_all(&p);
        p
    }
}
