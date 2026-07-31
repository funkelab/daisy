//! Persistent per-block tracking: which blocks are done, how often each
//! failed, and — when `resource_tracking` is on — what each block cost.
//!
//! On disk a task's tracking directory is a **Zarr v3 group** whose children
//! are single-chunk arrays of shape = block-grid shape. One element per
//! block, indexed by C-order grid coordinate:
//!
//! ```text
//! <dir>/
//!   zarr.json          group metadata, attributes.daisy_task_hash
//!   done/              uint8    1 = complete
//!   failures/          uint32   failed attempts for this block
//!   wall_seconds/      float32  \
//!   cpu_seconds/       float32   |
//!   peak_rss_bytes/    uint64    |  only when resource tracking is on
//!   io_read_bytes/     uint64    |
//!   io_write_bytes/    uint64    |
//!   gpu_util_pct/      float32   |  reserved (NaN)
//!   gpu_mem_bytes/     uint64   /   reserved (0)
//! ```
//!
//! Each array's layout is what `zarr.create(..., codecs=[BytesCodec()])`
//! emits in zarr-python 3, so the whole group opens in any zarr v3 reader
//! (`zarr.open_group(...)`, napari, neuroglancer) for inspection while a run
//! is in flight.
//!
//! All writes go through the central scheduler / server task, so no
//! atomicity is needed: a single-threaded writer with concurrent many-thread
//! readers is the worst case, and each element store is naturally torn-free
//! for `u8`; wider element stores can in principle tear for a concurrent
//! *reader*, which for diagnostics data is acceptable.
//!
//! On open we verify a `daisy_task_hash` entry in the group's `attributes`
//! matching the task's `(total_roi, read_roi, write_roi, fit)`. A mismatch
//! means the stored tracking was written for a *different* task layout and
//! would produce wrong skip decisions; we refuse to load it and return a
//! `TrackingError::LayoutMismatch` telling the user to delete it. A
//! directory written by an older daisy (a bare zarr *array* rather than a
//! group) fails the same way, with the same actionable message.
//!
//! ## This is the only place that counts blocks
//!
//! The tracking object also carries the run's *running aggregates* — blocks
//! recorded, Σcpu, Σio, max peak-RSS, and the per-block wall times used for
//! the end-of-run trend fit. The end-of-run summary is an agglomeration of
//! exactly what was written here, so there is one counter in the codebase
//! rather than a scheduler count and a stats count that can disagree.

use crate::block::Block;
use crate::block_profile::BlockStats;
use crate::roi::Roi;
use crate::run_stats::linear_trend;
use crate::task::Fit;
use memmap2::MmapMut;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum TrackingError {
    #[error("io error opening block tracking at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error(
        "block tracking at {path} was written for a different task layout \
         (stored hash = {stored_hash}, expected = {expected_hash}). \
         Delete it to start fresh:\n    rm -rf {path}"
    )]
    LayoutMismatch {
        path: PathBuf,
        stored_hash: String,
        expected_hash: String,
    },

    #[error("block tracking metadata at {path} is invalid: {reason}")]
    InvalidMetadata { path: PathBuf, reason: String },
}

impl TrackingError {
    fn io(path: impl Into<PathBuf>, source: io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }
}

/// Backwards-compatible alias: this error type used to be named after the
/// done marker, which was the only array in the group.
pub type DoneMarkerError = TrackingError;

/// One element type per array in the group. `Bytes` is the element width in
/// the raw single-chunk file; `zarr_dtype` is what zarr v3 readers expect.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Dtype {
    U8,
    U32,
    U64,
    F32,
}

impl Dtype {
    fn width(self) -> usize {
        match self {
            Dtype::U8 => 1,
            Dtype::U32 => 4,
            Dtype::U64 => 8,
            Dtype::F32 => 4,
        }
    }

    fn zarr_name(self) -> &'static str {
        match self {
            Dtype::U8 => "uint8",
            Dtype::U32 => "uint32",
            Dtype::U64 => "uint64",
            Dtype::F32 => "float32",
        }
    }
}

/// Array names. `done` and `failures` always exist; the rest are created
/// only when resource tracking is enabled.
pub const ARRAY_DONE: &str = "done";
pub const ARRAY_FAILURES: &str = "failures";
pub const ARRAY_WALL_SECONDS: &str = "wall_seconds";
pub const ARRAY_CPU_SECONDS: &str = "cpu_seconds";
pub const ARRAY_PEAK_RSS: &str = "peak_rss_bytes";
pub const ARRAY_IO_READ: &str = "io_read_bytes";
pub const ARRAY_IO_WRITE: &str = "io_write_bytes";
pub const ARRAY_GPU_UTIL: &str = "gpu_util_pct";
pub const ARRAY_GPU_MEM: &str = "gpu_mem_bytes";

/// Arrays present whenever tracking is on at all.
const BASE_ARRAYS: &[(&str, Dtype)] = &[(ARRAY_DONE, Dtype::U8), (ARRAY_FAILURES, Dtype::U32)];

/// Arrays added by `resource_tracking`.
const STAT_ARRAYS: &[(&str, Dtype)] = &[
    (ARRAY_WALL_SECONDS, Dtype::F32),
    (ARRAY_CPU_SECONDS, Dtype::F32),
    (ARRAY_PEAK_RSS, Dtype::U64),
    (ARRAY_IO_READ, Dtype::U64),
    (ARRAY_IO_WRITE, Dtype::U64),
    (ARRAY_GPU_UTIL, Dtype::F32),
    (ARRAY_GPU_MEM, Dtype::U64),
];

/// Hash a task's layout into a stable hex string. Anything that changes
/// the block grid (total/read/write ROIs, fit mode) must change this.
pub fn compute_task_hash(total_roi: &Roi, read_roi: &Roi, write_roi: &Roi, fit: &Fit) -> String {
    let mut h = Sha256::new();
    h.update(b"daisy-tracking:v1\n");
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
/// a zero-sized file; in practice the scheduler rejects those upstream).
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
/// components (boundary blocks under `Fit::Overhang`/`Shrink`) yield
/// `None` — tracking simply ignores those, since they aren't addressable.
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
    coord.iter().zip(strides.iter()).map(|(c, s)| c * s).sum()
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

/// Running aggregates for one task, folded as blocks are recorded.
///
/// These describe *this run* (the arrays on disk accumulate across runs),
/// which is what the end-of-run summary reports.
#[derive(Clone, Debug, Default)]
pub struct TaskSummary {
    /// Blocks recorded during this run — the one and only block count.
    pub blocks_recorded: u64,
    /// Failed attempts noted during this run.
    pub failures_recorded: u64,
    /// True once any recorded block carried resource stats.
    pub has_stats: bool,
    pub total_cpu_seconds: f64,
    pub total_wall_seconds: f64,
    pub max_peak_rss_bytes: u64,
    pub total_io_read_bytes: u64,
    pub total_io_write_bytes: u64,
    /// Per-block wall times in milliseconds, in completion order, for the
    /// mean/slope trend fit.
    pub block_ms: Vec<f64>,
}

impl TaskSummary {
    /// Mean and slope of per-block wall time in milliseconds.
    pub fn block_ms_trend(&self) -> (f64, f64) {
        linear_trend(&self.block_ms)
    }
}

/// Persistent per-block tracking for one task. Owns one mmap per array.
pub struct TaskTracking {
    path: PathBuf,
    grid_shape: Vec<usize>,
    strides: Vec<usize>,
    /// Cached so we don't subtract every check.
    total_offset: Vec<i64>,
    /// `write_roi.shape()` — used to convert block.write_roi.offset to a
    /// grid coordinate. Stored as a copy (i64) to avoid borrowing the
    /// task across the tracker.
    block_shape: Vec<i64>,
    /// One mmap'd chunk per array, keyed by array name. `BTreeMap` so
    /// iteration order (and therefore metadata write order) is stable.
    arrays: BTreeMap<&'static str, MmapMut>,
    /// Whether the resource-stat arrays exist.
    with_stats: bool,
    summary: TaskSummary,
}

impl TaskTracking {
    /// Open or create the tracking group for a task. Verifies the stored
    /// task hash matches; returns `LayoutMismatch` on conflict.
    ///
    /// `with_stats` creates the resource-stat arrays in addition to the
    /// always-present `done` and `failures`.
    pub fn open_or_create(
        path: &Path,
        total_roi: &Roi,
        read_roi: &Roi,
        write_roi: &Roi,
        fit: &Fit,
        with_stats: bool,
    ) -> Result<Self, TrackingError> {
        std::fs::create_dir_all(path).map_err(|e| TrackingError::io(path, e))?;

        let task_hash = compute_task_hash(total_roi, read_roi, write_roi, fit);
        let grid_shape = compute_grid_shape(total_roi, write_roi);
        let n_cells: usize = grid_shape.iter().product();

        let group_json_path = path.join("zarr.json");
        if group_json_path.exists() {
            verify_existing_group(&group_json_path, &task_hash, path)?;
        } else {
            let group = json!({
                "zarr_format": 3,
                "node_type": "group",
                "attributes": { "daisy_task_hash": task_hash },
            });
            write_json(&group_json_path, &group)
                .map_err(|e| TrackingError::io(&group_json_path, e))?;
        }

        let wanted: Vec<(&'static str, Dtype)> = BASE_ARRAYS
            .iter()
            .copied()
            .chain(if with_stats {
                STAT_ARRAYS.iter().copied()
            } else {
                [].iter().copied()
            })
            .collect();

        let mut arrays = BTreeMap::new();
        for (name, dtype) in wanted {
            let mmap = open_or_create_array(path, name, dtype, &grid_shape, n_cells)?;
            arrays.insert(name, mmap);
        }

        let strides = c_strides(&grid_shape);
        Ok(Self {
            path: path.to_path_buf(),
            grid_shape,
            strides,
            total_offset: total_roi.offset().as_slice().to_vec(),
            block_shape: write_roi.shape().as_slice().to_vec(),
            arrays,
            with_stats,
            summary: TaskSummary::default(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn grid_shape(&self) -> &[usize] {
        &self.grid_shape
    }

    /// Whether the resource-stat arrays are present.
    pub fn with_stats(&self) -> bool {
        self.with_stats
    }

    /// This run's aggregates.
    pub fn summary(&self) -> &TaskSummary {
        &self.summary
    }

    /// Total number of addressable blocks (cells per array).
    pub fn capacity(&self) -> usize {
        self.arrays
            .get(ARRAY_DONE)
            .map(|m| m.len())
            .unwrap_or_default()
    }

    /// Number of blocks currently marked done. O(N) — for diagnostics,
    /// not the hot path.
    pub fn count_done(&self) -> usize {
        self.arrays
            .get(ARRAY_DONE)
            .map(|m| m.iter().filter(|&&b| b != 0).count())
            .unwrap_or_default()
    }

    /// Index of a block in the flat arrays, or `None` if it isn't
    /// addressable (rank mismatch, or a boundary block outside the grid).
    fn index_of(&self, block: &Block) -> Option<usize> {
        let coord = grid_coord(block, &self.total_offset, &self.block_shape)?;
        for (c, dim) in coord.iter().zip(self.grid_shape.iter()) {
            if c >= dim {
                return None;
            }
        }
        let idx = linear_index(&coord, &self.strides);
        (idx < self.capacity()).then_some(idx)
    }

    /// Has this block already been processed?
    pub fn is_done(&self, block: &Block) -> bool {
        let Some(idx) = self.index_of(block) else {
            return false;
        };
        self.arrays
            .get(ARRAY_DONE)
            .map(|m| m[idx] != 0)
            .unwrap_or(false)
    }

    /// Mark this block successful, without recording resource stats.
    /// No-op if the coordinate is out of range (e.g. boundary blocks
    /// under `Fit::Overhang`).
    pub fn mark_success(&mut self, block: &Block) {
        let Some(idx) = self.index_of(block) else {
            return;
        };
        if let Some(m) = self.arrays.get_mut(ARRAY_DONE) {
            m[idx] = 1;
        }
    }

    /// Record a completed block: mark it done, persist any resource stats
    /// it carried, and fold both into this run's aggregates.
    pub fn record(&mut self, block: &Block) {
        let Some(idx) = self.index_of(block) else {
            // Not addressable (boundary block); still counts as work done
            // so the summary's block count matches the scheduler's.
            self.summary.blocks_recorded += 1;
            return;
        };
        if let Some(m) = self.arrays.get_mut(ARRAY_DONE) {
            m[idx] = 1;
        }
        self.summary.blocks_recorded += 1;

        if let Some(stats) = block.stats.as_ref() {
            self.write_stats(idx, stats);
            self.fold_stats(stats);
        }
    }

    /// Note one failed attempt for this block (a worker-reported failure
    /// or a timeout reclaim). Saturates rather than wrapping.
    pub fn note_failure(&mut self, block: &Block) {
        self.summary.failures_recorded += 1;
        let Some(idx) = self.index_of(block) else {
            return;
        };
        if let Some(m) = self.arrays.get_mut(ARRAY_FAILURES) {
            let cur = read_u32(m, idx);
            write_u32(m, idx, cur.saturating_add(1));
        }
    }

    /// Read back a block's failure count (diagnostics / tests).
    pub fn failure_count(&self, block: &Block) -> u32 {
        let Some(idx) = self.index_of(block) else {
            return 0;
        };
        self.arrays
            .get(ARRAY_FAILURES)
            .map(|m| read_u32(m, idx))
            .unwrap_or(0)
    }

    /// Read back a block's recorded stats, if the stat arrays exist
    /// (diagnostics / tests). `wall_seconds` and `cpu_seconds` are
    /// `float32` on disk, so they come back rounded.
    pub fn stats_of(&self, block: &Block) -> Option<BlockStats> {
        if !self.with_stats {
            return None;
        }
        let idx = self.index_of(block)?;
        Some(BlockStats {
            wall_seconds: self.read_f32(ARRAY_WALL_SECONDS, idx) as f64,
            cpu_seconds: self.read_f32(ARRAY_CPU_SECONDS, idx) as f64,
            peak_rss_bytes: self.read_u64(ARRAY_PEAK_RSS, idx),
            io_read_bytes: self.read_u64(ARRAY_IO_READ, idx),
            io_write_bytes: self.read_u64(ARRAY_IO_WRITE, idx),
            gpu_util_pct: self.read_f32(ARRAY_GPU_UTIL, idx),
            gpu_mem_bytes: self.read_u64(ARRAY_GPU_MEM, idx),
        })
    }

    fn write_stats(&mut self, idx: usize, stats: &BlockStats) {
        if !self.with_stats {
            return;
        }
        let f32_writes = [
            (ARRAY_WALL_SECONDS, stats.wall_seconds as f32),
            (ARRAY_CPU_SECONDS, stats.cpu_seconds as f32),
            (ARRAY_GPU_UTIL, stats.gpu_util_pct),
        ];
        for (name, value) in f32_writes {
            if let Some(m) = self.arrays.get_mut(name) {
                write_f32(m, idx, value);
            }
        }
        let u64_writes = [
            (ARRAY_PEAK_RSS, stats.peak_rss_bytes),
            (ARRAY_IO_READ, stats.io_read_bytes),
            (ARRAY_IO_WRITE, stats.io_write_bytes),
            (ARRAY_GPU_MEM, stats.gpu_mem_bytes),
        ];
        for (name, value) in u64_writes {
            if let Some(m) = self.arrays.get_mut(name) {
                write_u64(m, idx, value);
            }
        }
    }

    fn fold_stats(&mut self, stats: &BlockStats) {
        let s = &mut self.summary;
        s.has_stats = true;
        s.total_cpu_seconds += stats.cpu_seconds;
        s.total_wall_seconds += stats.wall_seconds;
        s.max_peak_rss_bytes = s.max_peak_rss_bytes.max(stats.peak_rss_bytes);
        s.total_io_read_bytes = s.total_io_read_bytes.saturating_add(stats.io_read_bytes);
        s.total_io_write_bytes = s.total_io_write_bytes.saturating_add(stats.io_write_bytes);
        s.block_ms.push(stats.wall_seconds * 1000.0);
    }

    fn read_f32(&self, name: &'static str, idx: usize) -> f32 {
        self.arrays.get(name).map(|m| read_f32(m, idx)).unwrap_or(0.0)
    }

    fn read_u64(&self, name: &'static str, idx: usize) -> u64 {
        self.arrays.get(name).map(|m| read_u64(m, idx)).unwrap_or(0)
    }

    /// msync every array to disk. Called on Drop too, but an explicit
    /// flush is useful at task-completion checkpoints.
    pub fn flush(&self) -> io::Result<()> {
        for m in self.arrays.values() {
            m.flush()?;
        }
        Ok(())
    }

    /// Delete the tracking group at `path` so the next run starts fresh.
    /// No-op if `path` doesn't exist. Removes the directory recursively
    /// (the normal case), falling back to a file unlink for older
    /// single-file layouts.
    pub fn clear(path: &Path) -> io::Result<()> {
        if !path.exists() {
            return Ok(());
        }
        if path.is_dir() {
            std::fs::remove_dir_all(path)
        } else {
            std::fs::remove_file(path)
        }
    }
}

impl Drop for TaskTracking {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

// Element accessors. Values are stored little-endian, matching the zarr v3
// `bytes` codec configuration written in the array metadata.

fn read_u32(m: &MmapMut, idx: usize) -> u32 {
    let o = idx * 4;
    u32::from_le_bytes([m[o], m[o + 1], m[o + 2], m[o + 3]])
}

fn write_u32(m: &mut MmapMut, idx: usize, v: u32) {
    let o = idx * 4;
    m[o..o + 4].copy_from_slice(&v.to_le_bytes());
}

fn read_u64(m: &MmapMut, idx: usize) -> u64 {
    let o = idx * 8;
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&m[o..o + 8]);
    u64::from_le_bytes(buf)
}

fn write_u64(m: &mut MmapMut, idx: usize, v: u64) {
    let o = idx * 8;
    m[o..o + 8].copy_from_slice(&v.to_le_bytes());
}

fn read_f32(m: &MmapMut, idx: usize) -> f32 {
    let o = idx * 4;
    f32::from_le_bytes([m[o], m[o + 1], m[o + 2], m[o + 3]])
}

fn write_f32(m: &mut MmapMut, idx: usize, v: f32) {
    let o = idx * 4;
    m[o..o + 4].copy_from_slice(&v.to_le_bytes());
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

/// Create (or reuse) one child array of the group and mmap its single chunk.
fn open_or_create_array(
    root: &Path,
    name: &str,
    dtype: Dtype,
    grid_shape: &[usize],
    n_cells: usize,
) -> Result<MmapMut, TrackingError> {
    let dir = root.join(name);
    std::fs::create_dir_all(&dir).map_err(|e| TrackingError::io(root, e))?;

    let zarr_json_path = dir.join("zarr.json");
    // Zarr v3 chunk for the single chunk at the all-zero coordinate under
    // the default encoding (separator "/"): `c/0/0/...`.
    let mut chunk_path = dir.join("c");
    for _ in 0..grid_shape.len().max(1) {
        chunk_path.push("0");
    }

    let n_bytes = n_cells * dtype.width();
    let shape_json: Vec<u64> = grid_shape.iter().map(|&v| v as u64).collect();
    let fill: serde_json::Value = match dtype {
        Dtype::F32 => json!(0.0),
        _ => json!(0),
    };
    let layout = json!({
        "zarr_format": 3,
        "node_type": "array",
        "shape": shape_json.clone(),
        "data_type": dtype.zarr_name(),
        "chunk_grid": {
            "name": "regular",
            "configuration": { "chunk_shape": shape_json }
        },
        "chunk_key_encoding": {
            "name": "default",
            "configuration": { "separator": "/" }
        },
        "fill_value": fill,
        "codecs": [ { "name": "bytes", "configuration": { "endian": "little" } } ],
        "attributes": {},
        "storage_transformers": []
    });

    if zarr_json_path.exists() {
        verify_existing_array(&zarr_json_path, grid_shape, dtype, root)?;
    } else {
        write_json(&zarr_json_path, &layout)
            .map_err(|e| TrackingError::io(&zarr_json_path, e))?;
    }
    ensure_chunk_file(&chunk_path, n_bytes, root)?;
    mmap_chunk(&chunk_path, n_bytes)
}

fn ensure_chunk_file(path: &Path, n_bytes: usize, root: &Path) -> Result<(), TrackingError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| TrackingError::io(root, e))?;
    }
    let f = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)
        .map_err(|e| TrackingError::io(root, e))?;
    let len = f
        .metadata()
        .map_err(|e| TrackingError::io(root, e))?
        .len() as usize;
    if len != n_bytes {
        f.set_len(n_bytes as u64)
            .map_err(|e| TrackingError::io(root, e))?;
    }
    Ok(())
}

fn mmap_chunk(path: &Path, expected_bytes: usize) -> Result<MmapMut, TrackingError> {
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|e| TrackingError::io(path, e))?;
    let len = f
        .metadata()
        .map_err(|e| TrackingError::io(path, e))?
        .len() as usize;
    if len != expected_bytes {
        return Err(TrackingError::InvalidMetadata {
            path: path.to_path_buf(),
            reason: format!("chunk file is {len} bytes, expected {expected_bytes}"),
        });
    }
    // SAFETY: We own the file; no one else mutates it concurrently while we
    // hold this TaskTracking. The mmap stays valid until Drop.
    unsafe { MmapMut::map_mut(&f) }.map_err(|e| TrackingError::io(path, e))
}

/// Verify the group's stored task hash.
///
/// A directory written by an older daisy holds a bare zarr *array* here
/// rather than a group. Its hash was computed with a different domain
/// separator, so it can never match — the user gets the same actionable
/// `LayoutMismatch` ("rm -rf …") as any other stale tracking directory,
/// which is the outcome we want.
fn verify_existing_group(
    group_json_path: &Path,
    expected_hash: &str,
    root: &Path,
) -> Result<(), TrackingError> {
    let metadata: serde_json::Value = read_json(group_json_path)?;

    let stored_hash = metadata
        .get("attributes")
        .and_then(|a| a.get("daisy_task_hash"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let stored_hash = stored_hash.ok_or_else(|| TrackingError::LayoutMismatch {
        path: root.to_path_buf(),
        stored_hash: "<missing zarr.json/attributes.daisy_task_hash>".to_string(),
        expected_hash: expected_hash.to_string(),
    })?;
    if stored_hash != expected_hash {
        return Err(TrackingError::LayoutMismatch {
            path: root.to_path_buf(),
            stored_hash,
            expected_hash: expected_hash.to_string(),
        });
    }

    // The hash matched, so this directory belongs to this task layout. If
    // it isn't a group, it was written by a daisy old enough to predate
    // the group layout (or has been tampered with) — report it as stale
    // tracking with the same delete-and-rerun remedy.
    let node_type = metadata.get("node_type").and_then(|v| v.as_str());
    if node_type != Some("group") {
        return Err(TrackingError::LayoutMismatch {
            path: root.to_path_buf(),
            stored_hash: format!("<zarr node_type = {node_type:?}, expected \"group\">"),
            expected_hash: expected_hash.to_string(),
        });
    }
    Ok(())
}

/// Cross-check a child array's shape and dtype. The hash lives on the
/// group, so a mismatch here means a corrupted or hand-edited array rather
/// than a layout change.
fn verify_existing_array(
    zarr_json_path: &Path,
    expected_grid_shape: &[usize],
    expected_dtype: Dtype,
    root: &Path,
) -> Result<(), TrackingError> {
    let metadata: serde_json::Value = read_json(zarr_json_path)?;

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
        return Err(TrackingError::InvalidMetadata {
            path: root.to_path_buf(),
            reason: format!(
                "{} shape inconsistent with task layout (stored = {:?}, expected = {:?})",
                zarr_json_path.display(),
                stored_shape,
                expected_grid_shape
            ),
        });
    }

    let stored_dtype = metadata.get("data_type").and_then(|v| v.as_str());
    if stored_dtype != Some(expected_dtype.zarr_name()) {
        return Err(TrackingError::InvalidMetadata {
            path: root.to_path_buf(),
            reason: format!(
                "{} dtype is {:?}, expected {:?}",
                zarr_json_path.display(),
                stored_dtype,
                expected_dtype.zarr_name()
            ),
        });
    }
    Ok(())
}

fn read_json(path: &Path) -> Result<serde_json::Value, TrackingError> {
    let bytes = std::fs::read(path).map_err(|e| TrackingError::io(path, e))?;
    serde_json::from_slice(&bytes).map_err(|e| TrackingError::InvalidMetadata {
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

    fn open(dir: &Path, total: &Roi, block_shape: &Roi, with_stats: bool) -> TaskTracking {
        TaskTracking::open_or_create(dir, total, block_shape, block_shape, &Fit::Valid, with_stats)
            .unwrap()
    }

    #[test]
    fn round_trip() {
        let dir = tempdir();
        let total = make_roi(&[0, 0], &[400, 400]);
        let block_shape = make_roi(&[0, 0], &[100, 100]);
        let mut m = open(&dir, &total, &block_shape, false);
        assert_eq!(m.grid_shape(), &[4, 4]);
        assert_eq!(m.capacity(), 16);

        let b = block_at(&total, &[100, 200], &[100, 100]); // grid (1, 2)
        assert!(!m.is_done(&b));
        m.mark_success(&b);
        assert!(m.is_done(&b));
        assert_eq!(m.count_done(), 1);

        // Reopen and confirm persistence.
        drop(m);
        let m2 = open(&dir, &total, &block_shape, false);
        assert!(m2.is_done(&b));
    }

    #[test]
    fn layout_mismatch_refuses_to_open() {
        let dir = tempdir();
        let total = make_roi(&[0, 0], &[400, 400]);
        let block_shape = make_roi(&[0, 0], &[100, 100]);
        let _ = open(&dir, &total, &block_shape, false);

        // Different block shape → different hash → must refuse.
        let other = make_roi(&[0, 0], &[50, 50]);
        let result =
            TaskTracking::open_or_create(&dir, &total, &other, &other, &Fit::Valid, false);
        let err = match result {
            Ok(_) => panic!("expected LayoutMismatch / InvalidMetadata, got Ok"),
            Err(e) => e,
        };
        assert!(matches!(
            err,
            TrackingError::LayoutMismatch { .. } | TrackingError::InvalidMetadata { .. }
        ));
        // The message must stay actionable — python tests assert on it.
        assert!(format!("{err}").contains("rm -rf"), "message: {err}");
    }

    #[test]
    fn legacy_array_layout_reports_layout_mismatch() {
        // A directory written by a daisy predating the group layout: a
        // bare zarr array at the root. Must produce the actionable
        // LayoutMismatch rather than a confusing metadata error.
        let dir = tempdir();
        std::fs::create_dir_all(&dir).unwrap();
        let total = make_roi(&[0, 0], &[400, 400]);
        let block_shape = make_roi(&[0, 0], &[100, 100]);
        write_json(
            &dir.join("zarr.json"),
            &json!({
                "zarr_format": 3,
                "node_type": "array",
                "shape": [4, 4],
                "data_type": "uint8",
                "attributes": { "daisy_task_hash": "some-old-hash" },
            }),
        )
        .unwrap();

        let err = match TaskTracking::open_or_create(
            &dir,
            &total,
            &block_shape,
            &block_shape,
            &Fit::Valid,
            false,
        ) {
            Ok(_) => panic!("expected LayoutMismatch for a legacy array layout, got Ok"),
            Err(e) => e,
        };
        assert!(matches!(err, TrackingError::LayoutMismatch { .. }), "got {err:?}");
        assert!(format!("{err}").contains("rm -rf"), "message: {err}");
    }

    #[test]
    fn failure_counts_accumulate_and_persist() {
        let dir = tempdir();
        let total = make_roi(&[0], &[100]);
        let block_shape = make_roi(&[0], &[10]);
        let mut m = open(&dir, &total, &block_shape, false);

        let b = block_at(&total, &[30], &[10]); // grid (3,)
        assert_eq!(m.failure_count(&b), 0);
        m.note_failure(&b);
        m.note_failure(&b);
        assert_eq!(m.failure_count(&b), 2);
        assert_eq!(m.summary().failures_recorded, 2);

        // A different block keeps its own count.
        let other = block_at(&total, &[50], &[10]);
        assert_eq!(m.failure_count(&other), 0);

        drop(m);
        let m2 = open(&dir, &total, &block_shape, false);
        assert_eq!(m2.failure_count(&b), 2);
        // Aggregates are per-run, so a fresh open starts at zero.
        assert_eq!(m2.summary().failures_recorded, 0);
    }

    #[test]
    fn stats_round_trip_through_drop_and_reopen() {
        let dir = tempdir();
        let total = make_roi(&[0], &[100]);
        let block_shape = make_roi(&[0], &[10]);
        let mut m = open(&dir, &total, &block_shape, true);
        assert!(m.with_stats());

        let mut b = block_at(&total, &[20], &[10]); // grid (2,)
        b.stats = Some(BlockStats {
            wall_seconds: 0.5,
            cpu_seconds: 0.25,
            peak_rss_bytes: 4096,
            io_read_bytes: 128,
            io_write_bytes: 256,
            gpu_util_pct: f32::NAN,
            gpu_mem_bytes: 0,
        });
        m.record(&b);

        let summary = m.summary();
        assert_eq!(summary.blocks_recorded, 1);
        assert!(summary.has_stats);
        assert!((summary.total_cpu_seconds - 0.25).abs() < 1e-6);
        assert_eq!(summary.max_peak_rss_bytes, 4096);
        assert_eq!(summary.total_io_read_bytes, 128);
        assert_eq!(summary.total_io_write_bytes, 256);
        assert_eq!(summary.block_ms.len(), 1);
        assert!((summary.block_ms[0] - 500.0).abs() < 1e-3);

        drop(m);
        let m2 = open(&dir, &total, &block_shape, true);
        let back = m2.stats_of(&b).expect("stats array present");
        assert!((back.wall_seconds - 0.5).abs() < 1e-6, "{back:?}");
        assert!((back.cpu_seconds - 0.25).abs() < 1e-6, "{back:?}");
        assert_eq!(back.peak_rss_bytes, 4096);
        assert_eq!(back.io_read_bytes, 128);
        assert_eq!(back.io_write_bytes, 256);
        assert!(back.gpu_util_pct.is_nan(), "gpu slot should stay NaN");
        assert!(m2.is_done(&b), "record must also mark done");
    }

    #[test]
    fn stat_arrays_absent_without_resource_tracking() {
        let dir = tempdir();
        let total = make_roi(&[0], &[100]);
        let block_shape = make_roi(&[0], &[10]);
        let mut m = open(&dir, &total, &block_shape, false);

        let mut b = block_at(&total, &[20], &[10]);
        b.stats = Some(BlockStats::default());
        m.record(&b);

        assert!(!m.with_stats());
        assert!(m.stats_of(&b).is_none());
        assert!(dir.join("done").is_dir());
        assert!(dir.join("failures").is_dir());
        assert!(!dir.join("cpu_seconds").exists());
        // Aggregates still count the block, and stats folded from the
        // payload are still summarised even though nothing is persisted.
        assert_eq!(m.summary().blocks_recorded, 1);
    }

    #[test]
    fn summary_trend_uses_recorded_wall_times() {
        let dir = tempdir();
        let total = make_roi(&[0], &[100]);
        let block_shape = make_roi(&[0], &[10]);
        let mut m = open(&dir, &total, &block_shape, true);

        // Blocks getting steadily slower: 10ms, 20ms, 30ms.
        for (i, ms) in [10.0f64, 20.0, 30.0].iter().enumerate() {
            let mut b = block_at(&total, &[(i as i64) * 10], &[10]);
            b.stats = Some(BlockStats {
                wall_seconds: ms / 1000.0,
                ..BlockStats::default()
            });
            m.record(&b);
        }
        let (mean, slope) = m.summary().block_ms_trend();
        assert!((mean - 20.0).abs() < 1e-3, "mean={mean}");
        assert!((slope - 10.0).abs() < 1e-3, "slope={slope}");
        assert_eq!(m.summary().blocks_recorded, 3);
    }

    #[test]
    fn group_and_arrays_are_zarr_v3_shaped() {
        let dir = tempdir();
        let total = make_roi(&[0, 0], &[40, 40]);
        let block_shape = make_roi(&[0, 0], &[10, 10]);
        let _m = open(&dir, &total, &block_shape, true);

        let group: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.join("zarr.json")).unwrap()).unwrap();
        assert_eq!(group["node_type"], "group");
        assert_eq!(group["zarr_format"], 3);
        assert!(group["attributes"]["daisy_task_hash"].is_string());

        for (name, dtype) in [
            (ARRAY_DONE, "uint8"),
            (ARRAY_FAILURES, "uint32"),
            (ARRAY_CPU_SECONDS, "float32"),
            (ARRAY_PEAK_RSS, "uint64"),
        ] {
            let meta: serde_json::Value =
                serde_json::from_slice(&std::fs::read(dir.join(name).join("zarr.json")).unwrap())
                    .unwrap();
            assert_eq!(meta["node_type"], "array", "{name}");
            assert_eq!(meta["data_type"], dtype, "{name}");
            assert_eq!(meta["shape"], json!([4, 4]), "{name}");
            // Single chunk covering the whole array, raw little-endian bytes.
            assert_eq!(
                meta["chunk_grid"]["configuration"]["chunk_shape"],
                json!([4, 4]),
                "{name}"
            );
            assert!(dir.join(name).join("c").join("0").join("0").is_file(), "{name}");
        }
    }

    #[test]
    fn capacity_for_billion_blocks_is_addressable() {
        // Sanity: 1000 × 1000 × 1000 grid → 1 GB for uint8. We don't
        // actually open it (would be slow), just verify the math.
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
        let p = std::env::temp_dir().join(format!("daisy-block-tracking-test-{n}"));
        let _ = std::fs::remove_dir_all(&p);
        p
    }
}
