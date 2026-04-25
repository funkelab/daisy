use crate::roi::Roi;
use std::path::PathBuf;
use std::sync::Arc;

use crate::block::Block;
use crate::error::GerberaError;

/// Called server-side during `acquire_block` to skip already-completed blocks.
/// Runs in the coordinator process and must be fast.
pub trait CheckBlock: Send + Sync {
    fn check(&self, block: &Block) -> bool;
}

/// Called worker-side (or in serial mode, in-process) to do the actual work on
/// a block. This is for 1-arg process functions.
pub trait ProcessBlock: Send {
    fn process(&self, block: &mut Block) -> Result<(), GerberaError>;
}

/// Called to spawn a worker. This is for 0-arg spawn functions (e.g.,
/// functions that call subprocess.run). The function blocks until the
/// worker is done.
pub trait SpawnWorker: Send + Sync {
    fn spawn(&self, env_context: &str) -> Result<(), GerberaError>;
}

/// No-op checker for tasks without a check function.
pub struct NoCheck;

impl CheckBlock for NoCheck {
    fn check(&self, _block: &Block) -> bool {
        false
    }
}

/// Fit mode controlling how blocks on the boundary of the total ROI are
/// handled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fit {
    /// Skip blocks whose write ROI extends beyond the total ROI.
    Valid,
    /// Include boundary blocks even if they extend beyond.
    Overhang,
    /// Include boundary blocks but shrink their ROIs to stay within bounds,
    /// preserving read-write context.
    Shrink,
}

impl Fit {
    pub fn from_str(s: &str) -> Self {
        match s {
            "valid" => Fit::Valid,
            "overhang" => Fit::Overhang,
            "shrink" => Fit::Shrink,
            other => panic!("unknown fit mode: {other}"),
        }
    }
}

/// Definition of a task to be run block-wise.
pub struct Task {
    pub task_id: String,
    pub total_roi: Roi,
    pub read_roi: Roi,
    pub write_roi: Roi,
    pub total_write_roi: Roi,
    pub read_write_conflict: bool,
    pub fit: Fit,
    pub num_workers: usize,
    pub max_retries: u32,
    pub timeout: Option<std::time::Duration>,
    pub upstream_tasks: Vec<Arc<Task>>,
    pub check_function: Option<Arc<dyn CheckBlock>>,
    /// 1-arg block processor. Server spawns worker threads that acquire
    /// blocks, call this function via GIL, and release blocks.
    pub process_function: Option<Arc<dyn ProcessBlock + Sync>>,
    /// 0-arg spawn function. Server spawns worker threads that call this
    /// function directly. The function typically launches a subprocess
    /// that connects back via TCP.
    pub spawn_function: Option<Arc<dyn SpawnWorker>>,
    /// If `Some`, the scheduler / runner persists per-block "done"
    /// state under this directory as a single-chunk Zarr v2 array. On
    /// the next run, blocks already marked done are skipped before the
    /// process function is called. See `crate::done_marker`.
    pub done_marker_path: Option<PathBuf>,
}

impl Task {
    pub fn builder(task_id: &str) -> TaskBuilder {
        TaskBuilder::new(task_id)
    }

    pub fn requires(&self) -> &[Arc<Task>] {
        &self.upstream_tasks
    }
}

pub struct TaskBuilder {
    task_id: String,
    total_roi: Option<Roi>,
    read_roi: Option<Roi>,
    write_roi: Option<Roi>,
    read_write_conflict: bool,
    fit: Fit,
    num_workers: usize,
    max_retries: u32,
    timeout: Option<std::time::Duration>,
    upstream_tasks: Vec<Arc<Task>>,
    check_function: Option<Arc<dyn CheckBlock>>,
    process_function: Option<Arc<dyn ProcessBlock + Sync>>,
    spawn_function: Option<Arc<dyn SpawnWorker>>,
    done_marker_path: Option<PathBuf>,
}

impl TaskBuilder {
    fn new(task_id: &str) -> Self {
        Self {
            task_id: task_id.to_string(),
            total_roi: None,
            read_roi: None,
            write_roi: None,
            read_write_conflict: true,
            fit: Fit::Valid,
            num_workers: 1,
            max_retries: 2,
            timeout: None,
            upstream_tasks: Vec::new(),
            check_function: None,
            process_function: None,
            spawn_function: None,
            done_marker_path: None,
        }
    }

    pub fn total_roi(mut self, roi: Roi) -> Self {
        self.total_roi = Some(roi);
        self
    }

    pub fn read_roi(mut self, roi: Roi) -> Self {
        self.read_roi = Some(roi);
        self
    }

    pub fn write_roi(mut self, roi: Roi) -> Self {
        self.write_roi = Some(roi);
        self
    }

    pub fn read_write_conflict(mut self, conflict: bool) -> Self {
        self.read_write_conflict = conflict;
        self
    }

    pub fn fit(mut self, fit: Fit) -> Self {
        self.fit = fit;
        self
    }

    pub fn num_workers(mut self, n: usize) -> Self {
        self.num_workers = n;
        self
    }

    pub fn max_retries(mut self, n: u32) -> Self {
        self.max_retries = n;
        self
    }

    pub fn timeout(mut self, t: std::time::Duration) -> Self {
        self.timeout = Some(t);
        self
    }

    pub fn upstream(mut self, task: Arc<Task>) -> Self {
        self.upstream_tasks.push(task);
        self
    }

    pub fn check_function(mut self, f: impl CheckBlock + 'static) -> Self {
        self.check_function = Some(Arc::new(f));
        self
    }

    pub fn process_function(mut self, f: impl ProcessBlock + Sync + 'static) -> Self {
        self.process_function = Some(Arc::new(f));
        self
    }

    pub fn spawn_function(mut self, f: impl SpawnWorker + 'static) -> Self {
        self.spawn_function = Some(Arc::new(f));
        self
    }

    pub fn done_marker_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.done_marker_path = Some(path.into());
        self
    }

    pub fn build(self) -> Task {
        let total_roi = self.total_roi.expect("total_roi is required");
        let read_roi = self.read_roi.expect("read_roi is required");
        let write_roi = self.write_roi.expect("write_roi is required");

        // Compute total_write_roi by shrinking total_roi by the read-write context.
        let context_begin = write_roi.begin() - read_roi.begin();
        let context_end = read_roi.end() - write_roi.end();
        let total_write_roi = total_roi.grow(&-&context_begin, &-&context_end);

        Task {
            task_id: self.task_id,
            total_roi,
            read_roi,
            write_roi,
            total_write_roi,
            read_write_conflict: self.read_write_conflict,
            fit: self.fit,
            num_workers: self.num_workers,
            max_retries: self.max_retries,
            timeout: self.timeout,
            upstream_tasks: self.upstream_tasks,
            check_function: self.check_function,
            process_function: self.process_function,
            spawn_function: self.spawn_function,
            done_marker_path: self.done_marker_path,
        }
    }
}
