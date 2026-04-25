use serde::{Deserialize, Serialize};
use std::fmt;

/// Counters tracking the progress of a task through block processing.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TaskState {
    pub started: bool,
    pub total_block_count: i64,
    pub ready_count: i64,
    pub processing_count: i64,
    pub completed_count: i64,
    pub skipped_count: i64,
    pub failed_count: i64,
    pub orphaned_count: i64,
}

impl TaskState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Blocks that haven't been categorized yet — may include orphans if
    /// `count_all_orphans` is off.
    pub fn pending_count(&self) -> i64 {
        self.total_block_count
            - self.ready_count
            - self.completed_count
            - self.failed_count
            - self.orphaned_count
            - self.processing_count
    }

    /// True when every block has been completed, failed, or orphaned.
    pub fn is_done(&self) -> bool {
        self.total_block_count - self.completed_count - self.failed_count - self.orphaned_count
            == 0
    }
}

impl fmt::Display for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Started: {}\n\
             Total Blocks: {}\n\
             Ready: {}\n\
             Processing: {}\n\
             Pending: {}\n\
             Completed: {}\n\
             Skipped: {}\n\
             Failed: {}\n\
             Orphaned: {}",
            self.started,
            self.total_block_count,
            self.ready_count,
            self.processing_count,
            self.pending_count(),
            self.completed_count,
            self.skipped_count,
            self.failed_count,
            self.orphaned_count,
        )
    }
}
