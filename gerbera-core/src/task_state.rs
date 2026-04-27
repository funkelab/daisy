use serde::{Deserialize, Serialize};
use std::fmt;

/// Counters tracking progress through block processing for a single
/// task that is *currently active*. The mutation methods on this
/// struct are the only legitimate way to advance the per-task
/// counters; the type system enforces that they can't be called once
/// the task has transitioned to a terminal state (Done / Abandoned),
/// because terminal variants of `TaskState` don't expose a
/// `&mut RunningTask`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RunningTask {
    pub started: bool,
    pub total_block_count: i64,
    pub ready_count: i64,
    pub processing_count: i64,
    pub completed_count: i64,
    pub skipped_count: i64,
    pub failed_count: i64,
    pub orphaned_count: i64,
    /// Total dirty exits the runner has observed for this task. Pure
    /// stat — can exceed `Task::max_worker_restarts` when several
    /// alive workers die after the runner has stopped respawning.
    pub worker_failure_count: u32,
    /// Refill spawns the runner has actually performed. Hard-capped
    /// at `Task::max_worker_restarts`. This is what the user
    /// configures the cap against and what the tqdm `♻=N` counter
    /// displays. Initial-fill spawns and growth-after-budget-loosens
    /// spawns do not bump this counter.
    pub worker_restart_count: u32,
}

impl RunningTask {
    pub fn new(total_block_count: i64) -> Self {
        Self {
            total_block_count,
            ..Self::default()
        }
    }

    /// Blocks not yet enqueued (in `Pending` per the abandonment doc).
    /// May include blocks awaiting upstream completion.
    pub fn pending_count(&self) -> i64 {
        self.total_block_count
            - self.ready_count
            - self.processing_count
            - self.completed_count
            - self.failed_count
            - self.orphaned_count
    }

    /// True when every block has settled into a terminal counter.
    /// A balanced `RunningTask` is eligible to transition to `Done`.
    pub fn balanced(&self) -> bool {
        self.completed_count + self.failed_count + self.orphaned_count
            == self.total_block_count
    }

    // -- Mutation methods. The only way to change the counters. --

    pub fn note_acquired(&mut self) {
        self.ready_count -= 1;
        self.processing_count += 1;
        self.started = true;
    }

    pub fn note_skip_inflight(&mut self) {
        self.skipped_count += 1;
    }

    pub fn note_completed(&mut self) {
        self.processing_count -= 1;
        self.completed_count += 1;
    }

    pub fn note_failed_permanently(&mut self) {
        self.processing_count -= 1;
        self.failed_count += 1;
    }

    pub fn note_failed_for_retry(&mut self) {
        self.processing_count -= 1;
        self.ready_count += 1;
    }

    pub fn note_ready(&mut self) {
        self.ready_count += 1;
    }

    pub fn note_orphaned(&mut self) {
        self.orphaned_count += 1;
    }

    pub fn note_worker_died(&mut self) {
        self.worker_failure_count = self.worker_failure_count.saturating_add(1);
    }

    pub fn note_worker_restarted(&mut self) {
        self.worker_restart_count = self.worker_restart_count.saturating_add(1);
    }
}

/// Frozen counter snapshot. Used as the data payload for terminal
/// variants and as the FFI-friendly view that observers receive.
/// Has no mutation methods — once you have a `TaskCounters`, the
/// counters can't change underneath you.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TaskCounters {
    pub started: bool,
    pub total_block_count: i64,
    pub ready_count: i64,
    pub processing_count: i64,
    pub completed_count: i64,
    pub skipped_count: i64,
    pub failed_count: i64,
    pub orphaned_count: i64,
    pub worker_failure_count: u32,
    pub worker_restart_count: u32,
}

impl TaskCounters {
    pub fn pending_count(&self) -> i64 {
        self.total_block_count
            - self.ready_count
            - self.processing_count
            - self.completed_count
            - self.failed_count
            - self.orphaned_count
    }

    /// Counters-only view of doneness. Equals `RunningTask::balanced()`
    /// when the snapshot was taken from a Running task; always true
    /// for snapshots taken from a Done or Abandoned task (we set
    /// the counters at the transition).
    pub fn balanced(&self) -> bool {
        self.completed_count + self.failed_count + self.orphaned_count
            == self.total_block_count
    }

    /// Alias for `balanced()`. The post-run snapshot is "done" iff
    /// counters balance — matches the previous `TaskState::is_done()`
    /// semantics so existing callers don't need to learn a new name.
    pub fn is_done(&self) -> bool {
        self.balanced()
    }
}

impl From<&RunningTask> for TaskCounters {
    fn from(t: &RunningTask) -> Self {
        Self {
            started: t.started,
            total_block_count: t.total_block_count,
            ready_count: t.ready_count,
            processing_count: t.processing_count,
            completed_count: t.completed_count,
            skipped_count: t.skipped_count,
            failed_count: t.failed_count,
            orphaned_count: t.orphaned_count,
            worker_failure_count: t.worker_failure_count,
            worker_restart_count: t.worker_restart_count,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AbandonReason {
    /// Worker restart cap exhausted while blocks remained for this
    /// task itself.
    RestartCapExhausted,
    /// This task is downstream (transitively) of a directly
    /// abandoned task — its input will never arrive.
    UpstreamAbandoned,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DoneTask {
    pub counters: TaskCounters,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AbandonedTask {
    pub counters: TaskCounters,
    pub reason: AbandonReason,
}

/// Per-task lifecycle state. State-changing methods are physically
/// confined to `RunningTask`; `Done` and `Abandoned` are read-only
/// terminal variants. The compiler enforces that mutation requires
/// pattern-matching out a `&mut RunningTask`, so a stale message
/// landing on a terminal task can't accidentally rewind the
/// counters.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TaskState {
    Running(RunningTask),
    Done(DoneTask),
    Abandoned(AbandonedTask),
}

impl Default for TaskState {
    fn default() -> Self {
        Self::Running(RunningTask::default())
    }
}

impl TaskState {
    pub fn running(total_block_count: i64) -> Self {
        Self::Running(RunningTask::new(total_block_count))
    }

    /// Read-only counter snapshot for any variant.
    pub fn counters(&self) -> TaskCounters {
        match self {
            Self::Running(t) => t.into(),
            Self::Done(t) => t.counters.clone(),
            Self::Abandoned(t) => t.counters.clone(),
        }
    }

    pub fn is_done(&self) -> bool {
        match self {
            Self::Running(t) => t.balanced(),
            Self::Done(_) | Self::Abandoned(_) => true,
        }
    }

    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running(_))
    }

    /// Mutable access to the running data. Returns `None` for
    /// terminal variants — the caller is expected to drop whatever
    /// stale event would have mutated the task. This is the single
    /// gate every counter writer goes through.
    pub fn as_running_mut(&mut self) -> Option<&mut RunningTask> {
        match self {
            Self::Running(t) => Some(t),
            _ => None,
        }
    }

    /// Total block count is invariant across all variants.
    pub fn total_block_count(&self) -> i64 {
        match self {
            Self::Running(t) => t.total_block_count,
            Self::Done(t) => t.counters.total_block_count,
            Self::Abandoned(t) => t.counters.total_block_count,
        }
    }

    pub fn worker_failure_count(&self) -> u32 {
        match self {
            Self::Running(t) => t.worker_failure_count,
            Self::Done(t) => t.counters.worker_failure_count,
            Self::Abandoned(t) => t.counters.worker_failure_count,
        }
    }

    pub fn worker_restart_count(&self) -> u32 {
        match self {
            Self::Running(t) => t.worker_restart_count,
            Self::Done(t) => t.counters.worker_restart_count,
            Self::Abandoned(t) => t.counters.worker_restart_count,
        }
    }

    // -- Transitions. All consume the Running variant in place. --

    /// If currently running and balanced, transition to `Done`. No-op
    /// otherwise. Idempotent for terminal states.
    pub fn try_finalize_done(&mut self) -> bool {
        if !matches!(self, Self::Running(t) if t.balanced()) {
            return false;
        }
        let placeholder = Self::Done(DoneTask {
            counters: TaskCounters::default(),
        });
        let old = std::mem::replace(self, placeholder);
        let Self::Running(rt) = old else { unreachable!() };
        let counters = TaskCounters::from(&rt);
        *self = Self::Done(DoneTask { counters });
        true
    }

    /// Account remaining blocks as orphaned and transition to
    /// `Abandoned`. No-op for terminal states. Returns the number of
    /// blocks accounted on transition (for logging) — `None` if the
    /// task wasn't running.
    pub fn abandon(&mut self, reason: AbandonReason) -> Option<i64> {
        if !matches!(self, Self::Running(_)) {
            return None;
        }
        let placeholder = Self::Done(DoneTask {
            counters: TaskCounters::default(),
        });
        let old = std::mem::replace(self, placeholder);
        let Self::Running(mut rt) = old else { unreachable!() };
        let remaining = (rt.total_block_count
            - rt.completed_count
            - rt.failed_count
            - rt.orphaned_count)
            .max(0);
        rt.orphaned_count += remaining;
        rt.ready_count = 0;
        rt.processing_count = 0;
        let counters = TaskCounters::from(&rt);
        *self = Self::Abandoned(AbandonedTask { counters, reason });
        Some(remaining)
    }
}

impl fmt::Display for TaskCounters {
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

impl fmt::Display for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let phase = match self {
            Self::Running(_) => "Running",
            Self::Done(_) => "Done",
            Self::Abandoned(t) => match t.reason {
                AbandonReason::RestartCapExhausted => "Abandoned(RestartCapExhausted)",
                AbandonReason::UpstreamAbandoned => "Abandoned(UpstreamAbandoned)",
            },
        };
        write!(f, "Phase: {phase}\n{}", self.counters())
    }
}
