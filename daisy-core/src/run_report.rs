//! The per-task run report: block aggregates from the tracking layer plus
//! the worker timeline, one `TaskReport` per task in the pipeline.
//!
//! This is what a run returns, what the end-of-run panel renders, and
//! (serialized as one JSON line) what is appended to the task's
//! `runs.jsonl`. Every figure is a fold over events the scheduler already
//! counts — there is no second block counter.

use std::collections::HashMap;

use serde::Serialize;

use crate::block_tracking::TaskSummary;
use crate::run_stats::Welford;
use crate::task_state::TaskCounters;
use crate::worker_timeline::{RunTimeline, WorkerReport};

/// Per-block aggregates for one task over this run.
#[derive(Clone, Debug, Default, Serialize)]
pub struct BlockReport {
    /// Blocks completed this run (from the tracking layer when the task
    /// has a store, else the scheduler's completed count).
    pub count: u64,
    /// Failed attempts this run.
    pub failures: u64,
    /// True once any block carried resource measurements.
    pub has_stats: bool,
    /// Server round trip per completed block (dispatch → return). Needs
    /// no tracking store, so it is reported for every distributed task.
    pub round_trip: Welford,
    pub wall: Welford,
    pub cpu: Welford,
    pub peak_rss: Welford,
    pub io_read: Welford,
    pub io_write: Welford,
    /// Server round trip minus the worker's `wall_seconds`, per measured
    /// block: daisy's own per-block cost.
    pub overhead: Welford,
    /// Least-squares slope of per-block wall time, ms per block in
    /// completion order.
    pub trend_slope_ms: f64,
}

impl BlockReport {
    fn from_summary(s: &TaskSummary, round_trip: Welford, overhead: Welford) -> Self {
        Self {
            count: s.blocks_recorded,
            failures: s.failures_recorded,
            has_stats: s.has_stats,
            round_trip,
            wall: s.wall.clone(),
            cpu: s.cpu.clone(),
            peak_rss: s.peak_rss.clone(),
            io_read: s.io_read.clone(),
            io_write: s.io_write.clone(),
            overhead,
            trend_slope_ms: s.wall_trend.slope(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct TaskReport {
    pub task_id: String,
    /// Run start, seconds since the UNIX epoch (server clock).
    pub started_at: f64,
    /// Report build time, seconds since the UNIX epoch.
    pub finished_at: f64,
    /// Whether the task had a tracking store this run (and therefore a
    /// `runs.jsonl` line).
    pub tracked: bool,
    pub blocks: BlockReport,
    pub workers: WorkerReport,
}

impl TaskReport {
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string())
    }
}

/// Build one report per task. `summaries` covers tasks with a tracking
/// store; the rest get their block count from the counters and no
/// resource figures.
pub fn build_reports(
    counters: &HashMap<String, TaskCounters>,
    summaries: &HashMap<String, TaskSummary>,
    timeline: &RunTimeline,
) -> HashMap<String, TaskReport> {
    let finished_at = timeline.epoch_seconds(std::time::Instant::now());
    counters
        .iter()
        .map(|(task_id, c)| {
            let round_trip = timeline.round_trip(task_id);
            let overhead = timeline.overhead(task_id);
            let (tracked, blocks) = match summaries.get(task_id) {
                Some(s) => (true, BlockReport::from_summary(s, round_trip, overhead)),
                None => (
                    false,
                    BlockReport {
                        count: c.completed_count.max(0) as u64,
                        round_trip,
                        overhead,
                        ..BlockReport::default()
                    },
                ),
            };
            let report = TaskReport {
                task_id: task_id.clone(),
                started_at: timeline.started_at(),
                finished_at,
                tracked,
                blocks,
                workers: timeline.report_for(task_id),
            };
            (task_id.clone(), report)
        })
        .collect()
}

/// Serialize a map of reports to JSON (`{task_id: report}`), the form the
/// Python layer consumes.
pub fn reports_to_json(reports: &HashMap<String, TaskReport>) -> String {
    serde_json::to_string(reports).unwrap_or_else(|_| "{}".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn untracked_task_reports_its_counters_and_no_stats() {
        let mut counters = HashMap::new();
        counters.insert(
            "t".to_string(),
            TaskCounters {
                completed_count: 7,
                ..TaskCounters::default()
            },
        );
        let timeline = RunTimeline::new();
        let reports = build_reports(&counters, &HashMap::new(), &timeline);
        let r = &reports["t"];
        assert!(!r.tracked);
        assert_eq!(r.blocks.count, 7);
        assert!(!r.blocks.has_stats);
        assert_eq!(r.workers.count, 0);
        let json: serde_json::Value = serde_json::from_str(&r.to_json()).unwrap();
        assert_eq!(json["blocks"]["count"], 7);
        assert_eq!(json["workers"]["exits"]["clean"], 0);
        assert!(json["blocks"]["wall"]["std"].is_number());
    }
}
