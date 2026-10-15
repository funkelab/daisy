use crate::block::BlockStatus;
use crate::block_profile::BlockProfiler;
use crate::run_report::{build_reports, TaskReport};
use crate::worker_timeline::RunTimeline;
use crate::error::DaisyError;
use crate::scheduler::Scheduler;
use crate::task::Task;
use crate::task_state::TaskCounters;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::debug;

/// Single-threaded runner for debugging. Processes all blocks in-process
/// without TCP or worker spawning. Both `CheckBlock` and `ProcessBlock` are
/// called directly in the calling thread.
pub struct SerialRunner;

impl SerialRunner {
    /// Run tasks to completion serially. The `process_function` on each `Task`
    /// must be set (via the builder) since this runner calls it directly.
    ///
    /// `block_tracking`: when `true`, per-task done-markers are opened and
    /// the scheduler's pre-check skips already-completed blocks. When
    /// `false`, marker initialization is skipped — every block is processed
    /// regardless of any `tracking_path` configured on the tasks.
    pub fn run(
        pipeline: &crate::pipeline::Pipeline,
        block_tracking: bool,
    ) -> Result<(HashMap<String, TaskCounters>, HashMap<String, TaskReport>), DaisyError> {
        crate::scheduler::validate_pipeline(pipeline)?;
        let mut scheduler = Scheduler::new(pipeline, true);
        if block_tracking {
            scheduler.init_tracking()?;
        }

        let mut started_tasks = HashSet::new();
        let mut finished_tasks: HashSet<String> = HashSet::new();
        let all_tasks: HashSet<String> =
            pipeline.tasks.iter().map(|t| t.task_id.clone()).collect();

        loop {
            let ready_tasks = scheduler.get_ready_tasks();
            if finished_tasks == all_tasks {
                break;
            }

            let mut acquired_block = None;
            for ready_task in &ready_tasks {
                if let Some(block) = scheduler.acquire_block(&ready_task.task_id) {
                    acquired_block = Some((ready_task.task_id.clone(), block));
                    break;
                }
            }

            let Some((task_id, mut block)) = acquired_block else {
                break;
            };

            if !started_tasks.contains(&task_id) {
                debug!(task_id = %task_id, "task started");
                started_tasks.insert(task_id.clone());
            }

            // Call the process function directly. Reset status to Processing
            // on every attempt — re-queued blocks come back with their last
            // attempt's terminal status (e.g. Failed), which would otherwise
            // poison the post-call promotion check below and prevent a
            // successful retry from being recorded.
            block.status = BlockStatus::Processing;
            let task = scheduler.task_map.get(&task_id).unwrap().clone();
            if let Some(ref process_fn) = task.process_function {
                // Serial mode runs the function in this thread, so it
                // measures the block here — same numbers, same payload
                // path (`Block::stats`) as every other mode.
                let profiler = task.resource_tracking.then(BlockProfiler::start);
                let outcome = process_fn.process(&mut block);
                if let Some(p) = profiler {
                    if block.stats.is_none() {
                        block.stats = Some(p.finish());
                    }
                }
                match outcome {
                    Ok(()) => {
                        if block.status != BlockStatus::Failed {
                            block.status = BlockStatus::Success;
                        }
                    }
                    Err(e) => {
                        // Serial mode is the debugging path — surface
                        // the user's exception immediately rather than
                        // catching it, marking the block `Failed`, and
                        // running through `max_retries` more attempts.
                        // The original Python exception is preserved
                        // via the per-thread stash in
                        // `daisy-py/src/py_callbacks.rs`; the caller
                        // (`_run_serial`) re-raises it.
                        debug!(block_id = %block.block_id, error = %e, "serial mode: aborting on block error");
                        return Err(e);
                    }
                }
            } else {
                // A task without a block function (a 0-argument worker
                // function, or no function at all) cannot run here: serial
                // mode has no server for a worker loop to connect to. Marking
                // blocks done without running anything would report success
                // for work that never happened.
                return Err(DaisyError::InvalidConfig(format!(
                    "task {task_id:?} has no 1-argument process_function; serial mode \
                     (multiprocessing=False) runs block functions in-process and \
                     cannot run a worker function that expects to connect to a server"
                )));
            }

            scheduler.release_block(block);

            if scheduler.task_states[&task_id].is_done() {
                debug!(task_id = %task_id, "task done");
                finished_tasks.insert(task_id.clone());
                started_tasks.remove(&task_id);
            }
        }

        // Serial mode has no workers, so the report's worker section is
        // empty; blocks are measured the same way as everywhere else.
        let summaries = scheduler.tracking_summaries();
        let counters: HashMap<String, TaskCounters> = scheduler
            .task_states
            .iter()
            .map(|(k, v)| (k.clone(), v.counters()))
            .collect();
        let reports = build_reports(&counters, &summaries, &RunTimeline::new());
        scheduler.append_run_records(&reports);
        Ok((counters, reports))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::Block;
    use crate::roi::Roi;

    /// Block function that does nothing: serial mode needs one, a task
    /// without a block function is refused.
    struct Noop;

    impl crate::task::ProcessBlock for Noop {
        fn process(&self, _block: &mut Block) -> Result<(), DaisyError> {
            Ok(())
        }
    }

    #[test]
    fn test_serial_refuses_task_without_block_function() {
        let task = Arc::new(
            Task::builder("test")
                .total_roi(Roi::from_slices(&[0], &[40]))
                .read_roi(Roi::from_slices(&[0], &[10]))
                .write_roi(Roi::from_slices(&[0], &[10]))
                .read_write_conflict(false)
                .build(),
        );
        let pipeline = crate::pipeline::Pipeline::from_task(task);
        let err = SerialRunner::run(&pipeline, true).unwrap_err();
        assert!(matches!(err, DaisyError::InvalidConfig(_)), "{err:?}");
    }

    #[test]
    fn test_serial_no_conflict() {
        let task = Arc::new(
            Task::builder("test")
                .total_roi(Roi::from_slices(&[0], &[40]))
                .read_roi(Roi::from_slices(&[0], &[10]))
                .write_roi(Roi::from_slices(&[0], &[10]))
                .read_write_conflict(false)
                .process_function(Noop)
                .build(),
        );

        let pipeline = crate::pipeline::Pipeline::from_task(task.clone());
        let (states, reports) = SerialRunner::run(&pipeline, true).unwrap();
        // No tracking_path configured: the report still exists (every task
        // gets one), with the scheduler's count, no resource figures and
        // no workers.
        let report = &reports["test"];
        assert!(!report.tracked);
        assert!(!report.blocks.has_stats);
        assert_eq!(report.blocks.count, 4);
        assert_eq!(report.workers.count, 0);
        let state = &states["test"];
        assert!(state.balanced());
        assert_eq!(state.total_block_count, 4);
        assert_eq!(state.completed_count, 4);
    }
}
