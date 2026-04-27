use crate::block::BlockStatus;
use crate::error::GerberaError;
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
    pub fn run(tasks: &[Arc<Task>]) -> Result<HashMap<String, TaskCounters>, GerberaError> {
        let mut scheduler = Scheduler::new(tasks, true);
        scheduler.init_done_markers()?;

        let mut started_tasks = HashSet::new();
        let mut finished_tasks: HashSet<String> = HashSet::new();
        let all_tasks: HashSet<String> = tasks.iter().map(|t| t.task_id.clone()).collect();

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

            // Call the process function directly.
            let task = scheduler.task_map.get(&task_id).unwrap().clone();
            if let Some(ref process_fn) = task.process_function {
                match process_fn.process(&mut block) {
                    Ok(()) => {
                        if block.status != BlockStatus::Failed {
                            block.status = BlockStatus::Success;
                        }
                    }
                    Err(e) => {
                        debug!(block_id = %block.block_id, error = %e, "block processing failed");
                        block.status = BlockStatus::Failed;
                    }
                }
            } else {
                // No process function — assume success (for testing).
                block.status = BlockStatus::Success;
            }

            scheduler.release_block(block);

            if scheduler.task_states[&task_id].is_done() {
                debug!(task_id = %task_id, "task done");
                finished_tasks.insert(task_id.clone());
                started_tasks.remove(&task_id);
            }
        }

        Ok(scheduler
            .task_states
            .into_iter()
            .map(|(k, v)| (k, v.counters()))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::roi::Roi;
    #[test]
    fn test_serial_no_conflict() {
        let task = Arc::new(
            Task::builder("test")
                .total_roi(Roi::from_slices(&[0], &[40]))
                .read_roi(Roi::from_slices(&[0], &[10]))
                .write_roi(Roi::from_slices(&[0], &[10]))
                .read_write_conflict(false)
                .build(),
        );

        let states = SerialRunner::run(&[task.clone()]).unwrap();
        let state = &states["test"];
        assert!(state.balanced());
        assert_eq!(state.total_block_count, 4);
        assert_eq!(state.completed_count, 4);
    }
}
