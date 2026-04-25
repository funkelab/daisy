use crate::block::Block;
use std::collections::{HashMap, VecDeque};

/// Tracks blocks that are ready, currently processing, and retry counts for a
/// single task.
pub struct ProcessingQueue {
    pub ready_queue: VecDeque<Block>,
    pub processing_blocks: std::collections::HashSet<crate::block::BlockId>,
    pub block_retries: HashMap<crate::block::BlockId, u32>,
    ready_roots: i64,
    root_generator: Option<Box<dyn Iterator<Item = Block> + Send>>,
}

impl ProcessingQueue {
    pub fn new(num_roots: i64, root_generator: Option<Box<dyn Iterator<Item = Block> + Send>>) -> Self {
        Self {
            ready_queue: VecDeque::new(),
            processing_blocks: std::collections::HashSet::new(),
            block_retries: HashMap::new(),
            ready_roots: num_roots,
            root_generator,
        }
    }

    pub fn num_ready(&self) -> i64 {
        self.ready_roots + self.ready_queue.len() as i64
    }

    /// Get the next ready block. Drains from the root generator first, then
    /// falls back to the ready queue.
    pub fn get_next(&mut self) -> Option<Block> {
        if self.num_ready() <= 0 {
            return None;
        }

        if self.ready_roots > 0 {
            self.ready_roots -= 1;
            self.root_generator
                .as_mut()
                .and_then(|iter| iter.next())
        } else {
            self.ready_queue.pop_front()
        }
    }
}
