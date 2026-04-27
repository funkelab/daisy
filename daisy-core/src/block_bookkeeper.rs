use crate::block::{Block, BlockId};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tracing::debug;

/// Tracks in-flight blocks to detect lost blocks (stream closed or timeout).
struct BlockLog {
    block: Block,
    client_addr: SocketAddr,
    time_sent: Instant,
    /// Per-block deadline. Set from `Task::timeout` when the block is
    /// dispatched. `None` means no timeout — the block can sit in
    /// processing indefinitely as long as the client stays connected.
    timeout: Option<Duration>,
}

pub struct BlockBookkeeper {
    sent_blocks: HashMap<BlockId, BlockLog>,
    /// Track which client addresses have disconnected so we can detect lost blocks.
    closed_clients: std::collections::HashSet<SocketAddr>,
}

impl BlockBookkeeper {
    pub fn new() -> Self {
        Self {
            sent_blocks: HashMap::new(),
            closed_clients: std::collections::HashSet::new(),
        }
    }

    /// Record that a block was sent to a client. `timeout` is the
    /// per-block processing deadline — when set, `get_lost_blocks`
    /// will reclaim this block once `now - time_sent > timeout`.
    pub fn notify_block_sent(
        &mut self,
        block: Block,
        client_addr: SocketAddr,
        timeout: Option<Duration>,
    ) {
        debug!(block_id = %block.block_id, %client_addr, "block sent to client");
        self.sent_blocks.insert(
            block.block_id.clone(),
            BlockLog {
                block,
                client_addr,
                time_sent: Instant::now(),
                timeout,
            },
        );
    }

    /// Record that a block was returned by a client. Returns the time
    /// the block spent in the worker (`Instant::now() - time_sent`)
    /// when the return is valid, otherwise `None`.
    pub fn notify_block_returned(
        &mut self,
        block: &Block,
        client_addr: SocketAddr,
    ) -> Option<std::time::Duration> {
        if let Some(log) = self.sent_blocks.get(&block.block_id) {
            if log.client_addr == client_addr {
                let elapsed = log.time_sent.elapsed();
                self.sent_blocks.remove(&block.block_id);
                return Some(elapsed);
            }
            debug!(
                block_id = %block.block_id,
                expected = %log.client_addr,
                actual = %client_addr,
                "block returned by wrong client"
            );
        }
        None
    }

    /// Check whether this return is valid (block was sent to this client and
    /// hasn't already been returned).
    pub fn is_valid_return(&self, block: &Block, client_addr: SocketAddr) -> bool {
        if let Some(log) = self.sent_blocks.get(&block.block_id) {
            log.client_addr == client_addr
        } else {
            false
        }
    }

    /// Mark a client address as disconnected.
    pub fn notify_client_disconnected(&mut self, client_addr: SocketAddr) {
        self.closed_clients.insert(client_addr);
    }

    /// Return blocks that are lost: the client disconnected, OR the
    /// per-block timeout (set on `notify_block_sent`) has elapsed.
    /// Removes them from the sent list. The runner releases each
    /// returned block as `Failed`, which goes through the normal
    /// retry / orphan path.
    pub fn get_lost_blocks(&mut self) -> Vec<Block> {
        let now = Instant::now();
        let mut lost_ids = Vec::new();

        for (block_id, log) in &self.sent_blocks {
            if self.closed_clients.contains(&log.client_addr) {
                lost_ids.push(block_id.clone());
                continue;
            }
            if let Some(timeout) = log.timeout {
                if now.duration_since(log.time_sent) > timeout {
                    debug!(
                        block_id = %block_id,
                        client_addr = %log.client_addr,
                        elapsed_ms = now.duration_since(log.time_sent).as_millis() as u64,
                        timeout_ms = timeout.as_millis() as u64,
                        "block timed out — reclaiming",
                    );
                    lost_ids.push(block_id.clone());
                }
            }
        }

        let mut lost_blocks = Vec::new();
        for id in lost_ids {
            if let Some(log) = self.sent_blocks.remove(&id) {
                lost_blocks.push(log.block);
            }
        }
        lost_blocks
    }
}
