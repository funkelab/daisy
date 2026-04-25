use crate::block::Block;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

/// Messages exchanged between server and workers over TCP.
#[derive(Clone, Debug, Serialize, Deserialize, Encode, Decode)]
pub enum Message {
    /// Worker requests a block to process.
    AcquireBlock { task_id: String },

    /// Server sends a block to a worker.
    SendBlock { block: Block },

    /// Worker returns a processed block.
    ReleaseBlock { block: Block },

    /// Worker reports a block failure with an error description.
    BlockFailed { block: Block, error: String },

    /// Server tells worker there is no more work.
    RequestShutdown,

    /// Worker notifies it is disconnecting.
    Disconnect,
}
