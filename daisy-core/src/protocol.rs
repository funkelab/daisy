use crate::block::Block;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

/// Messages exchanged between server and workers over TCP.
#[derive(Clone, Debug, Serialize, Deserialize, Encode, Decode)]
pub enum Message {
    /// Worker requests a block to process. `worker_id` is the id the
    /// server assigned when it ran the worker's spawn function (carried
    /// to the worker in its `DAISY_CONTEXT`); it lets the server tie the
    /// TCP peer back to the spawn call it is blocking on — to retire the
    /// worker's slot when a block times out, and to detect spawn
    /// functions that returned before their worker's lifetime ended.
    /// `None` for clients outside daisy's worker management.
    AcquireBlock {
        task_id: String,
        worker_id: Option<u64>,
    },

    /// Server sends a block to a worker. `timeout_secs` is the task's
    /// per-block deadline: the client arms a watchdog that kills the
    /// worker process if the block is still running after this long,
    /// mirroring the reclaim timer the server starts at send time.
    SendBlock {
        block: Block,
        timeout_secs: Option<f64>,
    },

    /// Worker returns a processed block.
    ReleaseBlock { block: Block },

    /// Worker reports a block failure with an error description.
    BlockFailed { block: Block, error: String },

    /// Server tells worker there is no more work.
    RequestShutdown,

    /// Worker notifies it is disconnecting.
    Disconnect,
}

const MAX_MESSAGE_SIZE: u32 = 64 * 1024 * 1024; // 64 MiB safety limit
const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

/// Wire-format version, sent as the first byte of every frame.
///
/// The payload is positional bincode, which is not self-describing: adding
/// a field to any message (or to `Block`) silently changes the byte layout.
/// Without a version marker a mismatched peer fails deep inside the
/// decoder — `UnexpectedEnd`, or an `Option` tag read out of an unrelated
/// string length — which tells an operator nothing about the real problem.
/// The realistic way to get there is external cluster workers loading daisy
/// from a different environment than the driver.
///
/// Bump this whenever the encoding of any `Message` variant changes.
///
/// Version history:
/// - 1: initial v2 protocol.
/// - 2: `AcquireBlock` carries `worker_id`, `SendBlock` carries
///   `timeout_secs` (client-side watchdog + server-side worker
///   retirement on timeout).
pub const PROTOCOL_VERSION: u8 = 2;

/// Write a length-prefixed, bincode-encoded message to a TCP stream.
pub async fn write_message(writer: &mut OwnedWriteHalf, msg: &Message) -> io::Result<()> {
    let encoded = bincode::encode_to_vec(msg, BINCODE_CONFIG)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    // Frame: u32 big-endian payload length, one version byte, payload.
    // The length counts the version byte so old readers see a coherent
    // frame boundary even though they cannot interpret the contents.
    let len = (encoded.len() + 1) as u32;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&[PROTOCOL_VERSION]).await?;
    writer.write_all(&encoded).await?;
    writer.flush().await?;
    Ok(())
}

/// Read a length-prefixed, bincode-encoded message from a TCP stream.
/// Returns `None` on clean EOF.
pub async fn read_message(reader: &mut OwnedReadHalf) -> io::Result<Option<Message>> {
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_be_bytes(len_buf);
    if len > MAX_MESSAGE_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("message too large: {len} bytes"),
        ));
    }
    if len == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "empty message frame (missing protocol version byte)",
        ));
    }
    let mut buf = vec![0u8; len as usize];
    reader.read_exact(&mut buf).await?;
    let version = buf[0];
    if version != PROTOCOL_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "daisy protocol version mismatch: peer speaks {version}, \
                 this build speaks {PROTOCOL_VERSION}. Rebuild your workers \
                 against the same daisy version as the driver."
            ),
        ));
    }
    let (msg, _): (Message, usize) = bincode::decode_from_slice(&buf[1..], BINCODE_CONFIG)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(Some(msg))
}
