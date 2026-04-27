use crate::block::Block;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

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

const MAX_MESSAGE_SIZE: u32 = 64 * 1024 * 1024; // 64 MiB safety limit
const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

/// Write a length-prefixed, bincode-encoded message to a TCP stream.
pub async fn write_message(writer: &mut OwnedWriteHalf, msg: &Message) -> io::Result<()> {
    let encoded = bincode::encode_to_vec(msg, BINCODE_CONFIG)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let len = encoded.len() as u32;
    writer.write_all(&len.to_be_bytes()).await?;
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
    let mut buf = vec![0u8; len as usize];
    reader.read_exact(&mut buf).await?;
    let (msg, _): (Message, usize) = bincode::decode_from_slice(&buf, BINCODE_CONFIG)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(Some(msg))
}
