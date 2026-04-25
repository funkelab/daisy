use crate::block::{Block, BlockStatus};
use crate::framing::{read_message, write_message};
use crate::protocol::Message;
use std::io;
use tokio::net::TcpStream;
use tracing::debug;

/// Worker-side client that communicates with the server to acquire and
/// release blocks over TCP.
pub struct Client {
    reader: tokio::net::tcp::OwnedReadHalf,
    writer: tokio::net::tcp::OwnedWriteHalf,
    task_id: String,
    connected: bool,
}

impl Client {
    /// Connect to the server at the given address.
    pub async fn connect(host: &str, port: u16, task_id: &str) -> io::Result<Self> {
        let addr = format!("{host}:{port}");
        debug!(%addr, task_id, "connecting to server");
        let stream = TcpStream::connect(&addr).await?;
        let (reader, writer) = stream.into_split();
        Ok(Self {
            reader,
            writer,
            task_id: task_id.to_string(),
            connected: true,
        })
    }

    /// Request the next block to process. Returns `None` when the server
    /// signals shutdown (no more work).
    pub async fn acquire_block(&mut self) -> io::Result<Option<Block>> {
        if !self.connected {
            return Ok(None);
        }

        write_message(
            &mut self.writer,
            &Message::AcquireBlock {
                task_id: self.task_id.clone(),
            },
        )
        .await?;

        match read_message(&mut self.reader).await? {
            Some(Message::SendBlock { block }) => {
                debug!(block_id = %block.block_id, "received block");
                Ok(Some(block))
            }
            Some(Message::RequestShutdown) => {
                debug!("server requested shutdown, no more blocks");
                self.connected = false;
                Ok(None)
            }
            Some(other) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected message: {other:?}"),
            )),
            None => {
                debug!("server closed connection");
                self.connected = false;
                Ok(None)
            }
        }
    }

    /// Return a processed block to the server.
    pub async fn release_block(&mut self, block: Block) -> io::Result<()> {
        if !self.connected {
            return Ok(());
        }
        write_message(&mut self.writer, &Message::ReleaseBlock { block }).await
    }

    /// Report a block failure to the server.
    pub async fn report_failure(&mut self, block: Block, error: String) -> io::Result<()> {
        if !self.connected {
            return Ok(());
        }
        let mut failed_block = block;
        failed_block.status = BlockStatus::Failed;
        write_message(
            &mut self.writer,
            &Message::BlockFailed {
                block: failed_block,
                error,
            },
        )
        .await
    }

    /// Notify the server that this client is disconnecting.
    pub async fn disconnect(&mut self) -> io::Result<()> {
        if self.connected {
            let _ = write_message(&mut self.writer, &Message::Disconnect).await;
            self.connected = false;
        }
        Ok(())
    }

    pub fn is_connected(&self) -> bool {
        self.connected
    }
}
