use crate::block::{Block, BlockStatus};
use crate::protocol::{read_message, write_message, Message};
use std::io;
use std::sync::mpsc::{RecvTimeoutError, Sender};
use std::time::Duration;
use tokio::net::TcpStream;
use tracing::debug;

/// Exit code a worker process uses when the block watchdog kills it
/// because a block exceeded `Task::timeout`. The parent spawn function
/// (`daisy._worker_processes`) maps this code to a "true preemption"
/// error message; keep the two in sync — the Python side imports this
/// value through the `daisy._daisy` bindings.
pub const EXIT_BLOCK_TIMEOUT: i32 = 87;

/// Worker-side client that communicates with the server to acquire and
/// release blocks over TCP.
///
/// Every acquired block is watched: when the server sends a per-block
/// timeout along with the block (`Task::timeout`), the client arms a
/// watchdog that kills this worker process if the block is still
/// unreleased after that long. This is what makes `timeout=` true
/// preemption for *every* worker loop — daisy's own subprocess shim and
/// hand-written cluster workers alike — including blocks stuck inside C
/// code. The server's reclaim timer runs the same duration from send
/// time, so a reclaimed block's worker dies within moments of the
/// retry being issued.
pub struct Client {
    reader: tokio::net::tcp::OwnedReadHalf,
    writer: tokio::net::tcp::OwnedWriteHalf,
    task_id: String,
    /// The server-assigned worker id from this worker's `DAISY_CONTEXT`,
    /// echoed back on every acquire so the server can tie this TCP peer
    /// to the spawn call it is blocking on. `None` for clients outside
    /// daisy's worker management.
    worker_id: Option<u64>,
    connected: bool,
    /// Dropping the sender disarms the watchdog for the current block.
    watchdog: Option<Sender<()>>,
}

impl Client {
    /// Connect to the server at the given address. `worker_id` is the
    /// server-assigned id from the worker's context; pass `None` when
    /// connecting outside daisy's worker management.
    pub async fn connect(
        host: &str,
        port: u16,
        task_id: &str,
        worker_id: Option<u64>,
    ) -> io::Result<Self> {
        let addr = format!("{host}:{port}");
        debug!(%addr, task_id, "connecting to server");
        let stream = TcpStream::connect(&addr).await?;
        // Disable Nagle: per-block acquire/release are tiny request/response
        // messages; Nagle + delayed-ACK adds ~40ms stalls each way.
        stream.set_nodelay(true)?;
        // Keepalive: a worker blocked in acquire_block must learn that the
        // server's *host* vanished (cloud scale-down), which no RST will
        // ever announce. Degrade with a warning rather than refuse to run —
        // a worker without keepalive is the old behaviour, not a hazard.
        if let Err(e) = crate::keepalive::apply(&stream) {
            tracing::warn!(
                error = %e,
                "could not enable TCP keepalive; a vanished server host \
                 will not be detected while this worker waits for a block"
            );
        }
        let (reader, writer) = stream.into_split();
        Ok(Self {
            reader,
            writer,
            task_id: task_id.to_string(),
            worker_id,
            connected: true,
            watchdog: None,
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
                worker_id: self.worker_id,
            },
        )
        .await?;

        match read_message(&mut self.reader).await? {
            Some(Message::SendBlock {
                block,
                timeout_secs,
            }) => {
                debug!(block_id = %block.block_id, "received block");
                if let Some(t) = timeout_secs {
                    self.arm_watchdog(&block, t);
                }
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
        self.disarm_watchdog();
        if !self.connected {
            return Ok(());
        }
        write_message(&mut self.writer, &Message::ReleaseBlock { block }).await
    }

    /// Report a block failure to the server.
    pub async fn report_failure(&mut self, block: Block, error: String) -> io::Result<()> {
        self.disarm_watchdog();
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
        self.disarm_watchdog();
        if self.connected {
            let _ = write_message(&mut self.writer, &Message::Disconnect).await;
            self.connected = false;
        }
        Ok(())
    }

    pub fn is_connected(&self) -> bool {
        self.connected
    }

    /// Arm the per-block watchdog: a thread that kills this worker
    /// process if the block hasn't been returned within `timeout_secs`.
    ///
    /// The kill is a self-`_exit`, not a signal from outside, because
    /// this is the only agent guaranteed to be co-located with the stuck
    /// code and to hold kill rights over it — the server may be on
    /// another machine and only knows this worker as a TCP peer. `_exit`
    /// (not `exit`) so no atexit handler can deadlock on a lock or a GIL
    /// the wedged main thread still holds.
    fn arm_watchdog(&mut self, block: &Block, timeout_secs: f64) {
        // Re-arming without a release in between (a custom loop that
        // acquires twice) just replaces the old watchdog.
        self.disarm_watchdog();
        if !(timeout_secs > 0.0) {
            return;
        }
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let block_id = block.block_id.to_string();
        let worker = self.worker_id;
        std::thread::spawn(move || {
            match rx.recv_timeout(Duration::from_secs_f64(timeout_secs)) {
                // Disarmed: either an explicit send or the sender dropped.
                Ok(()) | Err(RecvTimeoutError::Disconnected) => {}
                Err(RecvTimeoutError::Timeout) => {
                    let who = worker
                        .map(|w| format!("daisy worker {w}"))
                        .unwrap_or_else(|| "daisy worker".to_string());
                    eprintln!(
                        "{who}: block {block_id} still running after \
                         timeout={timeout_secs}s; killing worker process"
                    );
                    #[cfg(unix)]
                    unsafe {
                        libc::_exit(EXIT_BLOCK_TIMEOUT)
                    };
                    #[cfg(not(unix))]
                    std::process::exit(EXIT_BLOCK_TIMEOUT);
                }
            }
        });
        self.watchdog = Some(tx);
    }

    fn disarm_watchdog(&mut self) {
        // Dropping the sender wakes the watchdog thread with
        // `Disconnected`, which it treats as "block returned in time".
        self.watchdog.take();
    }
}
