use crate::block::BlockStatus;
use crate::block_bookkeeper::BlockBookkeeper;
use crate::client::Client;
use crate::framing::{read_message, write_message};
use crate::protocol::Message;
use crate::scheduler::Scheduler;
use crate::task::Task;
use crate::task_state::TaskState;
use crate::worker_pool::WorkerPool;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread::JoinHandle;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

struct ClientMessage {
    message: Message,
    addr: SocketAddr,
    reply_tx: mpsc::Sender<Message>,
}

pub struct Server {
    host: String,
    port: u16,
}

/// Info needed to spawn/respawn a worker thread.
struct WorkerSpec {
    task_id: String,
    task: Arc<Task>,
    worker_id: u64,
    host: String,
    port: u16,
}

enum WorkerThread {
    /// Running. The bool result: true = clean exit, false = should respawn.
    Running(JoinHandle<bool>),
    Finished,
}

impl Server {
    pub async fn bind(host: &str) -> std::io::Result<(Self, TcpListener)> {
        let listener = TcpListener::bind((host, 0u16)).await?;
        let local_addr = listener.local_addr()?;
        info!(%local_addr, "server listening");
        Ok((
            Self {
                host: local_addr.ip().to_string(),
                port: local_addr.port(),
            },
            listener,
        ))
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// Run blockwise with Rust-managed worker threads. Workers call back
    /// into Python via the trait implementations on the task's
    /// process_function / spawn_function.
    pub async fn run_blockwise(
        &self,
        listener: TcpListener,
        tasks: &[Arc<Task>],
        worker_pools: &mut HashMap<String, WorkerPool>,
    ) -> std::io::Result<HashMap<String, TaskState>> {
        let mut scheduler = Scheduler::new(tasks, true);
        let mut bookkeeper = BlockBookkeeper::new(None);

        let (msg_tx, mut msg_rx) = mpsc::channel::<ClientMessage>(256);

        // TCP accept loop.
        let accept_tx = msg_tx.clone();
        let accept_handle = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        debug!(%addr, "new client connection");
                        let (mut reader, writer) = stream.into_split();
                        let (reply_tx, mut reply_rx) = mpsc::channel::<Message>(32);

                        tokio::spawn(async move {
                            let mut writer: OwnedWriteHalf = writer;
                            while let Some(msg) = reply_rx.recv().await {
                                if let Err(e) = write_message(&mut writer, &msg).await {
                                    debug!(error = %e, "write reply failed");
                                    break;
                                }
                            }
                        });

                        let reader_tx = accept_tx.clone();
                        let reader_reply_tx = reply_tx.clone();
                        tokio::spawn(async move {
                            loop {
                                match read_message(&mut reader).await {
                                    Ok(Some(msg)) => {
                                        let cm = ClientMessage {
                                            message: msg,
                                            addr,
                                            reply_tx: reader_reply_tx.clone(),
                                        };
                                        if reader_tx.send(cm).await.is_err() {
                                            break;
                                        }
                                    }
                                    Ok(None) | Err(_) => {
                                        let _ = reader_tx
                                            .send(ClientMessage {
                                                message: Message::Disconnect,
                                                addr,
                                                reply_tx: reader_reply_tx.clone(),
                                            })
                                            .await;
                                        break;
                                    }
                                }
                            }
                        });
                    }
                    Err(e) => error!(error = %e, "accept failed"),
                }
            }
        });

        let mut pending: VecDeque<ClientMessage> = VecDeque::new();

        // Spawn worker threads for each task.
        let mut workers: Vec<(WorkerSpec, WorkerThread)> = Vec::new();
        let mut next_worker_id: u64 = 0;

        for task in tasks {
            if task.process_function.is_none() && task.spawn_function.is_none() {
                continue;
            }
            for _ in 0..task.num_workers {
                let spec = WorkerSpec {
                    task_id: task.task_id.clone(),
                    task: task.clone(),
                    worker_id: next_worker_id,
                    host: self.host.clone(),
                    port: self.port,
                };
                let handle = Self::spawn_worker(&spec);
                workers.push((spec, WorkerThread::Running(handle)));
                next_worker_id += 1;
            }
        }

        self.recruit_workers(&scheduler, worker_pools)?;

        let mut all_done = false;
        let mut health_interval = tokio::time::interval(std::time::Duration::from_millis(500));
        let mut done_check_interval = tokio::time::interval(std::time::Duration::from_secs(1));

        while !all_done {
            tokio::select! {
                Some(cm) = msg_rx.recv() => {
                    self.handle_message(
                        cm, &mut scheduler, &mut bookkeeper,
                        &mut pending, worker_pools,
                    )?;
                }

                _ = health_interval.tick() => {
                    let lost = bookkeeper.get_lost_blocks();
                    for mut block in lost {
                        warn!(block_id = %block.block_id, "block lost");
                        block.status = BlockStatus::Failed;
                        scheduler.release_block(block);
                    }

                    // Check Rust worker pool health (for external process workers).
                    for (task_id, pool) in worker_pools.iter_mut() {
                        if let Err(e) = pool.check_health(&self.host, self.port) {
                            error!(task_id, error = %e, "worker health check failed");
                        }
                    }

                    // Check thread-based worker health.
                    Self::check_thread_health(&mut workers, &mut next_worker_id);

                    self.recruit_workers(&scheduler, worker_pools)?;

                    if !pending.is_empty() {
                        self.retry_pending(
                            &mut scheduler, &mut bookkeeper,
                            &mut pending, worker_pools,
                        )?;
                    }
                }

                _ = done_check_interval.tick() => {}
            }

            all_done = self.check_all_done(&scheduler);
        }

        info!("all tasks completed");

        // Shutdown.
        for cm in pending.drain(..) {
            let _ = cm.reply_tx.try_send(Message::RequestShutdown);
        }
        msg_rx.close();
        while let Ok(cm) = msg_rx.try_recv() {
            if let Message::AcquireBlock { .. } = &cm.message {
                let _ = cm.reply_tx.try_send(Message::RequestShutdown);
            }
        }
        accept_handle.abort();
        tokio::task::yield_now().await;

        for pool in worker_pools.values_mut() {
            pool.stop_all();
        }

        // Wait for worker threads to exit (they'll see TCP close / RequestShutdown).
        for (_, wt) in &mut workers {
            if let WorkerThread::Running(handle) = std::mem::replace(wt, WorkerThread::Finished) {
                let _ = handle.join();
            }
        }

        Ok(scheduler.task_states)
    }

    /// Spawn a worker thread for a task. For 1-arg process functions, the
    /// thread creates a TCP client, loops acquiring blocks, and calls the
    /// function. For 0-arg spawn functions, the thread calls the function
    /// directly (it manages its own Client/subprocess).
    fn spawn_worker(spec: &WorkerSpec) -> JoinHandle<bool> {
        let task = spec.task.clone();
        let host = spec.host.clone();
        let port = spec.port;
        let task_id = spec.task_id.clone();
        let worker_id = spec.worker_id;

        std::thread::spawn(move || -> bool {
            if let Some(ref process_fn) = task.process_function {
                // 1-arg block processor: connect via TCP, loop.
                let rt = match tokio::runtime::Runtime::new() {
                    Ok(rt) => rt,
                    Err(e) => {
                        error!(worker_id, error = %e, "failed to create runtime");
                        return false;
                    }
                };
                let mut client = match rt.block_on(Client::connect(&host, port, &task_id)) {
                    Ok(c) => c,
                    Err(e) => {
                        error!(worker_id, error = %e, "failed to connect");
                        return false;
                    }
                };
                loop {
                    match rt.block_on(client.acquire_block()) {
                        Ok(Some(mut block)) => {
                            block.status = crate::block::BlockStatus::Processing;
                            match process_fn.process(&mut block) {
                                Ok(()) => {
                                    if block.status == crate::block::BlockStatus::Processing {
                                        block.status = crate::block::BlockStatus::Success;
                                    }
                                }
                                Err(e) => {
                                    warn!(worker_id, error = %e, "block processing failed");
                                    block.status = crate::block::BlockStatus::Failed;
                                }
                            }
                            if let Err(e) = rt.block_on(client.release_block(block)) {
                                error!(worker_id, error = %e, "failed to release block");
                                return false;
                            }
                        }
                        Ok(None) => {
                            debug!(worker_id, "no more blocks, exiting");
                            break;
                        }
                        Err(e) => {
                            debug!(worker_id, error = %e, "acquire failed");
                            return false;
                        }
                    }
                }
                let _ = rt.block_on(client.disconnect());
                true // clean exit
            } else if let Some(ref spawn_fn) = task.spawn_function {
                let env_ctx = format!(
                    "hostname={}:port={}:task_id={}:worker_id={}",
                    host, port, task_id, worker_id
                );
                match spawn_fn.spawn(&env_ctx) {
                    Ok(()) => true,
                    Err(e) => {
                        warn!(worker_id, error = %e, "spawn function failed");
                        false // should respawn
                    }
                }
            } else {
                true
            }
        })
    }

    /// Check thread-based workers for crashes and respawn.
    fn check_thread_health(
        workers: &mut Vec<(WorkerSpec, WorkerThread)>,
        next_id: &mut u64,
    ) {
        let mut to_respawn: Vec<WorkerSpec> = Vec::new();

        for (spec, wt) in workers.iter_mut() {
            if let &mut WorkerThread::Running(ref handle) = wt {
                if handle.is_finished() {
                    if let WorkerThread::Running(handle) =
                        std::mem::replace(wt, WorkerThread::Finished)
                    {
                        let should_respawn = match handle.join() {
                            Ok(true) => false,   // clean exit
                            Ok(false) => true,   // function failed
                            Err(_) => true,      // thread panicked
                        };
                        if should_respawn {
                            warn!(worker_id = spec.worker_id, "worker failed, respawning");
                            to_respawn.push(WorkerSpec {
                                task_id: spec.task_id.clone(),
                                task: spec.task.clone(),
                                worker_id: *next_id,
                                host: spec.host.clone(),
                                port: spec.port,
                            });
                            *next_id += 1;
                        }
                    }
                }
            }
        }

        for spec in to_respawn {
            let handle = Self::spawn_worker(&spec);
            workers.push((spec, WorkerThread::Running(handle)));
        }
    }

    fn retry_pending(
        &self,
        scheduler: &mut Scheduler,
        bookkeeper: &mut BlockBookkeeper,
        pending: &mut VecDeque<ClientMessage>,
        worker_pools: &mut HashMap<String, WorkerPool>,
    ) -> std::io::Result<()> {
        let count = pending.len();
        for _ in 0..count {
            if let Some(cm) = pending.pop_front() {
                self.handle_message(cm, scheduler, bookkeeper, pending, worker_pools)?;
            }
        }
        Ok(())
    }

    fn handle_message(
        &self,
        cm: ClientMessage,
        scheduler: &mut Scheduler,
        bookkeeper: &mut BlockBookkeeper,
        pending: &mut VecDeque<ClientMessage>,
        worker_pools: &mut HashMap<String, WorkerPool>,
    ) -> std::io::Result<()> {
        match cm.message {
            Message::AcquireBlock { .. } => {
                self.handle_acquire(cm, scheduler, bookkeeper, pending, worker_pools)?;
            }
            Message::ReleaseBlock { block } => {
                if bookkeeper.is_valid_return(&block, cm.addr) {
                    bookkeeper.notify_block_returned(&block, cm.addr);
                    scheduler.release_block(block);
                    self.recruit_workers(scheduler, worker_pools)?;
                    if !pending.is_empty() {
                        self.retry_pending(scheduler, bookkeeper, pending, worker_pools)?;
                    }
                } else {
                    debug!(block_id = %block.block_id, "invalid block return");
                }
            }
            Message::BlockFailed { mut block, error } => {
                warn!(block_id = %block.block_id, %error, "block failed");
                if bookkeeper.is_valid_return(&block, cm.addr) {
                    bookkeeper.notify_block_returned(&block, cm.addr);
                    block.status = BlockStatus::Failed;
                    scheduler.release_block(block);
                    self.recruit_workers(scheduler, worker_pools)?;
                    if !pending.is_empty() {
                        self.retry_pending(scheduler, bookkeeper, pending, worker_pools)?;
                    }
                }
            }
            Message::Disconnect => {
                debug!(addr = %cm.addr, "client disconnected");
                bookkeeper.notify_client_disconnected(cm.addr);
            }
            _ => {
                warn!(msg = ?cm.message, "unexpected message");
            }
        }
        Ok(())
    }

    fn handle_acquire(
        &self,
        cm: ClientMessage,
        scheduler: &mut Scheduler,
        bookkeeper: &mut BlockBookkeeper,
        pending: &mut VecDeque<ClientMessage>,
        worker_pools: &mut HashMap<String, WorkerPool>,
    ) -> std::io::Result<()> {
        let task_id = match &cm.message {
            Message::AcquireBlock { task_id } => task_id.clone(),
            _ => unreachable!(),
        };

        match scheduler.acquire_block(&task_id) {
            Some(block) => {
                debug!(block_id = %block.block_id, "sending block");
                bookkeeper.notify_block_sent(block.clone(), cm.addr);
                let _ = cm.reply_tx.try_send(Message::SendBlock { block });
            }
            None => {
                let state = &scheduler.task_states[&task_id];
                if state.pending_count() <= 0 && state.processing_count <= 0 {
                    debug!(task_id = %task_id, "no more blocks");
                    let _ = cm.reply_tx.try_send(Message::RequestShutdown);
                    self.recruit_workers(scheduler, worker_pools)?;
                } else {
                    debug!(task_id = %task_id, "parking request");
                    pending.push_back(cm);
                }
            }
        }
        Ok(())
    }

    fn recruit_workers(
        &self,
        scheduler: &Scheduler,
        worker_pools: &mut HashMap<String, WorkerPool>,
    ) -> std::io::Result<()> {
        let ready_tasks = scheduler.get_ready_tasks();
        for task in &ready_tasks {
            if let Some(pool) = worker_pools.get_mut(&task.task_id) {
                pool.set_num_workers(task.num_workers, &self.host, self.port)?;
            }
        }
        Ok(())
    }

    fn check_all_done(&self, scheduler: &Scheduler) -> bool {
        scheduler.task_states.values().all(|state| state.is_done())
    }
}
