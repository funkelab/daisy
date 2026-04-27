use daisy_core::block::BlockStatus;
use daisy_core::client::Client;
use daisy_core::protocol::{read_message, write_message, Message};
use daisy_core::resource_allocator::ResourceBudget;
use daisy_core::roi::Roi;
use daisy_core::server::Server;
use daisy_core::task::Task;
use daisy_core::worker_pool::WorkerPool;
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread")]
async fn test_framing_roundtrip() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (mut reader, mut writer) = stream.into_split();
        while let Ok(Some(msg)) = read_message(&mut reader).await {
            write_message(&mut writer, &msg).await.unwrap();
        }
    });

    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut reader, mut writer) = stream.into_split();

    let msgs = vec![
        Message::AcquireBlock {
            task_id: "test".into(),
        },
        Message::RequestShutdown,
        Message::Disconnect,
    ];

    for msg in &msgs {
        write_message(&mut writer, msg).await.unwrap();
        let echoed = read_message(&mut reader).await.unwrap().unwrap();
        match (&msg, &echoed) {
            (Message::AcquireBlock { task_id: a }, Message::AcquireBlock { task_id: b }) => {
                assert_eq!(a, b);
            }
            (Message::RequestShutdown, Message::RequestShutdown) => {}
            (Message::Disconnect, Message::Disconnect) => {}
            _ => panic!("message mismatch: {msg:?} vs {echoed:?}"),
        }
    }

    drop(writer);
    drop(reader);
    let _ = server_task.await;
}

async fn run_worker(host: String, port: u16, task_id: String) {
    let mut client = Client::connect(&host, port, &task_id).await.unwrap();
    loop {
        match client.acquire_block().await {
            Ok(Some(mut block)) => {
                block.status = BlockStatus::Success;
                client.release_block(block).await.unwrap();
            }
            Ok(None) | Err(_) => break,
        }
    }
    let _ = client.disconnect().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_server_client_no_conflict() {
    let task = Arc::new(
        Task::builder("test_tcp")
            .total_roi(Roi::from_slices(&[0], &[40]))
            .read_roi(Roi::from_slices(&[0], &[10]))
            .write_roi(Roi::from_slices(&[0], &[10]))
            .read_write_conflict(false)
            .num_workers(0)
            .build(),
    );

    let (server, listener) = Server::bind("127.0.0.1").await.unwrap();
    let host = server.host().to_string();
    let port = server.port();

    let mut worker_pools: HashMap<String, WorkerPool> = HashMap::new();

    let h = host.clone();
    let w1 = tokio::spawn(run_worker(h.clone(), port, "test_tcp".to_string()));
    let w2 = tokio::spawn(run_worker(h, port, "test_tcp".to_string()));

    let (states, _) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        server.run_blockwise(
            listener,
            &[task],
            &mut worker_pools,
            ResourceBudget::empty(),
            None,
            None,
        ),
    )
    .await
    .expect("server timed out")
    .unwrap();

    let state = &states["test_tcp"];
    assert!(state.balanced(), "task should be done: {state}");
    assert_eq!(state.total_block_count, 4);
    assert_eq!(state.completed_count, 4);

    // Workers should exit promptly after server shutdown — not linger as zombies.
    tokio::time::timeout(std::time::Duration::from_secs(2), w1)
        .await
        .expect("worker 1 did not exit within 2s after server shutdown")
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), w2)
        .await
        .expect("worker 2 did not exit within 2s after server shutdown")
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_server_client_with_conflict() {
    let task = Arc::new(
        Task::builder("conflict_tcp")
            .total_roi(Roi::from_slices(&[0], &[60]))
            .read_roi(Roi::from_slices(&[0], &[20]))
            .write_roi(Roi::from_slices(&[5], &[10]))
            .read_write_conflict(true)
            .num_workers(0)
            .max_retries(2)
            .build(),
    );

    let (server, listener) = Server::bind("127.0.0.1").await.unwrap();
    let host = server.host().to_string();
    let port = server.port();

    let mut worker_pools: HashMap<String, WorkerPool> = HashMap::new();

    let h = host.clone();
    let w1 = tokio::spawn(run_worker(h.clone(), port, "conflict_tcp".to_string()));
    let w2 = tokio::spawn(run_worker(h, port, "conflict_tcp".to_string()));

    let (states, _) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        server.run_blockwise(
            listener,
            &[task],
            &mut worker_pools,
            ResourceBudget::empty(),
            None,
            None,
        ),
    )
    .await
    .expect("server timed out")
    .unwrap();

    let state = &states["conflict_tcp"];
    assert!(state.balanced(), "task should be done: {state}");
    assert_eq!(state.completed_count, 5);

    tokio::time::timeout(std::time::Duration::from_secs(2), w1)
        .await
        .expect("worker 1 did not exit within 2s")
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), w2)
        .await
        .expect("worker 2 did not exit within 2s")
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_server_block_failure_and_retry() {
    use std::sync::atomic::{AtomicU32, Ordering};

    static ATTEMPTS: AtomicU32 = AtomicU32::new(0);
    ATTEMPTS.store(0, Ordering::SeqCst);

    let task = Arc::new(
        Task::builder("retry_test")
            .total_roi(Roi::from_slices(&[0], &[20]))
            .read_roi(Roi::from_slices(&[0], &[10]))
            .write_roi(Roi::from_slices(&[0], &[10]))
            .read_write_conflict(false)
            .num_workers(0)
            .max_retries(3)
            .build(),
    );

    let (server, listener) = Server::bind("127.0.0.1").await.unwrap();
    let host = server.host().to_string();
    let port = server.port();

    let mut worker_pools: HashMap<String, WorkerPool> = HashMap::new();

    let w = tokio::spawn(async move {
        let mut client = Client::connect(&host, port, "retry_test").await.unwrap();
        loop {
            match client.acquire_block().await {
                Ok(Some(mut block)) => {
                    let n = ATTEMPTS.fetch_add(1, Ordering::SeqCst);
                    if n == 0 {
                        client
                            .report_failure(block, "simulated failure".to_string())
                            .await
                            .unwrap();
                    } else {
                        block.status = BlockStatus::Success;
                        client.release_block(block).await.unwrap();
                    }
                }
                Ok(None) | Err(_) => break,
            }
        }
        let _ = client.disconnect().await;
    });

    let (states, _) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        server.run_blockwise(
            listener,
            &[task],
            &mut worker_pools,
            ResourceBudget::empty(),
            None,
            None,
        ),
    )
    .await
    .expect("server timed out")
    .unwrap();

    let state = &states["retry_test"];
    assert!(state.balanced(), "task should be done: {state}");
    assert!(ATTEMPTS.load(Ordering::SeqCst) >= 3);

    tokio::time::timeout(std::time::Duration::from_secs(2), w)
        .await
        .expect("worker did not exit within 2s")
        .unwrap();
}
