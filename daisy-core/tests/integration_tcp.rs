use daisy_core::block::BlockStatus;
use daisy_core::client::Client;
use daisy_core::pipeline::Pipeline;
use daisy_core::protocol::{read_message, write_message, Message, PROTOCOL_VERSION};
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
            worker_id: Some(3),
        },
        Message::RequestShutdown,
        Message::Disconnect,
    ];

    for msg in &msgs {
        write_message(&mut writer, msg).await.unwrap();
        let echoed = read_message(&mut reader).await.unwrap().unwrap();
        match (&msg, &echoed) {
            (
                Message::AcquireBlock {
                    task_id: a,
                    worker_id: wa,
                },
                Message::AcquireBlock {
                    task_id: b,
                    worker_id: wb,
                },
            ) => {
                assert_eq!(a, b);
                assert_eq!(wa, wb);
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

/// A peer speaking a different wire version must be told so, rather than
/// failing somewhere deep inside the bincode decoder.
///
/// The payload is positional bincode: adding a field to any message (or to
/// `Block`) silently changes the byte layout, and without this guard the
/// symptom is `UnexpectedEnd`, or an `Option` tag read out of an unrelated
/// string's length. The realistic way to get there is external cluster
/// workers loading daisy from a different environment than the driver, so
/// the error needs to name the actual problem.
#[tokio::test(flavor = "multi_thread")]
async fn test_protocol_version_mismatch_is_reported_clearly() {
    use tokio::io::AsyncWriteExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (mut reader, _writer) = stream.into_split();
        // Return whatever the read produced so the test can inspect it.
        read_message(&mut reader).await.map(|_| ()).map_err(|e| e.to_string())
    });

    // Hand-roll a frame claiming a version this build does not speak.
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let payload =
        bincode::encode_to_vec(Message::RequestShutdown, bincode::config::standard()).unwrap();
    let len = (payload.len() + 1) as u32;
    stream.write_all(&len.to_be_bytes()).await.unwrap();
    stream.write_all(&[PROTOCOL_VERSION.wrapping_add(7)]).await.unwrap();
    stream.write_all(&payload).await.unwrap();
    stream.flush().await.unwrap();

    let err = server_task
        .await
        .unwrap()
        .expect_err("expected a version-mismatch error");
    assert!(err.contains("protocol version mismatch"), "message: {err}");
    assert!(err.contains("Rebuild your workers"), "message: {err}");
}

/// Regression test: chained tasks (b depends on a) run cleanly in
/// distributed mode. The worker for "a" finishes its blocks, exits,
/// and the rebalance loop spawns a worker for "b" once a's
/// completion unlocks b's first blocks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_chained_tasks_distributed() {
    let task_a = Arc::new(
        Task::builder("a")
            .total_roi(Roi::from_slices(&[0], &[40]))
            .read_roi(Roi::from_slices(&[0], &[10]))
            .write_roi(Roi::from_slices(&[0], &[10]))
            .read_write_conflict(false)
            .max_workers(1)
            .build(),
    );
    let task_b = Arc::new(
        Task::builder("b")
            .total_roi(Roi::from_slices(&[0], &[40]))
            .read_roi(Roi::from_slices(&[0], &[10]))
            .write_roi(Roi::from_slices(&[0], &[10]))
            .read_write_conflict(false)
            .max_workers(1)
            .build(),
    );
    let pipeline = Pipeline::new(
        vec![task_a, task_b],
        vec![("a".to_string(), "b".to_string())],
    )
    .unwrap();

    let (server, listener) = Server::bind(Some("127.0.0.1")).await.unwrap();
    let host = server.host().to_string();
    let port = server.port();
    let mut worker_pools: HashMap<String, WorkerPool> = HashMap::new();

    let h = host.clone();
    let w_a = tokio::spawn(run_worker(h.clone(), port, "a".to_string()));
    let w_b = tokio::spawn(run_worker(h, port, "b".to_string()));

    let (states, _) = tokio::time::timeout(
        std::time::Duration::from_secs(8),
        server.run_blockwise(
            listener,
            &pipeline,
            &mut worker_pools,
            ResourceBudget::empty(),
            None,
            None,
            true,
        ),
    )
    .await
    .expect("chained-task run timed out — regression in worker rebalance for downstream tasks")
    .unwrap();

    assert_eq!(states["a"].total_block_count, 4);
    assert_eq!(states["a"].completed_count, 4);
    assert_eq!(states["b"].total_block_count, 4);
    assert_eq!(states["b"].completed_count, 4);

    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), w_a).await;
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), w_b).await;
}


async fn run_worker(host: String, port: u16, task_id: String) {
    let mut client = Client::connect(&host, port, &task_id, None).await.unwrap();
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
            .max_workers(0)
            .build(),
    );

    let pipeline = Pipeline::from_task(task);

    let (server, listener) = Server::bind(Some("127.0.0.1")).await.unwrap();
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
            &pipeline,
            &mut worker_pools,
            ResourceBudget::empty(),
            None,
            None,
            true,
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
            .max_workers(0)
            .max_retries(2)
            .build(),
    );

    let pipeline = Pipeline::from_task(task);

    let (server, listener) = Server::bind(Some("127.0.0.1")).await.unwrap();
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
            &pipeline,
            &mut worker_pools,
            ResourceBudget::empty(),
            None,
            None,
            true,
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
            .max_workers(0)
            .max_retries(3)
            .build(),
    );

    let pipeline = Pipeline::from_task(task);

    let (server, listener) = Server::bind(Some("127.0.0.1")).await.unwrap();
    let host = server.host().to_string();
    let port = server.port();

    let mut worker_pools: HashMap<String, WorkerPool> = HashMap::new();

    let w = tokio::spawn(async move {
        let mut client = Client::connect(&host, port, "retry_test", None).await.unwrap();
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
            &pipeline,
            &mut worker_pools,
            ResourceBudget::empty(),
            None,
            None,
            true,
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

/// Regression test for the idle-fleet tail problem: a worker that asks for
/// a block when every remaining block is IN FLIGHT (none ready, none
/// pending) must be told to shut down immediately — not parked until the
/// whole task completes. Before the fix, the tail of any large run pinned
/// every idle worker while the last slow blocks finished.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_idle_worker_released_while_blocks_in_flight() {
    let task = Arc::new(
        Task::builder("tail")
            .total_roi(Roi::from_slices(&[0], &[20]))
            .read_roi(Roi::from_slices(&[0], &[10]))
            .write_roi(Roi::from_slices(&[0], &[10]))
            .read_write_conflict(false)
            .max_workers(0)
            .build(),
    );
    let pipeline = Pipeline::from_task(task);

    let (server, listener) = Server::bind(Some("127.0.0.1")).await.unwrap();
    let host = server.host().to_string();
    let port = server.port();
    let mut worker_pools: HashMap<String, WorkerPool> = HashMap::new();

    // Signalled once the idle worker has been released; tells the slow
    // worker it can finally finish its block.
    let (finish_slow_tx, finish_slow_rx) = tokio::sync::oneshot::channel::<()>();
    // Signalled when the idle worker receives shutdown (acquire → None).
    let (idle_released_tx, idle_released_rx) = tokio::sync::oneshot::channel::<()>();

    // Slow worker: takes the first block and holds it in flight.
    let h = host.clone();
    let w_slow = tokio::spawn(async move {
        let mut client = Client::connect(&h, port, "tail", None).await.unwrap();
        let mut block = client
            .acquire_block()
            .await
            .unwrap()
            .expect("slow worker should get a block");
        finish_slow_rx.await.unwrap();
        block.status = BlockStatus::Success;
        client.release_block(block).await.unwrap();
        while let Ok(Some(mut b)) = client.acquire_block().await {
            b.status = BlockStatus::Success;
            client.release_block(b).await.unwrap();
        }
        let _ = client.disconnect().await;
    });

    // Fast worker: starts after the slow one holds its block, drains the
    // remaining ready block, then must promptly be handed None even
    // though the slow block is still processing.
    let h = host.clone();
    let w_fast = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let mut client = Client::connect(&h, port, "tail", None).await.unwrap();
        while let Some(mut block) = client.acquire_block().await.unwrap() {
            block.status = BlockStatus::Success;
            client.release_block(block).await.unwrap();
        }
        let _ = idle_released_tx.send(());
        let _ = client.disconnect().await;
    });

    let orchestrate = async move {
        tokio::time::timeout(std::time::Duration::from_secs(4), idle_released_rx)
            .await
            .expect(
                "idle worker was not released while the last block was in \
                 flight — tail-teardown regression",
            )
            .unwrap();
        let _ = finish_slow_tx.send(());
    };

    let run = server.run_blockwise(
        listener,
        &pipeline,
        &mut worker_pools,
        ResourceBudget::empty(),
        None,
        None,
        true,
    );
    let ((), run_result) = tokio::join!(orchestrate, async {
        tokio::time::timeout(std::time::Duration::from_secs(10), run)
            .await
            .expect("server timed out")
            .unwrap()
    });
    let (states, _) = run_result;

    assert_eq!(states["tail"].completed_count, 2);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), w_slow).await;
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), w_fast).await;
}

/// The scheduler must advertise an address workers can reach, and listen on
/// it. Regression: `bind` used `local_addr()` for both roles, so it bound
/// loopback and put `127.0.0.1` in every worker context — fine on one
/// machine, impossible across nodes, and invisible to every local test.
#[tokio::test]
async fn advertises_a_reachable_address_and_listens_on_it() {
    let detected = daisy_core::advertise::resolve(None);
    if detected.host == "127.0.0.1" {
        // Sandboxes and CI containers may genuinely have nothing but
        // loopback; there is no claim to test there.
        eprintln!("no non-loopback address on this host; skipping");
        return;
    }

    let (server, listener) = Server::bind(None).await.unwrap();
    assert_ne!(
        server.host(),
        "127.0.0.1",
        "a routable address exists ({}) but workers were told loopback",
        detected.host
    );

    let addr = format!("{}:{}", server.host(), server.port());
    let conn = tokio::net::TcpStream::connect(&addr).await;
    assert!(
        conn.is_ok(),
        "advertised {addr} but the listener does not accept there: {:?}",
        conn.err()
    );
    drop(listener);
}

/// An explicit loopback request stays on loopback — the scheduler must not
/// silently open other interfaces for a single-machine run, since the wire
/// protocol has no authentication.
#[tokio::test]
async fn explicit_loopback_is_honoured() {
    let (server, listener) = Server::bind(Some("127.0.0.1")).await.unwrap();
    assert_eq!(server.host(), "127.0.0.1");
    let local = listener.local_addr().unwrap();
    assert!(local.ip().is_loopback(), "bound {local}, expected loopback only");
}
