//! Regression test: slow requests (e.g. `host.exec` sleeping) must NOT delay
//! responses to fast requests interleaved on the same connection. `process_frame`
//! spawns the host and JSON-RPC dispatch slow paths, so the fast reply comes
//! back well before the slow one finishes. JSON-RPC correlates by `id`, so
//! out-of-order responses are the point.

#![cfg(unix)]

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use intent_core::WorkspaceApi;
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::serve_uds;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::OwnedReadHalf;
use tokio::net::UnixStream;
use tokio::sync::oneshot;
use tokio::time::timeout;
use uuid::Uuid;

struct TempDb {
    path: PathBuf,
}
impl TempDb {
    fn new() -> Self {
        Self {
            path: std::env::temp_dir().join(format!("intentd-uds-{}.db", Uuid::new_v4())),
        }
    }
}
impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
        }
    }
}

async fn connect_retry(socket: &PathBuf) -> UnixStream {
    for _ in 0..100 {
        if let Ok(s) = UnixStream::connect(socket).await {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("could not connect to {}", socket.display());
}

async fn send(write_half: &mut (impl AsyncWriteExt + Unpin), frame: &str) {
    write_half.write_all(frame.as_bytes()).await.unwrap();
    write_half.write_all(b"\n").await.unwrap();
    write_half.flush().await.unwrap();
}

async fn read_json(reader: &mut BufReader<OwnedReadHalf>, budget: Duration) -> Value {
    let mut line = String::new();
    let n = timeout(budget, reader.read_line(&mut line))
        .await
        .expect("timed out waiting for a frame")
        .expect("read failed");
    assert!(n > 0, "connection closed unexpectedly");
    serde_json::from_str(line.trim_end()).expect("invalid JSON frame")
}

/// A slow `host.exec` (sleeping) must not block a subsequent `workspace.list`
/// on the same connection. Concurrency is proven by response *ordering*: with
/// serialized dispatch the slow id=1 response would always be written first;
/// with concurrent dispatch the fast id=2 response arrives first. Ordering is
/// robust under host load, unlike fixed wall-clock latency budgets.
#[tokio::test(flavor = "multi_thread")]
async fn slow_host_exec_does_not_block_fast_workspace_list() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services: Arc<dyn WorkspaceApi> = Arc::new(Services::new(store));
    let socket = std::env::temp_dir().join(format!("intentd-uds-{}.sock", Uuid::new_v4()));

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server = tokio::spawn({
        let socket = socket.clone();
        async move {
            let _ = serve_uds(services, bus, &socket, None, async {
                let _ = shutdown_rx.await;
            })
            .await;
        }
    });

    let (read_half, mut write_half) = connect_retry(&socket).await.into_split();
    let mut reader = BufReader::new(read_half);

    // The slow sleep scales with INTENTD_TEST_TIMEOUT_MULTIPLIER so ordering
    // stays deterministic when everything else (including workspace.list) is
    // slowed down proportionally, e.g. under coverage instrumentation.
    let slow_sleep = common::test_timeout(Duration::from_secs(2));
    // Frame budgets are generous (and multiplier-scaled): they only guard
    // against a hung connection, not latency. The base comfortably exceeds the
    // sleep base so a serialized-dispatch regression fails on the ordering
    // assertion below rather than on a read timeout.
    let frame_budget = common::test_timeout(Duration::from_secs(30));

    // Slow request first: subprocess sleep. Without concurrency this would
    // pin the read loop until the child exits.
    let slow_frame = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"host.exec","params":{{"command":"sleep","args":["{}"]}}}}"#,
        slow_sleep.as_secs_f64()
    );
    // Fast request second: goes through the JSON-RPC dispatcher slow path,
    // which is now also spawned.
    let fast_frame = r#"{"jsonrpc":"2.0","id":2,"method":"workspace.list"}"#;

    send(&mut write_half, &slow_frame).await;
    send(&mut write_half, fast_frame).await;

    // Ordering is the regression signal: the fast response must arrive first.
    let first = read_json(&mut reader, frame_budget).await;
    assert_eq!(
        first["id"], 2,
        "fast request (workspace.list) must respond before slow host.exec: got {first}"
    );
    assert!(
        first.get("result").is_some(),
        "workspace.list must succeed: got {first}"
    );

    // The slow exec still completes and its response arrives afterwards.
    let second = read_json(&mut reader, frame_budget).await;
    assert_eq!(
        second["id"], 1,
        "second response must be host.exec: {second}"
    );
    assert!(
        second.get("result").is_some(),
        "host.exec must succeed: got {second}"
    );

    let _ = shutdown_tx.send(());
    let _ = server.await;
    let _ = std::fs::remove_file(&socket);
}
