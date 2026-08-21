//! Atomic rollback for failed settings batches (PR #157+): prove that when a
//! batch fails during apply-hook execution, ALL settings in the batch (not just
//! server.* keys) are rolled back to their pre-batch values.
//!
//! Covers mixed batches (server.* + non-server keys), single-key failures, and
//! successful batches (no rollback).

mod common;

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use intent_core::{Result as CoreResult, ServerControl, WorkspaceApi};
use intent_services::{EventBus, InMemorySecretStore, Services};
use intent_store::Store;
use intent_transport::serve_uds;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::OwnedReadHalf;
use tokio::net::UnixStream;
use tokio::sync::oneshot;
use tokio::time::timeout;
use uuid::Uuid;

/// Mock `ServerControl` that always fails `start_ws_listener` to test rollback.
struct FailingServerControl;

impl ServerControl for FailingServerControl {
    fn start_ws_listener(
        &self,
    ) -> Pin<Box<dyn std::future::Future<Output = CoreResult<u16>> + Send + '_>> {
        Box::pin(async { Err(intent_core::Error::Internal("mock failure".to_string())) })
    }

    fn stop_ws_listener(&self) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }

    fn ws_listener_port(
        &self,
    ) -> Pin<Box<dyn std::future::Future<Output = Option<u16>> + Send + '_>> {
        Box::pin(async { None })
    }

    fn is_tcp_connection(&self) -> bool {
        false
    }
}

struct TempDb {
    path: PathBuf,
}
impl TempDb {
    fn new() -> Self {
        Self {
            path: std::env::temp_dir().join(format!("intentd-atomic-{}.db", Uuid::new_v4())),
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

async fn read_json(reader: &mut BufReader<OwnedReadHalf>) -> Value {
    let mut line = String::new();
    let n = timeout(Duration::from_secs(2), reader.read_line(&mut line))
        .await
        .expect("timed out waiting for a frame")
        .expect("read failed");
    assert!(n > 0, "connection closed unexpectedly");
    serde_json::from_str(line.trim_end()).expect("invalid JSON frame")
}

async fn call(
    write_half: &mut (impl AsyncWriteExt + Unpin),
    reader: &mut BufReader<OwnedReadHalf>,
    id: i64,
    method: &str,
    params: Value,
) -> Value {
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    send(write_half, &serde_json::to_string(&frame).unwrap()).await;
    let resp = read_json(reader).await;
    assert_eq!(resp["id"], id, "response id mismatch for {method}");
    resp
}

async fn rpc(
    write_half: &mut (impl AsyncWriteExt + Unpin),
    reader: &mut BufReader<OwnedReadHalf>,
    id: i64,
    method: &str,
    params: Value,
) -> Value {
    let resp = call(write_half, reader, id, method, params).await;
    assert!(resp.get("error").is_none(), "rpc {method} errored: {resp}");
    resp["result"].clone()
}

/// Mixed batch (server.* + non-server key): when hook fails, ALL keys revert.
#[tokio::test]
async fn mixed_batch_full_rollback_on_hook_failure() {
    let tmpdb = TempDb::new();
    let store = Store::open(&tmpdb.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services = Services::new(store.clone())
        .with_event_bus(bus.clone())
        .with_secret_store(Arc::new(InMemorySecretStore::default()))
        .with_workspaces_root(ws_root.path().to_path_buf());

    // Attach a mock ServerControl that always fails start_ws_listener
    services.attach_server_control(Arc::new(FailingServerControl));

    let api: Arc<dyn WorkspaceApi> = Arc::new(services);

    // Socket lives in a guarded dir under /tmp so the path stays short
    // (macOS SUN_LEN) and the file is swept even if the test panics.
    let sock_dir = common::test_tempdir_in("/tmp", "itd-at-");
    let socket_path = sock_dir.path().join("uds.sock");
    let socket_path_clone = socket_path.clone();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        serve_uds(api, bus, &socket_path_clone, None, async {
            shutdown_rx.await.ok();
        })
        .await
        .unwrap();
    });

    let stream = connect_retry(&socket_path).await;
    let (r, mut w) = stream.into_split();
    let mut reader = BufReader::new(r);

    // Set baseline values for git.autoCommit and server.port
    rpc(
        &mut w,
        &mut reader,
        1,
        "settings.update",
        json!({ "changes": [
            { "path": "git.autoCommit", "value": true },
            { "path": "server.port", "value": 5181 },
        ] }),
    )
    .await;

    // Verify baseline
    let got = rpc(
        &mut w,
        &mut reader,
        2,
        "settings.get",
        json!({ "path": "git.autoCommit" }),
    )
    .await;
    assert_eq!(got["value"], true);
    let got = rpc(
        &mut w,
        &mut reader,
        3,
        "settings.get",
        json!({ "path": "server.port" }),
    )
    .await;
    assert_eq!(got["value"], 5181);

    // Attempt a mixed batch: git.autoCommit=false + server.port=6000 + server.wsApi.enabled=true
    // The server.wsApi.enabled hook will fail, so BOTH git.autoCommit and server.port should revert
    let resp = call(
        &mut w,
        &mut reader,
        4,
        "settings.update",
        json!({ "changes": [
            { "path": "git.autoCommit", "value": false },
            { "path": "server.port", "value": 6000 },
            { "path": "server.wsApi.enabled", "value": true },
        ] }),
    )
    .await;

    // Should return an error (hook failed)
    assert!(
        resp.get("error").is_some(),
        "expected error from hook failure, got: {resp}"
    );

    // Verify git.autoCommit reverted to baseline (true)
    let got = rpc(
        &mut w,
        &mut reader,
        5,
        "settings.get",
        json!({ "path": "git.autoCommit" }),
    )
    .await;
    assert_eq!(
        got["value"], true,
        "git.autoCommit should revert to baseline after hook failure"
    );

    // Verify server.port reverted to baseline (5181)
    let got = rpc(
        &mut w,
        &mut reader,
        6,
        "settings.get",
        json!({ "path": "server.port" }),
    )
    .await;
    assert_eq!(
        got["value"], 5181,
        "server.port should revert to baseline after hook failure"
    );

    // Verify server.wsApi.enabled reverted to baseline (false)
    let got = rpc(
        &mut w,
        &mut reader,
        7,
        "settings.get",
        json!({ "path": "server.wsApi.enabled" }),
    )
    .await;
    assert_eq!(
        got["value"], false,
        "server.wsApi.enabled should revert to baseline after hook failure"
    );

    shutdown_tx.send(()).ok();
    let _ = std::fs::remove_file(&socket_path);
}

/// Successful mixed batch persists all keys (no rollback).
#[tokio::test]
async fn successful_mixed_batch_persists_all() {
    let tmpdb = TempDb::new();
    let store = Store::open(&tmpdb.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services = Services::new(store)
        .with_event_bus(bus.clone())
        .with_secret_store(Arc::new(InMemorySecretStore::default()))
        .with_workspaces_root(ws_root.path().to_path_buf());

    // No ServerControl attached, so no hooks run → success
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);

    // Socket lives in a guarded dir under /tmp so the path stays short
    // (macOS SUN_LEN) and the file is swept even if the test panics.
    let sock_dir = common::test_tempdir_in("/tmp", "itd-at-");
    let socket_path = sock_dir.path().join("uds.sock");
    let socket_path_clone = socket_path.clone();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        serve_uds(api, bus, &socket_path_clone, None, async {
            shutdown_rx.await.ok();
        })
        .await
        .unwrap();
    });

    let stream = connect_retry(&socket_path).await;
    let (r, mut w) = stream.into_split();
    let mut reader = BufReader::new(r);

    // Apply a mixed batch: git.autoCommit=false + server.port=6000
    rpc(
        &mut w,
        &mut reader,
        1,
        "settings.update",
        json!({ "changes": [
            { "path": "git.autoCommit", "value": false },
            { "path": "server.port", "value": 6000 },
        ] }),
    )
    .await;

    // Verify both persisted
    let got = rpc(
        &mut w,
        &mut reader,
        2,
        "settings.get",
        json!({ "path": "git.autoCommit" }),
    )
    .await;
    assert_eq!(got["value"], false);
    let got = rpc(
        &mut w,
        &mut reader,
        3,
        "settings.get",
        json!({ "path": "server.port" }),
    )
    .await;
    assert_eq!(got["value"], 6000);

    shutdown_tx.send(()).ok();
    let _ = std::fs::remove_file(&socket_path);
}

/// Single-key failure behavior unchanged (still reverts that one key).
#[tokio::test]
async fn single_key_failure_reverts() {
    let tmpdb = TempDb::new();
    let store = Store::open(&tmpdb.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services = Services::new(store)
        .with_event_bus(bus.clone())
        .with_secret_store(Arc::new(InMemorySecretStore::default()))
        .with_workspaces_root(ws_root.path().to_path_buf());

    services.attach_server_control(Arc::new(FailingServerControl));
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);

    // Socket lives in a guarded dir under /tmp so the path stays short
    // (macOS SUN_LEN) and the file is swept even if the test panics.
    let sock_dir = common::test_tempdir_in("/tmp", "itd-at-");
    let socket_path = sock_dir.path().join("uds.sock");
    let socket_path_clone = socket_path.clone();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        serve_uds(api, bus, &socket_path_clone, None, async {
            shutdown_rx.await.ok();
        })
        .await
        .unwrap();
    });

    let stream = connect_retry(&socket_path).await;
    let (r, mut w) = stream.into_split();
    let mut reader = BufReader::new(r);

    // Attempt single-key update that will fail
    let resp = call(
        &mut w,
        &mut reader,
        1,
        "settings.update",
        json!({ "changes": [{ "path": "server.wsApi.enabled", "value": true }] }),
    )
    .await;

    assert!(resp.get("error").is_some());

    // Verify it didn't persist
    let got = rpc(
        &mut w,
        &mut reader,
        2,
        "settings.get",
        json!({ "path": "server.wsApi.enabled" }),
    )
    .await;
    assert_eq!(got["value"], false);

    shutdown_tx.send(()).ok();
    let _ = std::fs::remove_file(&socket_path);
}

/// Mixed batch with sensitive setting: hook failure reverts both sensitive and non-sensitive keys.
#[tokio::test]
async fn mixed_batch_with_sensitive_setting_full_rollback() {
    let tmpdb = TempDb::new();
    let store = Store::open(&tmpdb.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services = Services::new(store)
        .with_event_bus(bus.clone())
        .with_secret_store(Arc::new(InMemorySecretStore::default()))
        .with_workspaces_root(ws_root.path().to_path_buf());

    services.attach_server_control(Arc::new(FailingServerControl));
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);

    // Socket lives in a guarded dir under /tmp so the path stays short
    // (macOS SUN_LEN) and the file is swept even if the test panics.
    let sock_dir = common::test_tempdir_in("/tmp", "itd-at-");
    let socket_path = sock_dir.path().join("uds.sock");
    let socket_path_clone = socket_path.clone();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        serve_uds(api, bus, &socket_path_clone, None, async {
            shutdown_rx.await.ok();
        })
        .await
        .unwrap();
    });

    let stream = connect_retry(&socket_path).await;
    let (r, mut w) = stream.into_split();
    let mut reader = BufReader::new(r);

    // Set baseline: linear.token (sensitive) + git.autoCommit (non-sensitive)
    rpc(
        &mut w,
        &mut reader,
        1,
        "settings.update",
        json!({ "changes": [
            { "path": "linear.token", "value": "baseline-token" },
            { "path": "git.autoCommit", "value": true },
        ] }),
    )
    .await;

    // Verify baseline for git.autoCommit
    let got = rpc(
        &mut w,
        &mut reader,
        2,
        "settings.get",
        json!({ "path": "git.autoCommit" }),
    )
    .await;
    assert_eq!(got["value"], true);

    // Verify linear.token is redacted (sensitive setting)
    let got = rpc(
        &mut w,
        &mut reader,
        3,
        "settings.get",
        json!({ "path": "linear.token" }),
    )
    .await;
    assert_eq!(
        got["value"], "********",
        "sensitive setting should be redacted"
    );

    // Attempt batch: linear.token=new-token + git.autoCommit=false + server.wsApi.enabled=true
    // The server.wsApi.enabled hook will fail, so ALL keys should revert
    let resp = call(
        &mut w,
        &mut reader,
        4,
        "settings.update",
        json!({ "changes": [
            { "path": "linear.token", "value": "new-token" },
            { "path": "git.autoCommit", "value": false },
            { "path": "server.wsApi.enabled", "value": true },
        ] }),
    )
    .await;

    // Should return an error (hook failed)
    assert!(
        resp.get("error").is_some(),
        "expected error from hook failure"
    );

    // Verify git.autoCommit reverted to baseline (true)
    let got = rpc(
        &mut w,
        &mut reader,
        5,
        "settings.get",
        json!({ "path": "git.autoCommit" }),
    )
    .await;
    assert_eq!(
        got["value"], true,
        "git.autoCommit should revert to baseline"
    );

    // Verify linear.token still shows [redacted] (secret was restored, not deleted)
    let got = rpc(
        &mut w,
        &mut reader,
        6,
        "settings.get",
        json!({ "path": "linear.token" }),
    )
    .await;
    assert_eq!(
        got["value"], "********",
        "linear.token should still be present (redacted) after rollback"
    );

    shutdown_tx.send(()).ok();
    let _ = std::fs::remove_file(&socket_path);
}

/// Regression: DB read error during old-value capture fails the batch before
/// applying anything (Phase 3 wave 2, lib.rs:4484-4497). Proves that when
/// `Store::get_setting` returns Err during snapshot capture, the whole batch fails
/// with an error naming the key, and NO settings in the batch are applied.
#[tokio::test]
async fn db_read_error_during_capture_fails_batch() {
    let tmpdb = TempDb::new();
    let store = Store::open(&tmpdb.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services = Services::new(store.clone())
        .with_event_bus(bus.clone())
        .with_secret_store(Arc::new(InMemorySecretStore::default()))
        .with_workspaces_root(ws_root.path().to_path_buf());

    let api: Arc<dyn WorkspaceApi> = Arc::new(services);

    // Socket lives in a guarded dir under /tmp so the path stays short
    // (macOS SUN_LEN) and the file is swept even if the test panics.
    let sock_dir = common::test_tempdir_in("/tmp", "db-fail-");
    let socket_path = sock_dir.path().join("uds.sock");
    let socket_path_clone = socket_path.clone();
    let bus_clone = bus.clone();

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        serve_uds(api, bus_clone, &socket_path_clone, None, async {
            shutdown_rx.await.ok();
        })
        .await
        .unwrap();
    });

    let stream = connect_retry(&socket_path).await;
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // Establish baseline: set two non-sensitive keys
    rpc(
        &mut write_half,
        &mut reader,
        1,
        "settings.update",
        json!({ "changes": [
            { "path": "git.autoCommit", "value": false },
            { "path": "workspace.cowIsolation", "value": true },
        ] }),
    )
    .await;

    // Verify baseline
    let v1 = rpc(
        &mut write_half,
        &mut reader,
        2,
        "settings.get",
        json!({ "path": "git.autoCommit" }),
    )
    .await;
    assert_eq!(v1["value"], false);
    let v2 = rpc(
        &mut write_half,
        &mut reader,
        3,
        "settings.get",
        json!({ "path": "workspace.cowIsolation" }),
    )
    .await;
    assert_eq!(v2["value"], true);

    // Close both DB pools to inject failures into subsequent get_setting calls
    store.write_pool().close().await;
    store.read_pool().close().await;

    // Attempt a batch update - should fail during capture before applying anything
    let resp = call(
        &mut write_half,
        &mut reader,
        4,
        "settings.update",
        json!({ "changes": [
            { "path": "git.autoCommit", "value": true },
            { "path": "workspace.cowIsolation", "value": false },
        ] }),
    )
    .await;

    // Should be a JSON-RPC error
    assert!(
        resp.get("error").is_some(),
        "expected error response, got {resp}"
    );
    let err = &resp["error"];
    let err_data = err["data"].as_str().unwrap_or("");
    assert!(
        err_data.contains("git.autoCommit") || err_data.contains("workspace.cowIsolation"),
        "error should name the failing key in data field: {err}"
    );
    assert!(
        err_data.contains("during snapshot capture"),
        "error should indicate failure during snapshot capture (not later during apply): {err}"
    );

    shutdown_tx.send(()).ok();
    let _ = std::fs::remove_file(&socket_path);

    // Verify atomicity: open a fresh Store against the same DB and confirm the
    // baseline values remain unchanged (the batch failed during capture so
    // nothing should have been applied).
    let store2 = Store::open(&tmpdb.path).await.expect("reopen store");
    let v1_after = store2
        .get_setting("git.autoCommit")
        .await
        .expect("read git.autoCommit");
    let v2_after = store2
        .get_setting("workspace.cowIsolation")
        .await
        .expect("read workspace.cowIsolation");
    assert_eq!(
        v1_after,
        Some("false".to_string()),
        "git.autoCommit should still be false (baseline)"
    );
    assert_eq!(
        v2_after,
        Some("true".to_string()),
        "workspace.cowIsolation should still be true (baseline)"
    );
}
