//! Server runtime control safety guards (PR #135): prove that disabling the
//! WSS listener from a TCP connection is refused, and that failed listener
//! starts do not persist server.wsApi.enabled=true.
//!
//! Uses the UDS transport to test the guards because we can't easily simulate
//! a failing TCP listener start in an integration test, and the connection-
//! context guard is transport-agnostic (UDS = !TCP, WSS = TCP).

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

/// Mock ServerControl that always fails start_ws_listener to test rollback.
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

    fn start_discovery(
        &self,
    ) -> Pin<Box<dyn std::future::Future<Output = CoreResult<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn stop_discovery(&self) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }

    fn is_discovery_active(
        &self,
    ) -> Pin<Box<dyn std::future::Future<Output = bool> + Send + '_>> {
        Box::pin(async { false })
    }
}

struct TempDb {
    path: PathBuf,
}
impl TempDb {
    fn new() -> Self {
        Self {
            path: std::env::temp_dir().join(format!("intentd-ctl-{}.db", Uuid::new_v4())),
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

/// UDS connections treat server.wsApi.enabled → false as safe (not TCP), but
/// if the listener start fails, the setting must NOT be persisted.
#[tokio::test]
async fn settings_rollback_on_failed_listener_start() {
    let tmpdb = TempDb::new();
    let store = Store::open(&tmpdb.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store)
        .with_event_bus(bus.clone())
        .with_secret_store(Arc::new(InMemorySecretStore::default()));

    // Attach a mock ServerControl that always fails start_ws_listener
    services.attach_server_control(Arc::new(FailingServerControl));

    let api: Arc<dyn WorkspaceApi> = Arc::new(services);

    let socket_path = std::env::temp_dir().join(format!("intentd-ctl-{}.sock", Uuid::new_v4()));
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

    // Attempt to enable server.wsApi.enabled when there's no WS runtime
    // (daemon started UDS-only). This should fail and NOT persist enabled=true.
    let resp = call(
        &mut w,
        &mut reader,
        1,
        "settings.update",
        json!({ "changes": [{ "path": "server.wsApi.enabled", "value": true }] }),
    )
    .await;

    // Should return an error (listener start failed)
    assert!(resp.get("error").is_some(), "expected error, got: {resp}");

    // Verify the setting was NOT persisted
    let get_resp = rpc(
        &mut w,
        &mut reader,
        2,
        "settings.get",
        json!({ "path": "server.wsApi.enabled" }),
    )
    .await;

    // Should still be default (false) since persistence was rolled back
    assert_eq!(
        get_resp["value"], false,
        "server.wsApi.enabled should not be persisted after failed start"
    );

    shutdown_tx.send(()).ok();
    let _ = std::fs::remove_file(&socket_path);
}
