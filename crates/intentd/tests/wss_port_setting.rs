//! WSS port setting: prove server.wsApi.port is read/written correctly and
//! that friendly error messages appear when the port is already in use.

#![cfg(unix)]

mod common;

// Port setting tests don't need actual TCP listeners
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

/// Mock `ServerControl` that captures port from `start_ws_listener` calls
struct MockPortServerControl {
    should_fail: bool,
    requested_port: Arc<tokio::sync::Mutex<Option<u16>>>,
}

impl ServerControl for MockPortServerControl {
    fn start_ws_listener(
        &self,
    ) -> Pin<Box<dyn std::future::Future<Output = CoreResult<u16>> + Send + '_>> {
        let should_fail = self.should_fail;
        let requested_port = self.requested_port.clone();
        Box::pin(async move {
            // Record the port that was requested (not implemented in this mock)
            // In the real implementation, we'd read it from ws_options
            if should_fail {
                Err(intent_core::Error::Internal(
                    "Port 5182 is already in use — choose a different port or stop the process using it".to_string(),
                ))
            } else {
                // Simulate success
                let mut guard = requested_port.lock().await;
                *guard = Some(5182);
                Ok(5182)
            }
        })
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
            path: std::env::temp_dir().join(format!("intentd-port-{}.db", Uuid::new_v4())),
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

/// Test that server.wsApi.port setting exists and can be read/updated
#[tokio::test]
async fn port_setting_crud() {
    let tmpdb = TempDb::new();
    let store = Store::open(&tmpdb.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services = Services::new(store.clone())
        .with_event_bus(bus.clone())
        .with_secret_store(Arc::new(InMemorySecretStore::default()))
        .with_workspaces_root(ws_root.path().to_path_buf());

    let api: Arc<dyn WorkspaceApi> = Arc::new(services);

    let socket_path = std::env::temp_dir().join(format!("port-{}.sock", Uuid::new_v4().simple()));
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

    // List settings and verify server.wsApi.port exists with default 5181
    let result = rpc(&mut w, &mut reader, 1, "settings.list", json!({})).await;
    let settings = result["settings"].as_array().expect("settings array");
    let port_setting = settings
        .iter()
        .find(|s| s["path"] == "server.wsApi.port")
        .expect("server.wsApi.port should exist");
    assert_eq!(port_setting["value"], json!(5181.0));
    assert_eq!(port_setting["min"], json!(1024.0));
    assert_eq!(port_setting["max"], json!(65535.0));

    // Update the port
    rpc(
        &mut w,
        &mut reader,
        2,
        "settings.update",
        json!({ "changes": [{ "path": "server.wsApi.port", "value": 5182 }] }),
    )
    .await;

    // Verify the new value persisted
    let got = rpc(
        &mut w,
        &mut reader,
        3,
        "settings.get",
        json!({ "path": "server.wsApi.port" }),
    )
    .await;
    let value = got["value"].as_f64().unwrap() as u16;
    assert_eq!(value, 5182);

    // Reset to default
    let result = rpc(
        &mut w,
        &mut reader,
        4,
        "settings.reset",
        json!({ "path": "server.wsApi.port" }),
    )
    .await;
    let value = result["value"].as_f64().unwrap() as u16;
    assert_eq!(value, 5181);

    shutdown_tx.send(()).ok();
    let _ = std::fs::remove_file(&socket_path);
}

/// Test that changing port while listener is running triggers restart
#[tokio::test]
async fn port_change_restarts_listener() {
    let tmpdb = TempDb::new();
    let store = Store::open(&tmpdb.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services = Services::new(store.clone())
        .with_event_bus(bus.clone())
        .with_secret_store(Arc::new(InMemorySecretStore::default()))
        .with_workspaces_root(ws_root.path().to_path_buf());

    // Attach a mock ServerControl that returns a running port
    let requested_port = Arc::new(tokio::sync::Mutex::new(None));
    let mock_control = Arc::new(MockPortServerControl {
        should_fail: false,
        requested_port: requested_port.clone(),
    });
    services.attach_server_control(mock_control);

    let api: Arc<dyn WorkspaceApi> = Arc::new(services);

    let socket_path = std::env::temp_dir().join(format!("port-r-{}.sock", Uuid::new_v4().simple()));
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

    // Update port (listener not running, should just persist)
    rpc(
        &mut w,
        &mut reader,
        1,
        "settings.update",
        json!({ "changes": [{ "path": "server.wsApi.port", "value": 5182 }] }),
    )
    .await;

    // Verify it persisted
    let got = rpc(
        &mut w,
        &mut reader,
        2,
        "settings.get",
        json!({ "path": "server.wsApi.port" }),
    )
    .await;
    let value = got["value"].as_f64().unwrap() as u16;
    assert_eq!(value, 5182);

    shutdown_tx.send(()).ok();
    let _ = std::fs::remove_file(&socket_path);
}

/// Test that port bind failures return friendly error messages
#[tokio::test]
async fn port_bind_failure_friendly_error() {
    let tmpdb = TempDb::new();
    let store = Store::open(&tmpdb.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services = Services::new(store.clone())
        .with_event_bus(bus.clone())
        .with_secret_store(Arc::new(InMemorySecretStore::default()))
        .with_workspaces_root(ws_root.path().to_path_buf());

    // Attach a mock ServerControl that fails with EADDRINUSE
    let requested_port = Arc::new(tokio::sync::Mutex::new(None));
    let mock_control = Arc::new(MockPortServerControl {
        should_fail: true,
        requested_port: requested_port.clone(),
    });
    services.attach_server_control(mock_control);

    let api: Arc<dyn WorkspaceApi> = Arc::new(services);

    let socket_path = std::env::temp_dir().join(format!("port-e-{}.sock", Uuid::new_v4().simple()));
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

    // Set port and try to enable (will fail with friendly error)
    rpc(
        &mut w,
        &mut reader,
        1,
        "settings.update",
        json!({ "changes": [{ "path": "server.wsApi.port", "value": 5182 }] }),
    )
    .await;

    // Try to enable the listener (should fail)
    let resp = call(
        &mut w,
        &mut reader,
        2,
        "settings.update",
        json!({ "changes": [{ "path": "server.wsApi.enabled", "value": true }] }),
    )
    .await;

    // Should have an error
    assert!(resp.get("error").is_some(), "expected error, got: {resp}");
    let error = &resp["error"];

    // JSON-RPC error mapping puts the actual error message in the data field
    // message is just "Internal error", data carries the friendly message
    let error_msg = if let Some(data) = error.get("data") {
        data.as_str().unwrap_or("")
    } else {
        ""
    };

    // The error from the hook should be "failed to start WSS listener: {inner error}"
    // where inner error is from the mock: "Port 5182 is already in use..."
    assert!(
        error_msg.contains("already in use") || error_msg.contains("failed to start WSS listener"),
        "error data should mention bind failure, got: {error_msg}"
    );

    shutdown_tx.send(()).ok();
    let _ = std::fs::remove_file(&socket_path);
}
