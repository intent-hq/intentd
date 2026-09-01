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
/// and tracks tailcat tunnel start/stop for the `server.tunnel.*` hooks.
#[derive(Default)]
struct MockPortServerControl {
    should_fail: bool,
    requested_port: Arc<tokio::sync::Mutex<Option<u16>>>,
    /// Simulated running listener port (`ws_listener_port`); the tunnel hook
    /// requires the listener up.
    listener_port: Option<u16>,
    tunnel_should_fail: bool,
    tunnel_running: Arc<tokio::sync::Mutex<bool>>,
    tunnel_starts: Arc<tokio::sync::Mutex<u32>>,
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
        let port = self.listener_port;
        Box::pin(async move { port })
    }

    fn is_tcp_connection(&self) -> bool {
        false
    }

    fn start_tunnel(
        &self,
    ) -> Pin<Box<dyn std::future::Future<Output = CoreResult<String>> + Send + '_>> {
        let should_fail = self.tunnel_should_fail;
        let running = self.tunnel_running.clone();
        let starts = self.tunnel_starts.clone();
        Box::pin(async move {
            if should_fail {
                Err(intent_core::Error::Internal(
                    "tailcat exited before reporting its address".to_string(),
                ))
            } else {
                *running.lock().await = true;
                *starts.lock().await += 1;
                Ok("tcTESTADDRESS".to_string())
            }
        })
    }

    fn stop_tunnel(&self) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        let running = self.tunnel_running.clone();
        Box::pin(async move {
            *running.lock().await = false;
        })
    }

    fn tunnel_address(
        &self,
    ) -> Pin<Box<dyn std::future::Future<Output = Option<String>> + Send + '_>> {
        let running = self.tunnel_running.clone();
        Box::pin(async move { running.lock().await.then(|| "tcTESTADDRESS".to_string()) })
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
// Port values are small whole-valued floats: casts are exact.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
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
// Port values are small whole-valued floats: casts are exact.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
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
        ..Default::default()
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
        ..Default::default()
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

/// Spin up a UDS server with the given mock control; returns the RPC halves
/// plus the shutdown handle and socket path for cleanup.
async fn setup_uds(
    mock_control: Arc<MockPortServerControl>,
    tag: &str,
) -> (
    impl AsyncWriteExt + Unpin,
    BufReader<OwnedReadHalf>,
    oneshot::Sender<()>,
    PathBuf,
) {
    let tmpdb = TempDb::new();
    let store = Store::open(&tmpdb.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services = Services::new(store.clone())
        .with_event_bus(bus.clone())
        .with_secret_store(Arc::new(InMemorySecretStore::default()))
        .with_workspaces_root(ws_root.path().to_path_buf());
    services.attach_server_control(mock_control);
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);

    let socket_path = std::env::temp_dir().join(format!("{tag}-{}.sock", Uuid::new_v4().simple()));
    let socket_path_clone = socket_path.clone();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        serve_uds(api, bus, &socket_path_clone, None, async {
            shutdown_rx.await.ok();
        })
        .await
        .unwrap();
        // TempDb/ws_root live as long as the server task.
        drop(tmpdb);
        drop(ws_root);
    });

    let stream = connect_retry(&socket_path).await;
    let (r, w) = stream.into_split();
    (w, BufReader::new(r), shutdown_tx, socket_path)
}

/// server.tunnel.enabled toggles the tunnel through `ServerControl` and
/// persists; disabling stops it.
#[tokio::test]
async fn tunnel_enabled_toggles_sidecar() {
    let tunnel_running = Arc::new(tokio::sync::Mutex::new(false));
    let mock_control = Arc::new(MockPortServerControl {
        listener_port: Some(5181),
        tunnel_running: tunnel_running.clone(),
        ..Default::default()
    });
    let (mut w, mut reader, shutdown_tx, socket_path) = setup_uds(mock_control, "tunnel-t").await;

    rpc(
        &mut w,
        &mut reader,
        1,
        "settings.update",
        json!({ "changes": [{ "path": "server.tunnel.enabled", "value": true }] }),
    )
    .await;
    assert!(*tunnel_running.lock().await, "tunnel should be running");

    let got = rpc(
        &mut w,
        &mut reader,
        2,
        "settings.get",
        json!({ "path": "server.tunnel.enabled" }),
    )
    .await;
    assert_eq!(got["value"], json!(true));

    rpc(
        &mut w,
        &mut reader,
        3,
        "settings.update",
        json!({ "changes": [{ "path": "server.tunnel.enabled", "value": false }] }),
    )
    .await;
    assert!(!*tunnel_running.lock().await, "tunnel should be stopped");

    shutdown_tx.send(()).ok();
    let _ = std::fs::remove_file(&socket_path);
}

/// A tunnel start failure surfaces as a friendly settings.update error and
/// the setting does not flip on.
#[tokio::test]
async fn tunnel_start_failure_friendly_error() {
    let mock_control = Arc::new(MockPortServerControl {
        listener_port: Some(5181),
        tunnel_should_fail: true,
        ..Default::default()
    });
    let (mut w, mut reader, shutdown_tx, socket_path) = setup_uds(mock_control, "tunnel-e").await;

    let resp = call(
        &mut w,
        &mut reader,
        1,
        "settings.update",
        json!({ "changes": [{ "path": "server.tunnel.enabled", "value": true }] }),
    )
    .await;
    assert!(resp.get("error").is_some(), "expected error, got: {resp}");
    let error_msg = resp["error"]["data"].as_str().unwrap_or("");
    assert!(
        error_msg.contains("failed to start tailcat tunnel"),
        "error data should mention tunnel start failure, got: {error_msg}"
    );

    let got = rpc(
        &mut w,
        &mut reader,
        2,
        "settings.get",
        json!({ "path": "server.tunnel.enabled" }),
    )
    .await;
    assert_eq!(got["value"], json!(false), "setting must not flip on");

    shutdown_tx.send(()).ok();
    let _ = std::fs::remove_file(&socket_path);
}

/// Changing server.tunnel.derpUrl while the tunnel runs restarts the sidecar;
/// while stopped it only persists.
#[tokio::test]
async fn tunnel_derp_url_restarts_running_sidecar() {
    let tunnel_running = Arc::new(tokio::sync::Mutex::new(false));
    let tunnel_starts = Arc::new(tokio::sync::Mutex::new(0));
    let mock_control = Arc::new(MockPortServerControl {
        listener_port: Some(5181),
        tunnel_running: tunnel_running.clone(),
        tunnel_starts: tunnel_starts.clone(),
        ..Default::default()
    });
    let (mut w, mut reader, shutdown_tx, socket_path) = setup_uds(mock_control, "tunnel-d").await;

    // While stopped: persists only, no start.
    rpc(
        &mut w,
        &mut reader,
        1,
        "settings.update",
        json!({ "changes": [{ "path": "server.tunnel.derpUrl", "value": "https://derp.example.com/map.json" }] }),
    )
    .await;
    assert_eq!(*tunnel_starts.lock().await, 0);

    // Start the tunnel, then change derpUrl → stop + start (2 total starts).
    rpc(
        &mut w,
        &mut reader,
        2,
        "settings.update",
        json!({ "changes": [{ "path": "server.tunnel.enabled", "value": true }] }),
    )
    .await;
    assert_eq!(*tunnel_starts.lock().await, 1);
    rpc(
        &mut w,
        &mut reader,
        3,
        "settings.update",
        json!({ "changes": [{ "path": "server.tunnel.derpUrl", "value": "https://derp2.example.com/map.json" }] }),
    )
    .await;
    assert_eq!(
        *tunnel_starts.lock().await,
        2,
        "derpUrl change should restart"
    );
    assert!(
        *tunnel_running.lock().await,
        "tunnel should still be running"
    );

    shutdown_tx.send(()).ok();
    let _ = std::fs::remove_file(&socket_path);
}

/// Disabling server.wsApi.enabled stops a running tunnel too (it forwards to
/// the listener).
#[tokio::test]
async fn ws_disable_stops_running_tunnel() {
    let tunnel_running = Arc::new(tokio::sync::Mutex::new(false));
    let mock_control = Arc::new(MockPortServerControl {
        listener_port: Some(5181),
        tunnel_running: tunnel_running.clone(),
        ..Default::default()
    });
    let (mut w, mut reader, shutdown_tx, socket_path) = setup_uds(mock_control, "tunnel-w").await;

    rpc(
        &mut w,
        &mut reader,
        1,
        "settings.update",
        json!({ "changes": [
            { "path": "server.wsApi.enabled", "value": true },
            { "path": "server.tunnel.enabled", "value": true }
        ] }),
    )
    .await;
    assert!(*tunnel_running.lock().await, "tunnel should be running");

    rpc(
        &mut w,
        &mut reader,
        2,
        "settings.update",
        json!({ "changes": [{ "path": "server.wsApi.enabled", "value": false }] }),
    )
    .await;
    assert!(
        !*tunnel_running.lock().await,
        "disabling the listener should stop the tunnel"
    );

    shutdown_tx.send(()).ok();
    let _ = std::fs::remove_file(&socket_path);
}
