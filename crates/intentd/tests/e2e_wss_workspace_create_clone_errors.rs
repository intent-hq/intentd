//! WSS end-to-end test for classified workspace.create clone failure codes
//! (monorepo#826). Drives the real WS transport and asserts that clone
//! failures surface a machine-readable `error.data.code` plus a sanitized
//! human-readable detail instead of a bare -32603 "Internal error".

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use intent_core::WorkspaceApi;
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::{WsApiServer, WsOptions};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type PlainWs = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct TempDir(PathBuf);
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Fixture {
    _ws: WsApiServer,
    port: u16,
    _dir: TempDir,
}

async fn boot() -> Fixture {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = std::env::temp_dir().join(format!("intentd-clone-err-{}", &short[..8]));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(&dir.join("intentd.db")).await.expect("store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic root");
    let services = Services::new(store)
        .with_workspaces_root(workspaces_root)
        .with_event_bus(bus.clone());
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);
    let opts = WsOptions {
        base_port: 0,
        bind_addresses: vec![Ipv4Addr::LOCALHOST.into()],
        ..Default::default()
    };
    let ws = WsApiServer::new_insecure(api, bus, opts, None);
    let port = ws.start().await.expect("start");
    Fixture {
        _ws: ws,
        port,
        _dir: TempDir(dir),
    }
}

async fn connect(port: u16) -> PlainWs {
    let url = format!("ws://127.0.0.1:{port}/ws");
    let (sock, _resp) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("plain ws handshake");
    sock
}

async fn wss_rpc_raw(ws: &mut PlainWs, id: i64, method: &str, params: Value) -> Value {
    let req = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(req.to_string().into()))
        .await
        .unwrap();
    timeout(common::rpc_read_timeout(), async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v.get("id") == Some(&json!(id)) {
                        return v;
                    }
                }
                Message::Ping(_) | Message::Pong(_) => {}
                _ => panic!("unexpected message"),
            }
        }
    })
    .await
    .expect("response timeout")
}

/// Minimal hermetic HTTP server that answers every request with
/// `401 Unauthorized`, mimicking a private remote that demands credentials.
/// With `GIT_TERMINAL_PROMPT=0` (set by `run_clone`) git then fails with
/// "could not read Username ... terminal prompts disabled" — the exact
/// auth-required shape we classify.
async fn spawn_401_server() -> u16 {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind 401 server");
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let _ = sock
                    .write_all(
                        b"HTTP/1.1 401 Unauthorized\r\n\
                          WWW-Authenticate: Basic realm=\"git\"\r\n\
                          Content-Length: 0\r\n\
                          Connection: close\r\n\r\n",
                    )
                    .await;
                let _ = sock.shutdown().await;
            });
        }
    });
    port
}

/// A `clonePath` whose target has no file name is rejected pre-clone with a
/// typed `path-invalid` error (-32602) instead of a generic failure.
#[tokio::test]
async fn workspace_create_invalid_clone_path_returns_path_invalid() {
    let fx = boot().await;
    let mut rpc = connect(fx.port).await;

    let resp = wss_rpc_raw(
        &mut rpc,
        1,
        "workspace.create",
        json!({
            "githubUrl": "https://github.com/intent-hq/does-not-matter.git",
            "clonePath": "/",
        }),
    )
    .await;

    let err = resp.get("error").expect("workspace.create should error");
    assert_eq!(
        err["code"], -32602,
        "expected -32602 (InvalidParams), got: {resp}"
    );
    assert_eq!(
        err["data"]["code"],
        json!("path-invalid"),
        "expected data.code path-invalid, got: {resp}"
    );
    let detail = err["data"]["detail"].as_str().unwrap_or_default();
    assert!(
        !detail.is_empty(),
        "expected non-empty data.detail, got: {resp}"
    );
    let msg = err["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("path-invalid") && msg.contains(detail),
        "expected message to carry category + detail, got: {msg}"
    );
}

/// A remote that demands credentials (401 on every request, terminal prompts
/// disabled) surfaces as a typed `auth-required` error (-32603) whose message
/// and `data.detail` carry the sanitized git stderr tail — never a bare
/// "Internal error".
#[tokio::test]
async fn workspace_create_auth_required_clone_returns_typed_error() {
    let fx = boot().await;
    let mut rpc = connect(fx.port).await;
    let http_port = spawn_401_server().await;

    let resp = wss_rpc_raw(
        &mut rpc,
        1,
        "workspace.create",
        json!({
            "githubUrl": format!("http://127.0.0.1:{http_port}/private/repo.git"),
        }),
    )
    .await;

    let err = resp.get("error").expect("workspace.create should error");
    assert_eq!(
        err["code"], -32603,
        "expected -32603 for auth-required, got: {resp}"
    );
    assert_eq!(
        err["data"]["code"],
        json!("auth-required"),
        "expected data.code auth-required, got: {resp}"
    );
    let detail = err["data"]["detail"].as_str().unwrap_or_default();
    assert!(
        detail.to_lowercase().contains("username")
            || detail.to_lowercase().contains("authentication"),
        "expected git auth stderr in data.detail, got: {resp}"
    );
    let msg = err["message"].as_str().unwrap_or_default();
    assert_ne!(msg, "Internal error", "must not be a bare Internal error");
    assert!(
        msg.contains("auth-required"),
        "expected category in message, got: {msg}"
    );
}

/// A clone target that already exists (and is non-empty) is rejected
/// pre-clone with a typed `destination-exists-non-empty` error (-32602).
#[tokio::test]
async fn workspace_create_existing_clone_target_returns_destination_exists() {
    let fx = boot().await;
    let mut rpc = connect(fx.port).await;

    let occupied = std::env::temp_dir().join(format!(
        "clone-err-occupied-{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&occupied).unwrap();
    // Drop guard so a failing assertion below cannot leak the dir in /tmp.
    let _occupied_guard = TempDir(occupied.clone());
    std::fs::write(occupied.join("keep.txt"), "occupied").unwrap();

    let resp = wss_rpc_raw(
        &mut rpc,
        1,
        "workspace.create",
        json!({
            "githubUrl": "https://github.com/intent-hq/does-not-matter.git",
            "clonePath": occupied.to_string_lossy(),
        }),
    )
    .await;

    let err = resp.get("error").expect("workspace.create should error");
    assert_eq!(
        err["code"], -32602,
        "expected -32602 (InvalidParams), got: {resp}"
    );
    assert_eq!(
        err["data"]["code"],
        json!("destination-exists-non-empty"),
        "expected data.code destination-exists-non-empty, got: {resp}"
    );
}
