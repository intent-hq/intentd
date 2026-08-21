//! WSS end-to-end test for workspace.create worktree failure error codes.
//! Drives the real WS transport and asserts that a "branch already checked
//! out" failure surfaces as -32602 (`InvalidParams`) with an actionable message
//! instead of a generic -32603 (Internal error).

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
    let dir = std::env::temp_dir().join(format!("intentd-wt-err-{}", &short[..8]));
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
        bind_address: Ipv4Addr::LOCALHOST.into(),
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

/// Helper to initialize a git repository with one commit using the git CLI.
fn init_git_repo(path: &PathBuf) -> String {
    std::fs::create_dir_all(path).unwrap();
    std::process::Command::new("git")
        .args(["init"])
        .current_dir(path)
        .output()
        .expect("git init");
    std::process::Command::new("git")
        .args(["config", "user.name", "Test"])
        .current_dir(path)
        .output()
        .expect("git config user.name");
    std::process::Command::new("git")
        .args(["config", "user.email", "test@example.com"])
        .current_dir(path)
        .output()
        .expect("git config user.email");
    std::fs::write(path.join("README.md"), "# Test repo\n").unwrap();
    std::process::Command::new("git")
        .args(["add", "README.md"])
        .current_dir(path)
        .output()
        .expect("git add");
    std::process::Command::new("git")
        .args(["commit", "-m", "Initial commit"])
        .current_dir(path)
        .output()
        .expect("git commit");
    let output = std::process::Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .current_dir(path)
        .output()
        .expect("git rev-parse");
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

#[tokio::test]
async fn workspace_create_branch_already_checked_out_returns_invalid_params() {
    let fx = boot().await;
    let mut rpc = connect(fx.port).await;

    // Create a local git repository with one commit.
    let repo_dir =
        std::env::temp_dir().join(format!("wt-err-repo-{}", uuid::Uuid::new_v4().simple()));
    std::fs::create_dir_all(&repo_dir).unwrap();
    let branch_name = init_git_repo(&repo_dir);

    // Attempt to create a workspace on the same branch that's already checked
    // out in the main working tree (the repository's current HEAD).
    let resp = wss_rpc_raw(
        &mut rpc,
        1,
        "workspace.create",
        json!({
            "repositoryPath": repo_dir.to_string_lossy(),
            "branch": branch_name,
        }),
    )
    .await;

    // Assert the error envelope: -32602 (InvalidParams) with "already checked out" message.
    let err = resp.get("error").expect("workspace.create should error");
    assert_eq!(
        err["code"], -32602,
        "expected -32602 (InvalidParams), got: {resp}"
    );
    let msg = err["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("already checked out"),
        "expected 'already checked out' in message, got: {msg}"
    );
    assert!(
        msg.contains(&branch_name),
        "expected branch name in message, got: {msg}"
    );

    let _ = std::fs::remove_dir_all(&repo_dir);
}

#[tokio::test]
async fn workspace_create_unresolvable_base_ref_returns_invalid_params_with_data() {
    let fx = boot().await;
    let mut rpc = connect(fx.port).await;

    // Create a local git repository with one commit.
    let repo_dir =
        std::env::temp_dir().join(format!("wt-err-repo-{}", uuid::Uuid::new_v4().simple()));
    std::fs::create_dir_all(&repo_dir).unwrap();
    init_git_repo(&repo_dir);

    // Attempt to create a workspace from a base ref that does not exist.
    let resp = wss_rpc_raw(
        &mut rpc,
        1,
        "workspace.create",
        json!({
            "repositoryPath": repo_dir.to_string_lossy(),
            "branch": "feature-from-bogus-ref",
            "baseRef": "no-such-ref",
        }),
    )
    .await;

    // Assert the error envelope: -32602, unchanged human message, plus the
    // machine-readable data payload (monorepo#761).
    let err = resp.get("error").expect("workspace.create should error");
    assert_eq!(
        err["code"], -32602,
        "expected -32602 (InvalidParams), got: {resp}"
    );
    assert_eq!(
        err["message"],
        json!("invalid params: cannot resolve base ref 'no-such-ref'"),
        "human message must stay byte-identical, got: {resp}"
    );
    assert_eq!(
        err["data"],
        json!({ "code": "base-ref-unresolvable", "baseRef": "no-such-ref" }),
        "expected structured error.data, got: {resp}"
    );

    let _ = std::fs::remove_dir_all(&repo_dir);
}
