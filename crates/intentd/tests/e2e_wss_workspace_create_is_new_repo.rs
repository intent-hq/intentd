//! WSS end-to-end tests for the `workspace.create` new-project flow
//! (`isNewRepo`, intent-hq/monorepo#962). Drives the real WS transport and
//! asserts that a non-git `repositoryPath` marked `isNewRepo: true` is
//! initialized (`git init -b main` + initial commit) and provisioned like any
//! local repo, that an init failure surfaces as a typed JSON-RPC error with
//! no row persisted, and that the legacy row-only skip is preserved when the
//! flag is absent.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
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

fn scratch_dir(tag: &str) -> TempDir {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = std::env::temp_dir().join(format!("intentd-newrepo-e2e-{tag}-{}", &short[..8]));
    std::fs::create_dir_all(&dir).unwrap();
    TempDir(dir)
}

struct Fixture {
    _ws: WsApiServer,
    port: u16,
    _dir: TempDir,
}

async fn boot() -> Fixture {
    let dir = scratch_dir("home");
    let store = Store::open(&dir.0.join("intentd.db")).await.expect("store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.0.join("workspaces");
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
        _dir: dir,
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

fn run_git(args: &[&str], dir: &Path) -> String {
    let out = std::process::Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .expect("spawn git");
    assert!(
        out.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// `workspace.create` with `isNewRepo: true` on a fresh empty (non-git) dir
/// succeeds over WSS: the daemon initializes the directory (`git init -b
/// main` + initial commit) and the workspace works directly in the
/// initialized repository folder on its workspace branch — the result
/// carries `checkoutMode: "direct"`, `worktreePath` = the repository folder
/// (intent-hq/monorepo#2611), and `baseCommitSha` matches the seeded initial
/// commit (PROTOCOL §5.1).
#[tokio::test]
async fn workspace_create_is_new_repo_initializes_and_provisions() {
    let fx = boot().await;
    let mut rpc = connect(fx.port).await;
    let project = scratch_dir("fresh");

    let resp = wss_rpc_raw(
        &mut rpc,
        1,
        "workspace.create",
        json!({
            "title": "New Project E2E",
            "repositoryPath": project.0.to_string_lossy(),
            "isNewRepo": true,
        }),
    )
    .await;

    assert_eq!(resp["jsonrpc"], json!("2.0"), "envelope: {resp}");
    assert!(
        resp.get("error").is_none(),
        "create must succeed, got: {resp}"
    );
    let workspace = &resp["result"]["workspace"];

    // The source dir became a real repo with exactly one seeded commit.
    let head_sha = run_git(&["rev-parse", "HEAD"], &project.0);
    assert_eq!(run_git(&["rev-list", "--count", "HEAD"], &project.0), "1");
    assert_eq!(
        run_git(&["log", "-1", "--pretty=%s"], &project.0),
        "Initial commit"
    );
    assert!(project.0.join("README.md").exists(), "starter files seeded");

    // ...and the workspace works directly in the initialized repository
    // folder (standalone repo) on its workspace branch — the row carries the
    // repository folder as `worktreePath` (intent-hq/monorepo#2611).
    assert_eq!(
        workspace["worktreePath"],
        json!(project.0.to_string_lossy()),
        "worktreePath carries the initialized repository folder, got: {workspace}"
    );
    assert_eq!(
        workspace["checkoutMode"],
        json!("direct"),
        "got: {workspace}"
    );
    assert_eq!(workspace["baseCommitSha"], json!(head_sha));
    let branch = workspace["branch"].as_str().expect("branch populated");
    assert_eq!(
        run_git(&["rev-parse", "--abbrev-ref", "HEAD"], &project.0),
        branch,
        "workspace branch checked out in place"
    );
    assert_eq!(run_git(&["rev-parse", "HEAD"], &project.0), head_sha);
}

/// An `isNewRepo` initialization failure (`repositoryPath` points at a
/// regular file, so `mkdir -p` cannot succeed) fails the whole create with a
/// typed `-32603` whose `data` carries the initialization detail (PROTOCOL
/// §9) — and persists no row: `workspace.list` stays empty.
#[tokio::test]
async fn workspace_create_is_new_repo_init_failure_is_typed_with_no_row() {
    let fx = boot().await;
    let mut rpc = connect(fx.port).await;
    let parent = scratch_dir("initfail");
    let file = parent.0.join("not-a-dir");
    std::fs::write(&file, "plain file\n").unwrap();

    let resp = wss_rpc_raw(
        &mut rpc,
        1,
        "workspace.create",
        json!({
            "repositoryPath": file.to_string_lossy(),
            "isNewRepo": true,
        }),
    )
    .await;

    let err = resp.get("error").expect("workspace.create should error");
    assert_eq!(err["code"], -32603, "expected -32603, got: {resp}");
    assert_eq!(err["message"], json!("Internal error"));
    let detail = err["data"].as_str().unwrap_or_default();
    assert!(
        detail.starts_with("workspace.create: repository initialization failed: "),
        "expected init-failure detail in error.data, got: {resp}"
    );

    // No row persisted — no silent row-only workspace.
    let list = wss_rpc_raw(&mut rpc, 2, "workspace.list", json!({})).await;
    assert_eq!(
        list["result"]["workspaces"],
        json!([]),
        "failed create must not persist a row, got: {list}"
    );
}

/// Regression guard for the documented legacy behavior: a non-git
/// `repositoryPath` **without** `isNewRepo` still succeeds row-only — no
/// init, no worktree — and the directory stays non-git.
#[tokio::test]
async fn workspace_create_without_is_new_repo_keeps_row_only_skip() {
    let fx = boot().await;
    let mut rpc = connect(fx.port).await;
    let project = scratch_dir("legacy");

    let resp = wss_rpc_raw(
        &mut rpc,
        1,
        "workspace.create",
        json!({
            "title": "Legacy Row-Only",
            "repositoryPath": project.0.to_string_lossy(),
        }),
    )
    .await;

    assert!(
        resp.get("error").is_none(),
        "legacy create must succeed, got: {resp}"
    );
    let workspace = &resp["result"]["workspace"];
    assert!(
        workspace["worktreePath"].is_null(),
        "provisioning stays skipped, got: {workspace}"
    );
    assert!(workspace["baseCommitSha"].is_null());
    assert!(workspace["checkoutMode"].is_null());
    assert!(
        !project.0.join(".git").exists(),
        "absent isNewRepo must not initialize the dir"
    );

    // The row-only workspace is persisted and listable.
    let list = wss_rpc_raw(&mut rpc, 2, "workspace.list", json!({})).await;
    let rows = list["result"]["workspaces"]
        .as_array()
        .expect("workspaces array");
    assert_eq!(rows.len(), 1, "row-only workspace persisted, got: {list}");
    assert_eq!(rows[0]["id"], workspace["id"]);
}
