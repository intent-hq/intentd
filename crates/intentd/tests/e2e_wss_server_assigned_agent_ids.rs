//! WSS end-to-end regression tests for server-assigned agent ids: the
//! protocol no longer accepts client-supplied ids, so `agent.create` with an
//! `agentId` param and `workspace.create` with `initialAgent.agentId` are
//! both rejected with `-32602` ("server-assigned") before any side effect —
//! no agent row, no workspace row, no worktree directory under the
//! workspaces root, and no `workspace:created` event. Requests without the
//! field succeed and return a daemon-minted `agent-{uuid}` id.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

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
    workspaces_root: PathBuf,
    _dir: TempDir,
}

async fn boot() -> Fixture {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = std::env::temp_dir().join(format!("intentd-srv-id-{}", &short[..8]));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(&dir.join("intentd.db")).await.expect("store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic root");
    let services = Services::new(store)
        .with_workspaces_root(workspaces_root.clone())
        .with_settings_registry(common::registry_with_default_provider(&dir))
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
        workspaces_root,
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

/// Wait up to `secs` for the next `events.event` notification whose payload
/// `type` matches one of `types`; ignore other frames.
async fn next_event(ws: &mut PlainWs, types: &[&str], secs: u64) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!remaining.is_zero(), "timed out waiting for {types:?}");
        match timeout(remaining, ws.next())
            .await
            .expect("timeout elapsed")
        {
            Some(Ok(Message::Text(text))) => {
                let v: Value = match serde_json::from_str(&text) {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                if v["method"] == json!("events.event") {
                    let evt = &v["params"]["event"];
                    if types.contains(&evt["type"].as_str().unwrap_or("")) {
                        return evt.clone();
                    }
                }
            }
            Some(Ok(Message::Ping(_) | Message::Pong(_) | _)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// Sorted top-level entries of a directory (worktree/clone materialization probe).
fn dir_entries(path: &PathBuf) -> Vec<String> {
    let mut out: Vec<String> = std::fs::read_dir(path)
        .expect("read workspaces root")
        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
        .collect();
    out.sort();
    out
}

/// Initialize a git repository with one commit using the git CLI.
fn init_git_repo(path: &PathBuf) {
    std::fs::create_dir_all(path).unwrap();
    let run = |args: &[&str]| {
        let out = std::process::Command::new("git")
            .args(args)
            .current_dir(path)
            .output()
            .expect("git command");
        assert!(
            out.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    };
    run(&["init"]);
    run(&["config", "user.name", "Test"]);
    run(&["config", "user.email", "test@example.com"]);
    std::fs::write(path.join("README.md"), "# Test repo\n").unwrap();
    run(&["add", "README.md"]);
    run(&["commit", "-m", "Initial commit"]);
}

/// `agent.create` with a client-supplied `agentId` is rejected `-32602`
/// ("server-assigned"); the same request without the field succeeds and
/// returns a daemon-minted `agent-{uuid}` id.
#[tokio::test]
async fn agent_create_rejects_client_agent_id_and_mints_server_id() {
    let fx = boot().await;
    let mut rpc = connect(fx.port).await;

    let created = wss_rpc_raw(
        &mut rpc,
        1,
        "workspace.create",
        json!({ "title": "Host WS" }),
    )
    .await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // Stale-client shape: agentId supplied → -32602, no session persisted.
    let requested = format!("agent-{}", uuid::Uuid::new_v4());
    let rejected = wss_rpc_raw(
        &mut rpc,
        2,
        "agent.create",
        json!({ "workspaceId": ws_id, "agentId": requested, "name": "Stale" }),
    )
    .await;
    let err = rejected
        .get("error")
        .expect("agent.create with agentId must error");
    assert_eq!(err["code"], -32602, "expected -32602, got: {rejected}");
    let msg = err["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("server-assigned"),
        "error must say ids are server-assigned, got: {msg}"
    );
    let listed = wss_rpc_raw(&mut rpc, 3, "agent.list", json!({ "workspaceId": ws_id })).await;
    assert_eq!(
        listed["result"]["agents"].as_array().map(Vec::len),
        Some(0),
        "rejected create must not persist a session: {listed}"
    );

    // Without the field the daemon mints and returns the id.
    let ok = wss_rpc_raw(
        &mut rpc,
        4,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Fresh" }),
    )
    .await;
    let minted = ok["result"]["agent"]["id"]
        .as_str()
        .expect("server-minted agent id");
    let uuid_part = minted
        .strip_prefix("agent-")
        .expect("id uses the agent-{uuid} format");
    uuid::Uuid::parse_str(uuid_part).expect("suffix is a valid uuid");
}

/// `workspace.create` carrying `initialAgent.agentId` fails with `-32602`
/// BEFORE any provisioning side effect: no workspace row, no worktree/clone
/// directory under the workspaces root, and no `workspace:created` event.
#[tokio::test]
async fn workspace_create_with_initial_agent_id_leaves_no_partial_workspace() {
    let fx = boot().await;
    let mut rpc = connect(fx.port).await;

    let requested = format!("agent-{}", uuid::Uuid::new_v4());

    // Subscribe BEFORE the failing create so a leaked `workspace:created`
    // would be observed on this connection.
    let mut sub = connect(fx.port).await;
    let sub_res = wss_rpc_raw(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:created"] }),
    )
    .await;
    assert!(
        sub_res["result"]["subscriptionId"].is_string(),
        "subscription id: {sub_res}"
    );

    // A real git repo so worktree provisioning WOULD run if the create got
    // past the agentId rejection guard.
    let repo_dir =
        std::env::temp_dir().join(format!("srv-id-repo-{}", uuid::Uuid::new_v4().simple()));
    init_git_repo(&repo_dir);

    let list_before = wss_rpc_raw(&mut rpc, 3, "workspace.list", json!({})).await;
    let count_before = list_before["result"]["workspaces"]
        .as_array()
        .expect("workspaces array")
        .len();
    let entries_before = dir_entries(&fx.workspaces_root);

    let failed = wss_rpc_raw(
        &mut rpc,
        4,
        "workspace.create",
        json!({
            "title": "Stale Client",
            "repositoryPath": repo_dir.to_string_lossy(),
            "initialAgent": { "agentId": requested, "prompt": "create with stale id" },
        }),
    )
    .await;
    let err = failed
        .get("error")
        .expect("initialAgent.agentId must fail the create");
    assert_eq!(err["code"], -32602, "expected -32602, got: {failed}");
    let msg = err["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("server-assigned"),
        "error must say ids are server-assigned, got: {msg}"
    );

    // No partial workspace: row count and on-disk root entries unchanged.
    let list_after = wss_rpc_raw(&mut rpc, 5, "workspace.list", json!({})).await;
    let count_after = list_after["result"]["workspaces"]
        .as_array()
        .expect("workspaces array")
        .len();
    assert_eq!(
        count_after, count_before,
        "failed create must not persist a row"
    );
    assert_eq!(
        dir_entries(&fx.workspaces_root),
        entries_before,
        "failed create must not materialize a worktree/clone directory"
    );

    // No `workspace:created` leaked: the next event observed on the
    // subscription is the sentinel's (events are delivered in order).
    let sentinel = wss_rpc_raw(
        &mut rpc,
        6,
        "workspace.create",
        json!({ "title": "Sentinel" }),
    )
    .await;
    let sentinel_id = sentinel["result"]["workspace"]["id"]
        .as_str()
        .expect("sentinel id")
        .to_string();
    let evt = next_event(&mut sub, &["workspace:created"], 10).await;
    assert_eq!(
        evt["workspaceId"], sentinel_id,
        "first observed workspace:created must be the sentinel's (no leak from the failed create): {evt}"
    );

    let _ = std::fs::remove_dir_all(&repo_dir);
}
