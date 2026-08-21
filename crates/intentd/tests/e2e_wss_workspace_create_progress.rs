//! WSS end-to-end tests for unified `workspace.create` provisioning progress
//! (PROTOCOL §5.1 / §6.5): a client-supplied `progressId` is echoed as
//! `data.progressId` on every `git:clone:progress` / `git:clone:done` frame
//! the create emits, percent is one non-decreasing 0–100 series ending in
//! `complete 100`, exactly one terminal done closes every create (success or
//! failure), and requests without a `progressId` stay silent on paths that
//! historically emitted nothing. Drives a real [`WsApiServer`] over plain
//! `ws://` (insecure dev mode, same pattern as
//! `e2e_wss_workspace_create_is_new_repo.rs`): WebSocket upgrade → JSON-RPC →
//! router → services → store, with events observed via `events.subscribe`.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
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

fn scratch_dir(tag: &str) -> TempDir {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = std::env::temp_dir().join(format!("intentd-progress-e2e-{tag}-{}", &short[..8]));
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

fn run_git(args: &[&str], dir: &Path) {
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
}

/// Init a small local repo with one commit; returns its guard.
fn seed_repo(tag: &str) -> TempDir {
    let dir = scratch_dir(tag);
    run_git(&["init", "-q", "-b", "main"], &dir.0);
    run_git(&["config", "user.name", "Test"], &dir.0);
    run_git(&["config", "user.email", "test@example.com"], &dir.0);
    std::fs::write(dir.0.join("README.md"), "hello\n").unwrap();
    run_git(&["add", "README.md"], &dir.0);
    run_git(&["commit", "-q", "-m", "init"], &dir.0);
    dir
}

/// Drain `events.event` frames whose `data.progressId` matches, returning
/// them in arrival order. Stops at the terminal `git:clone:done` or after
/// `secs` of quiet, whichever comes first.
async fn drain_progress_events(ws: &mut PlainWs, progress_id: &str, secs: u64) -> Vec<Value> {
    let mut out: Vec<Value> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return out;
        }
        let next = match timeout(remaining, ws.next()).await {
            Ok(x) => x,
            Err(_) => return out,
        };
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = match serde_json::from_str(&text) {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                if v["method"] == json!("events.event") {
                    let evt = &v["params"]["event"];
                    if evt["data"]["progressId"].as_str() == Some(progress_id) {
                        let done = evt["type"] == json!("git:clone:done");
                        out.push(evt.clone());
                        if done {
                            return out;
                        }
                    }
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            None => return out,
            Some(Err(_)) => return out,
        }
    }
}

/// Subscribe `ws` to the `git:clone:*` event stream.
async fn subscribe_clone_events(ws: &mut PlainWs, id: i64) {
    let resp = wss_rpc_raw(
        ws,
        id,
        "events.subscribe",
        json!({ "eventTypes": ["git:clone:progress", "git:clone:done"] }),
    )
    .await;
    assert!(
        resp["result"]["subscriptionId"].is_string(),
        "subscription ack: {resp}"
    );
}

/// Shared assertions on a successful create's frame stream: every frame
/// echoes `progressId`, percent is non-decreasing and ends at `complete 100`,
/// and exactly one `git:clone:done { ok:true }` terminates the stream.
fn assert_progress_stream(events: &[Value], progress_id: &str) {
    assert!(!events.is_empty(), "create emitted progress frames");
    for e in events {
        assert_eq!(
            e["data"]["progressId"].as_str(),
            Some(progress_id),
            "every frame echoes progressId: {e}"
        );
    }
    let progress: Vec<&Value> = events
        .iter()
        .filter(|e| e["type"] == json!("git:clone:progress"))
        .collect();
    assert!(
        progress.len() >= 2,
        "at least two progress frames: {events:?}"
    );
    let mut last = 0i64;
    for e in &progress {
        assert!(e["data"]["phase"].is_string(), "phase: {e}");
        assert!(e["data"]["message"].is_string(), "message: {e}");
        let pct = e["data"]["percent"].as_i64().expect("percent");
        assert!(
            (0..=100).contains(&pct) && pct >= last,
            "percent in range and non-decreasing: {pct} after {last} ({e})"
        );
        last = pct;
    }
    let final_progress = progress.last().unwrap();
    assert_eq!(final_progress["data"]["phase"], json!("complete"));
    assert_eq!(final_progress["data"]["percent"], json!(100));
    let dones: Vec<&Value> = events
        .iter()
        .filter(|e| e["type"] == json!("git:clone:done"))
        .collect();
    assert_eq!(dones.len(), 1, "exactly one terminal done: {events:?}");
    assert_eq!(dones[0]["data"]["ok"], json!(true), "{:?}", dones[0]);
    assert_eq!(
        events.last().unwrap()["type"],
        json!("git:clone:done"),
        "done is the terminal frame"
    );
}

/// Worktree-mode create (local repo): `workspace.create { progressId }`
/// streams milestone frames — a path that historically emitted nothing —
/// each echoing the id, ending in `complete 100` + one `done { ok:true }`,
/// all observed over the real WSS transport.
#[tokio::test]
async fn workspace_create_progress_id_streams_milestones_over_wss() {
    let fx = boot().await;
    let repo = seed_repo("wt-src");

    let mut sub = connect(fx.port).await;
    subscribe_clone_events(&mut sub, 1).await;

    let mut rpc = connect(fx.port).await;
    let resp = wss_rpc_raw(
        &mut rpc,
        2,
        "workspace.create",
        json!({
            "title": "Progress E2E",
            "repositoryPath": repo.0.to_string_lossy(),
            "progressId": "prog-e2e-wt-1",
        }),
    )
    .await;
    assert_eq!(resp["jsonrpc"], json!("2.0"), "envelope: {resp}");
    assert!(
        resp.get("error").is_none(),
        "create must succeed, got: {resp}"
    );
    let ws_id = resp["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let events = drain_progress_events(&mut sub, "prog-e2e-wt-1", 30).await;
    assert_progress_stream(&events, "prog-e2e-wt-1");
    // Frames are scoped to the newly minted workspace id.
    for e in &events {
        assert_eq!(
            e["workspaceId"].as_str(),
            Some(ws_id.as_str()),
            "frame scoped to the new workspace: {e}"
        );
    }
    // The milestone phases include the worktree provisioning step.
    let phases: Vec<&str> = events
        .iter()
        .filter(|e| e["type"] == json!("git:clone:progress"))
        .filter_map(|e| e["data"]["phase"].as_str())
        .collect();
    assert!(
        phases.contains(&"worktree"),
        "worktree milestone present: {phases:?}"
    );
}

/// Failure path: a clone that cannot succeed (`file://` URL to a missing
/// path) fails the create with a JSON-RPC error AND still closes the
/// progress stream with exactly one `git:clone:done { ok:false }` echoing
/// the `progressId`, with a sanitized error detail.
#[tokio::test]
async fn workspace_create_progress_id_failure_emits_done_ok_false_over_wss() {
    let fx = boot().await;

    let mut sub = connect(fx.port).await;
    subscribe_clone_events(&mut sub, 1).await;

    let mut rpc = connect(fx.port).await;
    let target = scratch_dir("fail-target");
    let missing = format!("/does/not/exist/{}.git", uuid::Uuid::new_v4());
    let resp = wss_rpc_raw(
        &mut rpc,
        2,
        "workspace.create",
        json!({
            "githubUrl": format!("file://{missing}"),
            "clonePath": target.0.join("checkout").to_string_lossy(),
            "progressId": "prog-e2e-fail-1",
        }),
    )
    .await;
    assert!(resp.get("error").is_some(), "create must error: {resp}");

    let events = drain_progress_events(&mut sub, "prog-e2e-fail-1", 30).await;
    let dones: Vec<&Value> = events
        .iter()
        .filter(|e| e["type"] == json!("git:clone:done"))
        .collect();
    assert_eq!(dones.len(), 1, "exactly one terminal done: {events:?}");
    let done = dones[0];
    assert_eq!(done["data"]["ok"], json!(false), "{done}");
    assert_eq!(done["data"]["progressId"], json!("prog-e2e-fail-1"));
    assert!(
        done["data"]["error"]
            .as_str()
            .is_some_and(|s| !s.is_empty()),
        "done carries the sanitized error detail: {done}"
    );
}

/// Rollback safety over the wire: the same worktree-mode create WITHOUT a
/// `progressId` emits no `git:clone:*` frames at all (legacy behavior).
#[tokio::test]
async fn workspace_create_without_progress_id_stays_silent_over_wss() {
    let fx = boot().await;
    let repo = seed_repo("legacy-src");

    let mut sub = connect(fx.port).await;
    subscribe_clone_events(&mut sub, 1).await;

    let mut rpc = connect(fx.port).await;
    let resp = wss_rpc_raw(
        &mut rpc,
        2,
        "workspace.create",
        json!({
            "title": "Legacy Silent",
            "repositoryPath": repo.0.to_string_lossy(),
        }),
    )
    .await;
    assert!(
        resp.get("error").is_none(),
        "create must succeed, got: {resp}"
    );

    // Give any (erroneous) frames a moment to arrive, then assert silence.
    let mut leaked: Vec<Value> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match timeout(remaining, sub.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                let v: Value = match serde_json::from_str(&text) {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                if v["method"] == json!("events.event") {
                    leaked.push(v["params"]["event"].clone());
                }
            }
            Ok(Some(Ok(Message::Ping(p)))) => {
                let _ = sub.send(Message::Pong(p)).await;
            }
            Ok(Some(Ok(_))) => {}
            _ => break,
        }
    }
    assert!(
        leaked.is_empty(),
        "no git:clone frames without progressId on a local create: {leaked:?}"
    );
}
