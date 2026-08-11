//! WSS end-to-end test for `repo.warmCache` (PROTOCOL §5.6): drives the real
//! WS transport against a `file://` fixture repo and asserts the
//! `{ started, owner, repo }` result shape, the busy rejection envelope
//! (`error.data.code === "warm-in-flight"`), and the `-32602` invalid-URL arm.

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
    root: PathBuf,
    _dir: TempDir,
}

async fn boot() -> Fixture {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = std::env::temp_dir().join(format!("intentd-warm-{}", &short[..8]));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(&dir.join("intentd.db")).await.expect("store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic root");
    let services = Services::new(store)
        .with_workspaces_root(workspaces_root.clone())
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
        root: workspaces_root,
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

async fn wss_rpc(ws: &mut PlainWs, id: i64, method: &str, params: Value) -> Value {
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

/// Init a small git repo with one commit using the git CLI; returns its path.
fn seed_repo(dir: &PathBuf) {
    std::fs::create_dir_all(dir).unwrap();
    let git = |args: &[&str]| {
        let out = std::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .expect("git");
        assert!(out.status.success(), "git {args:?}: {out:?}");
    };
    git(&["init"]);
    git(&["config", "user.name", "Test"]);
    git(&["config", "user.email", "test@example.com"]);
    std::fs::write(dir.join("README.md"), "# warm fixture\n").unwrap();
    git(&["add", "README.md"]);
    git(&["commit", "-m", "Initial commit"]);
}

/// Owner/repo as the daemon derives them from a `file://` URL: the last two
/// path segments.
fn owner_repo_of(dir: &std::path::Path) -> (String, String) {
    let repo = dir.file_name().unwrap().to_str().unwrap().to_string();
    let owner = dir
        .parent()
        .and_then(|p| p.file_name())
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    (owner, repo)
}

/// Poll `repo.warmCache` until the detached warm completes: an accepted
/// re-warm proves the in-flight flag cleared; the populated cache slot
/// proves the ensure ran.
async fn wait_for_warm_completion(ws: &mut PlainWs, root: &std::path::Path, url: &str, base: i64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut id = base;
    loop {
        let v = wss_rpc(ws, id, "repo.warmCache", json!({ "githubUrl": url })).await;
        id += 1;
        if let Some(result) = v.get("result").filter(|r| !r.is_null()) {
            let cache = root
                .join(".repo-cache")
                .join(result["owner"].as_str().unwrap())
                .join(result["repo"].as_str().unwrap());
            assert!(cache.join(".git").exists(), "repo cache populated");
            return;
        }
        assert_eq!(
            v["error"]["data"]["code"],
            json!("warm-in-flight"),
            "only the busy error is expected while polling: {v}"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "warm did not complete in time"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Happy path over WSS: `repo.warmCache` returns the documented
/// `{ started: true, owner, repo }` result immediately, and the detached
/// ensure populates `<root>/.repo-cache/<owner>/<repo>` from the `file://`
/// fixture.
#[tokio::test]
async fn repo_warm_cache_starts_and_populates_cache_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(fx.port).await;

    let repo_dir = fx.root.parent().unwrap().join("warm-fixture-src");
    seed_repo(&repo_dir);
    let (owner, repo) = owner_repo_of(&repo_dir);
    let url = format!("file://{}", repo_dir.to_string_lossy());

    let v = wss_rpc(&mut rpc, 1, "repo.warmCache", json!({ "githubUrl": url })).await;
    assert_eq!(v["jsonrpc"], json!("2.0"));
    assert_eq!(v["id"], json!(1));
    assert_eq!(
        v["result"],
        json!({ "started": true, "owner": owner, "repo": repo }),
        "result shape per PROTOCOL §5.6"
    );

    wait_for_warm_completion(&mut rpc, &fx.root, &url, 100).await;
}

/// Busy rejection over WSS: with the warm's ensure parked behind the held
/// per-repo cache lock, a second `repo.warmCache` is rejected with `-32603`
/// carrying `error.data = { code: "warm-in-flight", owner, repo }`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repo_warm_cache_busy_rejection_envelope_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(fx.port).await;

    let repo_dir = fx.root.parent().unwrap().join("warm-busy-src");
    seed_repo(&repo_dir);
    let (owner, repo) = owner_repo_of(&repo_dir);
    let url = format!("file://{}", repo_dir.to_string_lossy());

    // Hold the per-repo cache lock (same in-process lock map as the daemon
    // services) so the accepted warm's ensure parks and the in-flight window
    // is deterministic.
    let cache_path = fx.root.join(".repo-cache").join(&owner).join(&repo);
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let (held_tx, held_rx) = std::sync::mpsc::channel::<()>();
    let lock_holder = tokio::spawn(async move {
        intent_git::repo_cache::with_cache_lock_blocking(&cache_path, move || {
            held_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok(())
        })
        .await
    });
    held_rx.recv_timeout(Duration::from_secs(10)).unwrap();

    let v = wss_rpc(&mut rpc, 1, "repo.warmCache", json!({ "githubUrl": url })).await;
    assert_eq!(v["result"]["started"], json!(true), "first warm accepted");

    let v = wss_rpc(&mut rpc, 2, "repo.warmCache", json!({ "githubUrl": url })).await;
    assert_eq!(v["jsonrpc"], json!("2.0"));
    assert_eq!(v["id"], json!(2));
    assert_eq!(v["error"]["code"], json!(-32603));
    assert_eq!(
        v["error"]["data"],
        json!({ "code": "warm-in-flight", "owner": owner, "repo": repo }),
        "busy envelope names the repo being warmed"
    );

    release_tx.send(()).unwrap();
    lock_holder.await.unwrap().unwrap();
    wait_for_warm_completion(&mut rpc, &fx.root, &url, 100).await;
}

/// Invalid URL over WSS: a `githubUrl` with no owner/repo pair is `-32602`,
/// and a missing `githubUrl` param is `-32602` as well.
#[tokio::test]
async fn repo_warm_cache_invalid_params_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(fx.port).await;

    let v = wss_rpc(
        &mut rpc,
        1,
        "repo.warmCache",
        json!({ "githubUrl": "not-a-repo-url" }),
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32602));

    let v = wss_rpc(&mut rpc, 2, "repo.warmCache", json!({})).await;
    assert_eq!(v["error"]["code"], json!(-32602));
}
