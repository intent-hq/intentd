//! `git.commit` change-event emissions (FIX 2 parity): drives `git.commit`
//! against a live UDS listener and asserts the daemon publishes both
//! `git:commit` (reserved `GitOperationEvent` FE shape) and
//! `changes:git-status` (feeds the FE bridge's `git:status-changed` relay)
//! per PROTOCOL §6.5. Gated on `git` being on PATH; skips cleanly otherwise.

#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use intent_core::{now_iso, WorkspaceApi, WorkspaceAttention, WorkspaceStatus};
use intent_core::{Workspace, WorkspaceActivity, WorkspaceId};
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::serve_uds;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{unix::OwnedReadHalf, UnixStream};
use tokio::sync::oneshot;
use tokio::time::timeout;
use uuid::Uuid;

struct TempDb {
    path: PathBuf,
}

impl TempDb {
    fn new() -> Self {
        Self {
            path: std::env::temp_dir().join(format!("intentd-uds-{}.db", Uuid::new_v4())),
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

struct TempRepo {
    path: PathBuf,
}

impl TempRepo {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("intentd-repo-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&path).expect("mkdir repo");
        Self { path }
    }
}

impl Drop for TempRepo {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn gate() -> bool {
    Command::new("git")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

fn git(cwd: &Path, args: &[&str]) {
    let status = Command::new("git")
        .args(args)
        .env("GIT_AUTHOR_NAME", "e2e")
        .env("GIT_AUTHOR_EMAIL", "e2e@example.com")
        .env("GIT_COMMITTER_NAME", "e2e")
        .env("GIT_COMMITTER_EMAIL", "e2e@example.com")
        .current_dir(cwd)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("run git");
    assert!(status.success(), "git {args:?} failed");
}

fn workspace_row(id: &WorkspaceId, worktree: &Path, branch: &str) -> Workspace {
    let ts = now_iso();
    let path = worktree.to_string_lossy().to_string();
    Workspace {
        id: id.clone(),
        title: "GC WS".to_string(),
        branch: branch.to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: ts.clone(),
        updated_at: ts,
        last_activity: None,
        tags: vec![],
        path: Some(path.clone()),
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: Some(path),
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        active_pull_request: None,
        pull_requests: None,
        context_links: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
    }
}

async fn connect_retry(socket: &Path) -> UnixStream {
    let budget = common::daemon_startup_timeout();
    tokio::time::timeout(budget, async {
        loop {
            if let Ok(s) = UnixStream::connect(socket).await {
                return s;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "daemon startup timed out: no connection to {} within {budget:?}",
            socket.display()
        )
    })
}

async fn send(w: &mut (impl AsyncWriteExt + Unpin), frame: &str) {
    w.write_all(frame.as_bytes()).await.unwrap();
    w.write_all(b"\n").await.unwrap();
    w.flush().await.unwrap();
}

async fn read_json(r: &mut BufReader<OwnedReadHalf>) -> Value {
    let mut line = String::new();
    let n = timeout(common::rpc_read_timeout(), r.read_line(&mut line))
        .await
        .expect("timed out reading frame")
        .expect("read failed");
    assert!(n > 0, "unexpected EOF");
    serde_json::from_str(line.trim_end()).expect("invalid JSON")
}

async fn wait_for_sub_count(bus: &EventBus, target: usize) {
    for _ in 0..200 {
        if bus.subscriber_count() == target {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "subscriber_count never reached {target} (last={})",
        bus.subscriber_count()
    );
}

fn boot(
    store: Store,
) -> (
    PathBuf,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
    EventBus,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> = Arc::new(
        Services::new(store)
            .with_workspaces_root(ws_root.path().to_path_buf())
            .with_event_bus(bus.clone()),
    );
    // Socket lives in a guarded dir under /tmp so the path stays short
    // (macOS SUN_LEN) and the file is swept even if the test panics.
    let sock_dir = common::test_tempdir_in("/tmp", "itd-uds-");
    let socket = sock_dir.path().join("uds.sock");
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let bus_clone = bus.clone();
    let socket_clone = socket.clone();
    let server = tokio::spawn(async move {
        let _ = serve_uds(services, bus_clone, &socket_clone, None, async {
            let _ = shutdown_rx.await;
        })
        .await;
    });
    (socket, shutdown_tx, server, bus, ws_root, sock_dir)
}

/// `git.commit` inside a workspace worktree publishes both `git:commit`
/// (reserved `GitOperationEvent` FE shape) and `changes:git-status` (feeds
/// the FE bridge's `git:status-changed` relay) per PROTOCOL §6.5. Emissions
/// live inside the idempotency scope so a replayed commit (same
/// idempotencyKey) returns the cached result without re-firing.
#[tokio::test]
async fn git_commit_emits_git_commit_and_changes_git_status_over_uds() {
    if !gate() {
        eprintln!("skipping git.commit UDS e2e: git not on PATH");
        return;
    }
    let tmp_db = TempDb::new();
    let store = Store::open(&tmp_db.path).await.expect("open store");

    // Real git worktree with an initial commit + one modified file staged.
    let repo_dir = TempRepo::new();
    let repo = repo_dir.path.as_path();
    git(repo, &["init", "-q", "-b", "main"]);
    // Repo-local identity: the daemon-side commit uses libgit2's
    // `repo.signature()`, which reads git config (not GIT_AUTHOR_* env vars),
    // so bare CI runners need these set (same as uds_git.rs).
    git(repo, &["config", "user.name", "Test"]);
    git(repo, &["config", "user.email", "test@example.com"]);
    git(repo, &["config", "commit.gpgsign", "false"]);
    std::fs::write(repo.join("README.md"), "hello\n").unwrap();
    git(repo, &["add", "README.md"]);
    git(repo, &["commit", "-q", "-m", "init"]);
    // A second dirty change to give the commit content.
    std::fs::write(repo.join("README.md"), "hello world\n").unwrap();
    git(repo, &["add", "README.md"]);

    // Persist the workspace row pointing at the worktree so `git.commit`
    // (which is workspace-scoped) can resolve the path.
    let ws_id = WorkspaceId::from("ws-gc-1");
    let ws = workspace_row(&ws_id, repo, "main");
    store.insert_workspace(&ws).await.expect("insert workspace");

    let (socket, shutdown, server, bus, _ws_root, _sock_dir) = boot(store);

    // Subscriber first (change events are point-in-time; no replay).
    let (sub_read, mut sub_write) = connect_retry(&socket).await.into_split();
    let mut sub_reader = BufReader::new(sub_read);
    send(
        &mut sub_write,
        &serde_json::to_string(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "events.subscribe",
            "params": {
                "eventTypes": ["git:commit", "changes:git-status"],
                "workspaceId": ws_id,
            },
        }))
        .unwrap(),
    )
    .await;
    let sub_resp = read_json(&mut sub_reader).await;
    assert!(sub_resp["result"]["subscriptionId"].is_string());
    wait_for_sub_count(&bus, 1).await;

    // Drive git.commit over a separate connection.
    let (rpc_read, mut rpc_write) = connect_retry(&socket).await.into_split();
    let mut rpc_reader = BufReader::new(rpc_read);
    send(
        &mut rpc_write,
        &serde_json::to_string(&json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "git.commit",
            "params": {
                "workspaceId": ws_id,
                "message": "second commit",
                "idempotencyKey": "gc-key-1",
            },
        }))
        .unwrap(),
    )
    .await;
    let rpc_resp = read_json(&mut rpc_reader).await;
    assert_eq!(rpc_resp["result"]["ok"], json!(true));
    let commit_hash = rpc_resp["result"]["hash"]
        .as_str()
        .expect("hash")
        .to_string();

    // Read the next two events; ordering is git:commit then
    // changes:git-status per the emitter (see intent-services::lib.rs).
    let ev1 = read_json(&mut sub_reader).await;
    let e1 = &ev1["params"]["event"];
    assert_eq!(e1["type"], "git:commit");
    assert_eq!(e1["workspaceId"], ws_id.as_str());
    assert_eq!(e1["data"]["operation"], "commit");
    assert_eq!(e1["data"]["commit"], commit_hash);
    assert_eq!(e1["data"]["message"], "second commit");
    assert!(e1["data"]["files"].is_array(), "files array present: {e1}");

    let ev2 = read_json(&mut sub_reader).await;
    let e2 = &ev2["params"]["event"];
    assert_eq!(e2["type"], "changes:git-status");
    assert_eq!(e2["workspaceId"], ws_id.as_str());
    // The status payload is the GitStatus snapshot; assert at least the
    // camelCase envelope so a client can render without further reads.
    assert!(e2["data"]["status"].is_object(), "status object: {e2}");

    let _ = shutdown.send(());
    let _ = server.await;
}
