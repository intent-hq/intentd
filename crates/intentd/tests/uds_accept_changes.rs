//! Over-the-wire `accept-changes.execute` slice for the deferred git actions
//! (undo-commit / undo-push / reset-to-trunk / rebase-onto-trunk / merge): drive
//! each action against a real worktree through the daemon over a temp UDS.

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use intent_core::{
    now_iso, Config, Workspace, WorkspaceActivity, WorkspaceApi, WorkspaceAttention, WorkspaceId,
    WorkspaceStatus,
};
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::serve_uds;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

fn git(repo: &Path, args: &[&str]) {
    let ok = Command::new("git")
        .current_dir(repo)
        .args(args)
        .status()
        .expect("run git")
        .success();
    assert!(ok, "git {args:?} failed");
}

fn git_out(repo: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .current_dir(repo)
        .args(args)
        .output()
        .expect("run git");
    assert!(out.status.success(), "git {args:?} failed");
    String::from_utf8(out.stdout)
        .expect("utf8")
        .trim()
        .to_string()
}

/// Init a repo on `main` with a single committed file; returns the commit SHA.
fn seed_main(repo: &Path) -> String {
    std::fs::create_dir_all(repo).unwrap();
    git(repo, &["-c", "init.defaultBranch=main", "init", "-q"]);
    git(repo, &["config", "user.name", "Test"]);
    git(repo, &["config", "user.email", "test@example.com"]);
    git(repo, &["config", "commit.gpgsign", "false"]);
    std::fs::write(repo.join("seed.txt"), "seed\n").unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-q", "-m", "seed"]);
    git_out(repo, &["rev-parse", "HEAD"])
}

fn commit_file(repo: &Path, name: &str, body: &str, msg: &str) -> String {
    std::fs::write(repo.join(name), body).unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-q", "-m", msg]);
    git_out(repo, &["rev-parse", "HEAD"])
}

fn make_config(data_dir: &Path) -> Config {
    Config {
        data_dir: data_dir.to_path_buf(),
        config_path: data_dir.join("config.toml"),
        db_path: data_dir.join("intentd.db"),
        socket_path: data_dir.join("intentd.sock"),
        pid_path: data_dir.join("intentd.pid"),
        idle_reap_minutes: 0,
        stream_retention_hours: 0,
        hooks_max_per_agent: 5,
        server_max_outstanding_rpcs: 0,
        wake_resume_enabled: true,
        wake_resume_threshold_seconds: 10,
    }
}

fn workspace(id: &WorkspaceId, worktree: &str, branch: &str) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "AC WS".to_string(),
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
        path: Some(worktree.to_string()),
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: Some(worktree.to_string()),
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
        execution_environment: None,
        disk_usage: None,
        pending_delete_at: None,
    }
}

async fn send(socket: &Path, frame: &str) -> Value {
    let stream = UnixStream::connect(socket).await.expect("connect");
    let (read_half, mut write_half) = stream.into_split();
    write_half.write_all(frame.as_bytes()).await.expect("write");
    write_half.write_all(b"\n").await.expect("write nl");
    write_half.flush().await.expect("flush");
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    reader.read_line(&mut line).await.expect("read");
    serde_json::from_str(line.trim()).expect("valid json")
}

/// Insert `ws` and start the daemon over `config.socket_path`; returns the server
/// handle and the shutdown sender.
async fn serve(
    config: &Config,
    ws: &Workspace,
) -> (
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Sender<()>,
    tempfile::TempDir,
) {
    {
        let store = Store::open(&config.db_path).await.expect("open store");
        store.insert_workspace(ws).await.expect("seed ws");
    }
    let store = Store::open(&config.db_path).await.expect("reopen store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> =
        Arc::new(Services::new(store).with_workspaces_root(ws_root.path().to_path_buf()));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let socket = config.socket_path.clone();
    let handle = tokio::spawn(async move {
        serve_uds(services, bus, &socket, None, async move {
            let _ = rx.await;
        })
        .await
        .expect("serve");
    });
    for _ in 0..50 {
        if config.socket_path.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    (handle, tx, ws_root)
}

fn tmp_base(tag: &str) -> PathBuf {
    let short = uuid::Uuid::new_v4().simple().to_string();
    Path::new("/tmp").join(format!("intentd-ac-{tag}-{}", &short[..8]))
}

#[tokio::test]
async fn undo_commit_soft_resets_and_restores_staging() {
    let base = tmp_base("undo-commit");
    let data_dir = base.join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let repo = base.join("repo");
    let first = seed_main(&repo);
    commit_file(&repo, "f2.txt", "two\n", "second");

    let config = make_config(&data_dir);
    let ws_id = WorkspaceId::from("ws-undo-commit");
    let ws = workspace(&ws_id, repo.to_str().unwrap(), "main");
    let (handle, tx, _ws_root) = serve(&config, &ws).await;

    // Missing hash → parity error with no step pushed.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":1,"method":"accept-changes.execute","params":{"workspaceId":"ws-undo-commit","action":"undo-commit"}}"#,
    )
    .await;
    assert_eq!(resp["result"]["success"], json!(false));
    assert_eq!(
        resp["result"]["error"],
        json!("Commit hash required for undo-commit")
    );
    assert_eq!(resp["result"]["steps"], json!([]));

    // Soft-reset back to the first commit, keeping the later change staged.
    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"accept-changes.execute","params":{{"workspaceId":"ws-undo-commit","action":"undo-commit","upToCommitHash":"{first}"}}}}"#
    );
    let resp = send(&config.socket_path, &frame).await;
    assert_eq!(resp["result"]["success"], json!(true), "resp: {resp}");
    assert!(resp["result"].get("result").is_none());
    let steps = resp["result"]["steps"].as_array().unwrap();
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0]["id"], json!("undo-commit"));
    assert_eq!(steps[0]["status"], json!("completed"));

    assert_eq!(git_out(&repo, &["rev-parse", "HEAD"]), first);
    let staged = git_out(&repo, &["diff", "--cached", "--name-only"]);
    assert!(
        staged.contains("f2.txt"),
        "f2.txt should be staged: {staged}"
    );

    let _ = tx.send(());
    let _ = handle.await;
    std::fs::remove_dir_all(&base).ok();
}

#[tokio::test]
async fn reset_to_trunk_guards_dirty_then_hard_resets() {
    let base = tmp_base("reset");
    let data_dir = base.join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let repo = base.join("repo");
    let main_tip = seed_main(&repo);
    git(&repo, &["checkout", "-q", "-b", "feature"]);
    commit_file(&repo, "feature.txt", "feat\n", "feature work");

    let config = make_config(&data_dir);
    let ws_id = WorkspaceId::from("ws-reset");
    let ws = workspace(&ws_id, repo.to_str().unwrap(), "feature");
    let (handle, tx, _ws_root) = serve(&config, &ws).await;

    // Dirty worktree → reset is refused.
    std::fs::write(repo.join("dirty.txt"), "x\n").unwrap();
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":1,"method":"accept-changes.execute","params":{"workspaceId":"ws-reset","action":"reset-to-trunk"}}"#,
    )
    .await;
    assert_eq!(resp["result"]["success"], json!(false));
    assert!(resp["result"]["error"]
        .as_str()
        .unwrap()
        .contains("uncommitted or staged changes"));
    std::fs::remove_file(repo.join("dirty.txt")).unwrap();

    // Clean worktree → hard reset to the trunk tip.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":2,"method":"accept-changes.execute","params":{"workspaceId":"ws-reset","action":"reset-to-trunk"}}"#,
    )
    .await;
    assert_eq!(resp["result"]["success"], json!(true), "resp: {resp}");
    assert_eq!(resp["result"]["result"]["newHeadSha"], json!(main_tip));
    assert_eq!(git_out(&repo, &["rev-parse", "HEAD"]), main_tip);

    let _ = tx.send(());
    let _ = handle.await;
    std::fs::remove_dir_all(&base).ok();
}

#[tokio::test]
async fn rebase_onto_trunk_replays_branch() {
    let base = tmp_base("rebase");
    let data_dir = base.join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let repo = base.join("repo");
    seed_main(&repo);
    git(&repo, &["checkout", "-q", "-b", "feature"]);
    commit_file(&repo, "feature.txt", "feat\n", "feature work");
    git(&repo, &["checkout", "-q", "main"]);
    let main_tip = commit_file(&repo, "trunk.txt", "trunk\n", "trunk advance");
    git(&repo, &["checkout", "-q", "feature"]);

    let config = make_config(&data_dir);
    let ws_id = WorkspaceId::from("ws-rebase");
    let ws = workspace(&ws_id, repo.to_str().unwrap(), "feature");
    let (handle, tx, _ws_root) = serve(&config, &ws).await;

    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":1,"method":"accept-changes.execute","params":{"workspaceId":"ws-rebase","action":"rebase-onto-trunk"}}"#,
    )
    .await;
    assert_eq!(resp["result"]["success"], json!(true), "resp: {resp}");
    assert_eq!(resp["result"]["result"]["autoRebased"], json!(true));
    assert_eq!(resp["result"]["result"]["newBaseSha"], json!(main_tip));
    // Trunk is now an ancestor of the rebased feature head; both files present.
    assert_eq!(git_out(&repo, &["merge-base", "main", "HEAD"]), main_tip);
    assert!(repo.join("trunk.txt").exists());
    assert!(repo.join("feature.txt").exists());

    let _ = tx.send(());
    let _ = handle.await;
    std::fs::remove_dir_all(&base).ok();
}

#[tokio::test]
async fn merge_local_fast_forwards_trunk() {
    let base = tmp_base("merge");
    let data_dir = base.join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let repo = base.join("repo");
    seed_main(&repo);
    git(&repo, &["checkout", "-q", "-b", "feature"]);
    let feature_head = commit_file(&repo, "feature.txt", "feat\n", "feature work");

    let config = make_config(&data_dir);
    let ws_id = WorkspaceId::from("ws-merge");
    let ws = workspace(&ws_id, repo.to_str().unwrap(), "feature");
    let (handle, tx, _ws_root) = serve(&config, &ws).await;

    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":1,"method":"accept-changes.execute","params":{"workspaceId":"ws-merge","action":"merge"}}"#,
    )
    .await;
    assert_eq!(resp["result"]["success"], json!(true), "resp: {resp}");
    assert_eq!(
        resp["result"]["result"]["mergeCommitHash"],
        json!(feature_head)
    );
    assert_eq!(git_out(&repo, &["rev-parse", "main"]), feature_head);

    let _ = tx.send(());
    let _ = handle.await;
    std::fs::remove_dir_all(&base).ok();
}

#[tokio::test]
async fn merge_squash_creates_single_commit_on_trunk() {
    let base = tmp_base("squash");
    let data_dir = base.join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let repo = base.join("repo");
    let first = seed_main(&repo);
    git(&repo, &["checkout", "-q", "-b", "feature"]);
    let feature_head = commit_file(&repo, "feature.txt", "feat\n", "feature work");

    let config = make_config(&data_dir);
    let ws_id = WorkspaceId::from("ws-squash");
    let ws = workspace(&ws_id, repo.to_str().unwrap(), "feature");
    let (handle, tx, _ws_root) = serve(&config, &ws).await;

    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":1,"method":"accept-changes.execute","params":{"workspaceId":"ws-squash","action":"merge","mergeStrategy":"squash","commitMessage":"squashed"}}"#,
    )
    .await;
    assert_eq!(resp["result"]["success"], json!(true), "resp: {resp}");
    let squash = resp["result"]["result"]["mergeCommitHash"]
        .as_str()
        .unwrap()
        .to_string();
    assert_ne!(squash, feature_head);
    assert_eq!(git_out(&repo, &["rev-parse", "main"]), squash);
    // The squash commit sits directly on the old trunk tip.
    assert_eq!(git_out(&repo, &["rev-parse", "main^"]), first);

    let _ = tx.send(());
    let _ = handle.await;
    std::fs::remove_dir_all(&base).ok();
}

#[allow(clippy::similar_names)] // deliberate parallel naming across the scenario's instances
#[tokio::test]
async fn undo_push_rewinds_remote_branch() {
    let base = tmp_base("undo-push");
    let data_dir = base.join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let repo = base.join("repo");
    let first = seed_main(&repo);
    let second = commit_file(&repo, "f2.txt", "two\n", "second");

    // Bare remote carrying `main` at the second commit.
    let bare = base.join("remote.git");
    git(&base, &["init", "-q", "--bare", "remote.git"]);
    git(&repo, &["remote", "add", "origin", bare.to_str().unwrap()]);
    git(&repo, &["push", "-q", "origin", "main"]);
    assert_eq!(git_out(&bare, &["rev-parse", "main"]), second);

    let config = make_config(&data_dir);
    let ws_id = WorkspaceId::from("ws-undo-push");
    let ws = workspace(&ws_id, repo.to_str().unwrap(), "main");
    let (handle, tx, _ws_root) = serve(&config, &ws).await;

    let frame = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"accept-changes.execute","params":{{"workspaceId":"ws-undo-push","action":"undo-push","upToCommitHash":"{first}"}}}}"#
    );
    let resp = send(&config.socket_path, &frame).await;
    assert_eq!(resp["result"]["success"], json!(true), "resp: {resp}");
    let steps = resp["result"]["steps"].as_array().unwrap();
    assert_eq!(steps[0]["id"], json!("undo-push"));
    assert_eq!(steps[0]["status"], json!("completed"));

    // The remote branch was rewound to the first commit; local HEAD is unchanged.
    assert_eq!(git_out(&bare, &["rev-parse", "main"]), first);
    assert_eq!(git_out(&repo, &["rev-parse", "HEAD"]), second);

    let _ = tx.send(());
    let _ = handle.await;
    std::fs::remove_dir_all(&base).ok();
}
