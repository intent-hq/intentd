//! Over-the-wire git write-ops slice: drive `git.status`, `git.agentCommit`, and
//! `git.commit` against a real worktree through the daemon over a temp UDS.

mod common;

use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Serializes the `INTENTD_DATA_DIR` env-var set + `Config::resolve()` across the
/// tests in this binary: the var is process-global, so concurrent setup would
/// race and make both tests resolve the same db path (→ "database is locked").
static ENV_LOCK: Mutex<()> = Mutex::new(());

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

fn seed_repo(repo: &Path) {
    std::fs::create_dir_all(repo).unwrap();
    git(repo, &["init", "-q"]);
    git(repo, &["config", "user.name", "Test"]);
    git(repo, &["config", "user.email", "test@example.com"]);
    git(repo, &["config", "commit.gpgsign", "false"]);
    std::fs::write(repo.join("seed.txt"), "seed\n").unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-q", "-m", "seed"]);
}

fn seed_workspace(id: &WorkspaceId, worktree: &str) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "Git WS".to_string(),
        branch: "main".to_string(),
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

#[tokio::test]
async fn uds_git_write_ops_round_trip() {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let base = Path::new("/tmp").join(format!("intentd-git-{}", &short[..8]));
    let data_dir = base.join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let repo = base.join("repo");
    seed_repo(&repo);

    let config = {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("INTENTD_DATA_DIR", &data_dir);
        Config::resolve().expect("resolve config")
    };

    let ws_id = WorkspaceId::from("ws-git");
    {
        let store = Store::open(&config.db_path).await.expect("open store");
        store
            .insert_workspace(&seed_workspace(&ws_id, repo.to_str().unwrap()))
            .await
            .expect("seed ws");
    }

    let store = Store::open(&config.db_path).await.expect("reopen store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> =
        Arc::new(Services::new(store).with_workspaces_root(ws_root.path().to_path_buf()));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let socket = config.socket_path.clone();
    let server = tokio::spawn(async move {
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

    // Modify a tracked file and add an untracked one.
    std::fs::write(repo.join("seed.txt"), "seed changed\n").unwrap();
    std::fs::write(repo.join("new.txt"), "new file\n").unwrap();

    // (a) git.status reflects the uncommitted + untracked changes.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":1,"method":"git.status","params":{"workspaceId":"ws-git"}}"#,
    )
    .await;
    assert_eq!(resp["result"]["hasUncommittedChanges"], json!(true));
    assert_eq!(resp["result"]["hasUntrackedFiles"], json!(true));

    // (b) git.agentCommit stages + commits the named files.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":2,"method":"git.agentCommit","params":{"workspaceId":"ws-git","message":"commit work","files":["seed.txt","new.txt"]}}"#,
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true));
    assert_eq!(resp["result"]["fileCount"], json!(2));
    assert_eq!(resp["result"]["hash"].as_str().expect("hash").len(), 40);
    let files = resp["result"]["files"].as_array().expect("files array");
    assert!(files.iter().any(|f| f == &json!("seed.txt")));
    assert!(files.iter().any(|f| f == &json!("new.txt")));

    // (c) git.status is now clean.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":3,"method":"git.status","params":{"workspaceId":"ws-git"}}"#,
    )
    .await;
    assert_eq!(resp["result"]["hasUncommittedChanges"], json!(false));
    assert_eq!(resp["result"]["files"], json!([]));
    let branch = resp["result"]["branch"]
        .as_str()
        .expect("branch")
        .to_string();

    // (d) git.commit with nothing staged → -32603 (nothing to commit).
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":4,"method":"git.commit","params":{"workspaceId":"ws-git","message":"empty"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32603));

    // (e) git.checkMergeConflicts against the same branch short-circuits clean.
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":5,"method":"git.checkMergeConflicts","params":{{"workspaceId":"ws-git","targetBranch":"{branch}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["hasConflicts"], json!(false));
    assert_eq!(resp["result"]["targetBranch"], json!(branch));
    assert_eq!(resp["result"]["currentBranch"], json!(branch));

    // (f) git.stage → git.unstage round-trip mirrors the git.stage shape.
    std::fs::write(repo.join("seed.txt"), "seed changed again\n").unwrap();
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":6,"method":"git.stage","params":{"workspaceId":"ws-git","paths":["seed.txt"]}}"#,
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true));
    assert_eq!(resp["result"]["paths"], json!(["seed.txt"]));
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":7,"method":"git.status","params":{"workspaceId":"ws-git"}}"#,
    )
    .await;
    let staged = resp["result"]["files"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["path"] == json!("seed.txt"))
        .expect("seed.txt status");
    assert_eq!(staged["staged"], json!(true));

    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":8,"method":"git.unstage","params":{"workspaceId":"ws-git","paths":["seed.txt"]}}"#,
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true));
    assert_eq!(resp["result"]["paths"], json!(["seed.txt"]));
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":9,"method":"git.status","params":{"workspaceId":"ws-git"}}"#,
    )
    .await;
    let unstaged = resp["result"]["files"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["path"] == json!("seed.txt"))
        .expect("seed.txt status");
    assert_eq!(unstaged["staged"], json!(false));

    // (g) git.unstage is idempotent — a second call on the already-unstaged
    // path returns ok without erroring.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":10,"method":"git.unstage","params":{"workspaceId":"ws-git","paths":["seed.txt"]}}"#,
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true));
    assert_eq!(resp["result"]["paths"], json!(["seed.txt"]));

    // (h) git.agentCommit userRequested with no `files` commits only the
    // already-staged paths — the unstaged edit stays in the worktree.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":11,"method":"git.stage","params":{"workspaceId":"ws-git","paths":["seed.txt"]}}"#,
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true));
    std::fs::write(repo.join("other.txt"), "unstaged\n").unwrap();
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":12,"method":"git.agentCommit","params":{"workspaceId":"ws-git","message":"user checkpoint","userRequested":true}}"#,
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true));
    assert_eq!(resp["result"]["files"], json!(["seed.txt"]));
    assert_eq!(resp["result"]["fileCount"], json!(1));
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":13,"method":"git.status","params":{"workspaceId":"ws-git"}}"#,
    )
    .await;
    let files = resp["result"]["files"].as_array().expect("files array");
    assert!(
        files.iter().any(|f| f["path"] == json!("other.txt")),
        "unstaged file survives the userRequested commit: {files:?}"
    );
    assert!(
        files.iter().all(|f| f["path"] != json!("seed.txt")),
        "staged file was committed: {files:?}"
    );

    // (i) userRequested with a clean index → -32603 (nothing staged).
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":14,"method":"git.agentCommit","params":{"workspaceId":"ws-git","message":"empty checkpoint","userRequested":true}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32603));

    // (i2) agent-initiated (no `files`, no `userRequested`) with no agent
    // context: the FE/transport path never carries an agentId, so the
    // attribution-filtered fallback (monorepo#939) refuses with -32603
    // instead of sweeping the dirty worktree (other.txt is still untracked
    // from step (h)).
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":30,"method":"git.agentCommit","params":{"workspaceId":"ws-git","message":"agent sweep"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32603));
    assert!(
        resp["error"]["data"]
            .as_str()
            .unwrap()
            .contains("cannot be attributed"),
        "refusal names the attribution gap: {resp}"
    );
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":31,"method":"git.status","params":{"workspaceId":"ws-git"}}"#,
    )
    .await;
    let files = resp["result"]["files"].as_array().expect("files array");
    assert!(
        files.iter().any(|f| f["path"] == json!("other.txt")),
        "refusal left the dirty worktree untouched: {files:?}"
    );

    // (j) git.discard restores an unstaged tracked modification from the
    // index. After step (h) HEAD/index have seed.txt = "seed changed again\n"
    // (the userRequested checkpoint), so discard must restore to that content
    // — matching `git checkout -- seed.txt` semantics.
    std::fs::write(repo.join("seed.txt"), "seed dirty\n").unwrap();
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":15,"method":"git.discard","params":{"workspaceId":"ws-git","paths":["seed.txt"]}}"#,
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true));
    assert_eq!(resp["result"]["paths"], json!(["seed.txt"]));
    let on_disk = std::fs::read_to_string(repo.join("seed.txt")).unwrap();
    assert_eq!(on_disk, "seed changed again\n");

    // (k) git.discard on an untracked path deletes it from disk. other.txt
    // was left untracked by step (h) with contents "unstaged\n".
    assert!(repo.join("other.txt").exists());
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":16,"method":"git.discard","params":{"workspaceId":"ws-git","paths":["other.txt"]}}"#,
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true));
    assert_eq!(resp["result"]["paths"], json!(["other.txt"]));
    assert!(!repo.join("other.txt").exists());

    // (l) idempotent on a now-clean tracked path.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":17,"method":"git.discard","params":{"workspaceId":"ws-git","paths":["seed.txt"]}}"#,
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true));
    assert_eq!(resp["result"]["paths"], json!(["seed.txt"]));

    // (m) missing untracked path is ignored (ENOENT parity).
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":18,"method":"git.discard","params":{"workspaceId":"ws-git","paths":["nope.txt"]}}"#,
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true));
    assert_eq!(resp["result"]["paths"], json!(["nope.txt"]));

    // (n) `.`/`*`/`--all` is rejected with -32603 (matches git.stage).
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":19,"method":"git.discard","params":{"workspaceId":"ws-git","paths":"."}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32603));

    // (o) missing `paths` param → -32602.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":20,"method":"git.discard","params":{"workspaceId":"ws-git"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));

    let _ = tx.send(());
    let _ = server.await;
    std::fs::remove_dir_all(&base).ok();
}

/// Over-the-wire coverage for the write methods added alongside the
/// Wave B remediation: hunk stage/unstage, branch create/checkout/rename,
/// and the index.lock removal helper. `git.push`/`git.fetch` are covered by
/// their own crate-level tests (they need a bare-remote fixture).
#[tokio::test]
async fn uds_git_write_ops_wave_b_round_trip() {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let base = Path::new("/tmp").join(format!("intentd-gitwb-{}", &short[..8]));
    let data_dir = base.join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let repo = base.join("repo");
    seed_repo(&repo);

    let config = {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("INTENTD_DATA_DIR", &data_dir);
        Config::resolve().expect("resolve config")
    };

    let ws_id = WorkspaceId::from("ws-gitwb");
    {
        let store = Store::open(&config.db_path).await.expect("open store");
        store
            .insert_workspace(&seed_workspace(&ws_id, repo.to_str().unwrap()))
            .await
            .expect("seed ws");
    }

    let store = Store::open(&config.db_path).await.expect("reopen store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> =
        Arc::new(Services::new(store).with_workspaces_root(ws_root.path().to_path_buf()));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let socket = config.socket_path.clone();
    let server = tokio::spawn(async move {
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

    // (a) createBranch without checkout leaves HEAD alone.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":1,"method":"git.createBranch","params":{"workspaceId":"ws-gitwb","branchName":"feature","checkout":false}}"#,
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true));
    assert_eq!(resp["result"]["branch"], json!("feature"));

    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":2,"method":"git.status","params":{"workspaceId":"ws-gitwb"}}"#,
    )
    .await;
    let branch_before = resp["result"]["branch"].as_str().unwrap().to_string();
    assert_ne!(branch_before, "feature");

    // (b) checkoutBranch switches HEAD.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":3,"method":"git.checkoutBranch","params":{"workspaceId":"ws-gitwb","branchName":"feature"}}"#,
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true));
    assert_eq!(resp["result"]["branch"], json!("feature"));
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":4,"method":"git.status","params":{"workspaceId":"ws-gitwb"}}"#,
    )
    .await;
    assert_eq!(resp["result"]["branch"], json!("feature"));

    // (c) renameBranch swaps the current branch's name.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":5,"method":"git.renameBranch","params":{"workspaceId":"ws-gitwb","oldBranchName":"feature","newBranchName":"feature-2"}}"#,
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true));
    assert_eq!(resp["result"]["oldBranch"], json!("feature"));
    assert_eq!(resp["result"]["newBranch"], json!("feature-2"));
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":6,"method":"git.status","params":{"workspaceId":"ws-gitwb"}}"#,
    )
    .await;
    assert_eq!(resp["result"]["branch"], json!("feature-2"));

    // (d) renameBranch with an empty name → -32602.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":7,"method":"git.renameBranch","params":{"workspaceId":"ws-gitwb","oldBranchName":"feature-2","newBranchName":""}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));

    // (e) stageHunk on a working-tree modification stages just the hunk.
    std::fs::write(repo.join("seed.txt"), "seed\nnew line\n").unwrap();
    let patch = "diff --git a/seed.txt b/seed.txt\n--- a/seed.txt\n+++ b/seed.txt\n@@ -1 +1,2 @@\n seed\n+new line\n";
    let params = json!({
        "jsonrpc": "2.0",
        "id": 8,
        "method": "git.stageHunk",
        "params": {
            "workspaceId": "ws-gitwb",
            "filePath": "seed.txt",
            "hunkPatch": patch,
        },
    });
    let resp = send(&config.socket_path, &params.to_string()).await;
    assert_eq!(resp["result"]["ok"], json!(true));
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":9,"method":"git.status","params":{"workspaceId":"ws-gitwb"}}"#,
    )
    .await;
    let staged = resp["result"]["files"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["path"] == json!("seed.txt"))
        .expect("seed.txt in status");
    assert_eq!(staged["staged"], json!(true));

    // (f) unstageHunk reverses the previously-staged hunk.
    let params = json!({
        "jsonrpc": "2.0",
        "id": 10,
        "method": "git.unstageHunk",
        "params": {
            "workspaceId": "ws-gitwb",
            "filePath": "seed.txt",
            "hunkPatch": patch,
        },
    });
    let resp = send(&config.socket_path, &params.to_string()).await;
    assert_eq!(resp["result"]["ok"], json!(true));
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":11,"method":"git.status","params":{"workspaceId":"ws-gitwb"}}"#,
    )
    .await;
    let unstaged = resp["result"]["files"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["path"] == json!("seed.txt"))
        .expect("seed.txt in status");
    assert_eq!(unstaged["staged"], json!(false));

    // (g) removeLockFile is `removed=false` when no lock file is present.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":12,"method":"git.removeLockFile","params":{"workspaceId":"ws-gitwb"}}"#,
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true));
    assert_eq!(resp["result"]["removed"], json!(false));

    // (h) removeLockFile deletes a present index.lock.
    let lock = repo.join(".git").join("index.lock");
    std::fs::write(&lock, b"pid").unwrap();
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":13,"method":"git.removeLockFile","params":{"workspaceId":"ws-gitwb"}}"#,
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true));
    assert_eq!(resp["result"]["removed"], json!(true));
    assert!(!lock.exists());

    let _ = tx.send(());
    let _ = server.await;
    std::fs::remove_dir_all(&base).ok();
}

/// Over-the-wire git read slice: `git.changes`, `git.diffs` (+ `git.diff`
/// alias), and `git.commits` (+ `git.log` alias) populate the FE panels.
#[tokio::test]
async fn uds_git_read_ops_round_trip() {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let base = Path::new("/tmp").join(format!("intentd-gitr-{}", &short[..8]));
    let data_dir = base.join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let repo = base.join("repo");
    seed_repo(&repo);

    let config = {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("INTENTD_DATA_DIR", &data_dir);
        Config::resolve().expect("resolve config")
    };

    let ws_id = WorkspaceId::from("ws-gitr");
    {
        let store = Store::open(&config.db_path).await.expect("open store");
        store
            .insert_workspace(&seed_workspace(&ws_id, repo.to_str().unwrap()))
            .await
            .expect("seed ws");
    }

    let store = Store::open(&config.db_path).await.expect("reopen store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> =
        Arc::new(Services::new(store).with_workspaces_root(ws_root.path().to_path_buf()));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let socket = config.socket_path.clone();
    let server = tokio::spawn(async move {
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

    // Modify a tracked file (unstaged) so changes/diffs are non-empty.
    std::fs::write(repo.join("seed.txt"), "seed\nadded\n").unwrap();

    // (a) git.changes → working-tree FileStatus[] with the modified file.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":1,"method":"git.changes","params":{"workspaceId":"ws-gitr"}}"#,
    )
    .await;
    let changes = resp["result"].as_array().expect("changes array");
    let seed = changes
        .iter()
        .find(|c| c["path"] == json!("seed.txt"))
        .expect("seed.txt in changes");
    assert_eq!(seed["status"], json!("M"));
    assert_eq!(seed["staged"], json!(false));

    // (b) git.diffs (unstaged) → [{ path, hunks }] with tagged DiffLines.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":2,"method":"git.diffs","params":{"workspaceId":"ws-gitr"}}"#,
    )
    .await;
    let diffs = resp["result"].as_array().expect("diffs array");
    let f = diffs
        .iter()
        .find(|d| d["path"] == json!("seed.txt"))
        .expect("seed.txt diff");
    let hunks = f["hunks"].as_array().expect("hunks");
    assert!(!hunks.is_empty());
    let lines = hunks[0]["lines"].as_array().expect("lines");
    assert!(lines
        .iter()
        .any(|l| l["type"] == json!("Addition")
            && l["content"].as_str().unwrap_or("").contains("added")));

    // (b') git.diff alias resolves to the same handler.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":3,"method":"git.diff","params":{"workspaceId":"ws-gitr","path":"seed.txt"}}"#,
    )
    .await;
    let arr = resp["result"].as_array().expect("diff alias array");
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["path"], json!("seed.txt"));

    // (c) git.commits → §5.5 { items, nextToken } page of CommitSummary.
    // Metadata-only: `files` is omitted (fetched on demand via
    // git.commitDetails) so the list walk never tree-diffs per commit.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":4,"method":"git.commits","params":{"workspaceId":"ws-gitr","page":{"limit":10}}}"#,
    )
    .await;
    let items = resp["result"]["items"].as_array().expect("items array");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["message"].as_str().unwrap(), "seed");
    assert!(items[0].get("files").is_none());
    assert_eq!(items[0]["email"], json!("test@example.com"));
    assert_eq!(resp["result"]["nextToken"], Value::Null);

    // (c') git.log alias resolves to the same handler (top-level limit form).
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":5,"method":"git.log","params":{"workspaceId":"ws-gitr","limit":10}}"#,
    )
    .await;
    assert_eq!(resp["result"]["items"].as_array().expect("items").len(), 1);

    // (d) git.showFile → committed content at HEAD (the worktree edit above is
    // not visible at the ref), empty content for a path missing at the ref,
    // and -32603 for an unresolvable ref (PROTOCOL §5.6 extensions).
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":6,"method":"git.showFile","params":{"workspaceId":"ws-gitr","filePath":"seed.txt","ref":"HEAD"}}"#,
    )
    .await;
    assert_eq!(resp["result"]["content"], json!("seed\n"));
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":7,"method":"git.showFile","params":{"workspaceId":"ws-gitr","filePath":"nope.txt","ref":"HEAD"}}"#,
    )
    .await;
    assert_eq!(resp["result"]["content"], json!(""));
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":8,"method":"git.showFile","params":{"workspaceId":"ws-gitr","filePath":"seed.txt","ref":"no-such-ref"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32603));

    let _ = tx.send(());
    let _ = server.await;
    std::fs::remove_dir_all(&base).ok();
}

/// Register `child` as a submodule of `parent` at `sub_rel` via a real `git`
/// CLI round trip (mirroring `intent_git::testutil::add_submodule`, private
/// to that crate) — a real gitlink + `.gitmodules` fixture for the
/// submodule-guard e2e below.
fn add_submodule(parent: &Path, child: &Path, sub_rel: &str) {
    git(
        parent,
        &[
            "-c",
            "protocol.file.allow=always",
            "submodule",
            "add",
            "-q",
            child.to_str().unwrap(),
            sub_rel,
        ],
    );
    git(parent, &["commit", "-q", "-m", "add submodule"]);
}

/// Over-the-wire submodule-gitlink guard (monorepo#1714 follow-up):
/// `git.agentCommit`'s explicit `files` list refuses a path strictly inside a
/// registered submodule with a clear error naming the offending path and its
/// containing submodule, and no commit lands.
#[tokio::test]
async fn uds_git_agent_commit_rejects_submodule_internal_file() {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let base = Path::new("/tmp").join(format!("intentd-gitsub-{}", &short[..8]));
    let data_dir = base.join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let child = base.join("child");
    seed_repo(&child);
    let repo = base.join("repo");
    seed_repo(&repo);
    add_submodule(&repo, &child, "sub");

    let config = {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("INTENTD_DATA_DIR", &data_dir);
        Config::resolve().expect("resolve config")
    };

    let ws_id = WorkspaceId::from("ws-gitsub");
    {
        let store = Store::open(&config.db_path).await.expect("open store");
        store
            .insert_workspace(&seed_workspace(&ws_id, repo.to_str().unwrap()))
            .await
            .expect("seed ws");
    }

    let store = Store::open(&config.db_path).await.expect("reopen store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> =
        Arc::new(Services::new(store).with_workspaces_root(ws_root.path().to_path_buf()));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let socket = config.socket_path.clone();
    let server = tokio::spawn(async move {
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

    let head_before = String::from_utf8(
        Command::new("git")
            .current_dir(&repo)
            .args(["rev-parse", "HEAD"])
            .output()
            .expect("rev-parse")
            .stdout,
    )
    .expect("utf8")
    .trim()
    .to_string();

    // Dirty a file strictly inside the submodule's own worktree, then try to
    // commit it explicitly through the superproject workspace.
    std::fs::write(repo.join("sub").join("seed.txt"), "changed inside sub\n").unwrap();
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":1,"method":"git.agentCommit","params":{"workspaceId":"ws-gitsub","message":"flatten sub","files":["sub/seed.txt"]}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32603));
    let msg = resp["error"]["data"]
        .as_str()
        .or_else(|| resp["error"]["message"].as_str())
        .unwrap_or_default();
    assert!(msg.contains("sub/seed.txt"), "names the path: {resp}");
    assert!(
        msg.contains("submodule 'sub'"),
        "names the containing submodule: {resp}"
    );

    // No commit landed on the superproject.
    let head_after = String::from_utf8(
        Command::new("git")
            .current_dir(&repo)
            .args(["rev-parse", "HEAD"])
            .output()
            .expect("rev-parse")
            .stdout,
    )
    .expect("utf8")
    .trim()
    .to_string();
    assert_eq!(head_before, head_after, "no commit must have been made");

    let _ = tx.send(());
    let _ = server.await;
    std::fs::remove_dir_all(&base).ok();
}

/// Over-the-wire submodule-gitlink guard for discard (monorepo#1733):
/// `git.discard` refuses a path strictly inside a registered submodule instead
/// of classifying it as untracked in the superproject and unlinking it — the
/// submodule's uncommitted edit survives and the gitlink stays a `160000` entry.
#[tokio::test]
async fn uds_git_discard_rejects_submodule_internal_path() {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let base = Path::new("/tmp").join(format!("intentd-gitsubd-{}", &short[..8]));
    let data_dir = base.join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let child = base.join("child");
    seed_repo(&child);
    let repo = base.join("repo");
    seed_repo(&repo);
    add_submodule(&repo, &child, "sub");

    let config = {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("INTENTD_DATA_DIR", &data_dir);
        Config::resolve().expect("resolve config")
    };

    let ws_id = WorkspaceId::from("ws-gitsubd");
    {
        let store = Store::open(&config.db_path).await.expect("open store");
        store
            .insert_workspace(&seed_workspace(&ws_id, repo.to_str().unwrap()))
            .await
            .expect("seed ws");
    }

    let store = Store::open(&config.db_path).await.expect("reopen store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> =
        Arc::new(Services::new(store).with_workspaces_root(ws_root.path().to_path_buf()));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let socket = config.socket_path.clone();
    let server = tokio::spawn(async move {
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

    // Dirty a file strictly inside the submodule's own worktree, then try to
    // discard it through the superproject workspace.
    let inner = repo.join("sub").join("seed.txt");
    std::fs::write(&inner, "uncommitted inside sub\n").unwrap();
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":1,"method":"git.discard","params":{"workspaceId":"ws-gitsubd","paths":["sub/seed.txt"]}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32603));
    let msg = resp["error"]["data"]
        .as_str()
        .or_else(|| resp["error"]["message"].as_str())
        .unwrap_or_default();
    assert!(msg.contains("sub/seed.txt"), "names the path: {resp}");
    assert!(
        msg.contains("submodule 'sub'"),
        "names the containing submodule: {resp}"
    );

    // The submodule working-copy edit survived and the gitlink is intact.
    assert_eq!(
        std::fs::read_to_string(&inner).unwrap(),
        "uncommitted inside sub\n"
    );
    let ls = String::from_utf8(
        Command::new("git")
            .current_dir(&repo)
            .args(["ls-files", "-s", "sub"])
            .output()
            .expect("ls-files")
            .stdout,
    )
    .expect("utf8");
    assert!(ls.starts_with("160000"), "gitlink intact, got: {ls:?}");

    let _ = tx.send(());
    let _ = server.await;
    std::fs::remove_dir_all(&base).ok();
}

/// Over-the-wire per-commit read slice: `git.commitDetails` returns metadata +
/// `fileDetails`, and `git.diffs` with `commitHash` returns the commit's own
/// per-file hunks.
#[tokio::test]
async fn uds_git_commit_details_round_trip() {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let base = Path::new("/tmp").join(format!("intentd-gitc-{}", &short[..8]));
    let data_dir = base.join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let repo = base.join("repo");
    seed_repo(&repo);
    // Land a second commit so HEAD has a non-empty per-file diff against its parent.
    std::fs::write(repo.join("seed.txt"), "seed\nadded\n").unwrap();
    std::fs::write(repo.join("new.txt"), "hello\n").unwrap();
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-q", "-m", "second"]);
    let head = String::from_utf8(
        Command::new("git")
            .current_dir(&repo)
            .args(["rev-parse", "HEAD"])
            .output()
            .expect("rev-parse")
            .stdout,
    )
    .expect("utf8")
    .trim()
    .to_string();

    let config = {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("INTENTD_DATA_DIR", &data_dir);
        Config::resolve().expect("resolve config")
    };

    let ws_id = WorkspaceId::from("ws-gitc");
    {
        let store = Store::open(&config.db_path).await.expect("open store");
        store
            .insert_workspace(&seed_workspace(&ws_id, repo.to_str().unwrap()))
            .await
            .expect("seed ws");
    }

    let store = Store::open(&config.db_path).await.expect("reopen store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> =
        Arc::new(Services::new(store).with_workspaces_root(ws_root.path().to_path_buf()));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let socket = config.socket_path.clone();
    let server = tokio::spawn(async move {
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

    // (a) git.commitDetails → metadata + fileDetails for the commit's two files.
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"git.commitDetails","params":{{"workspaceId":"ws-gitc","commitHash":"{head}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["commitHash"], json!(head));
    assert_eq!(resp["result"]["author"], json!("Test"));
    assert_eq!(resp["result"]["authorEmail"], json!("test@example.com"));
    assert_eq!(resp["result"]["message"], json!("second"));
    let files = resp["result"]["files"].as_array().expect("files");
    assert!(files.iter().any(|f| f == &json!("seed.txt")));
    assert!(files.iter().any(|f| f == &json!("new.txt")));
    let file_details = resp["result"]["fileDetails"]
        .as_array()
        .expect("fileDetails");
    let seed = file_details
        .iter()
        .find(|f| f["path"] == json!("seed.txt"))
        .expect("seed.txt fileDetails");
    assert_eq!(seed["additions"], json!(1));
    assert_eq!(seed["deletions"], json!(0));
    let new = file_details
        .iter()
        .find(|f| f["path"] == json!("new.txt"))
        .expect("new.txt fileDetails");
    assert_eq!(new["additions"], json!(1));
    assert_eq!(new["deletions"], json!(0));

    // (b) git.diffs with commitHash → per-file hunks for <hash>^..<hash>.
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"git.diffs","params":{{"workspaceId":"ws-gitc","commitHash":"{head}","path":"seed.txt"}}}}"#
        ),
    )
    .await;
    let arr = resp["result"].as_array().expect("diffs array");
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["path"], json!("seed.txt"));
    let lines = arr[0]["hunks"][0]["lines"].as_array().expect("lines");
    assert!(lines
        .iter()
        .any(|l| l["type"] == json!("Addition")
            && l["content"].as_str().unwrap_or("").contains("added")));

    // (c) git.commitDetails for an unresolvable hash → graceful empty envelope.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":3,"method":"git.commitDetails","params":{"workspaceId":"ws-gitc","commitHash":"deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"}}"#,
    )
    .await;
    assert_eq!(
        resp["result"]["commitHash"],
        json!("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
    );
    assert_eq!(resp["result"]["files"], json!([]));
    assert_eq!(resp["result"]["fileDetails"], json!([]));

    // (d) git.diffs with an unresolvable commitHash → empty array (graceful).
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":4,"method":"git.diffs","params":{"workspaceId":"ws-gitc","commitHash":"deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"}}"#,
    )
    .await;
    assert_eq!(resp["result"], json!([]));

    // (e) Missing commitHash → -32602.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":5,"method":"git.commitDetails","params":{"workspaceId":"ws-gitc"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));

    let _ = tx.send(());
    let _ = server.await;
    std::fs::remove_dir_all(&base).ok();
}

/// Over-the-wire `git.branchStatus` slice: path-based ahead/behind + dirty-tree
/// flag for the workspace-initializer BranchSelector seam (PROTOCOL §5.6).
#[tokio::test]
async fn uds_git_branch_status_round_trip() {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let base = Path::new("/tmp").join(format!("intentd-gitbs-{}", &short[..8]));
    let data_dir = base.join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let repo = base.join("repo");
    seed_repo(&repo);

    let config = {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("INTENTD_DATA_DIR", &data_dir);
        Config::resolve().expect("resolve config")
    };

    let ws_id = WorkspaceId::from("ws-gitbs");
    {
        let store = Store::open(&config.db_path).await.expect("open store");
        store
            .insert_workspace(&seed_workspace(&ws_id, repo.to_str().unwrap()))
            .await
            .expect("seed ws");
    }

    let store = Store::open(&config.db_path).await.expect("reopen store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> =
        Arc::new(Services::new(store).with_workspaces_root(ws_root.path().to_path_buf()));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let socket = config.socket_path.clone();
    let server = tokio::spawn(async move {
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

    // Discover the actual checked-out branch so we can query against it (the
    // initial branch name is git-config-dependent — `main` vs `master`).
    let head_branch = String::from_utf8(
        Command::new("git")
            .current_dir(&repo)
            .args(["branch", "--show-current"])
            .output()
            .expect("branch --show-current")
            .stdout,
    )
    .expect("utf8")
    .trim()
    .to_string();

    // (a) Clean repo, current branch queried → ahead/behind 0, isCurrentBranch.
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"git.branchStatus","params":{{"repoPath":"{}","branchName":"{}"}}}}"#,
            repo.display(),
            head_branch,
        ),
    )
    .await;
    assert_eq!(resp["result"]["branch"], json!(head_branch));
    assert_eq!(resp["result"]["currentBranch"], json!(head_branch));
    assert_eq!(resp["result"]["isCurrentBranch"], json!(true));
    assert_eq!(resp["result"]["ahead"], json!(0));
    assert_eq!(resp["result"]["behind"], json!(0));
    assert_eq!(resp["result"]["hasUncommittedChanges"], json!(false));

    // (b) Modify a tracked file → hasUncommittedChanges flips to true.
    std::fs::write(repo.join("seed.txt"), "seed changed\n").unwrap();
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"git.branchStatus","params":{{"repoPath":"{}","branchName":"{}"}}}}"#,
            repo.display(),
            head_branch,
        ),
    )
    .await;
    assert_eq!(resp["result"]["hasUncommittedChanges"], json!(true));

    // (c) Untracked file alone still counts as uncommitted changes (porcelain
    // semantics: any output ⇒ dirty).
    std::fs::write(repo.join("seed.txt"), "seed\n").unwrap();
    std::fs::write(repo.join("new.txt"), "fresh\n").unwrap();
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"git.branchStatus","params":{{"repoPath":"{}","branchName":"{}"}}}}"#,
            repo.display(),
            head_branch,
        ),
    )
    .await;
    assert_eq!(resp["result"]["hasUncommittedChanges"], json!(true));
    std::fs::remove_file(repo.join("new.txt")).unwrap();

    // (d) Querying a non-checked-out branch sets isCurrentBranch=false but the
    // worktree's currentBranch is still reported.
    git(&repo, &["branch", "feature-x"]);
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":4,"method":"git.branchStatus","params":{{"repoPath":"{}","branchName":"feature-x"}}}}"#,
            repo.display(),
        ),
    )
    .await;
    assert_eq!(resp["result"]["branch"], json!("feature-x"));
    assert_eq!(resp["result"]["currentBranch"], json!(head_branch));
    assert_eq!(resp["result"]["isCurrentBranch"], json!(false));

    // (e) Missing branchName → -32602.
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":5,"method":"git.branchStatus","params":{{"repoPath":"{}"}}}}"#,
            repo.display(),
        ),
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));
    assert_eq!(
        resp["error"]["message"],
        json!("Missing required parameter: branchName")
    );

    // (f) Nonexistent repo path → -32602 with the verbatim validation message
    // (mirrors git.getBranches).
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":6,"method":"git.branchStatus","params":{"repoPath":"/no/such/repo","branchName":"main"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));
    assert_eq!(
        resp["error"]["message"],
        json!("Repository path does not exist: /no/such/repo")
    );

    let _ = tx.send(());
    let _ = server.await;
    std::fs::remove_dir_all(&base).ok();
}

/// Over-the-wire `git.getBranches` slice: the path-based branch listing used by
/// the workspace-initializer BranchSelector (PROTOCOL §5.6). The repo does NOT
/// need to be a registered workspace — the create flow lists branches before
/// the workspace exists — but nonexistent paths and non-git directories are
/// rejected with distinct -32602 errors.
#[tokio::test]
async fn uds_git_get_branches_round_trip() {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let base = Path::new("/tmp").join(format!("intentd-gitgb-{}", &short[..8]));
    let data_dir = base.join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    // `known` is registered as a workspace; `unreg` is a valid git repo the
    // daemon has never seen; `plain` is an existing non-git directory.
    let known = base.join("known");
    seed_repo(&known);
    let unreg = base.join("unreg");
    seed_repo(&unreg);
    git(&unreg, &["branch", "feature-y"]);
    let plain = base.join("plain");
    std::fs::create_dir_all(&plain).unwrap();

    let config = {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("INTENTD_DATA_DIR", &data_dir);
        Config::resolve().expect("resolve config")
    };

    let ws_id = WorkspaceId::from("ws-gitgb");
    {
        let store = Store::open(&config.db_path).await.expect("open store");
        store
            .insert_workspace(&seed_workspace(&ws_id, known.to_str().unwrap()))
            .await
            .expect("seed ws");
    }

    let store = Store::open(&config.db_path).await.expect("reopen store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> =
        Arc::new(Services::new(store).with_workspaces_root(ws_root.path().to_path_buf()));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let socket = config.socket_path.clone();
    let server = tokio::spawn(async move {
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

    let head_branch = |repo: &Path| {
        String::from_utf8(
            Command::new("git")
                .current_dir(repo)
                .args(["branch", "--show-current"])
                .output()
                .expect("branch --show-current")
                .stdout,
        )
        .expect("utf8")
        .trim()
        .to_string()
    };

    // (a) Known-workspace repo → branch payload (unchanged behavior).
    let known_head = head_branch(&known);
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"git.getBranches","params":{{"repoPath":"{}"}}}}"#,
            known.display(),
        ),
    )
    .await;
    assert_eq!(resp["result"]["currentBranch"], json!(known_head));
    assert!(resp["result"]["branches"]
        .as_array()
        .unwrap()
        .contains(&json!(known_head)));

    // (b) Unregistered-but-valid local repo → succeeds (the workspace-create
    // flow needs branches before the repo is known to the daemon).
    let unreg_head = head_branch(&unreg);
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"git.getBranches","params":{{"repoPath":"{}","includeRemote":true}}}}"#,
            unreg.display(),
        ),
    )
    .await;
    assert_eq!(resp["result"]["currentBranch"], json!(unreg_head));
    let branches = resp["result"]["branches"].as_array().unwrap();
    assert!(branches.contains(&json!(unreg_head)));
    assert!(branches.contains(&json!("feature-y")));
    assert_eq!(resp["result"]["remoteBranches"], json!([]));

    // (c) Existing directory that is not a git repository → -32602.
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"git.getBranches","params":{{"repoPath":"{}"}}}}"#,
            plain.display(),
        ),
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));
    assert_eq!(
        resp["error"]["message"],
        json!(format!("Path is not a git repository: {}", plain.display()))
    );

    // (d) Nonexistent path → -32602 with the distinct message.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":4,"method":"git.getBranches","params":{"repoPath":"/no/such/repo"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));
    assert_eq!(
        resp["error"]["message"],
        json!("Repository path does not exist: /no/such/repo")
    );

    let _ = tx.send(());
    let _ = server.await;
    std::fs::remove_dir_all(&base).ok();
}

/// Over-the-wire `git.pull` slice: the workspace-create auto-pull (PROTOCOL
/// §5.6). Path-based like `git.getBranches` — the repo does NOT need to be a
/// registered workspace. Covers the checked-out fast-forward pull (with a
/// dirty worktree exercising the auto-stash bookends), the structured
/// `{ ok: false, error }` failure, and the -32602 param rejections.
#[tokio::test]
async fn uds_git_pull_round_trip() {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let base = Path::new("/tmp").join(format!("intentd-gitpl-{}", &short[..8]));
    let data_dir = base.join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    // `repo` tracks a bare `origin` and is one commit behind it; `lone` has no
    // remote at all (the structured-failure path).
    let repo = base.join("repo");
    seed_repo(&repo);
    let bare = base.join("origin.git");
    git(&base, &["init", "-q", "--bare", "origin.git"]);
    git(&repo, &["remote", "add", "origin", bare.to_str().unwrap()]);
    let branch = String::from_utf8(
        Command::new("git")
            .current_dir(&repo)
            .args(["branch", "--show-current"])
            .output()
            .expect("branch --show-current")
            .stdout,
    )
    .expect("utf8")
    .trim()
    .to_string();
    git(&repo, &["push", "-q", "origin", &branch]);
    std::fs::write(repo.join("remote.txt"), "from-remote\n").unwrap();
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-q", "-m", "remote change"]);
    git(&repo, &["push", "-q", "origin", &branch]);
    git(&repo, &["reset", "-q", "--hard", "HEAD~1"]);
    // A dirty untracked file must survive the pull (auto-stash + pop).
    std::fs::write(repo.join("local.txt"), "uncommitted\n").unwrap();
    let lone = base.join("lone");
    seed_repo(&lone);

    let config = {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("INTENTD_DATA_DIR", &data_dir);
        Config::resolve().expect("resolve config")
    };
    let store = Store::open(&config.db_path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> =
        Arc::new(Services::new(store).with_workspaces_root(ws_root.path().to_path_buf()));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let socket = config.socket_path.clone();
    let server = tokio::spawn(async move {
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

    // (a) Behind + dirty checked-out branch → fast-forward pull succeeds, the
    // remote commit arrives, and the local change is restored.
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"git.pull","params":{{"repoPath":"{}","branchName":"{}"}}}}"#,
            repo.display(),
            branch,
        ),
    )
    .await;
    assert_eq!(resp["result"], json!({ "ok": true }));
    assert!(repo.join("remote.txt").exists());
    assert_eq!(
        std::fs::read_to_string(repo.join("local.txt")).unwrap(),
        "uncommitted\n"
    );

    // (b) Repo without an `origin` remote → structured { ok: false, error }
    // (an ordinary pull failure is never a JSON-RPC error).
    let lone_branch = String::from_utf8(
        Command::new("git")
            .current_dir(&lone)
            .args(["branch", "--show-current"])
            .output()
            .expect("branch --show-current")
            .stdout,
    )
    .expect("utf8")
    .trim()
    .to_string();
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"git.pull","params":{{"repoPath":"{}","branchName":"{}"}}}}"#,
            lone.display(),
            lone_branch,
        ),
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(false));
    assert!(!resp["result"]["error"].as_str().unwrap().is_empty());

    // (c) Nonexistent repo path → -32602 with the validation message verbatim.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":3,"method":"git.pull","params":{"repoPath":"/no/such/repo","branchName":"main"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));
    assert_eq!(
        resp["error"]["message"],
        json!("Repository path does not exist: /no/such/repo")
    );

    // (d) Missing branchName → -32602.
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":4,"method":"git.pull","params":{{"repoPath":"{}"}}}}"#,
            repo.display(),
        ),
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));
    assert_eq!(
        resp["error"]["message"],
        json!("Missing required parameter: branchName")
    );

    let _ = tx.send(());
    let _ = server.await;
    std::fs::remove_dir_all(&base).ok();
}

/// Over-the-wire coverage for the read-side git extensions added alongside
/// the deferred `#126` cleanup: `git.numstat`, `git.branchDiff`, and
/// `git.getRemoteUrl`. Uses a small on-disk repo (no bare-remote fixture,
/// so `origin` is a configured URL only, not a reachable remote).
#[tokio::test]
async fn uds_git_read_ops_extensions_round_trip() {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let base = Path::new("/tmp").join(format!("intentd-gitreads-{}", &short[..8]));
    let data_dir = base.join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let repo = base.join("repo");
    seed_repo(&repo);
    // Configure an origin URL so `git.getRemoteUrl` has something to return.
    git(
        &repo,
        &["remote", "add", "origin", "https://example.invalid/o/r.git"],
    );

    let config = {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("INTENTD_DATA_DIR", &data_dir);
        Config::resolve().expect("resolve config")
    };

    let ws_id = WorkspaceId::from("ws-gitreads");
    {
        let store = Store::open(&config.db_path).await.expect("open store");
        store
            .insert_workspace(&seed_workspace(&ws_id, repo.to_str().unwrap()))
            .await
            .expect("seed ws");
    }

    let store = Store::open(&config.db_path).await.expect("reopen store");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> =
        Arc::new(Services::new(store).with_workspaces_root(ws_root.path().to_path_buf()));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let socket = config.socket_path.clone();
    let server = tokio::spawn(async move {
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

    // (a) numstat — default (HEAD→workdir tracked) picks up the tracked
    // modification but not the untracked file.
    std::fs::write(repo.join("seed.txt"), "seed\nnew line\n").unwrap();
    std::fs::write(repo.join("untracked.txt"), "hello\n").unwrap();
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":1,"method":"git.numstat","params":{"workspaceId":"ws-gitreads"}}"#,
    )
    .await;
    let items = resp["result"].as_array().expect("numstat items");
    assert_eq!(items.len(), 1, "only tracked change: {resp}");
    assert_eq!(items[0]["filePath"], json!("seed.txt"));
    assert_eq!(items[0]["additions"], json!(1));
    assert_eq!(items[0]["deletions"], json!(0));

    // (b) numstat — staged=true with nothing staged → empty array.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":2,"method":"git.numstat","params":{"workspaceId":"ws-gitreads","staged":true}}"#,
    )
    .await;
    assert_eq!(resp["result"], json!([]));

    // (c) branchDiff — missing both bases → -32602.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":3,"method":"git.branchDiff","params":{"workspaceId":"ws-gitreads"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));

    // (d) getRemoteUrl — configured origin URL.
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":4,"method":"git.getRemoteUrl","params":{{"repoPath":"{}"}}}}"#,
            repo.display(),
        ),
    )
    .await;
    assert_eq!(
        resp["result"]["url"],
        json!("https://example.invalid/o/r.git")
    );

    // (e) getRemoteUrl — missing remote → { url: null }.
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":5,"method":"git.getRemoteUrl","params":{{"repoPath":"{}","remoteName":"no-such-remote"}}}}"#,
            repo.display(),
        ),
    )
    .await;
    assert!(resp["result"]["url"].is_null(), "{resp}");

    // (f) getRemoteUrl — nonexistent path → -32602.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":6,"method":"git.getRemoteUrl","params":{"repoPath":"/no/such/repo"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));

    let _ = tx.send(());
    let _ = server.await;
    std::fs::remove_dir_all(&base).ok();
}
