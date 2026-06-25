//! Over-the-wire git write-ops slice: drive `git.status`, `git.agentCommit`, and
//! `git.commit` against a real worktree through the daemon over a temp UDS.

use std::path::Path;
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
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
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

    std::env::set_var("INTENTD_DATA_DIR", &data_dir);
    let config = Config::resolve().expect("resolve config");

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
    let services: Arc<dyn WorkspaceApi> = Arc::new(Services::new(store));
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

    let _ = tx.send(());
    let _ = server.await;
    std::fs::remove_dir_all(&base).ok();
}
