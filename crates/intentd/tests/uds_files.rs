//! Over-the-wire `file.*` slice: drive `file.tree`, `file.exists`, and
//! `file.stat` against a real workspace root through the daemon over a temp
//! UDS and assert the exact response shapes each method promises.

mod common;

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Serializes the `INTENTD_DATA_DIR` env-var set + `Config::resolve()` across the
/// tests in this binary: the var is process-global, so concurrent setup would
/// race and make tests resolve the same db path.
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

fn seed_workspace(id: &WorkspaceId, worktree: &str) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "Files WS".to_string(),
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
async fn uds_file_tree_returns_root_entries() {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let base = Path::new("/tmp").join(format!("intentd-files-{}", &short[..8]));
    let data_dir = base.join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let repo = base.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    // Canonicalize so the within-workspace prefix check matches (macOS /tmp is a
    // symlink into /private/var).
    let repo = std::fs::canonicalize(&repo).unwrap();
    std::fs::write(repo.join("a.txt"), "hi\n").unwrap();
    std::fs::create_dir_all(repo.join("src")).unwrap();

    let config = {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("INTENTD_DATA_DIR", &data_dir);
        Config::resolve().expect("resolve config")
    };

    let ws_id = WorkspaceId::from("ws-files");
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

    // file.tree with no `path` → bare array of root entries.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":1,"method":"file.tree","params":{"workspaceId":"ws-files"}}"#,
    )
    .await;
    let entries = resp["result"].as_array().expect("bare array result");
    assert_eq!(entries.len(), 2);

    // Every entry carries the three camelCase fields.
    for e in entries {
        assert!(e.get("path").and_then(Value::as_str).is_some());
        assert!(e.get("name").and_then(Value::as_str).is_some());
        assert!(e.get("isDirectory").and_then(Value::as_bool).is_some());
    }

    let file = entries
        .iter()
        .find(|e| e["name"] == json!("a.txt"))
        .expect("a.txt entry");
    assert_eq!(file["path"], json!("a.txt"));
    assert_eq!(file["isDirectory"], json!(false));

    let dir = entries
        .iter()
        .find(|e| e["name"] == json!("src"))
        .expect("src entry");
    assert_eq!(dir["path"], json!("src"));
    assert_eq!(dir["isDirectory"], json!(true));

    let _ = tx.send(());
    let _ = server.await;
    std::fs::remove_dir_all(&base).ok();
}

async fn boot(
    repo_name: &str,
) -> (
    std::path::PathBuf,
    std::path::PathBuf,
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Sender<()>,
    Config,
    tempfile::TempDir,
) {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let base = Path::new("/tmp").join(format!("intentd-{}-{}", repo_name, &short[..8]));
    let data_dir = base.join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let repo = base.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    let repo = std::fs::canonicalize(&repo).unwrap();

    let config = {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("INTENTD_DATA_DIR", &data_dir);
        Config::resolve().expect("resolve config")
    };

    let ws_id = WorkspaceId::from("ws-files");
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
    (base, repo, server, tx, config, ws_root)
}

#[tokio::test]
async fn uds_file_exists_reports_type_and_absent() {
    let (base, repo, server, tx, config, _ws_root) = boot("exists").await;
    std::fs::write(repo.join("hello.txt"), "hi\n").unwrap();
    std::fs::create_dir_all(repo.join("subdir")).unwrap();

    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":1,"method":"file.exists","params":{"workspaceId":"ws-files","path":"hello.txt"}}"#,
    )
    .await;
    assert_eq!(
        resp["result"],
        json!({ "exists": true, "isFile": true, "isDirectory": false })
    );

    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":2,"method":"file.exists","params":{"workspaceId":"ws-files","path":"subdir"}}"#,
    )
    .await;
    assert_eq!(
        resp["result"],
        json!({ "exists": true, "isFile": false, "isDirectory": true })
    );

    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":3,"method":"file.exists","params":{"workspaceId":"ws-files","path":"missing"}}"#,
    )
    .await;
    assert_eq!(
        resp["result"],
        json!({ "exists": false, "isFile": false, "isDirectory": false })
    );

    // Path escape must be rejected as -32603, matching the other file.* ops.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":4,"method":"file.exists","params":{"workspaceId":"ws-files","path":"../escape"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32603));

    let _ = tx.send(());
    let _ = server.await;
    std::fs::remove_dir_all(&base).ok();
}

#[tokio::test]
async fn uds_file_stat_returns_legacy_shape() {
    let (base, repo, server, tx, config, _ws_root) = boot("stat").await;
    std::fs::write(repo.join("hello.txt"), "hello").unwrap();

    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":1,"method":"file.stat","params":{"workspaceId":"ws-files","path":"hello.txt"}}"#,
    )
    .await;
    let r = &resp["result"];
    assert_eq!(r["size"], json!(5u64));
    assert_eq!(r["isFile"], json!(true));
    assert_eq!(r["isDirectory"], json!(false));
    assert_eq!(r["isSymlink"], json!(false));
    let mtime = r["mtime"].as_str().expect("mtime string");
    assert!(mtime.ends_with('Z'));
    assert!(mtime.contains('T'));
    let perms = r["permissions"].as_str().expect("perms string");
    assert!(perms.starts_with('0'));

    // Missing path surfaces as -32603.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":2,"method":"file.stat","params":{"workspaceId":"ws-files","path":"nope"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32603));

    // Path escape must be rejected as -32603.
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":3,"method":"file.stat","params":{"workspaceId":"ws-files","path":"../escape"}}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32603));

    let _ = tx.send(());
    let _ = server.await;
    std::fs::remove_dir_all(&base).ok();
}
