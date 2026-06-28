//! Over-the-wire `file.tree` slice: drive the method against a real workspace
//! root through the daemon over a temp UDS and assert the bare-array shape of
//! `{ path, name, isDirectory }` entries, including a directory flag.

use std::path::Path;
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
        token_usage: None,
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

    std::env::set_var("INTENTD_DATA_DIR", &data_dir);
    let config = Config::resolve().expect("resolve config");

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
