//! End-to-end UDS slice test: seed via the store, then drive the daemon as a
//! JSON-RPC client over a temp Unix-domain socket (§5.7 DoD).

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use intent_core::{
    now_iso, Config, ContentType, Note, NoteId, NoteVisibility, Workspace, WorkspaceActivity,
    WorkspaceApi, WorkspaceAttention, WorkspaceId, WorkspaceStatus,
};
use intent_services::Services;
use intent_store::Store;
use intent_transport::serve_uds;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

fn seed_workspace(id: &WorkspaceId) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "Seed WS".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: ts.clone(),
        updated_at: ts.clone(),
        last_activity: None,
        tags: vec!["seed".to_string()],
        path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: None,
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        archived: false,
        archived_at: None,
    }
}

fn seed_note(ws: &WorkspaceId) -> Note {
    let ts = now_iso();
    Note {
        id: NoteId::from("note-seed"),
        workspace_id: ws.clone(),
        title: "Spec".to_string(),
        content: "# Seed".to_string(),
        content_type: ContentType::Markdown,
        tags: vec![],
        is_pinned: false,
        is_archived: false,
        is_default: true,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        task: None,
        created_at: ts.clone(),
        updated_at: ts,
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
async fn uds_slice_end_to_end() {
    // Use a short base path: macOS caps UDS paths at ~104 bytes (SUN_LEN) and
    // `temp_dir()` resolves to a long `/var/folders/...` path.
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = Path::new("/tmp").join(format!("intentd-it-{}", &short[..8]));
    std::fs::create_dir_all(&dir).unwrap();
    std::env::set_var("INTENTD_DATA_DIR", &dir);
    let config = Config::resolve().expect("resolve config");

    let ws_id = WorkspaceId::from("ws-seed");
    {
        let store = Store::open(&config.db_path).await.expect("open store");
        store
            .insert_workspace(&seed_workspace(&ws_id))
            .await
            .expect("seed ws");
        store
            .insert_note(&seed_note(&ws_id))
            .await
            .expect("seed note");
    }

    let store = Store::open(&config.db_path).await.expect("reopen store");
    let services: Arc<dyn WorkspaceApi> = Arc::new(Services::new(store));
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let socket = config.socket_path.clone();
    let server = tokio::spawn(async move {
        serve_uds(services, &socket, async move {
            let _ = rx.await;
        })
        .await
        .expect("serve");
    });

    // Wait for the socket to appear.
    for _ in 0..50 {
        if config.socket_path.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // (a) workspace.list
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.list"}"#,
    )
    .await;
    assert!(resp["result"].is_object(), "result must be an object");
    let wss = resp["result"]["workspaces"]
        .as_array()
        .expect("workspaces array");
    assert!(wss.iter().any(|w| w["id"] == json!("ws-seed")));

    // (b) note.list with the seeded workspaceId
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":2,"method":"note.list","params":{"workspaceId":"ws-seed"}}"#,
    )
    .await;
    assert!(resp["result"].is_object(), "result must be an object");
    let notes = resp["result"]["notes"].as_array().expect("notes array");
    assert!(notes.iter().any(|n| n["id"] == json!("note-seed")));

    // (c) malformed request (jsonrpc != "2.0") → -32600
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"1.0","id":3,"method":"workspace.list"}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32600));

    // (d) unknown method as a request → -32601
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":4,"method":"does.notExist"}"#,
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32601));

    // (e) workspace.* CRUD lifecycle: create → get → update → archive →
    //     unarchive → dismissAttention → delete (PROTOCOL §5.1).
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":5,"method":"workspace.create","params":{"title":"Lifecycle WS"}}"#,
    )
    .await;
    let new_id = resp["result"]["workspace"]["id"]
        .as_str()
        .expect("created id")
        .to_string();
    assert_eq!(resp["result"]["workspace"]["title"], json!("Lifecycle WS"));

    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":6,"method":"workspace.get","params":{{"workspaceId":"{new_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["workspace"]["id"], json!(new_id));

    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":7,"method":"workspace.update","params":{{"workspaceId":"{new_id}","title":"Renamed"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["workspace"]["title"], json!("Renamed"));

    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":8,"method":"workspace.archive","params":{{"workspaceId":"{new_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["success"], json!(true));

    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":9,"method":"workspace.unarchive","params":{{"workspaceId":"{new_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["success"], json!(true));

    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":10,"method":"workspace.dismissAttention","params":{{"workspaceId":"{new_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["workspace"]["attention"], json!("none"));

    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":11,"method":"workspace.delete","params":{{"workspaceId":"{new_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["result"]["success"], json!(true));

    // (f) get after delete → -32602 "Workspace not found".
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":12,"method":"workspace.get","params":{{"workspaceId":"{new_id}"}}}}"#
        ),
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));
    assert_eq!(resp["error"]["message"], json!("Workspace not found"));

    let _ = tx.send(());
    let _ = server.await;
    let _ = std::fs::remove_dir_all(&dir);
}
