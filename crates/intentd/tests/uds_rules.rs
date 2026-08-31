//! Over-the-wire `rules.*` slice (PROTOCOL §5.21, §18.1): drive
//! `rules.update`/`rules.get`/`rules.list` against the daemon over a temp UDS,
//! proving camelCase shapes, the editable user-override entry, the read-only
//! file-sourced (`CLAUDE.md`) entry, and that an edit surfaces via
//! `settings:changed` carrying the `endUserRules` payload (no extra fetch).

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use intent_core::{
    Workspace, WorkspaceActivity, WorkspaceApi, WorkspaceAttention, WorkspaceId, WorkspaceStatus,
};
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::serve_uds;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::OwnedReadHalf;
use tokio::net::UnixStream;
use tokio::sync::oneshot;
use tokio::time::timeout;
use uuid::Uuid;

struct TempDir(PathBuf);
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct TempDb {
    path: PathBuf,
}
impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
        }
    }
}

fn sample_ws(id: &WorkspaceId, worktree: &std::path::Path) -> Workspace {
    Workspace {
        id: id.clone(),
        title: "WS".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: "t0".to_string(),
        updated_at: "t0".to_string(),
        last_activity: None,
        tags: vec![],
        path: None,
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: Some(worktree.to_string_lossy().to_string()),
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

async fn connect_retry(socket: &PathBuf) -> UnixStream {
    for _ in 0..100 {
        if let Ok(s) = UnixStream::connect(socket).await {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("could not connect to {}", socket.display());
}

async fn send(write_half: &mut (impl AsyncWriteExt + Unpin), frame: &str) {
    write_half.write_all(frame.as_bytes()).await.unwrap();
    write_half.write_all(b"\n").await.unwrap();
    write_half.flush().await.unwrap();
}

async fn read_json(reader: &mut BufReader<OwnedReadHalf>) -> Value {
    let mut line = String::new();
    let n = timeout(Duration::from_secs(2), reader.read_line(&mut line))
        .await
        .expect("timed out waiting for a frame")
        .expect("read failed");
    assert!(n > 0, "connection closed unexpectedly");
    serde_json::from_str(line.trim_end()).expect("invalid JSON frame")
}

async fn rpc(
    write_half: &mut (impl AsyncWriteExt + Unpin),
    reader: &mut BufReader<OwnedReadHalf>,
    id: i64,
    method: &str,
    params: Value,
) -> Value {
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    send(write_half, &serde_json::to_string(&frame).unwrap()).await;
    let resp = read_json(reader).await;
    assert_eq!(resp["id"], id, "response id mismatch for {method}");
    assert!(resp.get("error").is_none(), "rpc {method} errored: {resp}");
    resp["result"].clone()
}

async fn wait_for_subscriber_count(bus: &EventBus, target: usize) {
    for _ in 0..100 {
        if bus.subscriber_count() == target {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("subscriber_count never reached {target}");
}

#[tokio::test]
async fn rules_round_trip_overrides_files_and_event() {
    let work = TempDir(std::env::temp_dir().join(format!("intentd-rules-{}", Uuid::new_v4())));
    std::fs::create_dir_all(&work.0).unwrap();
    std::fs::write(work.0.join("CLAUDE.md"), "ALWAYS run the linter.").unwrap();

    let tmp = TempDb {
        path: std::env::temp_dir().join(format!("intentd-rules-{}.db", Uuid::new_v4())),
    };
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_ws(&ws, &work.0))
        .await
        .expect("insert workspace");
    let bus = EventBus::new(store.clone());
    let ws_root = common::hermetic_workspaces_root();
    let services: Arc<dyn WorkspaceApi> = Arc::new(
        Services::new(store)
            .with_workspaces_root(ws_root.path().to_path_buf())
            .with_event_bus(bus.clone()),
    );
    // Socket lives in a guarded dir under /tmp so the path stays short
    // (macOS SUN_LEN) and the file is swept even if the test panics.
    let sock_dir = common::test_tempdir_in("/tmp", "ir-");
    let socket = sock_dir.path().join("uds.sock");

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server = tokio::spawn({
        let bus = bus.clone();
        let socket = socket.clone();
        async move {
            let _ = serve_uds(services, bus, &socket, None, async {
                let _ = shutdown_rx.await;
            })
            .await;
        }
    });

    let (rpc_read, mut w) = connect_retry(&socket).await.into_split();
    let mut r = BufReader::new(rpc_read);

    // Subscribe (no workspace scope → matches the global settings event).
    let (sub_read, mut sw) = connect_retry(&socket).await.into_split();
    let mut sr = BufReader::new(sub_read);
    send(
        &mut sw,
        &serde_json::to_string(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "events.subscribe",
            "params": { "eventTypes": ["settings:changed"] },
        }))
        .unwrap(),
    )
    .await;
    let _ = read_json(&mut sr).await;
    wait_for_subscriber_count(&bus, 1).await;

    // rules.update — upsert the workspace override; result re-reads the set.
    let updated = rpc(
        &mut w,
        &mut r,
        1,
        "rules.update",
        json!({ "workspaceId": ws.as_str(), "ruleType": "workspace",
                "content": "Prefer small PRs." }),
    )
    .await;
    assert_eq!(updated["rules"]["workspaceId"], ws.as_str());

    // settings:changed carries the endUserRules payload (no extra fetch).
    let ev = read_json(&mut sr).await;
    assert_eq!(ev["params"]["event"]["type"], "settings:changed");
    assert_eq!(
        ev["params"]["event"]["data"]["changes"][0]["path"],
        "endUserRules"
    );
    assert_eq!(
        ev["params"]["event"]["data"]["changes"][0]["value"]["workspace"]["content"],
        "Prefer small PRs."
    );

    // rules.get — { enabled, content, updatedAt } for the one override type.
    let got = rpc(
        &mut w,
        &mut r,
        2,
        "rules.get",
        json!({ "workspaceId": ws.as_str(), "ruleType": "workspace" }),
    )
    .await;
    assert_eq!(got["content"], "Prefer small PRs.");
    assert_eq!(got["enabled"], true);
    assert!(got["updatedAt"].as_i64().unwrap() > 0);

    // rules.list — editable user-override entry + read-only CLAUDE.md entry.
    let list = rpc(
        &mut w,
        &mut r,
        3,
        "rules.list",
        json!({ "workspaceId": ws.as_str() }),
    )
    .await;
    let entries = list["rules"]["rules"].as_array().expect("rules array");
    let user = entries
        .iter()
        .find(|e| e["ruleType"] == "workspace" && e["source"] == "user-override")
        .expect("user-override entry");
    assert_eq!(user["editable"], true);
    let file = entries
        .iter()
        .find(|e| e["source"] == "CLAUDE.md")
        .expect("CLAUDE.md entry");
    assert_eq!(file["editable"], false);
    assert_eq!(file["content"], "ALWAYS run the linter.");

    let _ = shutdown_tx.send(());
    let _ = server.await;
    drop(tmp);
}
