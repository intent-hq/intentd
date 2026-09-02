//! Regression coverage for the daemon-known virtual "Chief of Staff" workspace
//! (TS `CHIEF_WORKSPACE_ID = '__chief__'` in `shared/types/branded-ids.ts`).
//!
//! The wire contract mirrors the reference app:
//! - `workspace.get({ workspaceId: "__chief__" })` returns the synthesized
//!   [`intent_core::chief_workspace`] shape (pinned title/timestamps, empty
//!   branch, no repo/worktree). It never round-trips through the seeded row.
//! - `workspace.list` NEVER surfaces `__chief__` (filtered at the store).
//! - `agent.create({ workspaceId: "__chief__", … })` succeeds and persists a
//!   session under Chief's seeded row (satisfies the `agent_session ↦ workspace`
//!   FK from migration 0004) so Chief-of-Staff agents work end-to-end.
//! - `workspace.update`, `archive`, `unarchive`, `dismissAttention`, and
//!   `delete` on `__chief__` are safe no-ops that return the synthesized shape
//!   (or `success: true` for delete) — the seeded row is never mutated.

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use intent_core::{Config, WorkspaceApi, CHIEF_WORKSPACE_ID, CHIEF_WORKSPACE_TIMESTAMP};
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::serve_uds;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

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
async fn chief_workspace_over_uds() {
    // Short UDS path (`SUN_LEN ~ 104B` on macOS).
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = Path::new("/tmp").join(format!("intentd-chief-{}", &short[..8]));
    std::fs::create_dir_all(&dir).unwrap();
    std::env::set_var("INTENTD_DATA_DIR", &dir);
    let config = Config::resolve().expect("resolve config");

    // Open the store once so migration 0033 seeds the `__chief__` row before
    // the daemon's UDS listener comes up. No user workspaces are seeded — this
    // test only exercises the Chief-only slice.
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

    // (a) `workspace.get({ workspaceId: "__chief__" })` returns the synthesized
    //     Chief shape (parity with TS `getChiefWorkspace`).
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"workspace.get","params":{{"workspaceId":"{CHIEF_WORKSPACE_ID}"}}}}"#
        ),
    )
    .await;
    let ws = &resp["result"]["workspace"];
    assert_eq!(ws["id"], json!(CHIEF_WORKSPACE_ID));
    assert_eq!(ws["title"], json!("Chief of Staff"));
    assert_eq!(ws["branch"], json!(""));
    assert_eq!(ws["status"], json!("Active"));
    assert_eq!(ws["attention"], json!("none"));
    assert_eq!(ws["createdAt"], json!(CHIEF_WORKSPACE_TIMESTAMP));
    assert_eq!(ws["updatedAt"], json!(CHIEF_WORKSPACE_TIMESTAMP));
    assert_eq!(ws["archived"], json!(false));
    // Chief has no worktree/repo/PR shape on the wire.
    assert!(ws.get("path").is_none_or(Value::is_null));
    assert!(ws.get("worktreePath").is_none_or(Value::is_null));
    assert!(ws.get("repositoryName").is_none_or(Value::is_null));

    // (b) `workspace.list` MUST NOT include `__chief__` — Chief is only ever
    //     reachable by explicit id (TS `findAll` virtual-workspace filter).
    let resp = send(
        &config.socket_path,
        r#"{"jsonrpc":"2.0","id":2,"method":"workspace.list"}"#,
    )
    .await;
    let list = resp["result"]["workspaces"]
        .as_array()
        .expect("workspaces array");
    assert!(
        !list.iter().any(|w| w["id"] == json!(CHIEF_WORKSPACE_ID)),
        "workspace.list must not surface Chief: {list:?}"
    );

    // (c) `agent.create({ workspaceId: "__chief__", … })` succeeds. This
    //     exercises the `agent_session ↦ workspace(id)` FK: without the
    //     migration-seeded row the insert would fail with a FK violation and
    //     Chief-of-Staff agents would be unreachable.
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"agent.create","params":{{"workspaceId":"{CHIEF_WORKSPACE_ID}","name":"Chief Assistant","model":"default","provider":"mock"}}}}"#
        ),
    )
    .await;
    assert!(
        resp["error"].is_null() || resp.get("error").is_none(),
        "agent.create must succeed for Chief: {resp}"
    );
    let agent = &resp["result"]["agent"];
    assert_eq!(agent["workspaceId"], json!(CHIEF_WORKSPACE_ID));
    assert_eq!(agent["name"], json!("Chief Assistant"));
    let agent_id = agent["id"].as_str().expect("agent id").to_string();

    // `agent.list({ workspaceId: "__chief__" })` sees the newly-created row.
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":4,"method":"agent.list","params":{{"workspaceId":"{CHIEF_WORKSPACE_ID}"}}}}"#
        ),
    )
    .await;
    let agents = resp["result"]["agents"]
        .as_array()
        .expect("agents array under Chief");
    assert!(
        agents.iter().any(|a| a["id"] == json!(agent_id)),
        "created Chief agent must appear in agent.list: {agents:?}"
    );

    // (d) `workspace.update` on Chief returns the applied delta layered over
    //     the synthesized shape without persisting (parity with TS
    //     `saveWorkspaceUpdates`). A follow-up `workspace.get` still returns
    //     the canonical Chief shape.
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":5,"method":"workspace.update","params":{{"workspaceId":"{CHIEF_WORKSPACE_ID}","statusMessage":"hello"}}}}"#
        ),
    )
    .await;
    let updated = &resp["result"]["workspace"];
    assert_eq!(
        updated["id"],
        json!(CHIEF_WORKSPACE_ID),
        "update returns Chief shape: {resp}"
    );
    assert_eq!(updated["statusMessage"], json!("hello"));
    // Chief's canonical timestamps are pinned to `CHIEF_WORKSPACE_TIMESTAMP` —
    // even `workspace.update` must not diverge them from `workspace.get`.
    assert_eq!(
        updated["createdAt"],
        json!(CHIEF_WORKSPACE_TIMESTAMP),
        "update response pins createdAt: {resp}"
    );
    assert_eq!(
        updated["updatedAt"],
        json!(CHIEF_WORKSPACE_TIMESTAMP),
        "update response pins updatedAt: {resp}"
    );
    assert_eq!(
        updated["lastActivity"],
        json!(CHIEF_WORKSPACE_TIMESTAMP),
        "update response pins lastActivity: {resp}"
    );
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":6,"method":"workspace.get","params":{{"workspaceId":"{CHIEF_WORKSPACE_ID}"}}}}"#
        ),
    )
    .await;
    // Not persisted: synthesized shape has no statusMessage.
    assert!(resp["result"]["workspace"]
        .get("statusMessage")
        .is_none_or(Value::is_null));

    // (e) Archive / delete on Chief are safe no-ops — the seeded row is never
    //     torn down or flipped to `archived = 1` and Chief remains reachable
    //     via `workspace.get` after both. `workspace.archive` returns the
    //     synthesized Chief `Workspace` (§5.1) with `archived = false` so
    //     callers can trust the wire without a follow-up `workspace.get`;
    //     `workspace.delete` still responds with `{ success: true }`
    //     (`intent-transport/src/router.rs` §5.1); `dismissAttention` returns
    //     `{ workspace: ... }` (the synthesized Chief shape). We assert on
    //     each exact envelope, then re-`get` to verify Chief is unchanged.
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":7,"method":"workspace.archive","params":{{"workspaceId":"{CHIEF_WORKSPACE_ID}"}}}}"#
        ),
    )
    .await;
    assert!(
        resp.get("error").is_none() || resp["error"].is_null(),
        "workspace.archive on Chief must succeed: {resp}"
    );
    assert_eq!(resp["result"]["workspace"]["id"], json!(CHIEF_WORKSPACE_ID));
    assert_eq!(resp["result"]["workspace"]["archived"], json!(false));
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":8,"method":"workspace.delete","params":{{"workspaceId":"{CHIEF_WORKSPACE_ID}"}}}}"#
        ),
    )
    .await;
    assert!(
        resp.get("error").is_none() || resp["error"].is_null(),
        "workspace.delete on Chief must succeed: {resp}"
    );
    assert_eq!(
        resp["result"]["success"],
        json!(true),
        "workspace.delete returns {{success:true}}: {resp}"
    );
    // dismissAttention returns the synthesized Chief workspace, not
    // { success: true } — assert the workspace envelope + pinned invariants.
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":9,"method":"workspace.dismissAttention","params":{{"workspaceId":"{CHIEF_WORKSPACE_ID}"}}}}"#
        ),
    )
    .await;
    assert!(
        resp.get("error").is_none() || resp["error"].is_null(),
        "workspace.dismissAttention on Chief must succeed: {resp}"
    );
    let dismissed = &resp["result"]["workspace"];
    assert_eq!(dismissed["id"], json!(CHIEF_WORKSPACE_ID));
    assert_eq!(dismissed["attention"], json!("none"));
    assert_eq!(dismissed["updatedAt"], json!(CHIEF_WORKSPACE_TIMESTAMP));
    let resp = send(
        &config.socket_path,
        &format!(
            r#"{{"jsonrpc":"2.0","id":10,"method":"workspace.get","params":{{"workspaceId":"{CHIEF_WORKSPACE_ID}"}}}}"#
        ),
    )
    .await;
    let ws = &resp["result"]["workspace"];
    assert_eq!(
        ws["id"],
        json!(CHIEF_WORKSPACE_ID),
        "Chief remains reachable after archive/delete: {resp}"
    );
    assert_eq!(ws["archived"], json!(false), "Chief is never archived");
    assert_eq!(ws["status"], json!("Active"));
    assert_eq!(ws["updatedAt"], json!(CHIEF_WORKSPACE_TIMESTAMP));

    let _ = tx.send(());
    let _ = server.await;
    let _ = std::fs::remove_dir_all(&dir);
}
