//! Cold preparation uses the real authenticated WSS dispatch and error envelope.
use super::*;
use intent_core::WorkspaceRole;
use serde_json::json;

async fn current_heads(store: &intent_store::Store, workspace: &WorkspaceId) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM note_page_head WHERE workspace_id=? AND indexed_rev=current_rev",
    )
    .bind(workspace.as_str())
    .fetch_one(store.read_pool())
    .await
    .unwrap()
}

#[tokio::test]
async fn demand_wss_authorization_retry_and_one_note_publication() {
    let srv = start(WsOptions::default()).await;
    let mut alice = Guest::connect(&srv, &"91".repeat(32)).await;
    let mut denied = Guest::connect(&srv, &"92".repeat(32)).await;
    let ws = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&ws))
        .await
        .unwrap();
    srv.store
        .add_workspace_member(&ws, &alice.principal.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    for id in ["spec", "unopened"] {
        srv.store
            .insert_note(&fixture_note(
                &ws,
                id,
                "😀 [Task](intent://local/task/task-a)\r\n",
            ))
            .await
            .unwrap();
    }
    sqlx::query("UPDATE note_page_head SET indexed_rev=-1,profile_revision='retired-profile' WHERE workspace_id=?")
        .bind(ws.as_str()).execute(srv.store.write_pool()).await.unwrap();
    let params = json!({"workspaceId":ws,"noteId":"spec","page":{"kind":"source"}});
    let refusal = denied.call("note.get", params.clone()).await;
    assert_eq!(refusal["error"]["data"]["code"], "not-found", "{refusal}");
    assert_eq!(current_heads(&srv.store, &ws).await, 0);
    // Legacy reads stay available and cannot start an eager conversion.
    let legacy = alice
        .call("note.get", json!({"workspaceId":ws,"noteId":"spec"}))
        .await;
    assert!(legacy.get("error").is_none(), "{legacy}");
    assert_eq!(current_heads(&srv.store, &ws).await, 0);

    // Holding only the serialized writer makes preparation deterministically
    // pending while authorization and normal bounded reads remain available.
    let writer = srv.store.write_pool().acquire().await.unwrap();
    let pending = alice.call("note.get", params.clone()).await;
    assert_eq!(pending["error"]["code"], -32006, "{pending}");
    assert_eq!(
        pending["error"]["data"],
        json!({"code":"note-index-preparing","retryAfterMs":1000})
    );
    // Disconnect while the owned job is blocked on its write connection.
    // Reconnection starts fresh view demand, not replay of an old snapshot.
    assert_eq!(current_heads(&srv.store, &ws).await, 0);
    alice.ws.close(None).await.unwrap();
    drop(writer);
    let url = format!("wss://localhost:{}/ws?token={}", srv.port, "91".repeat(32));
    alice.ws = common::wss_connect_with_retry(srv.port, srv.cfg.clone(), &url).await;
    let ready = tokio::time::timeout(
        intentd_test_support::test_timeout(std::time::Duration::from_secs(5)),
        async {
            loop {
                let frame = alice.call("note.get", params.clone()).await;
                if frame["error"]["data"]["code"] != "note-index-preparing" {
                    break frame;
                }
            }
        },
    )
    .await
    .unwrap();
    assert_eq!(ready["result"]["kind"], "noteSourcePage", "{ready}");
    assert_eq!(
        ready["result"]["text"],
        "😀 [Task](intent://local/task/task-a)\r\n"
    );
    assert_eq!(current_heads(&srv.store, &ws).await, 1);
    let tasks = alice
        .call(
            "note.get",
            json!({"workspaceId":ws,"noteId":"spec","page":{"kind":"taskIds"}}),
        )
        .await;
    assert_eq!(tasks["result"]["totalItems"], 1, "{tasks}");
    assert_eq!(current_heads(&srv.store, &ws).await, 1);
    srv.ws.stop().await;
}
