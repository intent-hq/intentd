//! Content activity survives bookkeeping timestamps, including historical rows.

use super::*;
use serde_json::json;

type Ws = tokio_tungstenite::WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;

async fn frame(client: &mut Ws) -> Value {
    tokio::time::timeout(common::rpc_read_timeout(), async {
        loop {
            match client.next().await {
                Some(Ok(Message::Text(text))) => return serde_json::from_str(&text).unwrap(),
                Some(Ok(Message::Ping(payload))) => {
                    client.send(Message::Pong(payload)).await.unwrap();
                }
                Some(Ok(_)) => {}
                other => panic!("WSS closed: {other:?}"),
            }
        }
    })
    .await
    .expect("WSS response deadline")
}

async fn rpc(client: &mut Ws, id: u64, method: &str, params: Value) -> Value {
    client
        .send(Message::Text(
            json!({"jsonrpc":"2.0", "id":id, "method":method, "params":params})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    loop {
        let value = frame(client).await;
        if value["id"] == id {
            assert_eq!(value["jsonrpc"], "2.0");
            assert!(value.get("error").is_none(), "{value}");
            return value;
        }
    }
}

async fn subscribed_activity(client: &mut Ws, workspace: &str, stamp: &Value) -> Value {
    tokio::time::timeout(common::rpc_read_timeout(), async {
        loop {
            let value = frame(client).await;
            if value["method"] == "subscription.push"
                && value["params"]["seq"].as_u64().unwrap_or(0) > 0
                && value["params"]["delta"]["updated"]
                    .as_array()
                    .is_some_and(|rows| {
                        rows.iter().any(|row| {
                            row["id"] == workspace && row["lastContentActivity"] == *stamp
                        })
                    })
            {
                return value;
            }
        }
    })
    .await
    .expect("committed content activity must reach workspace subscribers")
}

#[intent_test_macros::daemon_test]
async fn content_activity_backfill_and_writes_are_truthful_over_wss() {
    let srv = start(WsOptions::default()).await;
    // Reconstruct the pre-addition schema in this isolated test database,
    // then run the production migration against genuinely old content.
    sqlx::raw_sql(
        "DROP TRIGGER workspace_content_note_insert;
         DROP TRIGGER workspace_content_note_update;
         DROP TRIGGER workspace_content_message_insert;
         DROP TRIGGER workspace_content_message_update;
         ALTER TABLE workspace DROP COLUMN last_content_activity;
         INSERT INTO workspace (id,title,branch,status,created_at,updated_at,last_activity)
         VALUES ('content-old','Old content','main','Active','2026-01-01T00:00:00Z','2026-09-30T14:04:41Z','2026-09-30T14:04:41Z'),
                ('content-empty','Empty','main','Active','2026-01-01T00:00:00Z','2026-09-30T14:04:41Z',NULL);
         INSERT INTO agent_session (id,workspace_id,name,status,created_at,updated_at)
         VALUES ('content-agent','content-old','Agent','idle','2026-01-01T00:00:00Z','2026-09-30T14:04:41Z');
         INSERT INTO agent_message (id,agent_id,seq,role,content,created_at)
         VALUES ('content-user','content-agent',1,'user','[]','2026-09-10T18:29:01Z'),
                ('content-assistant','content-agent',2,'assistant','[]','2026-09-10T18:29:13Z'),
                ('content-system','content-agent',3,'system','[]','2026-09-30T14:04:41Z'),
                ('content-invalid','content-agent',4,'user','[]','not-a-date');
         INSERT INTO note (id,workspace_id,title,content,created_at,updated_at)
         VALUES ('content-note','content-old','Note','','2026-01-01T00:00:00Z','2026-09-10T18:28:31Z'),
                ('content-invalid-note','content-empty','Invalid','','not-a-date','not-a-date');",
    ).execute(srv.store.write_pool()).await.unwrap();
    sqlx::raw_sql(include_str!(
        "../../../intent-store/migrations/0143_workspace_content_activity.sql"
    ))
    .execute(srv.store.write_pool())
    .await
    .unwrap();
    let mut client = connect_ws(srv.port, srv.cfg.clone()).await;
    let mut evidence = Vec::new();
    let listed = rpc(&mut client, 1, "workspace.list", json!({})).await;
    let rows = listed["result"]["workspaces"].as_array().unwrap();
    let old = rows.iter().find(|row| row["id"] == "content-old").unwrap();
    assert_eq!(old["lastContentActivity"], "2026-09-10T18:29:13Z");
    assert_eq!(
        old["updatedAt"], "2026-09-30T14:04:41Z",
        "no destructive repair"
    );
    assert_eq!(
        old["lastActivity"], "2026-09-30T14:04:41Z",
        "legacy semantics stay intact"
    );
    assert!(rows
        .iter()
        .find(|row| row["id"] == "content-empty")
        .unwrap()
        .get("lastContentActivity")
        .is_none());
    evidence.push(listed);

    // Neither metadata nor usage bookkeeping counts as recorded content.
    sqlx::raw_sql("UPDATE workspace SET title='Renamed',updated_at='2026-10-01T00:00:00Z' WHERE id='content-old'; UPDATE agent_session SET updated_at='2026-10-01T00:00:00Z' WHERE id='content-agent';")
        .execute(srv.store.write_pool()).await.unwrap();
    srv.store
        .update_workspace_token_usage(&WorkspaceId::from("content-old"), |_, _| {
            Some(intent_core::TokenUsage::default())
        })
        .await
        .unwrap();
    let unchanged = rpc(
        &mut client,
        2,
        "workspace.get",
        json!({"workspaceId":"content-old"}),
    )
    .await;
    assert_eq!(
        unchanged["result"]["workspace"]["lastContentActivity"],
        "2026-09-10T18:29:13Z"
    );
    evidence.push(unchanged);

    // Compare real instants, not lexical order; older appends cannot regress.
    sqlx::raw_sql("UPDATE note SET updated_at='2026-09-10T20:30:00+02:00' WHERE id='content-note';
        INSERT INTO agent_message (id,agent_id,seq,role,content,created_at) VALUES ('content-older','content-agent',5,'assistant','[]','2026-09-10T18:29:59Z'), ('content-tool','content-agent',6,'tool','[]','2026-10-01T00:00:00Z');")
        .execute(srv.store.write_pool()).await.unwrap();
    let note = rpc(
        &mut client,
        3,
        "workspace.get",
        json!({"workspaceId":"content-old"}),
    )
    .await;
    assert_eq!(
        note["result"]["workspace"]["lastContentActivity"],
        "2026-09-10T20:30:00+02:00"
    );
    evidence.push(note);
    sqlx::query(
        "UPDATE agent_message SET created_at='2026-09-10T18:30:00.500Z' WHERE id='content-older'",
    )
    .execute(srv.store.write_pool())
    .await
    .unwrap();
    sqlx::raw_sql("DELETE FROM agent_message WHERE id='content-older'; DELETE FROM note WHERE id='content-note';")
        .execute(srv.store.write_pool()).await.unwrap();
    let deleted = rpc(
        &mut client,
        4,
        "workspace.get",
        json!({"workspaceId":"content-old"}),
    )
    .await;
    assert_eq!(
        deleted["result"]["workspace"]["lastContentActivity"], "2026-09-10T18:30:00.500Z",
        "recorded activity survives content deletion"
    );
    evidence.push(deleted);

    let mut subscriber = connect_ws(srv.port, srv.cfg.clone()).await;
    subscriber
        .send(Message::Text(
            json!({"jsonrpc":"2.0","id":5,"method":"workspace.subscribe","params":{}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let mut snapshot = None;
    let mut response = None;
    while snapshot.is_none() || response.is_none() {
        let value = frame(&mut subscriber).await;
        if value["id"] == 5 {
            response = Some(value);
        } else if value["method"] == "subscription.push" {
            snapshot = Some(value);
        }
    }
    assert!(response.unwrap().get("error").is_none());
    let snapshot = snapshot.unwrap();
    assert_eq!(snapshot["params"]["seq"], 0);
    let row = snapshot["params"]["snapshot"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == "content-old")
        .unwrap();
    assert_eq!(row["lastContentActivity"], "2026-09-10T18:30:00.500Z");
    evidence.push(snapshot);

    // Real wire writes also update the materialized mark before their response.
    let created = rpc(
        &mut client,
        6,
        "note.create",
        json!({
            "workspaceId":"content-empty", "title":"A real note", "content":"Recorded through WSS"
        }),
    )
    .await;
    let fresh = rpc(
        &mut client,
        7,
        "workspace.get",
        json!({"workspaceId":"content-empty"}),
    )
    .await;
    assert_eq!(
        fresh["result"]["workspace"]["lastContentActivity"],
        created["result"]["note"]["updatedAt"]
    );
    assert!(fresh["result"]["workspace"]["lastContentActivity"].is_string());
    evidence.push(
        subscribed_activity(
            &mut subscriber,
            "content-empty",
            &created["result"]["note"]["updatedAt"],
        )
        .await,
    );
    let edited = rpc(&mut client, 10, "note.update", json!({"workspaceId":"content-empty", "noteId":created["result"]["note"]["id"], "content":"Ordinary note edit"})).await;
    evidence.push(
        subscribed_activity(
            &mut subscriber,
            "content-empty",
            &edited["result"]["note"]["updatedAt"],
        )
        .await,
    );
    evidence.extend([created, fresh, edited]);
    let appended = rpc(
        &mut client,
        8,
        "agent.appendMessage",
        json!({
            "workspaceId":"content-old", "agentId":"content-agent", "role":"user",
            "contentBlocks":[{"type":"text","text":"Recorded through WSS"}]
        }),
    )
    .await;
    let fresh = rpc(
        &mut client,
        9,
        "workspace.get",
        json!({"workspaceId":"content-old"}),
    )
    .await;
    assert_eq!(
        fresh["result"]["workspace"]["lastContentActivity"],
        appended["result"]["message"]["timestamp"]
    );
    assert!(fresh["result"]["workspace"]["lastContentActivity"].is_string());
    evidence.push(
        subscribed_activity(
            &mut subscriber,
            "content-old",
            &appended["result"]["message"]["timestamp"],
        )
        .await,
    );
    let assistant = rpc(&mut client, 11, "agent.appendMessage", json!({"workspaceId":"content-old", "agentId":"content-agent", "role":"assistant", "contentBlocks":[{"type":"text","text":"Assistant content"}]})).await;
    evidence.push(
        subscribed_activity(
            &mut subscriber,
            "content-old",
            &assistant["result"]["message"]["timestamp"],
        )
        .await,
    );
    evidence.extend([appended, fresh, assistant]);
    let artifact = srv.dir.path().join("content-activity-wire.json");
    std::fs::write(&artifact, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
    println!(
        "content activity wire artifact: {} (retain with INTENTD_TEST_KEEP_TMP=1)",
        artifact.display()
    );
    srv.ws.stop().await;
}
