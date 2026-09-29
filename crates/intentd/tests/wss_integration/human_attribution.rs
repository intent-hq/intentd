use super::*;
use intent_core::{AgentId, PrincipalIdentity, WorkspaceRole};
use serde_json::json;

async fn owner(srv: &Server, login: &str, id: i64) -> Guest {
    let mut principal = srv.store.get_primary_principal().await.unwrap();
    principal.login = Some(login.into());
    principal.identity = Some(PrincipalIdentity::github(id));
    srv.store.upsert_principal(&principal).await.unwrap();
    Guest {
        principal,
        ws: connect_ws(srv.port, srv.cfg.clone()).await,
        next_id: 1,
    }
}

async fn relay(source: &Server, actor: &mut Guest, target: &mut Guest, ws: &WorkspaceId) {
    let mut events = PresenceClient::open(source.port, source.cfg.clone(), TOKEN).await;
    let ack=events.call(1,"events.subscribe",json!({"workspaceId":ws,"eventTypes":["workspace:transfer:ready","workspace:transfer:failed"]})).await;
    assert!(ack["result"]["subscriptionId"].is_string(), "{ack}");
    let started = actor
        .call("workspace.export.start", json!({"workspaceId":ws}))
        .await;
    let export_id = started["result"]["exportId"]
        .as_str()
        .unwrap_or_else(|| panic!("{started}"));
    let ready = events.event("workspace:transfer:ready").await;
    let data = &ready["data"];
    assert_eq!(data["exportId"], export_id);
    assert_eq!(data["manifest"]["formatVersion"], 2);
    let begin=target.call("workspace.import.begin",json!({"manifest":data["manifest"],"archiveSizeBytes":data["archiveSizeBytes"],"archiveSha256":data["archiveSha256"]})).await;
    let import_id = begin["result"]["importId"]
        .as_str()
        .unwrap_or_else(|| panic!("{begin}"));
    for seq in 0..data["totalChunks"].as_u64().unwrap() {
        let chunk = actor
            .call(
                "workspace.export.read",
                json!({"exportId":export_id,"seq":seq}),
            )
            .await;
        let put = target
            .call(
                "workspace.import.chunk",
                json!({"importId":import_id,"seq":seq,"data":chunk["result"]["data"]}),
            )
            .await;
        assert_eq!(put["result"]["seq"], seq, "{put}");
    }
    let committed = target
        .call("workspace.import.commit", json!({"importId":import_id}))
        .await;
    assert_eq!(committed["result"]["workspace"]["id"], ws.0, "{committed}");
    let aborted = actor
        .call("workspace.export.abort", json!({"exportId":export_id}))
        .await;
    assert!(aborted.get("error").is_none(), "{aborted}");
    events.close().await;
}

#[tokio::test]
async fn transfer_human_authors_comments_and_pending_queue_over_wss() {
    let a = start(WsOptions::default()).await;
    let b = start(WsOptions::default()).await;
    let mut owner_a = owner(&a, "panghy", 7).await;
    let mut owner_b = owner(&b, "shared-instance-github-handle", 8).await;
    let mut member_a = sharing::member(&a, &"a1".repeat(32), "gitlab", "gitlab.example").await;
    let mut member_b = sharing::member(&b, &"b1".repeat(32), "gitlab", "gitlab.example").await;
    let mut guest_b = Guest::connect(&b, &"c1".repeat(32)).await;
    let ws = WorkspaceId::new();
    a.store
        .insert_workspace(&fixture_workspace(&ws))
        .await
        .unwrap();
    a.store
        .insert_note(&fixture_note(&ws, "authors", "Anchor"))
        .await
        .unwrap();
    let agent = AgentId::new();
    sqlx::query("INSERT INTO agent_session (id,workspace_id,name,status,created_at,updated_at) VALUES (?,?,'Historical','idle','2020-01-01','2020-01-01')").bind(agent.as_str()).bind(ws.as_str()).execute(a.store.write_pool()).await.unwrap();
    for (id, metadata) in [
        ("legacy", None),
        ("legacy-scalar", Some(json!("original scalar"))),
        (
            "legacy-array",
            Some(
                json!(["original",null,{"fromPrincipalId":"forged","humanAuthor":{"login":"forged"}}]),
            ),
        ),
        ("legacy-null", Some(serde_json::Value::Null)),
        (
            "other",
            Some(json!({"fromPrincipalId":member_a.principal.id})),
        ),
        (
            "unresolved",
            Some(json!({"fromPrincipalId":owner_b.principal.id})),
        ),
    ] {
        a.store
            .append_agent_message_with_id(
                &agent,
                id,
                "user",
                &json!([{"type":"text","text":id}]),
                metadata.as_ref(),
                "2020-01-01T00:00:00Z",
            )
            .await
            .unwrap();
    }
    a.store
        .append_agent_message_with_id(
            &agent,
            "assistant",
            "assistant",
            &json!([{"type":"text","text":"unchanged reply"}]),
            None,
            "2020-01-01T00:00:01Z",
        )
        .await
        .unwrap();
    let comment=member_a.call("comment.add",json!({"workspaceId":ws,"noteId":"authors","searchContext":"Anchor","commentTarget":"Anchor","comment":"Original comment","authorPrincipalId":owner_a.principal.id,"authorIdentity":{"provider":"github","host":"github.com","externalUserId":"forged"}})).await;
    let comment_id = comment["result"]["commentId"]
        .as_str()
        .unwrap_or_else(|| panic!("{comment}"));
    let pending = member_a
        .call(
            "agent.queueMessage",
            json!({"agentId":agent,"content":"Pending original input","messageMetadata":{"humanAuthorOriginalMetadata":["old",null,{"humanAuthor":{"login":"forged"},"fromPrincipalId":owner_a.principal.id}]}}),
        )
        .await;
    let pending_id = pending["result"]["queuedMessage"]["id"]
        .as_str()
        .unwrap_or_else(|| panic!("{pending}"));
    // A same local ID on the receiving host is a different person and must
    // never become the historical author or gain a workspace grant.
    let mut collision = member_a.principal.clone();
    collision.login = Some("unrelated-local".into());
    collision.identity = Some(PrincipalIdentity::github(999));
    b.store.upsert_principal(&collision).await.unwrap();
    relay(&a, &mut member_a, &mut owner_b, &ws).await;
    let history = owner_b
        .call("agent.getConversation", json!({"agentId":agent}))
        .await;
    let rows = history["result"]["messages"].as_array().unwrap();
    let slim = owner_b
        .call(
            "agent.getConversation",
            json!({"agentId":agent,"projection":"slim"}),
        )
        .await;
    for row in rows {
        let slim_row = slim["result"]["messages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["id"] == row["id"])
            .unwrap();
        assert_eq!(slim_row["author"], row["author"]);
    }
    for (id, login) in [("legacy", "panghy"), ("other", "guest")] {
        let row = rows.iter().find(|r| r["id"] == id).unwrap();
        assert_eq!(row["author"]["login"], login, "{row}");
        assert!(row["author"]["principalId"].is_null());
        assert!(row["metadata"].get("fromPrincipalId").is_none());
    }
    for (id, original) in [
        ("legacy-scalar", json!("original scalar")),
        (
            "legacy-array",
            json!(["original",null,{"fromPrincipalId":"forged","humanAuthor":{"login":"forged"}}]),
        ),
        ("legacy-null", serde_json::Value::Null),
    ] {
        let row = rows.iter().find(|r| r["id"] == id).unwrap();
        assert_eq!(row["author"]["login"], "panghy");
        assert!(row["author"]["principalId"].is_null());
        assert_eq!(
            row["metadata"].get("humanAuthorOriginalMetadata"),
            Some(&original)
        );
    }
    let unknown = rows.iter().find(|r| r["id"] == "unresolved").unwrap();
    assert!(
        unknown["author"]["login"].is_null(),
        "foreign local ID cannot resolve: {unknown}"
    );
    assert!(rows
        .iter()
        .find(|r| r["id"] == "assistant")
        .unwrap()
        .get("author")
        .is_none());
    assert!(!b
        .store
        .list_workspace_members(&ws)
        .await
        .unwrap()
        .iter()
        .any(|m| m.principal_id == collision.id));
    let thread = owner_b
        .call(
            "comment.getThread",
            json!({"workspaceId":ws,"noteId":"authors","commentId":comment_id}),
        )
        .await;
    let root = &thread["result"]["rootComment"];
    assert_eq!(root["author"], "guest", "{thread}");
    assert_eq!(root["authorIdentity"]["host"], "gitlab.example");
    assert!(root.get("authorPrincipalId").is_none());
    let list = owner_b
        .call("comment.list", json!({"workspaceId":ws,"noteId":"authors"}))
        .await;
    assert_eq!(
        list["result"]["threads"][0]["latestCommentAuthorIdentity"],
        root["authorIdentity"]
    );
    assert!(list["result"]["threads"][0]
        .get("latestCommentAuthorPrincipalId")
        .is_none());
    b.store
        .add_workspace_member(&ws, &guest_b.principal.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    for caller in [&mut member_b, &mut guest_b] {
        let queue = caller
            .call("agent.getQueue", json!({"agentId":agent}))
            .await;
        assert_eq!(queue["result"]["queue"], json!([]), "{queue}");
        for method in [
            "agent.sendQueuedMessageNow",
            "agent.removeQueuedMessage",
            "agent.editQueuedMessage",
        ] {
            let refused=caller.call(method,json!({"workspaceId":ws,"agentId":agent,"messageId":pending_id,"content":"forged"})).await;
            assert!(refused.get("error").is_some(), "{method}: {refused}");
        }
    }
    let owner_edit = owner_b
        .call(
            "agent.editQueuedMessage",
            json!({"agentId":agent,"messageId":pending_id,"content":"forged"}),
        )
        .await;
    assert!(owner_edit.get("error").is_some(), "{owner_edit}");
    let sent = owner_b
        .call(
            "agent.sendQueuedMessageNow",
            json!({"workspaceId":ws,"agentId":agent,"messageId":pending_id}),
        )
        .await;
    assert_eq!(sent["result"]["queued"], false, "{sent}");
    let new=owner_b.call("agent.appendMessage",json!({"agentId":agent,"role":"user","contentBlocks":[{"type":"text","text":"B contribution"}],"metadata":{"humanAuthor":{"login":"forged"}}})).await;
    let new_id = new["result"]["message"]["id"]
        .as_str()
        .unwrap_or_else(|| panic!("{new}"));
    // Remove only this disposable original so the return archive can retain
    // its workspace ID. Imported B sharing configuration must stay behind.
    a.store.delete_workspace(&ws).await.unwrap();
    relay(&b, &mut owner_b, &mut owner_a, &ws).await;
    let returned = owner_a
        .call("agent.getConversation", json!({"agentId":agent}))
        .await;
    let returned = returned["result"]["messages"].as_array().unwrap();
    for old in rows {
        let row = returned.iter().find(|r| r["id"] == old["id"]).unwrap();
        for key in ["author", "timestamp", "contentBlocks", "metadata"] {
            assert_eq!(row[key], old[key], "{key}: {row}");
        }
    }
    assert_eq!(
        returned.iter().find(|r| r["id"] == new_id).unwrap()["author"]["login"],
        "shared-instance-github-handle"
    );
    let sent = returned.iter().find(|r| r["id"] == pending_id).unwrap();
    assert_eq!(sent["author"]["identity"]["host"], "gitlab.example");
    assert!(sent["author"]["principalId"].is_null());
    assert_eq!(
        sent["metadata"]["humanAuthorOriginalMetadata"],
        json!(["old",null,{"humanAuthor":{"login":"forged"},"fromPrincipalId":owner_a.principal.id}])
    );
    assert!(!a
        .store
        .list_workspace_members(&ws)
        .await
        .unwrap()
        .iter()
        .any(|m| m.principal_id == guest_b.principal.id));
    drop((owner_a, owner_b, member_a, member_b, guest_b));
    a.ws.stop().await;
    b.ws.stop().await;
}

#[tokio::test]
async fn qualified_comment_subscription_and_repairs_keep_creation_identity_over_wss() {
    let srv = start(WsOptions::default()).await;
    let mut owner = owner(&srv, "owner", 7).await;
    let mut member = sharing::member(&srv, &"de".repeat(32), "gitlab", "gitlab.example").await;
    let ws = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&ws))
        .await
        .unwrap();
    srv.store
        .insert_note(&fixture_note(&ws, "identity", "Anchor text"))
        .await
        .unwrap();
    let mut stream = PresenceClient::open(srv.port, srv.cfg.clone(), TOKEN).await;
    let sub = stream
        .call(
            1,
            "comment.subscribe",
            json!({"workspaceId":ws,"noteId":"identity"}),
        )
        .await;
    let sub_id = sub["result"]["subscriptionId"]
        .as_str()
        .unwrap_or_else(|| panic!("{sub}"));
    assert_eq!(stream.push(sub_id).await["snapshot"], json!([]));
    let mut events = PresenceClient::open(srv.port, srv.cfg.clone(), TOKEN).await;
    events
        .call(
            1,
            "events.subscribe",
            json!({"workspaceId":ws,"eventTypes":["comment:added","comment:resolved"]}),
        )
        .await;
    let added=member.call("comment.add",json!({"workspaceId":ws,"noteId":"identity","searchContext":"Anchor text","commentTarget":"Anchor","comment":"Original","authorPrincipalId":42,"authorIdentity":["invalid"]})).await;
    let id = added["result"]["commentId"]
        .as_str()
        .unwrap_or_else(|| panic!("{added}"));
    let event = events.event("comment:added").await;
    assert_eq!(event["data"], json!({"noteId":"identity","commentId":id}));
    let delta = stream.push(sub_id).await;
    let thread = &delta["delta"]["updated"][0];
    let author = thread["comments"][0].clone();
    assert_eq!(
        author["authorPrincipalId"], member.principal.id.0,
        "{delta}"
    );
    assert_eq!(
        author["authorIdentity"],
        json!(member.principal.identity_key().unwrap())
    );
    assert_eq!(
        thread["latestCommentAuthorIdentity"],
        author["authorIdentity"]
    );
    // Durable role/profile changes cannot rewrite prior comments. No grant
    // is needed for the owner's later repair and resolution.
    sqlx::query("DELETE FROM host_member WHERE principal_id=?")
        .bind(member.principal.id.as_str())
        .execute(srv.store.write_pool())
        .await
        .unwrap();
    member.principal.identity = None;
    member.principal.login = Some("renamed".into());
    srv.store.upsert_principal(&member.principal).await.unwrap();
    let edit = owner
        .call(
            "note.edit",
            json!({"workspaceId":ws,"noteId":"identity","old":"text","new":"updated text"}),
        )
        .await;
    assert!(edit.get("error").is_none(), "{edit}");
    let resolve = owner
        .call(
            "comment.resolveThread",
            json!({"workspaceId":ws,"noteId":"identity","commentId":id,"resolved":true}),
        )
        .await;
    assert!(resolve.get("error").is_none(), "{resolve}");
    let event = events.event("comment:resolved").await;
    assert_eq!(event["data"].as_object().unwrap().len(), 3);
    let read = owner
        .call(
            "comment.getThread",
            json!({"workspaceId":ws,"noteId":"identity","commentId":id}),
        )
        .await;
    let root = &read["result"]["rootComment"];
    for key in [
        "author",
        "authorType",
        "authorPrincipalId",
        "authorIdentity",
    ] {
        assert_eq!(root[key], author[key], "{key}: {read}");
    }
    let fresh = stream
        .call(
            2,
            "comment.subscribe",
            json!({"workspaceId":ws,"noteId":"identity"}),
        )
        .await;
    let snapshot = stream
        .push(fresh["result"]["subscriptionId"].as_str().unwrap())
        .await;
    assert_eq!(
        snapshot["snapshot"][0]["comments"][0]["authorIdentity"],
        author["authorIdentity"]
    );
    events.close().await;
    stream.close().await;
    drop((owner, member));
    srv.ws.stop().await;
}
