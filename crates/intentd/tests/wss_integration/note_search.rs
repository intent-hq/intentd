use super::*;
use serde_json::json;

async fn search(client: &mut Guest, params: Value) -> Value {
    let reply = client.call("search.notes", params).await;
    assert!(reply.get("error").is_none(), "{reply}");
    let result = reply["result"].clone();
    assert_eq!(result["indexed"], true, "{reply}");
    assert!(result["requestId"].is_string(), "{reply}");
    assert!(result["matches"].is_array(), "always inline: {reply}");
    result
}

async fn member(srv: &Server, token: &str) -> Guest {
    let client = Guest::connect(srv, token).await;
    sqlx::query("INSERT INTO host_member(principal_id,added_at) VALUES (?,?)")
        .bind(client.principal.id.as_str())
        .bind(now_iso())
        .execute(srv.store.write_pool())
        .await
        .unwrap();
    client
}

#[tokio::test]
async fn indexed_notes_permissions_ranking_filters_and_identity() {
    let srv = start(WsOptions::default()).await;
    let mut owner = member(&srv, &"31".repeat(32)).await;
    let mut guest = Guest::connect(&srv, &"32".repeat(32)).await;
    let mut outsider = Guest::connect(&srv, &"33".repeat(32)).await;
    let shared = WorkspaceId::from("search-shared");
    let archived = WorkspaceId::from("search-archived");
    let hidden = WorkspaceId::from("search-hidden");
    for ws in [&shared, &archived, &hidden] {
        let mut row = fixture_workspace(ws);
        row.archived = ws == &archived;
        srv.store.insert_workspace(&row).await.unwrap();
        let mut note = fixture_note(ws, "spec", "Body-only nebula\n\tdeployment checklist.");
        if ws == &hidden {
            note.title = "nebula deployment".into();
        }
        srv.store.insert_note(&note).await.unwrap();
    }
    for ws in [&shared, &archived] {
        srv.store
            .add_workspace_member(
                ws,
                &guest.principal.id,
                intent_core::WorkspaceRole::Collaborator,
            )
            .await
            .unwrap();
    }
    let mut old = fixture_note(&shared, "old", "nebula deployment");
    old.is_archived = true;
    srv.store.insert_note(&old).await.unwrap();
    // The hidden title match really ranks first; permission filtering must
    // happen inside selection, not after this top-1 is chosen.
    let params = json!({"query":"NEBULA depl", "preferWorkspaceId":hidden,
        "limit":1, "includeArchived":false});
    let all = search(&mut owner, params.clone()).await;
    assert_eq!(all["matches"][0]["workspaceId"], hidden.as_str());
    let visible = search(&mut guest, params).await;
    assert_eq!(visible["matches"].as_array().unwrap().len(), 1);
    assert_eq!(visible["matches"][0]["workspaceId"], shared.as_str());
    assert!(
        search(&mut outsider, json!({"query":"nebula"})).await["matches"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    for params in [
        json!({"query":"nebula", "limit":0}),
        json!({"query":""}),
        json!({"query":"*(:\""}),
        json!({"query":"nebula OR nonexistent"}),
    ] {
        assert!(search(&mut guest, params).await["matches"]
            .as_array()
            .unwrap()
            .is_empty());
    }
    let rows = search(
        &mut guest,
        json!({"query":"nebula", "includeArchived":false,
        "requestId":"notes-ranked"}),
    )
    .await;
    assert_eq!(rows["requestId"], "notes-ranked");
    let hits = rows["matches"].as_array().unwrap();
    assert_eq!(hits.len(), 2);
    for (hit, ws, archived_flag) in [(&hits[0], &shared, false), (&hits[1], &archived, true)] {
        assert_eq!(hit["noteId"], "spec");
        assert_eq!(hit["workspaceId"], ws.as_str());
        assert_eq!(hit["title"], "spec");
        assert_eq!(hit["isArchived"], false);
        assert_eq!(hit["workspaceArchived"], archived_flag);
        assert!(hit["updatedAt"].is_string());
        assert!(hit["score"].is_number());
        assert!(hit["preview"]
            .as_str()
            .unwrap()
            .contains("nebula deployment"));
        assert!(!hit["preview"].as_str().unwrap().contains('\n'));
    }
    // Null means omission, including legacy archive inclusion and no cap.
    let legacy = search(
        &mut guest,
        json!({"query":"nebula", "workspaceId":null,
        "preferWorkspaceId":null,"includeArchived":null,"limit":null,"requestId":null}),
    )
    .await;
    assert_eq!(legacy["matches"].as_array().unwrap().len(), 3);
    let scoped = search(
        &mut guest,
        json!({"query":"nebula", "workspaceId":archived,
        "preferWorkspaceId":shared,"includeArchived":false}),
    )
    .await;
    assert_eq!(scoped["matches"].as_array().unwrap().len(), 1);
    assert_eq!(scoped["matches"][0]["workspaceId"], archived.as_str());
    for ws in [hidden.as_str(), "missing-workspace"] {
        // Even an empty query and zero limit must authorize the hard scope.
        let reply = guest
            .call(
                "search.notes",
                json!({"query":"", "workspaceId":ws,"limit":0}),
            )
            .await;
        assert_eq!(reply["error"]["code"], -32602, "{reply}");
        assert_eq!(reply["error"]["data"]["code"], "not-found", "{reply}");
    }
    let missing = owner
        .call("search.notes", json!({"query":"", "workspaceId":"missing"}))
        .await;
    assert_eq!(missing["error"]["data"]["code"], "not-found");
    srv.ws.stop().await;
}

#[tokio::test]
async fn indexed_notes_validation_and_empty_capability() {
    let srv = start(WsOptions::default()).await;
    let mut client = member(&srv, &"34".repeat(32)).await;
    for params in [
        json!({"query":""}),
        json!({"query":" *(\"-:"}),
        json!({"query":"needle", "limit":0}),
        json!({"query":"unmatched"}),
    ] {
        assert!(search(&mut client, params).await["matches"]
            .as_array()
            .unwrap()
            .is_empty());
    }
    for params in [
        json!({}),
        json!({"query":null}),
        json!({"query":3}),
        json!({"query":true}),
        json!({"query":[]}),
        json!({"query":{}}),
    ] {
        let reply = client.call("search.notes", params).await;
        assert_eq!(reply["error"]["code"], -32602, "{reply}");
        assert_eq!(reply["error"]["data"]["code"], "invalid-params", "{reply}");
    }
    for (key, values) in [
        (
            "workspaceId",
            vec![json!(""), json!(5), json!(false), json!([]), json!({})],
        ),
        (
            "preferWorkspaceId",
            vec![json!(""), json!(5), json!(false), json!([]), json!({})],
        ),
        (
            "requestId",
            vec![json!(5), json!(false), json!([]), json!({})],
        ),
        (
            "limit",
            vec![
                json!(-1),
                json!(0.5),
                json!(1.0),
                json!("1"),
                json!(false),
                json!([]),
                json!({}),
                json!(9_223_372_036_854_775_808_u64),
            ],
        ),
        (
            "includeArchived",
            vec![json!(0), json!("false"), json!([]), json!({})],
        ),
    ] {
        for value in values {
            let mut params = json!({"query":""});
            params[key] = value;
            let reply = client.call("search.notes", params.clone()).await;
            assert_eq!(reply["error"]["code"], -32602, "{params}: {reply}");
            assert_eq!(
                reply["error"]["data"]["code"], "invalid-params",
                "{params}: {reply}"
            );
        }
    }
    let reply = search(
        &mut client,
        json!({"query":"", "limit":i64::MAX,
        "preferWorkspaceId":"missing", "requestId":""}),
    )
    .await;
    assert_eq!(reply["requestId"], "");
    srv.ws.stop().await;
}

#[tokio::test]
async fn indexed_notes_mutations_reconnect_and_large_scoped_inline() {
    let srv = start(WsOptions::default()).await;
    let token = "35".repeat(32);
    let mut client = member(&srv, &token).await;
    let ws = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&ws))
        .await
        .unwrap();
    let created = client
        .call(
            "note.create",
            json!({"workspaceId":ws,
        "title":"Ordinary", "content":"initialuniqueterm", "tags":["tagunique"]}),
        )
        .await;
    assert!(created.get("error").is_none(), "{created}");
    let result = search(&mut client, json!({"query":"initialuniqueterm"})).await;
    let id = result["matches"][0]["noteId"].as_str().unwrap().to_string();
    assert_eq!(
        search(&mut client, json!({"query":"tagunique"})).await["matches"][0]["noteId"],
        id
    );
    let update = client
        .call(
            "note.setContent",
            json!({"workspaceId":ws,"noteId":id,
        "content":"replacementuniqueterm", "confirmReplacement":true}),
        )
        .await;
    assert!(update.get("error").is_none(), "{update}");
    assert!(
        search(&mut client, json!({"query":"initialuniqueterm"})).await["matches"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let update = client
        .call(
            "note.updateMetadata",
            json!({"workspaceId":ws,"noteId":id,
        "title":"renameduniqueterm", "tags":["retaggedunique"]}),
        )
        .await;
    assert!(update.get("error").is_none(), "{update}");
    assert!(
        search(&mut client, json!({"query":"tagunique"})).await["matches"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    for query in [
        "replacementuniqueterm",
        "renameduniqueterm",
        "retaggedunique",
    ] {
        assert_eq!(
            search(&mut client, json!({"query":query})).await["matches"][0]["noteId"],
            id
        );
    }
    // Reconnect the same authenticated principal, then open the returned identity.
    client.ws.close(None).await.unwrap();
    let url = format!("wss://localhost:{}/ws?token={token}", srv.port);
    client.ws = common::wss_connect_with_retry(srv.port, srv.cfg.clone(), &url).await;
    let found = search(&mut client, json!({"query":"replacementuniqueterm"})).await;
    let hit = &found["matches"][0];
    let opened = client
        .call(
            "note.get",
            json!({"workspaceId":hit["workspaceId"],"noteId":hit["noteId"]}),
        )
        .await;
    assert!(opened.get("error").is_none(), "{opened}");
    let deleted = client
        .call("note.delete", json!({"workspaceId":ws,"noteId":id}))
        .await;
    assert!(deleted.get("error").is_none(), "{deleted}");
    assert!(
        search(&mut client, json!({"query":"replacementuniqueterm"})).await["matches"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    for n in 0..30 {
        srv.store
            .insert_note(&fixture_note(
                &ws,
                &format!("large-{n:02}"),
                "bulkuniqueterm",
            ))
            .await
            .unwrap();
    }
    for limit in [json!(null), json!(30)] {
        let got = search(
            &mut client,
            json!({"query":"bulkuniqueterm", "workspaceId":ws,
            "limit":limit,"requestId":"large-scoped"}),
        )
        .await;
        assert_eq!(got["matches"].as_array().unwrap().len(), 30);
        assert_eq!(got["requestId"], "large-scoped");
    }
    assert_eq!(
        client
            .call("search.cancel", json!({"requestId":"large-scoped"}))
            .await["result"]["ok"],
        true
    );
    let archived = client
        .call("workspace.archive", json!({"workspaceId":ws}))
        .await;
    assert!(archived.get("error").is_none(), "{archived}");
    assert_eq!(
        search(
            &mut client,
            json!({"query":"bulkuniqueterm","includeArchived":false,"limit":1})
        )
        .await["matches"][0]["workspaceArchived"],
        true
    );
    let deleted = client
        .call("workspace.delete", json!({"workspaceId":ws}))
        .await;
    assert!(deleted.get("error").is_none(), "{deleted}");
    assert!(
        search(&mut client, json!({"query":"bulkuniqueterm"})).await["matches"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    srv.ws.stop().await;
}
