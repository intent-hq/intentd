use super::*;
use intent_core::WorkspaceRole;
use serde_json::json;

#[tokio::test]
async fn discussion_owner_and_inherited_member_access_over_wss() {
    let srv = start(WsOptions::default()).await;
    let mut member = sharing::member(&srv, &"dd".repeat(32), "github", "github.com").await;
    let ws = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&ws))
        .await
        .unwrap();
    let mut owner = PresenceClient::open(srv.port, srv.cfg.clone(), TOKEN).await;
    let primary = srv.store.get_primary_principal().await.unwrap();
    let sent = owner
        .call(
            1,
            "discussion.send",
            json!({"workspaceId":ws,"messageId":"owner","body":"Owner here"}),
        )
        .await;
    assert_eq!(
        sent["result"]["message"]["author"]["principalId"],
        primary.id.0
    );
    let sent = member
        .call(
            "discussion.send",
            json!({"workspaceId":ws,"messageId":"member","body":"Host member here"}),
        )
        .await;
    assert_eq!(
        sent["result"]["message"]["author"]["principalId"],
        member.principal.id.0
    );
    let history = member
        .call("discussion.list", json!({"workspaceId":ws}))
        .await;
    assert_eq!(history["result"]["messages"].as_array().unwrap().len(), 2);
    let denied = member
        .call(
            "discussion.list",
            json!({"workspaceId":WorkspaceId::chief()}),
        )
        .await;
    assert_eq!(denied["error"]["data"]["code"], "not-found");
    srv.store
        .remove_host_member(&member.principal.id)
        .await
        .unwrap();
    let denied = member
        .call("discussion.list", json!({"workspaceId":ws}))
        .await;
    assert_eq!(denied["error"]["data"]["code"], "not-found");
    owner.close().await;
    srv.ws.stop().await;
}

#[tokio::test]
async fn discussion_human_attribution_isolation_live_history_and_retry_over_wss() {
    let srv = start(WsOptions::default()).await;
    let mut alice = Guest::connect(&srv, &"da".repeat(32)).await;
    let mut bob = Guest::connect(&srv, &"db".repeat(32)).await;
    let mut outsider = Guest::connect(&srv, &"dc".repeat(32)).await;
    alice.principal.login = Some("alice".into());
    alice.principal.avatar_url = Some("https://example.org/alice.png".into());
    srv.store.upsert_principal(&alice.principal).await.unwrap();
    let ws = WorkspaceId::new();
    let private = WorkspaceId::new();
    for id in [&ws, &private] {
        srv.store
            .insert_workspace(&fixture_workspace(id))
            .await
            .unwrap();
    }
    for person in [&alice.principal, &bob.principal] {
        srv.store
            .add_workspace_member(&ws, &person.id, WorkspaceRole::Collaborator)
            .await
            .unwrap();
    }
    let mut events = PresenceClient::open(srv.port, srv.cfg.clone(), &"db".repeat(32)).await;
    let mut hidden_events = PresenceClient::open(srv.port, srv.cfg.clone(), &"dc".repeat(32)).await;
    for client in [&mut events, &mut hidden_events] {
        let sub = client
            .call(
                1,
                "events.subscribe",
                json!({"eventTypes":["discussion:message"]}),
            )
            .await;
        assert!(sub["result"]["subscriptionId"].is_string(), "{sub}");
    }
    let empty = bob.call("discussion.list", json!({"workspaceId":ws})).await;
    assert_eq!(empty["result"], json!({"messages":[],"hasMore":false}));
    let sent = alice
        .call(
            "discussion.send",
            json!({
                "workspaceId":ws,"messageId":"same-client-id","body":"Hello\nteam",
                "author":{"principalId":bob.principal.id,"login":"forged"},
                "principalId":bob.principal.id,"createdAt":"forged","sequence":99
            }),
        )
        .await;
    let first = sent["result"]["message"].clone();
    assert!(first["id"].is_string(), "{sent}");
    assert_ne!(first["id"], "same-client-id");
    assert_eq!(first.as_object().unwrap().len(), 6);
    assert_eq!(first["workspaceId"], ws.0);
    assert_eq!(first["sequence"], 1);
    assert_eq!(first["body"], "Hello\nteam");
    assert_eq!(
        first["author"],
        json!({"principalId":alice.principal.id,"login":"alice","displayName":"Guest User","avatarUrl":"https://example.org/alice.png"})
    );
    assert!(intent_core::parse_iso(first["createdAt"].as_str().unwrap()).is_some());
    let event = events.event("discussion:message").await;
    assert_eq!(event["workspaceId"], ws.0);
    assert_eq!(event["data"], json!({"workspaceId":ws,"message":first}));
    assert_eq!(event["actor"]["id"], alice.principal.id.0);
    assert_eq!(event["actor"]["type"], "user");
    assert_eq!(event["actor"]["name"], "alice");
    assert!(
        srv.store
            .query_events(&intent_store::EventQuery {
                workspace_id: Some(ws.clone()),
                event_type_prefix: Some("discussion:".into()),
                ..Default::default()
            })
            .await
            .unwrap()
            .is_empty(),
        "live discussion messages must not duplicate durable history in the event log"
    );
    alice.principal.login = Some("renamed".into());
    srv.store.upsert_principal(&alice.principal).await.unwrap();
    let mismatch = alice
        .call(
            "discussion.send",
            json!({"workspaceId":ws,"messageId":"same-client-id","body":"attempted change"}),
        )
        .await;
    assert_eq!(mismatch["error"]["code"], -32602);
    assert_eq!(mismatch["error"]["data"]["code"], "invalid-params");
    let retry = alice
        .call(
            "discussion.send",
            json!({"workspaceId":ws,"messageId":"same-client-id","body":"Hello\nteam"}),
        )
        .await;
    assert_eq!(retry["result"]["message"], first);
    let second = bob
        .call(
            "discussion.send",
            json!({"workspaceId":ws,"messageId":"same-client-id","body":"Hi Alice"}),
        )
        .await["result"]["message"]
        .clone();
    assert_eq!(second["sequence"], 2);
    assert_ne!(first["id"], second["id"]);
    assert_eq!(second["author"]["principalId"], bob.principal.id.0);
    let second_event = events.event("discussion:message").await;
    assert_eq!(
        second_event["data"]["message"], second,
        "retry must not rebroadcast the first row"
    );
    let recent = alice
        .call("discussion.list", json!({"workspaceId":ws,"limit":1}))
        .await;
    assert_eq!(
        recent["result"],
        json!({"messages":[second],"hasMore":true})
    );
    let earlier = alice
        .call(
            "discussion.list",
            json!({"workspaceId":ws,"beforeSequence":2,"limit":1}),
        )
        .await;
    assert_eq!(
        earlier["result"],
        json!({"messages":[first],"hasMore":false})
    );
    for (after, messages, more) in [
        (0, json!([first]), true),
        (1, json!([second]), false),
        (2, json!([]), false),
    ] {
        let page = alice
            .call(
                "discussion.list",
                json!({"workspaceId":ws,"afterSequence":after,"limit":1}),
            )
            .await;
        assert_eq!(page["result"], json!({"messages":messages,"hasMore":more}));
    }
    // A fresh authenticated connection catches up from durable history.
    let mut reconnect = PresenceClient::open(srv.port, srv.cfg.clone(), &"da".repeat(32)).await;
    let history = reconnect
        .call(1, "discussion.list", json!({"workspaceId":ws}))
        .await;
    assert_eq!(
        history["result"],
        json!({"messages":[first,second],"hasMore":false})
    );
    let alice_id = alice.principal.id.clone();
    for client in [&mut alice, &mut outsider] {
        let denied_workspace = if client.principal.id == alice_id {
            &private
        } else {
            &ws
        };
        for method in ["discussion.list", "discussion.send"] {
            let denied = client
                .call(
                    method,
                    json!({"workspaceId":denied_workspace,"messageId":"denied","body":"hidden"}),
                )
                .await;
            assert_eq!(denied["error"]["data"]["code"], "not-found", "{denied}");
        }
    }
    for params in [
        json!({"limit":0}),
        json!({"limit":101}),
        json!({"limit":1.5}),
        json!({"beforeSequence":0}),
        json!({"beforeSequence":"1"}),
        json!({"afterSequence":-1}),
        json!({"afterSequence":"1"}),
        json!({"afterSequence":1.5}),
        json!({"afterSequence":null}),
        json!({"afterSequence":u64::MAX}),
        json!({"beforeSequence":2,"afterSequence":0}),
    ] {
        let mut params = params;
        params["workspaceId"] = json!(ws);
        assert_eq!(
            alice.call("discussion.list", params).await["error"]["code"],
            -32602
        );
    }
    for body in [" \n".to_string(), "é".repeat(8193)] {
        assert_eq!(
            alice
                .call(
                    "discussion.send",
                    json!({"workspaceId":ws,"messageId":"bad","body":body})
                )
                .await["error"]["code"],
            -32602
        );
    }
    assert!(hidden_events
        .all_events("discussion:message")
        .await
        .is_empty());
    let mut owner = PresenceClient::open(srv.port, srv.cfg.clone(), TOKEN).await;
    let removed = owner
        .call(
            1,
            "workspace.members.remove",
            json!({"workspaceId":ws,"principalId":bob.principal.id}),
        )
        .await;
    assert_eq!(removed["result"]["removed"], true, "{removed}");
    for method in ["discussion.list", "discussion.send"] {
        let denied = bob
            .call(
                method,
                json!({"workspaceId":ws,"messageId":"same-client-id","body":"retry after removal"}),
            )
            .await;
        assert_eq!(denied["error"]["data"]["code"], "not-found", "{denied}");
    }
    let third = alice
        .call(
            "discussion.send",
            json!({"workspaceId":ws,"messageId":"third","body":"after removal"}),
        )
        .await;
    assert_eq!(third["result"]["message"]["sequence"], 3);
    assert!(
        events.all_events("discussion:message").await.is_empty(),
        "removed collaborator must not receive later messages"
    );
    events.close().await;
    hidden_events.close().await;
    reconnect.close().await;
    owner.close().await;
    srv.ws.stop().await;
}
