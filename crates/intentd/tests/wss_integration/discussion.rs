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
    let conversation = empty["result"]["conversation"].clone();
    assert_eq!(conversation["id"], "default");
    assert_eq!(
        empty["result"],
        json!({"conversation":conversation,"messages":[],"hasMore":false})
    );
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
    assert_eq!(first.as_object().unwrap().len(), 7);
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
    assert_eq!(
        event["data"],
        json!({"workspaceId":ws,"conversationId":"default","conversation":conversation,"message":first})
    );
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
        json!({"conversation":conversation,"messages":[second],"hasMore":true})
    );
    let earlier = alice
        .call(
            "discussion.list",
            json!({"workspaceId":ws,"beforeSequence":2,"limit":1}),
        )
        .await;
    assert_eq!(
        earlier["result"],
        json!({"conversation":conversation,"messages":[first],"hasMore":false})
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
        assert_eq!(
            page["result"],
            json!({"conversation":conversation,"messages":messages,"hasMore":more})
        );
    }
    // A fresh authenticated connection catches up from durable history.
    let mut reconnect = PresenceClient::open(srv.port, srv.cfg.clone(), &"da".repeat(32)).await;
    let history = reconnect
        .call(1, "discussion.list", json!({"workspaceId":ws}))
        .await;
    assert_eq!(
        history["result"],
        json!({"conversation":conversation,"messages":[first,second],"hasMore":false})
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

#[intent_test_macros::daemon_test]
async fn discussion_typing_presence_over_wss_is_transient_scoped_and_connection_owned() {
    use intent_core::events::PRESENCE_CHANGED;
    let srv = start(WsOptions::default()).await;
    let ws = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&ws))
        .await
        .unwrap();
    let alice_token = "e1".repeat(32);
    let bob_token = "e2".repeat(32);
    let outsider_token = "e3".repeat(32);
    let alice = seed_principal(&srv.store, "alice", &alice_token).await;
    let bob = seed_principal(&srv.store, "bob", &bob_token).await;
    seed_principal(&srv.store, "outsider", &outsider_token).await;
    for principal in [&alice.id, &bob.id] {
        srv.store
            .add_workspace_member(&ws, principal, WorkspaceRole::Collaborator)
            .await
            .unwrap();
    }
    let mut one = PresenceClient::open(srv.port, srv.cfg.clone(), &alice_token).await;
    let mut two = PresenceClient::open(srv.port, srv.cfg.clone(), &alice_token).await;
    let mut observer = PresenceClient::open(srv.port, srv.cfg.clone(), &bob_token).await;
    let mut outsider = PresenceClient::open(srv.port, srv.cfg.clone(), &outsider_token).await;
    let active = json!({"focus":[],"discussionTyping":{"workspaceId":ws}});
    let before_hello = one.call(1, "presence.update", active.clone()).await;
    assert_eq!(before_hello["error"]["code"], -32602);
    assert_eq!(before_hello["error"]["data"]["code"], "invalid-params");
    for client in [&mut one, &mut two, &mut observer, &mut outsider] {
        let hello = client
            .call(
                2,
                "client.hello",
                json!({"clientId":"typing-client","name":"Typing"}),
            )
            .await;
        assert!(hello.get("error").is_none(), "{hello}");
    }
    for client in [&mut observer, &mut outsider] {
        let sub = client
            .call(
                3,
                "events.subscribe",
                json!({"eventTypes":["presence:changed"]}),
            )
            .await;
        assert!(sub["result"]["subscriptionId"].is_string(), "{sub}");
    }
    let row = |data: &Value| {
        data["members"]
            .as_array()
            .unwrap()
            .iter()
            .find(|member| member["principalId"] == alice.id.0)
            .unwrap()
            .clone()
    };
    let snapshot = observer
        .call(4, "presence.snapshot", json!({"workspaceId":ws}))
        .await;
    assert_eq!(row(&snapshot["result"])["discussionTyping"], json!([]));
    let sent = one.call(4, "presence.update", active.clone()).await;
    assert_eq!(sent["jsonrpc"], "2.0");
    assert_eq!(sent["id"], 4);
    let source = sent["result"]["typingSource"].as_str().unwrap().to_string();
    assert_eq!(sent["result"], json!({"ok":true,"typingSource":source}));
    let event = observer.event(PRESENCE_CHANGED).await;
    assert_eq!(event["workspaceId"], ws.0);
    let first = row(&event["data"])["discussionTyping"][0].clone();
    assert_eq!(first["source"], source);
    assert_eq!(first.as_object().unwrap().len(), 4);
    assert!(intent_core::parse_iso(first["since"].as_str().unwrap()).is_some());
    assert!(first["pulse"].as_u64().unwrap() > 0);
    one.call(5, "presence.update", active.clone()).await;
    let event = observer.event(PRESENCE_CHANGED).await;
    let repeated = row(&event["data"])["discussionTyping"][0].clone();
    assert_eq!(repeated["since"], first["since"]);
    assert!(repeated["pulse"].as_u64().unwrap() > first["pulse"].as_u64().unwrap());
    let sent = two.call(4, "presence.update", active.clone()).await;
    let source2 = sent["result"]["typingSource"].as_str().unwrap();
    assert_ne!(source2, source);
    let event = observer.event(PRESENCE_CHANGED).await;
    let both = row(&event["data"])["discussionTyping"].clone();
    let entries = both.as_array().unwrap();
    assert_eq!(entries.len(), 2);
    assert!(entries[0]["source"].as_str().unwrap() < entries[1]["source"].as_str().unwrap());
    assert_eq!(
        entries.iter().find(|t| t["source"] == source).unwrap(),
        &repeated
    );
    observer
        .call(5, "presence.update", json!({"focus":[{"workspaceId":ws}]}))
        .await;
    let event = observer.event(PRESENCE_CHANGED).await;
    assert_eq!(
        row(&event["data"])["discussionTyping"],
        both,
        "unrelated updates do not refresh another source"
    );
    let snapshot = observer
        .call(6, "presence.snapshot", json!({"workspaceId":ws}))
        .await;
    assert_eq!(row(&snapshot["result"])["discussionTyping"], both);
    for (i, target) in [
        json!({}),
        json!([]),
        json!({"workspaceId":""}),
        json!({"workspaceId":7}),
    ]
    .into_iter()
    .enumerate()
    {
        let invalid = one
            .call(
                10 + u64::try_from(i).unwrap(),
                "presence.update",
                json!({"focus":[],"discussionTyping":target}),
            )
            .await;
        assert_eq!(invalid["error"]["data"]["code"], "invalid-params");
    }
    let denied = outsider.call(4, "presence.update", active.clone()).await;
    assert_eq!(denied["error"]["data"]["code"], "not-found");
    let hidden = outsider
        .call(5, "presence.snapshot", json!({"workspaceId":ws}))
        .await;
    assert_eq!(hidden["error"]["data"]["code"], "not-found");
    two.call(
        5,
        "presence.update",
        json!({"focus":[],"discussionTyping":null}),
    )
    .await;
    let event = observer.event(PRESENCE_CHANGED).await;
    assert_eq!(row(&event["data"])["discussionTyping"], json!([repeated]));
    one.call(20, "presence.update", json!({"focus":[]})).await;
    let event = observer.event(PRESENCE_CHANGED).await;
    assert_eq!(
        row(&event["data"])["discussionTyping"],
        json!([]),
        "omission clears"
    );
    one.call(21, "presence.update", active).await;
    let event = observer.event(PRESENCE_CHANGED).await;
    assert!(
        row(&event["data"])["discussionTyping"][0]["pulse"]
            .as_u64()
            .unwrap()
            > repeated["pulse"].as_u64().unwrap()
    );
    one.close().await;
    let event = observer.event(PRESENCE_CHANGED).await;
    assert_eq!(
        row(&event["data"])["discussionTyping"],
        json!([]),
        "disconnect clears only that connection"
    );
    assert!(srv
        .store
        .query_events(&intent_store::EventQuery {
            workspace_id: Some(ws.clone()),
            event_types: vec![PRESENCE_CHANGED.into()],
            ..Default::default()
        })
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        observer
            .call(7, "discussion.list", json!({"workspaceId":ws}))
            .await["result"]["messages"],
        json!([])
    );
    assert!(
        outsider.all_events(PRESENCE_CHANGED).await.is_empty(),
        "outsider receives no roster events"
    );
    two.close().await;
    observer.close().await;
    outsider.close().await;
    srv.ws.stop().await;
}

#[intent_test_macros::daemon_test]
async fn discussion_private_conversations_events_typing_and_retry_over_wss() {
    let srv = start(WsOptions::default()).await;
    let ws = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&ws))
        .await
        .unwrap();
    let tokens: Vec<_> = ["f1", "f2", "f3", "f4"]
        .iter()
        .map(|s| s.repeat(32))
        .collect();
    let mut people = Vec::new();
    for (name, token) in ["alice", "bob", "carol", "offline"].iter().zip(&tokens) {
        let p = seed_principal(&srv.store, name, token).await;
        srv.store
            .add_workspace_member(&ws, &p.id, WorkspaceRole::Collaborator)
            .await
            .unwrap();
        people.push(p);
    }
    let mut alice = PresenceClient::open(srv.port, srv.cfg.clone(), &tokens[0]).await;
    let mut bob = PresenceClient::open(srv.port, srv.cfg.clone(), &tokens[1]).await;
    let mut carol = PresenceClient::open(srv.port, srv.cfg.clone(), &tokens[2]).await;
    let mut owner = PresenceClient::open(srv.port, srv.cfg.clone(), TOKEN).await;
    for client in [&mut alice, &mut bob, &mut carol, &mut owner] {
        let hello = client
            .call(
                1,
                "client.hello",
                json!({"clientId":"private-discussion","name":"Discussion"}),
            )
            .await;
        assert!(hello.get("error").is_none(), "{hello}");
        assert_eq!(
            hello["result"]["server"]["capabilities"]["discussionConversations"],
            1
        );
        let sub = client
            .call(
                2,
                "events.subscribe",
                json!({"eventTypes":["discussion:message","presence:changed"]}),
            )
            .await;
        assert!(sub["result"]["subscriptionId"].is_string(), "{sub}");
        client.all_events("presence:changed").await;
    }
    let caps = alice.call(3, "system.capabilities", json!({})).await;
    assert_eq!(caps["result"]["discussionConversations"], 1, "{caps}");
    let picker = alice
        .call(
            4,
            "discussion.recipients",
            json!({"workspaceId":ws,"query":"offline","limit":1}),
        )
        .await;
    assert_eq!(
        picker["result"]["members"][0]["principalId"], people[3].id.0,
        "{picker}"
    );
    assert_eq!(picker["result"]["hasMore"], false);
    let params = json!({"workspaceId":ws,"messageId":"create","body":"Secret","recipientPrincipalIds":[people[1].id]});
    let created = alice.call(5, "discussion.send", params.clone()).await;
    let conversation = created["result"]["conversation"].clone();
    let id = conversation["id"].as_str().expect("private id").to_string();
    assert_eq!(conversation["kind"], "direct");
    assert_eq!(conversation["participants"].as_array().unwrap().len(), 2);
    assert_eq!(created["result"]["message"]["sequence"], 1);
    assert_eq!(
        created["result"]["message"]["author"]["principalId"],
        people[0].id.0
    );
    let event = bob.event("discussion:message").await;
    assert_eq!(
        event["data"],
        json!({"workspaceId":ws,"conversationId":id,"conversation":conversation,"message":created["result"]["message"]})
    );
    assert!(carol.all_events("discussion:message").await.is_empty());
    assert!(
        owner.all_events("discussion:message").await.is_empty(),
        "owner is not a participant"
    );
    let blocked_export = owner
        .call(20, "workspace.export.start", json!({"workspaceId":ws}))
        .await;
    assert_eq!(blocked_export["error"]["code"], -32603, "{blocked_export}");
    assert!(blocked_export["error"]["message"]
        .as_str()
        .unwrap()
        .starts_with("unsupported:"));
    let retry = alice.call(6, "discussion.send", params.clone()).await;
    assert_eq!(retry["result"], created["result"]);
    let mut mismatch = params.clone();
    mismatch["body"] = json!("changed");
    assert_eq!(
        alice.call(7, "discussion.send", mismatch).await["error"]["code"],
        -32602
    );
    let mut mismatch = params;
    mismatch["recipientPrincipalIds"] = json!([people[2].id]);
    assert_eq!(
        alice.call(8, "discussion.send", mismatch).await["error"]["code"],
        -32602
    );
    assert!(
        bob.all_events("discussion:message").await.is_empty(),
        "retries never rebroadcast"
    );
    for client in [&mut carol, &mut owner] {
        for (method, params) in [
            (
                "discussion.list",
                json!({"workspaceId":ws,"conversationId":id}),
            ),
            (
                "discussion.send",
                json!({"workspaceId":ws,"conversationId":id,"messageId":"intrude","body":"intrude"}),
            ),
            (
                "presence.update",
                json!({"focus":[],"discussionTyping":{"workspaceId":ws,"conversationId":id}}),
            ),
        ] {
            let denied = client.call(9, method, params).await;
            assert_eq!(denied["error"]["data"]["code"], "not-found", "{denied}");
        }
        let list = client
            .call(10, "discussion.conversations", json!({"workspaceId":ws}))
            .await;
        assert_eq!(list["result"]["conversations"].as_array().unwrap().len(), 1);
        assert_eq!(list["result"]["conversations"][0]["id"], "default");
    }
    let first = bob
        .call(
            11,
            "discussion.conversations",
            json!({"workspaceId":ws,"limit":1}),
        )
        .await;
    assert_eq!(first["result"]["conversations"][0]["id"], "default");
    assert_eq!(first["result"]["hasMore"], true);
    let next = bob
        .call(
            12,
            "discussion.conversations",
            json!({"workspaceId":ws,"beforeId":"default","limit":1}),
        )
        .await;
    assert_eq!(next["result"]["conversations"][0], conversation);
    assert_eq!(next["result"]["hasMore"], false);
    let typed = alice
        .call(
            13,
            "presence.update",
            json!({"focus":[],"discussionTyping":{"workspaceId":ws,"conversationId":id}}),
        )
        .await;
    assert!(typed.get("error").is_none(), "{typed}");
    let typing = presence_matching_frame(
        &mut bob.rx,
        &mut bob.skipped,
        |v| {
            v["method"] == "events.event"
                && v["params"]["event"]["type"] == "presence:changed"
                && v["params"]["event"]["data"]["members"]
                    .as_array()
                    .is_some_and(|members| {
                        members.iter().any(|m| {
                            m["discussionTyping"].as_array().is_some_and(|entries| {
                                entries.iter().any(|t| t["conversationId"] == id)
                            })
                        })
                    })
        },
        "private discussion typing event",
    )
    .await["params"]["event"]
        .clone();
    let row = |data: &Value| {
        data["members"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["principalId"] == people[0].id.0)
            .unwrap()
            .clone()
    };
    assert_eq!(
        row(&typing["data"])["discussionTyping"][0]["conversationId"],
        id
    );
    for client in [&mut carol, &mut owner] {
        let changed = presence_matching_frame(
            &mut client.rx,
            &mut client.skipped,
            |v| v["method"] == "events.event" && v["params"]["event"]["id"] == typing["id"],
            "same typing event projected for nonparticipant",
        )
        .await["params"]["event"]
            .clone();
        assert_eq!(row(&changed["data"])["discussionTyping"], json!([]));
        let snap = client
            .call(14, "presence.snapshot", json!({"workspaceId":ws}))
            .await;
        assert_eq!(row(&snap["result"])["discussionTyping"], json!([]));
    }
    let sent = bob
        .call(
            15,
            "discussion.send",
            json!({"workspaceId":ws,"conversationId":id,"messageId":"reply","body":"Reply"}),
        )
        .await;
    assert_eq!(sent["result"]["message"]["sequence"], 2);
    let history = bob
        .call(
            16,
            "discussion.list",
            json!({"workspaceId":ws,"conversationId":id,"afterSequence":0,"limit":1}),
        )
        .await;
    assert_eq!(history["result"]["conversation"], conversation);
    assert_eq!(
        history["result"]["messages"][0],
        created["result"]["message"]
    );
    assert_eq!(history["result"]["hasMore"], true);
    let default = alice
        .call(
            17,
            "discussion.send",
            json!({"workspaceId":ws,"messageId":"public","body":"Everyone"}),
        )
        .await;
    assert_eq!(default["result"]["message"]["conversationId"], "default");
    assert_eq!(default["result"]["message"]["sequence"], 1);
    let public = owner.event("discussion:message").await;
    assert_eq!(public["data"]["conversationId"], "default");
    let mut reconnected = PresenceClient::open(srv.port, srv.cfg.clone(), &tokens[1]).await;
    let catchup = reconnected
        .call(
            18,
            "discussion.list",
            json!({"workspaceId":ws,"conversationId":id,"afterSequence":1}),
        )
        .await;
    assert_eq!(
        catchup["result"]["messages"],
        json!([sent["result"]["message"]])
    );
    let subscribed = reconnected
        .call(
            21,
            "events.subscribe",
            json!({"eventTypes":["discussion:message","presence:changed"]}),
        )
        .await;
    assert!(subscribed["result"]["subscriptionId"].is_string());
    let removed = owner
        .call(
            22,
            "workspace.members.remove",
            json!({"workspaceId":ws,"principalId":people[1].id}),
        )
        .await;
    assert_eq!(removed["result"]["removed"], true, "{removed}");
    let denied_send = reconnected
        .call(
            23,
            "discussion.send",
            json!({"workspaceId":ws,"conversationId":id,"messageId":"removed","body":"denied"}),
        )
        .await;
    assert_eq!(denied_send["error"]["data"]["code"], "not-found");
    let later = alice.call(24,"discussion.send",
        json!({"workspaceId":ws,"conversationId":id,"messageId":"after-removal","body":"remaining participant"})).await;
    assert_eq!(later["result"]["message"]["sequence"], 3, "{later}");
    let pulse = alice
        .call(
            25,
            "presence.update",
            json!({"focus":[],"discussionTyping":{"workspaceId":ws,"conversationId":id}}),
        )
        .await;
    assert!(pulse.get("error").is_none(), "{pulse}");
    assert!(
        reconnected
            .all_events("discussion:message")
            .await
            .is_empty(),
        "removed participant receives no private message"
    );
    assert!(
        reconnected.all_events("presence:changed").await.is_empty(),
        "removed participant receives no private typing"
    );
    let denied = reconnected
        .call(
            19,
            "discussion.list",
            json!({"workspaceId":ws,"conversationId":id}),
        )
        .await;
    assert_eq!(denied["error"]["data"]["code"], "not-found");
    assert!(srv
        .store
        .query_events(&intent_store::EventQuery {
            workspace_id: Some(ws),
            event_type_prefix: Some("discussion:".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .is_empty());
    for client in [alice, bob, carol, owner, reconnected] {
        client.close().await;
    }
    srv.ws.stop().await;
}
