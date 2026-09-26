//! Device attribution through real pinned TLS, admission, services and `SQLite`.
use super::*;
use serde_json::json;

async fn start_roster() -> (Server, Services) {
    let (_, bus, store, settings, dir) = make_services(None, None).await;
    let reverse_registry = Arc::new(PrimaryReverseRegistry::new());
    let services = Services::new(store.clone())
        .with_settings_registry(settings.clone())
        .with_event_bus(bus.clone())
        .with_reverse_dispatch(reverse_registry.clone());
    let api: Arc<dyn WorkspaceApi> = Arc::new(services.clone());
    let tls = ensure_tls_certificate(dir.path()).unwrap();
    let tokens = Arc::new(AsyncTokenStore::new(Arc::new(MemTokenStore::default())));
    tokens.store_token(TOKEN).await.unwrap();
    let ws = WsApiServer::new_with_reverse(
        api.clone(),
        bus.clone(),
        &tls,
        &tokens,
        WsOptions {
            base_port: 0,
            bind_addresses: vec![Ipv4Addr::LOCALHOST.into()],
            ..Default::default()
        },
        reverse_registry.clone(),
        None,
    )
    .unwrap();
    let port = ws.start().await.unwrap();
    (
        Server {
            ws,
            port,
            cfg: client_config(&tls.fingerprint256),
            api,
            bus,
            store,
            registry: settings,
            reverse_registry,
            dir,
        },
        services,
    )
}

async fn reconnect(srv: &Server, person: &intent_core::Principal, token: &str) -> Guest {
    let url = format!("wss://localhost:{}/ws?token={token}", srv.port);
    Guest {
        principal: person.clone(),
        ws: common::wss_connect_with_retry(srv.port, srv.cfg.clone(), &url).await,
        next_id: 0,
    }
}

async fn owner(srv: &Server) -> Guest {
    reconnect(
        srv,
        &srv.store.get_primary_principal().await.unwrap(),
        TOKEN,
    )
    .await
}

async fn rows(client: &mut Guest) -> Vec<Value> {
    let reply = client.call("client.list", json!({})).await;
    assert!(reply.get("error").is_none(), "client.list: {reply}");
    reply["result"]["clients"].as_array().unwrap().clone()
}

#[tokio::test]
async fn authenticated_devices_durable_paging_and_search_do_not_reveal_hidden_people() {
    let (srv, _) = start_roster().await;
    let mut guest = Guest::connect(&srv, &"12".repeat(32)).await;
    let workspace = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&workspace))
        .await
        .unwrap();
    srv.store
        .add_workspace_member(
            &workspace,
            &guest.principal.id,
            intent_core::WorkspaceRole::Collaborator,
        )
        .await
        .unwrap();
    for scope in [workspace.clone(), WorkspaceId::from("")] {
        for (index, principal) in [
            Some(guest.principal.id.as_str()),
            Some("hidden-person"),
            None,
            Some("hidden-person"),
            Some("hidden-person"),
        ]
        .into_iter()
        .enumerate()
        {
            for kind in intent_core::events::CLIENT_EVENT_TYPES {
                srv.store.insert_event(&intent_store::NewEvent {
                    workspace_id: scope.clone(), timestamp: now_iso(), event_type: (*kind).into(),
                    actor: intent_core::EventActor::default(), session_id: None, correlation_id: None, parent_event_id: None, metadata: None,
                    data: json!({"principalId":principal,"clientId":format!("device-audit-{index}"),"name":format!("device-audit-{index}")}),
                }).await.unwrap();
            }
        }
    }
    let first = guest
        .call(
            "event.query",
            json!({"workspaceId":workspace,"eventType":"client:*","paginate":true,"limit":2}),
        )
        .await;
    assert!(first.get("error").is_none(), "{first}");
    let first_items = first["result"]["items"].as_array().unwrap();
    assert_eq!(first_items.len(), 2);
    assert!(first_items
        .iter()
        .all(|e| e["data"]["principalId"] == guest.principal.id.as_str()));
    let second = guest.call("event.query",json!({"workspaceId":workspace,"eventType":"client:*","paginate":true,"limit":2,"nextToken":first["result"]["nextToken"]})).await;
    assert_eq!(second["result"]["items"].as_array().unwrap().len(), 1);
    assert!(
        second["result"]["nextToken"].is_null(),
        "hidden rows must not create more pages"
    );
    let found = guest
        .call("search.events", json!({"query":"device-audit"}))
        .await;
    assert!(found.get("error").is_none(), "{found}");
    let text = found.to_string();
    assert!(
        text.contains("device-audit-0"),
        "own global event is searchable: {found}"
    );
    for i in 1..5 {
        assert!(!text.contains(&format!("device-audit-{i}")), "{found}");
    }
    assert!(!text.contains("hidden-person"));
    let mut owner = owner(&srv).await;
    let all = owner
        .call(
            "event.query",
            json!({"workspaceId":workspace,"eventType":"client:*","limit":50}),
        )
        .await;
    assert_eq!(all["result"].as_array().unwrap().len(), 15);
    srv.ws.stop().await;
}

async fn watch(client: &mut Guest) {
    let r = client
        .call("events.subscribe", json!({"eventTypes":["client:*"]}))
        .await;
    assert!(r.get("error").is_none(), "{r}");
}

async fn own_event(client: &mut Guest, kind: &str, id: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match client.ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v["method"] != "events.event" {
                        continue;
                    }
                    assert_eq!(v["jsonrpc"], "2.0");
                    let event = &v["params"]["event"];
                    if event["type"]
                        .as_str()
                        .is_some_and(|t| t.starts_with("client:"))
                    {
                        assert_eq!(
                            event["data"]["principalId"],
                            client.principal.id.as_str(),
                            "guest saw another person"
                        );
                    }
                    if event["type"] == kind && event["data"]["clientId"] == id {
                        return event["data"].clone();
                    }
                }
                Some(Ok(Message::Ping(bytes))) => {
                    client.ws.send(Message::Pong(bytes)).await.unwrap();
                }
                other => panic!("expected device event: {other:?}"),
            }
        }
    })
    .await
    .expect("own device event")
}

#[tokio::test]
async fn authenticated_devices_live_metadata_disconnect_reconnect_and_real_removal() {
    for (member, self_revoke) in [(true, false), (true, true), (false, true)] {
        let (srv, _) = start_roster().await;
        let mut observer = owner(&srv).await;
        let mut owner = owner(&srv).await;
        watch(&mut observer).await;
        let token = "c1".repeat(32);
        let mut person = Guest::connect(&srv, &token).await;
        if member {
            sqlx::query("INSERT INTO host_member(principal_id,added_at) VALUES (?,?)")
                .bind(person.principal.id.as_str())
                .bind(now_iso())
                .execute(srv.store.write_pool())
                .await
                .unwrap();
        }
        let hello = person
            .call(
                "client.hello",
                json!({"clientId":"laptop","name":"Original"}),
            )
            .await;
        let id = hello["result"]["clientId"].as_str().unwrap().to_string();
        let connected = await_client_event(&mut observer.ws, "client:connected", &id).await;
        assert_eq!(
            connected["data"]["principalId"],
            person.principal.id.as_str()
        );
        assert_eq!(
            connected["data"]["hostRole"],
            if member { "member" } else { "guest" }
        );
        person
            .call(
                "client.hello",
                json!({"clientId":id,"name":"Renamed","deviceKind":"desktop"}),
            )
            .await;
        let updated = await_client_event(&mut observer.ws, "client:updated", &id).await;
        assert_eq!(updated["data"], rows(&mut owner).await[0]);
        let mut second = reconnect(&srv, &person.principal, &token).await;
        second
            .call("client.hello", json!({"clientId":id,"name":"Newest"}))
            .await;
        assert_eq!(
            await_client_event(&mut observer.ws, "client:updated", &id).await["data"]
                ["connections"],
            2
        );
        second.ws.close(None).await.unwrap();
        let fallback = await_client_event(&mut observer.ws, "client:updated", &id).await;
        assert_eq!(fallback["data"]["name"], "Renamed");
        assert_eq!(fallback["data"]["connections"], 1);
        person.ws.close(None).await.unwrap();
        await_client_event(&mut observer.ws, "client:disconnected", &id).await;
        assert!(rows(&mut owner).await.is_empty());
        person = reconnect(&srv, &person.principal, &token).await;
        person.call("client.hello", json!({"clientId":id})).await;
        await_client_event(&mut observer.ws, "client:connected", &id).await;
        let mut paired = reconnect(&srv, &person.principal, &token).await;
        let phone = paired
            .call("client.hello", json!({"clientId":"phone"}))
            .await["result"]["clientId"]
            .as_str()
            .unwrap()
            .to_string();
        await_client_event(&mut observer.ws, "client:connected", &phone).await;
        let mut unaffected = Guest::connect(&srv, &"d1".repeat(32)).await;
        unaffected
            .call("client.hello", json!({"clientId":"unrelated"}))
            .await;
        if self_revoke {
            assert_eq!(
                person.call("principal.revokeSelf", json!({})).await["result"]["revoked"],
                true
            );
        } else {
            assert_eq!(
                owner
                    .call(
                        "host.members.remove",
                        json!({"principalId":person.principal.id})
                    )
                    .await["result"]["removed"],
                true
            );
        }
        await_client_event(&mut observer.ws, "client:disconnected", &id).await;
        await_client_event(&mut observer.ws, "client:disconnected", &phone).await;
        let remaining = rows(&mut owner).await;
        assert_eq!(remaining.len(), 1);
        assert_eq!(
            remaining[0]["principalId"],
            unaffected.principal.id.as_str()
        );
        assert_eq!(
            status_code(
                &https_request(
                    srv.port,
                    srv.cfg.clone(),
                    &upgrade_req("/ws", None, Some(&token))
                )
                .await
            ),
            401
        );
        assert_eq!(
            unaffected.call("principal.me", json!({})).await["result"]["id"],
            unaffected.principal.id.as_str()
        );
        let persisted = srv
            .store
            .query_events(&intent_store::EventQuery {
                event_types: vec!["client:updated".into()],
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(persisted.is_empty(), "metadata updates must be transient");
        srv.ws.stop().await;
    }
}

#[tokio::test]
async fn authenticated_devices_guest_event_privacy_survives_rehello_and_scope_changes() {
    let (srv, services) = start_roster().await;
    let mut owner = owner(&srv).await;
    let token = "e1".repeat(32);
    let mut guest = Guest::connect(&srv, &token).await;
    let mut observer = reconnect(&srv, &guest.principal, &token).await;
    watch(&mut observer).await;
    owner
        .call(
            "client.hello",
            json!({"clientId":"hidden-owner","name":"Private owner"}),
        )
        .await;
    let mut other = Guest::connect(&srv, &"f1".repeat(32)).await;
    other
        .call("client.hello", json!({"clientId":"hidden-guest"}))
        .await;
    let id = guest.call("client.hello", json!({"clientId":"self"})).await["result"]["clientId"]
        .as_str()
        .unwrap()
        .to_string();
    let connected = own_event(&mut observer, "client:connected", &id).await;
    assert_eq!(connected["hostRole"], "guest");
    guest
        .call("client.hello", json!({"clientId":id,"name":"My phone"}))
        .await;
    let updated = own_event(&mut observer, "client:updated", &id).await;
    assert_eq!(updated, rows(&mut guest).await[0]);
    // Seed a verified identity and invitation; use the real returning-person
    // acceptance transaction to promote the already connected guest.
    guest
        .principal
        .set_identity(intent_core::PrincipalIdentity::github(7654));
    guest.principal.display_name = Some("Updated person".into());
    srv.store.upsert_principal(&guest.principal).await.unwrap();
    let invite = intent_core::HostInvite::new(
        "device-promotion".into(),
        owner.principal.id.clone(),
        guest.principal.identity_key().unwrap(),
        "guest".into(),
        sha256_hex(b"fixture-secret"),
        Some("fixture-secret".into()),
    )
    .unwrap();
    srv.store.insert_host_invite(&invite).await.unwrap();
    let accepted = intent_core::with_caller(
        intent_core::Caller::Daemon,
        services.invite_accept(
            invite.id,
            "fixture-secret".into(),
            intent_core::InviteScope::Host,
            token,
        ),
    )
    .await
    .unwrap();
    assert_eq!(accepted["hostRole"], "member");
    let updated = await_client_event(&mut observer.ws, "client:updated", &id).await;
    assert_eq!(updated["data"]["displayName"], "Updated person");
    assert_eq!(updated["data"]["hostRole"], "member");
    assert_eq!(rows(&mut guest).await.len(), 3);
    // A newly visible unrelated event must now reach the same subscription.
    other
        .call("client.hello", json!({"clientId":"now-visible"}))
        .await;
    let other_id = format!("{}:now-visible", other.principal.id);
    let visible = await_client_event(&mut observer.ws, "client:connected", &other_id).await;
    assert_eq!(visible["data"]["principalId"], other.principal.id.as_str());
    let current = rows(&mut guest).await;
    assert_eq!(current.len(), 3);
    assert!(!current
        .iter()
        .any(|r| r["clientId"] == format!("{}:hidden-guest", other.principal.id)));
    assert!(current.iter().any(|r| r["clientId"] == other_id));
    srv.ws.stop().await;
}

#[tokio::test]
async fn authenticated_devices_bind_people_group_sockets_and_keep_guest_private() {
    let (srv, _) = start_roster().await;
    let mut owner = owner(&srv).await;
    let mut guest = Guest::connect(&srv, &"a1".repeat(32)).await;
    let mut member = Guest::connect(&srv, &"b1".repeat(32)).await;
    sqlx::query("INSERT INTO host_member(principal_id,added_at) VALUES (?,?)")
        .bind(member.principal.id.as_str())
        .bind(now_iso())
        .execute(srv.store.write_pool())
        .await
        .unwrap();
    let hello = guest.call("client.hello", json!({"clientId":"same-device","name":"Claimed owner",
        "principalId":owner.principal.id,"hostRole":"owner","login":"forged","capabilities":{"browserExec":true}})).await;
    let guest_id = hello["result"]["clientId"].as_str().unwrap().to_string();
    member
        .call(
            "client.hello",
            json!({"clientId":"same-device","name":"Member"}),
        )
        .await;
    owner
        .call("client.hello", json!({"clientId":"legacy-owner"}))
        .await;
    let mut extra = reconnect(&srv, &guest.principal, &"a1".repeat(32)).await;
    extra
        .call(
            "client.hello",
            json!({"clientId":guest_id,"name":"Second window","deviceKind":"phone"}),
        )
        .await;
    let mut phone = reconnect(&srv, &guest.principal, &"a1".repeat(32)).await;
    phone
        .call(
            "client.hello",
            json!({"clientId":"other-device","deviceKind":"phone"}),
        )
        .await;
    let own = rows(&mut guest).await;
    assert_eq!(own.len(), 2);
    assert!(own
        .iter()
        .all(|r| r["principalId"] == guest.principal.id.as_str() && r["hostRole"] == "guest"));
    let grouped = own.iter().find(|r| r["clientId"] == guest_id).unwrap();
    assert_eq!(grouped["connections"], 2);
    assert_eq!(grouped["name"], "Second window");
    assert_eq!(grouped["capabilities"]["browserExec"], false);
    assert_eq!(grouped["login"], "guest");
    let all = rows(&mut owner).await;
    assert_eq!(all.len(), 4);
    assert_eq!(rows(&mut member).await, all);
    let legacy = all
        .iter()
        .find(|r| r["clientId"] == "legacy-owner")
        .unwrap();
    assert_eq!(legacy["principalId"], owner.principal.id.as_str());
    assert_eq!(legacy["hostRole"], "owner");
    for key in ["login", "displayName", "avatarUrl"] {
        assert_eq!(legacy.get(key), Some(&Value::Null));
    }
    assert!(legacy.get("deviceKind").is_none());
    assert!(!serde_json::to_string(&all)
        .unwrap()
        .contains(&"a1".repeat(32)));
    srv.ws.stop().await;
}
