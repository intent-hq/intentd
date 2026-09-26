use super::*;
use intent_core::events::*;
use intent_core::EventActor;
use intent_store::NewEvent;
use intent_transport::tunnel::Frame;
use serde_json::json;

async fn tunnel_frame(ws: &mut common::TlsWs) -> Frame {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Binary(bytes))) => return Frame::decode(&bytes).unwrap(),
                Some(Ok(Message::Ping(bytes))) => ws.send(Message::Pong(bytes)).await.unwrap(),
                other => panic!("expected tunnel frame: {other:?}"),
            }
        }
    })
    .await
    .expect("tunnel frame")
}

async fn send_tunnel(ws: &mut common::TlsWs, frame: Frame) {
    ws.send(Message::Binary(frame.encode().into()))
        .await
        .unwrap();
}

async fn revoked_close(ws: &mut common::TlsWs) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Close(Some(frame)))) => {
                    assert_eq!(
                        frame.code,
                        tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Policy
                    );
                    assert_eq!(frame.reason, "credential revoked");
                    return;
                }
                Some(Ok(Message::Ping(bytes))) => ws.send(Message::Pong(bytes)).await.unwrap(),
                other => panic!("revoked connection must close without more data: {other:?}"),
            }
        }
    })
    .await
    .expect("revoked connection close");
}

#[tokio::test]
async fn member_transport_forward_revocation_stops_accepted_streams() {
    let srv = start(WsOptions::default()).await;
    let token = "a8".repeat(32);
    let mut member = Guest::connect(&srv, &token).await;
    promote(&srv, &member.principal).await;
    let mut other = Guest::connect(&srv, &"c8".repeat(32)).await;
    promote(&srv, &other.principal).await;
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let created = member
        .call("forward.create", json!({"remotePort":port}))
        .await;
    assert!(created.get("error").is_none(), "{created}");
    let forwarded_port = u16::try_from(created["result"]["localPort"].as_u64().unwrap()).unwrap();
    let mut downstream = TcpStream::connect(("127.0.0.1", forwarded_port))
        .await
        .unwrap();
    let (mut upstream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
        .await
        .unwrap()
        .unwrap();
    downstream.write_all(b"preview").await.unwrap();
    let mut bytes = [0; 7];
    upstream.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"preview");
    upstream.write_all(b"preview").await.unwrap();
    downstream.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"preview");
    let other_created = other
        .call("forward.create", json!({"remotePort":port}))
        .await;
    assert!(other_created.get("error").is_none(), "{other_created}");
    let other_port = u16::try_from(other_created["result"]["localPort"].as_u64().unwrap()).unwrap();
    let mut other_downstream = TcpStream::connect(("127.0.0.1", other_port)).await.unwrap();
    let (mut other_upstream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        member.call("principal.revokeSelf", json!({})).await["result"]["revoked"],
        true
    );
    revoked_close(&mut member.ws).await;
    let read = tokio::time::timeout(Duration::from_secs(5), downstream.read(&mut bytes))
        .await
        .expect("revocation must stop an accepted forward");
    assert!(
        matches!(read, Ok(0)) || read.is_err(),
        "forward remained open: {read:?}"
    );
    assert!(TcpStream::connect(("127.0.0.1", forwarded_port))
        .await
        .is_err());
    tokio::time::timeout(Duration::from_secs(5), async {
        other_downstream.write_all(b"stillok").await.unwrap();
        other_upstream.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"stillok");
        other_upstream.write_all(b"healthy").await.unwrap();
        other_downstream.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"healthy");
    })
    .await
    .expect("another member's accepted forward must survive");
    assert!(TcpStream::connect(("127.0.0.1", other_port)).await.is_ok());
    assert_eq!(
        other.call("principal.me", json!({})).await["result"]["hostRole"],
        "member"
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
    srv.ws.stop().await;
}

#[tokio::test]
async fn member_transport_tunnel_lifecycle_revocation_and_trust_guards() {
    let srv = start(WsOptions::default()).await;
    let token = "a9".repeat(32);
    let mut member = Guest::connect(&srv, &token).await;
    let guest_token = "b9".repeat(32);
    let guest = Guest::connect(&srv, &guest_token).await;
    promote(&srv, &member.principal).await;
    let other_token = "c9".repeat(32);
    let mut other = Guest::connect(&srv, &other_token).await;
    promote(&srv, &other.principal).await;
    assert_eq!(
        status_code(
            &https_request(
                srv.port,
                srv.cfg.clone(),
                &upgrade_req("/tunnel", None, Some(&guest_token))
            )
            .await
        ),
        403
    );
    assert_eq!(
        status_code(
            &https_request(
                srv.port,
                srv.cfg.clone(),
                &upgrade_req("/tunnel", Some("https://untrusted.example"), Some(&token))
            )
            .await
        ),
        403
    );
    let tcp = TcpStream::connect(("127.0.0.1", srv.port)).await.unwrap();
    let wrong_pin = tokio_rustls::TlsConnector::from(client_config(&"00:".repeat(32)));
    assert!(wrong_pin
        .connect(ServerName::try_from("localhost").unwrap(), tcp)
        .await
        .is_err());
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let url = format!("wss://localhost:{}/tunnel?token={token}", srv.port);
    let mut tunnel = common::wss_connect_with_retry(srv.port, srv.cfg.clone(), &url).await;
    send_tunnel(&mut tunnel, Frame::Open { stream_id: 1, port }).await;
    assert_eq!(
        tunnel_frame(&mut tunnel).await,
        Frame::OpenOk { stream_id: 1 }
    );
    let (mut peer, _) = listener.accept().await.unwrap();
    send_tunnel(
        &mut tunnel,
        Frame::Data {
            stream_id: 1,
            payload: b"hello".to_vec(),
        },
    )
    .await;
    let mut buf = [0; 5];
    peer.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"hello");
    peer.write_all(b"reply").await.unwrap();
    assert_eq!(
        tunnel_frame(&mut tunnel).await,
        Frame::Data {
            stream_id: 1,
            payload: b"reply".to_vec()
        }
    );
    send_tunnel(&mut tunnel, Frame::Eof { stream_id: 1 }).await;
    assert_eq!(peer.read(&mut buf).await.unwrap(), 0);
    peer.shutdown().await.unwrap();
    assert_eq!(tunnel_frame(&mut tunnel).await, Frame::Eof { stream_id: 1 });
    assert_eq!(
        tunnel_frame(&mut tunnel).await,
        Frame::Close { stream_id: 1 }
    );
    send_tunnel(&mut tunnel, Frame::Open { stream_id: 2, port }).await;
    assert_eq!(
        tunnel_frame(&mut tunnel).await,
        Frame::OpenOk { stream_id: 2 }
    );
    let (mut peer, _) = listener.accept().await.unwrap();
    let other_url = format!("wss://localhost:{}/tunnel?token={other_token}", srv.port);
    let mut other_tunnel =
        common::wss_connect_with_retry(srv.port, srv.cfg.clone(), &other_url).await;
    send_tunnel(&mut other_tunnel, Frame::Open { stream_id: 3, port }).await;
    assert_eq!(
        tunnel_frame(&mut other_tunnel).await,
        Frame::OpenOk { stream_id: 3 }
    );
    let (mut other_peer, _) = listener.accept().await.unwrap();
    assert_eq!(
        member.call("principal.revokeSelf", json!({})).await["result"]["revoked"],
        true
    );
    revoked_close(&mut member.ws).await;
    revoked_close(&mut tunnel).await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), peer.read(&mut buf))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    send_tunnel(
        &mut other_tunnel,
        Frame::Data {
            stream_id: 3,
            payload: b"alive".to_vec(),
        },
    )
    .await;
    tokio::time::timeout(Duration::from_secs(5), other_peer.read_exact(&mut buf))
        .await
        .expect("another member's tunnel must survive")
        .unwrap();
    assert_eq!(&buf, b"alive");
    other_peer.write_all(b"reply").await.unwrap();
    assert_eq!(
        tunnel_frame(&mut other_tunnel).await,
        Frame::Data {
            stream_id: 3,
            payload: b"reply".to_vec()
        }
    );
    assert_eq!(
        other.call("principal.me", json!({})).await["result"]["hostRole"],
        "member"
    );
    assert_eq!(
        status_code(
            &https_request(
                srv.port,
                srv.cfg.clone(),
                &upgrade_req("/tunnel", None, Some(&token))
            )
            .await
        ),
        401
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
    drop(guest);
    srv.ws.stop().await;
}

async fn promote(srv: &Server, principal: &intent_core::Principal) {
    sqlx::query("INSERT INTO host_member(principal_id,added_at) VALUES (?,?)")
        .bind(principal.id.as_str())
        .bind(now_iso())
        .execute(srv.store.write_pool())
        .await
        .unwrap();
}

async fn publish(srv: &Server, ws: &WorkspaceId, ty: &str, data: Value) {
    srv.bus
        .publish(&NewEvent {
            workspace_id: ws.clone(),
            timestamp: now_iso(),
            event_type: ty.into(),
            actor: EventActor::default(),
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn member_transport_browser_and_forward_admission() {
    let srv = start(WsOptions::default()).await;
    let mut member = Guest::connect(&srv, &"a5".repeat(32)).await;
    let mut guest = Guest::connect(&srv, &"b5".repeat(32)).await;
    promote(&srv, &member.principal).await;
    let ws = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&ws))
        .await
        .unwrap();
    for client in [&mut member, &mut guest] {
        let hello = client.call("client.hello", json!({"clientId":"same-browser","kind":"desktop","capabilities":{"browserExec":true}})).await;
        assert!(hello.get("error").is_none(), "{hello}");
    }
    for (method, params) in [
        ("browser.listTabs", json!({"workspaceId":ws})),
        ("forward.list", json!({})),
    ] {
        assert_eq!(
            guest.call(method, params.clone()).await["error"]["code"],
            -32003
        );
        let got = member.call(method, params).await;
        assert!(got.get("error").is_none(), "{method}: {got}");
    }
    let bad = member
        .call(
            "browser.listTabs",
            json!({"workspaceId":WorkspaceId::chief()}),
        )
        .await;
    assert_eq!(bad["error"]["data"]["code"], "not-found", "{bad}");
    assert_eq!(
        member.call("host.exec", json!({"command":"true"})).await["error"]["code"],
        -32003
    );
    drop((member, guest));
    srv.ws.stop().await;
}

#[tokio::test]
async fn member_transport_durable_event_scope_and_guest_controls() {
    let srv = start(WsOptions::default()).await;
    let mut member = Guest::connect(&srv, &"a6".repeat(32)).await;
    let mut guest = Guest::connect(&srv, &"b6".repeat(32)).await;
    promote(&srv, &member.principal).await;
    let ws = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&ws))
        .await
        .unwrap();
    srv.store
        .add_workspace_member(
            &ws,
            &guest.principal.id,
            intent_core::WorkspaceRole::Collaborator,
        )
        .await
        .unwrap();
    for ty in [
        TERMINAL_DATA,
        SCRIPT_STATE,
        BROWSER_TAB_UPDATED,
        AGENT_PERMISSION_REQUEST,
        NOTE_UPDATED,
        SETTINGS_CHANGED,
    ] {
        publish(&srv, &ws, ty, json!({"marker":ty})).await;
    }
    let types = |v: &Value| {
        v["result"]
            .as_array()
            .unwrap_or_else(|| panic!("{v}"))
            .iter()
            .map(|r| {
                r["type"]
                    .as_str()
                    .unwrap_or_else(|| panic!("{r}"))
                    .to_string()
            })
            .collect::<std::collections::BTreeSet<_>>()
    };
    let got = member.call("event.query", json!({"workspaceId":ws})).await;
    assert_eq!(
        types(&got),
        [
            TERMINAL_DATA,
            SCRIPT_STATE,
            BROWSER_TAB_UPDATED,
            AGENT_PERMISSION_REQUEST,
            NOTE_UPDATED
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
        "{got}"
    );
    assert_eq!(
        types(&guest.call("event.query", json!({"workspaceId":ws})).await),
        [NOTE_UPDATED.to_string()].into_iter().collect()
    );
    srv.store
        .remove_host_member(&member.principal.id)
        .await
        .unwrap();
    assert_eq!(
        member.call("event.query", json!({"workspaceId":ws})).await["error"]["data"]["code"],
        "not-found"
    );
    drop((member, guest));
    srv.ws.stop().await;
}

#[tokio::test]
async fn member_transport_live_events_upgrade_and_workspace_deltas() {
    let srv = start(WsOptions::default()).await;
    let token = "a7".repeat(32);
    let member = seed_principal(&srv.store, "member", &token).await;
    let guest_token = "b7".repeat(32);
    let guest = seed_principal(&srv.store, "guest", &guest_token).await;
    let shared = WorkspaceId::new();
    let hidden = WorkspaceId::new();
    for ws in [&shared, &hidden] {
        srv.store
            .insert_workspace(&fixture_workspace(ws))
            .await
            .unwrap();
    }
    for principal in [&member, &guest] {
        srv.store
            .add_workspace_member(
                &shared,
                &principal.id,
                intent_core::WorkspaceRole::Collaborator,
            )
            .await
            .unwrap();
    }
    let mut channel = PresenceClient::open(srv.port, srv.cfg.clone(), &token).await;
    let sub = channel.call(1, "workspace.subscribe", json!({})).await["result"]["subscriptionId"]
        .as_str()
        .unwrap()
        .to_string();
    let initial = channel.push(&sub).await;
    assert_eq!(initial["kind"], "snapshot");
    assert_eq!(initial["snapshot"].as_array().unwrap().len(), 1);
    assert_eq!(initial["snapshot"][0]["id"], shared.as_str());
    let mut raw = PresenceClient::open(srv.port, srv.cfg.clone(), &token).await;
    let mut guest_raw = PresenceClient::open(srv.port, srv.cfg.clone(), &guest_token).await;
    for client in [&mut raw, &mut guest_raw] {
        let result = client.call(1, "events.subscribe", json!({"eventTypes":["note:*","workspace:*","terminal:*","script:*","agent:*","browser:*","host:*","settings:*"]})).await;
        assert!(result.get("error").is_none(), "{result}");
    }
    // Prime a negative workspace verdict on both existing connections.
    publish(
        &srv,
        &hidden,
        NOTE_UPDATED,
        json!({"noteId":"hidden-prime"}),
    )
    .await;
    publish(&srv, &shared, NOTE_UPDATED, json!({"noteId":"barrier"})).await;
    println!("waiting for shared member event before upgrade");
    assert_eq!(
        raw.event(NOTE_UPDATED).await["workspaceId"],
        shared.as_str()
    );
    println!("waiting for shared guest event before upgrade");
    assert_eq!(
        guest_raw.event(NOTE_UPDATED).await["workspaceId"],
        shared.as_str()
    );
    promote(&srv, &member).await;
    publish(
        &srv,
        &WorkspaceId::from(""),
        HOST_MEMBERS_CHANGED,
        json!({"principalId":member.id,"hostRole":"member","action":"added","revision":1}),
    )
    .await;
    assert_eq!(
        raw.event(HOST_MEMBERS_CHANGED).await["data"]["principalId"],
        member.id.as_str()
    );
    let upgraded = channel.push(&sub).await;
    assert_eq!(upgraded["seq"], 1);
    let rows = upgraded["delta"]["updated"].as_array().unwrap();
    assert_eq!(rows.len(), 1, "{upgraded}");
    assert!(rows
        .iter()
        .all(|row| row["canManage"] == true && row["myRole"] == "collaborator"));
    assert_eq!(upgraded["delta"]["added"][0]["id"], hidden.as_str());
    assert_eq!(upgraded["delta"]["added"][0]["canManage"], true);
    let future = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&future))
        .await
        .unwrap();
    publish(
        &srv,
        &future,
        WORKSPACE_CREATED,
        json!({"workspaceId":future}),
    )
    .await;
    let delta = channel.push(&sub).await;
    assert_eq!(delta["delta"]["added"][0]["id"], future.as_str());
    for ty in [
        TERMINAL_DATA,
        SCRIPT_OUTPUT,
        BROWSER_TAB_UPDATED,
        AGENT_PERMISSION_REQUEST,
    ] {
        publish(&srv, &hidden, ty, json!({"marker":ty})).await;
        assert_eq!(raw.event(ty).await["workspaceId"], hidden.as_str());
    }
    publish(
        &srv,
        &WorkspaceId::chief(),
        TERMINAL_DATA,
        json!({"data":"chief-private"}),
    )
    .await;
    publish(
        &srv,
        &shared,
        SETTINGS_CHANGED,
        json!({"private":"host-setting"}),
    )
    .await;
    publish(
        &srv,
        &hidden,
        NOTE_UPDATED,
        json!({"noteId":"visible-after-upgrade"}),
    )
    .await;
    println!("waiting for newly visible member event after upgrade");
    assert_eq!(
        raw.event(NOTE_UPDATED).await["workspaceId"],
        hidden.as_str()
    );
    publish(
        &srv,
        &hidden,
        WORKSPACE_DELETED,
        json!({"workspaceId":hidden}),
    )
    .await;
    let deleted = channel.push(&sub).await;
    assert_eq!(deleted["delta"]["removedIds"], json!([hidden]));
    publish(
        &srv,
        &shared,
        NOTE_UPDATED,
        json!({"noteId":"final-barrier"}),
    )
    .await;
    println!("waiting for final shared guest barrier");
    assert_eq!(
        guest_raw.event(NOTE_UPDATED).await["workspaceId"],
        shared.as_str()
    );
    for ty in [
        HOST_MEMBERS_CHANGED,
        TERMINAL_DATA,
        SCRIPT_OUTPUT,
        BROWSER_TAB_UPDATED,
        AGENT_PERMISSION_REQUEST,
        WORKSPACE_CREATED,
        WORKSPACE_DELETED,
    ] {
        assert!(
            guest_raw.all_events(ty).await.is_empty(),
            "guest received {ty}"
        );
    }
    assert!(raw.all_events(SETTINGS_CHANGED).await.is_empty());
    assert!(!raw
        .all_events(TERMINAL_DATA)
        .await
        .iter()
        .any(|e| e["workspaceId"] == WorkspaceId::chief().as_str()));
    channel.close().await;
    raw.close().await;
    guest_raw.close().await;
    srv.ws.stop().await;
}

async fn answer_reverse(ws: &mut common::TlsWs, method: &str, workspace: &WorkspaceId) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let frame: Value = serde_json::from_str(&text).unwrap();
                    assert_eq!(frame["method"], method, "{frame}");
                    assert_eq!(frame["params"]["workspaceId"], workspace.as_str());
                    ws.send(Message::Text(
                        json!({"jsonrpc":"2.0","id":frame["id"],"result":{"success":true}})
                            .to_string()
                            .into(),
                    ))
                    .await
                    .unwrap();
                    return;
                }
                Some(Ok(Message::Ping(bytes))) => ws.send(Message::Pong(bytes)).await.unwrap(),
                other => panic!("expected reverse request: {other:?}"),
            }
        }
    })
    .await
    .expect("authenticated browser reverse request");
}

async fn default_browser_round_trip(
    srv: &Server,
    receiver: &mut common::TlsWs,
    workspace: &WorkspaceId,
) {
    let registry = srv.reverse_registry.clone();
    let params = json!({"workspaceId":workspace,"actions":[{"action":"screenshot"}]});
    let request = intent_core::spawn_daemon(async move {
        registry
            .dispatch("browser.exec", params, ReverseTarget::Default)
            .await
    });
    answer_reverse(receiver, "browser.exec", workspace).await;
    assert_eq!(request.await.unwrap().unwrap()["success"], true);
}

#[intent_test_macros::daemon_test]
async fn member_transport_browser_guest_first_ignores_other_member_change() {
    let srv = start(WsOptions::default()).await;
    let mut guest = Guest::connect(&srv, &"d5".repeat(32)).await;
    let guest_id = guest
        .call(
            "client.hello",
            json!({"clientId":"guest-first","capabilities":{"browserExec":true}}),
        )
        .await["result"]["clientId"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(srv.reverse_registry.live_clients().is_empty());
    let mut member = Guest::connect(&srv, &"e6".repeat(32)).await;
    promote(&srv, &member.principal).await;
    let member_id = member
        .call(
            "client.hello",
            json!({"clientId":"member-second","capabilities":{"browserExec":true}}),
        )
        .await["result"]["clientId"]
        .as_str()
        .unwrap()
        .to_string();
    let ws = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&ws))
        .await
        .unwrap();
    assert_eq!(srv.reverse_registry.live_clients().len(), 1);
    assert_eq!(
        srv.reverse_registry
            .resolve(&ReverseTarget::Default)
            .unwrap()
            .client_id
            .as_str(),
        member_id
    );
    default_browser_round_trip(&srv, &mut member.ws, &ws).await;

    publish(&srv, &WorkspaceId::from(""), HOST_MEMBERS_CHANGED, json!({"principalId":member.principal.id,"hostRole":"member","action":"added","revision":1})).await;
    // Observe the forbidden transition without re-hello, which would mask it
    // by reapplying the correct per-request guest authority.
    let guest_appeared = tokio::time::timeout(Duration::from_secs(2), async {
        let mut poll = tokio::time::interval(Duration::from_millis(10));
        loop {
            poll.tick().await;
            if srv
                .reverse_registry
                .live_clients()
                .iter()
                .any(|c| c.client_id.as_str() == guest_id)
            {
                break;
            }
        }
    })
    .await
    .is_ok();
    assert_eq!(
        guest.call("principal.me", json!({})).await["result"]["hostRole"],
        "guest"
    );
    assert!(srv
        .reverse_registry
        .dispatch(
            "browser.exec",
            json!({"workspaceId":ws,"actions":[{"action":"screenshot"}]}),
            ReverseTarget::Client(intent_core::ClientId::from(guest_id.clone()))
        )
        .await
        .is_err());
    assert!(
        tokio::time::timeout(Duration::from_millis(100), guest.ws.next())
            .await
            .is_err(),
        "guest received unexpected browser payload"
    );
    assert!(
        !guest_appeared,
        "another principal's membership event admitted the unchanged guest"
    );
    assert_eq!(srv.reverse_registry.live_clients().len(), 1);
    assert_eq!(
        srv.reverse_registry
            .resolve(&ReverseTarget::Default)
            .unwrap()
            .client_id
            .as_str(),
        member_id
    );
    default_browser_round_trip(&srv, &mut member.ws, &ws).await;
    assert_eq!(
        guest.call("settings.list", json!({})).await["error"]["code"],
        -32003
    );
    srv.ws.stop().await;
}

#[intent_test_macros::daemon_test]
async fn member_transport_browser_demotion_unbinds_and_keeps_default_controls() {
    let srv = start(WsOptions::default()).await;
    let mut member = Guest::connect(&srv, &"d6".repeat(32)).await;
    let mut remaining = Guest::connect(&srv, &"e7".repeat(32)).await;
    for client in [&mut member, &mut remaining] {
        promote(&srv, &client.principal).await;
    }
    let member_id = member
        .call(
            "client.hello",
            json!({"clientId":"member-first","capabilities":{"browserExec":true}}),
        )
        .await["result"]["clientId"]
        .as_str()
        .unwrap()
        .to_string();
    let remaining_id = remaining
        .call(
            "client.hello",
            json!({"clientId":"member-second","capabilities":{"browserExec":true}}),
        )
        .await["result"]["clientId"]
        .as_str()
        .unwrap()
        .to_string();
    let mut owner = connect_ws(srv.port, srv.cfg.clone()).await;
    hello_browser_host(&mut owner, "owner-control").await;
    let ws = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&ws))
        .await
        .unwrap();
    assert_eq!(
        srv.reverse_registry
            .resolve(&ReverseTarget::Default)
            .unwrap()
            .client_id
            .as_str(),
        member_id
    );
    default_browser_round_trip(&srv, &mut member.ws, &ws).await;
    srv.store
        .remove_host_member(&member.principal.id)
        .await
        .unwrap();
    publish(&srv, &WorkspaceId::from(""), HOST_MEMBERS_CHANGED, json!({"principalId":member.principal.id,"hostRole":"guest","action":"removed","revision":2})).await;
    assert_eq!(
        member.call("principal.me", json!({})).await["result"]["hostRole"],
        "guest"
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        while srv
            .reverse_registry
            .live_clients()
            .iter()
            .any(|c| c.client_id.as_str() == member_id)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("demoted member must leave the browser registry");
    assert_eq!(
        srv.reverse_registry
            .resolve(&ReverseTarget::Default)
            .unwrap()
            .client_id
            .as_str(),
        remaining_id
    );
    default_browser_round_trip(&srv, &mut remaining.ws, &ws).await;
    assert!(srv
        .reverse_registry
        .dispatch(
            "browser.exec",
            json!({"workspaceId":ws,"actions":[{"action":"screenshot"}]}),
            ReverseTarget::Client(intent_core::ClientId::from(member_id))
        )
        .await
        .is_err());
    assert!(
        tokio::time::timeout(Duration::from_millis(100), member.ws.next())
            .await
            .is_err(),
        "demoted member received unexpected browser payload"
    );
    assert_eq!(
        remaining.call("principal.me", json!({})).await["result"]["hostRole"],
        "member"
    );
    assert_eq!(
        member.call("settings.list", json!({})).await["error"]["code"],
        -32003
    );
    default_browser_round_trip(&srv, &mut owner, &WorkspaceId::chief()).await;
    drop(remaining);
    tokio::time::timeout(Duration::from_secs(5), async {
        while srv
            .reverse_registry
            .live_clients()
            .iter()
            .any(|c| c.client_id.as_str() == remaining_id)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("remaining member disconnects");
    assert_eq!(
        srv.reverse_registry
            .resolve(&ReverseTarget::Default)
            .unwrap()
            .client_id
            .as_str(),
        "owner-control"
    );
    default_browser_round_trip(&srv, &mut owner, &ws).await;
    srv.ws.stop().await;
}

#[intent_test_macros::daemon_test]
async fn member_transport_browser_reverse_scope_and_live_upgrade() {
    let srv = start(WsOptions::default()).await;
    let mut member = Guest::connect(&srv, &"aa".repeat(32)).await;
    let mut guest = Guest::connect(&srv, &"bb".repeat(32)).await;
    let ws = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&ws))
        .await
        .unwrap();
    let hello = json!({"clientId":"owner-browser","capabilities":{"browserExec":true}});
    let member_id = member.call("client.hello", hello.clone()).await["result"]["clientId"]
        .as_str()
        .unwrap()
        .to_string();
    let guest_id = guest.call("client.hello", hello).await["result"]["clientId"]
        .as_str()
        .unwrap()
        .to_string();
    assert_ne!(member_id, guest_id);
    assert_ne!(member_id, "owner-browser");
    assert!(
        !srv.reverse_registry.is_connected(),
        "hello never grants guest reverse authority"
    );
    promote(&srv, &member.principal).await;
    publish(&srv, &WorkspaceId::from(""), HOST_MEMBERS_CHANGED, json!({"principalId":member.principal.id,"hostRole":"member","action":"added","revision":1})).await;
    assert_eq!(
        member.call("principal.me", json!({})).await["result"]["hostRole"],
        "member"
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        while !srv.reverse_registry.is_connected() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("live browser admission");
    let registry = srv.reverse_registry.clone();
    let params = json!({"workspaceId":ws,"actions":[{"action":"screenshot"}]});
    let request = intent_core::spawn_daemon(async move {
        registry
            .dispatch("browser.exec", params, ReverseTarget::Default)
            .await
    });
    answer_reverse(&mut member.ws, "browser.exec", &ws).await;
    assert_eq!(request.await.unwrap().unwrap()["success"], true);
    assert!(srv
        .reverse_registry
        .dispatch(
            "browser.exec",
            json!({"workspaceId":ws}),
            ReverseTarget::Client(intent_core::ClientId::from(guest_id))
        )
        .await
        .is_err());
    assert!(srv
        .reverse_registry
        .dispatch(
            "host.openExternal",
            json!({"url":"https://example.org"}),
            ReverseTarget::Default
        )
        .await
        .is_err());
    assert!(srv
        .reverse_registry
        .dispatch(
            "browser.exec",
            json!({"workspaceId":WorkspaceId::chief()}),
            ReverseTarget::Default
        )
        .await
        .is_err());
    let mut owner = connect_ws(srv.port, srv.cfg.clone()).await;
    hello_browser_host(&mut owner, "owner-browser").await;
    let registry = srv.reverse_registry.clone();
    let request = intent_core::spawn_daemon(async move {
        registry
            .dispatch(
                "browser.exec",
                json!({"workspaceId":WorkspaceId::chief(),"actions":[{"action":"screenshot"}]}),
                ReverseTarget::Default,
            )
            .await
    });
    answer_reverse(&mut owner, "browser.exec", &WorkspaceId::chief()).await;
    assert_eq!(request.await.unwrap().unwrap()["success"], true);
    srv.store
        .remove_host_member(&member.principal.id)
        .await
        .unwrap();
    let result = srv
        .reverse_registry
        .dispatch(
            "browser.exec",
            json!({"workspaceId":ws}),
            ReverseTarget::Client(intent_core::ClientId::from(member_id)),
        )
        .await;
    assert!(
        result.is_err(),
        "stale registry authority must never dispatch: {result:?}"
    );
    srv.ws.stop().await;
}

#[tokio::test]
async fn member_transport_global_events_and_workspace_read_boundaries() {
    let srv = start(WsOptions::default()).await;
    let mut member = Guest::connect(&srv, &"ac".repeat(32)).await;
    let mut guest = Guest::connect(&srv, &"bc".repeat(32)).await;
    promote(&srv, &member.principal).await;
    let global = WorkspaceId::from("");
    for ty in [
        HOST_MEMBERS_CHANGED,
        HOST_EXECUTION_CONTEXT_CHANGED,
        HOST_INVITES_CHANGED,
        SETTINGS_CHANGED,
        NOTE_UPDATED,
    ] {
        publish(&srv, &global, ty, json!({"marker":ty})).await;
    }
    // Global history uses the existing unscoped search surface. event.query
    // still requires a real workspace, without a new sentinel RPC shape.
    let got = member
        .call("search.events", json!({"query":"marker"}))
        .await;
    let rows = got["result"]["matches"]
        .as_array()
        .unwrap_or_else(|| panic!("safe durable global events: {got}"));
    assert_eq!(rows.len(), 2, "{got}");
    for ty in [HOST_MEMBERS_CHANGED, HOST_EXECUTION_CONTEXT_CHANGED] {
        assert!(
            rows.iter()
                .any(|r| r["preview"].as_str().unwrap().contains(ty)),
            "{got}"
        );
    }
    assert!(
        guest.call("search.events", json!({"query":"marker"})).await["result"]["matches"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(guest
        .call("event.query", json!({"workspaceId":""}))
        .await
        .get("error")
        .is_some());
    for method in ["agent.diagnostics", "rules.list", "rules.get"] {
        let got = member
            .call(
                method,
                json!({"workspaceId":WorkspaceId::chief(),"ruleType":"endUserRules"}),
            )
            .await;
        assert_eq!(got["error"]["data"]["code"], "not-found", "{method}: {got}");
    }
    srv.ws.stop().await;
}

#[tokio::test]
async fn member_transport_search_and_cross_workspace_privacy() {
    let srv = start(WsOptions::default()).await;
    let mut member = Guest::connect(&srv, &"ad".repeat(32)).await;
    let mut guest = Guest::connect(&srv, &"bd".repeat(32)).await;
    promote(&srv, &member.principal).await;
    let shared = WorkspaceId::new();
    let hidden = WorkspaceId::new();
    for ws in [&shared, &hidden] {
        let mut row = fixture_workspace(ws);
        row.repository_owner = Some("intent-hq".into());
        row.repository_name = Some("intentd".into());
        srv.store.insert_workspace(&row).await.unwrap();
        srv.store
            .insert_note(&fixture_note(ws, "spec", "transport-search-marker"))
            .await
            .unwrap();
        publish(
            &srv,
            ws,
            NOTE_UPDATED,
            json!({"marker":"transport-search-marker"}),
        )
        .await;
        publish(
            &srv,
            ws,
            TERMINAL_DATA,
            json!({"marker":"transport-search-marker"}),
        )
        .await;
        publish(
            &srv,
            ws,
            SETTINGS_CHANGED,
            json!({"marker":"transport-search-marker"}),
        )
        .await;
    }
    srv.store
        .add_workspace_member(
            &shared,
            &guest.principal.id,
            intent_core::WorkspaceRole::Collaborator,
        )
        .await
        .unwrap();
    assert_eq!(
        member
            .call("crossWorkspace.listSiblings", json!({"workspaceId":shared}))
            .await["result"][0]["id"],
        hidden.as_str()
    );
    assert!(guest
        .call("crossWorkspace.listSiblings", json!({"workspaceId":shared}))
        .await["result"]
        .as_array()
        .unwrap()
        .is_empty());
    for method in ["crossWorkspace.listNotes", "crossWorkspace.readNote"] {
        let params = json!({"workspaceId":shared,"targetWorkspaceId":hidden,"noteId":"spec"});
        let read = member.call(method, params.clone()).await;
        assert!(read.get("error").is_none(), "{read}");
        assert_eq!(
            guest.call(method, params).await["error"]["data"]["code"],
            "not-found"
        );
    }
    for (method, member_count, guest_count) in [("search.notes", 2, 1), ("search.events", 4, 1)] {
        for (client, count) in [(&mut member, member_count), (&mut guest, guest_count)] {
            let got = client
                .call(method, json!({"query":"transport-search-marker"}))
                .await;
            assert_eq!(
                got["result"]["matches"]
                    .as_array()
                    .unwrap_or_else(|| panic!("{got}"))
                    .len(),
                count,
                "{method}: {got}"
            );
        }
    }
    assert_eq!(
        guest
            .call(
                "search.events",
                json!({"workspaceId":hidden,"query":"transport-search-marker"})
            )
            .await["error"]["data"]["code"],
        "not-found"
    );
    srv.store
        .remove_host_member(&member.principal.id)
        .await
        .unwrap();
    for method in ["search.notes", "search.events"] {
        assert!(member
            .call(method, json!({"query":"transport-search-marker"}))
            .await["result"]["matches"]
            .as_array()
            .unwrap()
            .is_empty());
    }
    srv.ws.stop().await;
}

#[intent_test_macros::daemon_test]
async fn member_transport_browser_reports_keep_bound_identity_and_chief_private() {
    let srv = start(WsOptions::default()).await;
    let mut member = Guest::connect(&srv, &"ae".repeat(32)).await;
    promote(&srv, &member.principal).await;
    let ws = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&ws))
        .await
        .unwrap();
    let hello = member
        .call(
            "client.hello",
            json!({"clientId":"owner-browser","capabilities":{"browserExec":true}}),
        )
        .await;
    let id = hello["result"]["clientId"].as_str().unwrap();
    let created = member.call("browser.upsertTab", json!({"workspaceId":ws,"hostClientId":"owner-browser","tab":{"tabId":"member-tab","url":"http://daemon.localhost:3000/","hostClientId":"owner-browser"}})).await;
    assert_eq!(created["result"]["tab"]["hostClientId"], id, "{created}");
    assert_eq!(
        member
            .call("browser.listTabs", json!({"workspaceId":ws}))
            .await["result"]["tabs"][0]["hostConnected"],
        true
    );
    let owner_host = intent_core::ClientId::from("owner-browser");
    let mut owner = connect_ws(srv.port, srv.cfg.clone()).await;
    hello_browser_host(&mut owner, owner_host.as_str()).await;
    let private_tab = intent_core::BrowserTabInput {
        tab_id: "chief-tab".into(),
        workspace_id: WorkspaceId::chief(),
        url: "https://private.invalid".into(),
        requested_url: None,
        title: None,
        owner_agent_id: None,
        owner_agent_name: None,
        visibility: intent_core::BrowserTabVisibility::default(),
        emulated_size: None,
        displayed: None,
    };
    srv.store
        .upsert_browser_tab(&owner_host, private_tab)
        .await
        .unwrap();
    for (method, params) in [
        (
            "browser.navigateTab",
            json!({"tabId":"chief-tab","url":"https://example.org"}),
        ),
        (
            "browser.closeTab",
            json!({"tabId":"chief-tab","force":true}),
        ),
        ("browser.removeTab", json!({"tabId":"chief-tab"})),
        (
            "browser.upsertTab",
            json!({"workspaceId":ws,"tab":{"tabId":"chief-tab","url":"https://example.org"}}),
        ),
        (
            "browser.syncTabs",
            json!({"tabs":[{"workspaceId":WorkspaceId::chief(),"tabId":"spoof","url":"https://example.org"}]}),
        ),
    ] {
        let got = member.call(method, params).await;
        assert_eq!(got["error"]["data"]["code"], "not-found", "{method}: {got}");
    }
    assert!(srv
        .store
        .get_browser_tab("chief-tab")
        .await
        .unwrap()
        .is_some());
    let synced = member.call("browser.syncTabs", json!({"tabs":[]})).await;
    assert!(synced.get("error").is_none(), "{synced}");
    assert!(srv
        .store
        .get_browser_tab("member-tab")
        .await
        .unwrap()
        .is_none());
    assert!(srv
        .store
        .get_browser_tab("chief-tab")
        .await
        .unwrap()
        .is_some());
    srv.ws.stop().await;
}
