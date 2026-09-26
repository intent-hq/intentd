//! Personal pairing uses the actual TLS admission, router, services and `SQLite` store.
use super::*;
use intent_transport::{PairingSnapshot, ServerPairingInfo};
use serde_json::json;
use std::future::Future;
use std::pin::Pin;

struct PairingInfo {
    snapshot: Mutex<PairingSnapshot>,
    barrier: Mutex<Option<SnapshotBarrier>>,
    dir: std::path::PathBuf,
    tokens: Arc<AsyncTokenStore>,
}

struct SnapshotBarrier {
    entered: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}

impl ServerPairingInfo for PairingInfo {
    fn pairing_snapshot(&self) -> Pin<Box<dyn Future<Output = PairingSnapshot> + Send + '_>> {
        let snapshot = self.snapshot.lock().unwrap().clone();
        let barrier = self.barrier.lock().unwrap().take();
        Box::pin(async move {
            if let Some(barrier) = barrier {
                let _ = barrier.entered.send(());
                let _ = barrier.release.await;
            }
            snapshot
        })
    }
    fn host_environment(&self) -> intent_transport::host_env::HostEnvironment {
        intent_transport::host_env::HostEnvironment {
            hostname: "pairing-fixture".into(),
            pretty_hostname: "Pairing fixture".into(),
            device_kind: None,
            hardware_model: None,
        }
    }
    fn data_dir(&self) -> &Path {
        &self.dir
    }
    fn token_store(&self) -> &AsyncTokenStore {
        &self.tokens
    }
}

async fn start_pairing() -> (Server, Arc<PairingInfo>) {
    start_pairing_with_admission(None).await
}

async fn start_pairing_with_admission(
    gate: Option<Arc<RemovalAdmissionGate>>,
) -> (Server, Arc<PairingInfo>) {
    let (api, bus, store, registry, dir) = make_services(None, None).await;
    start_pairing_services(api, bus, store, registry, dir, gate).await
}

async fn start_pairing_services(
    api: Arc<dyn WorkspaceApi>,
    bus: EventBus,
    store: Store,
    registry: Arc<intent_services::SettingsRegistry>,
    dir: tempfile::TempDir,
    gate: Option<Arc<RemovalAdmissionGate>>,
) -> (Server, Arc<PairingInfo>) {
    let api: Arc<dyn WorkspaceApi> = if let Some(gate) = gate {
        Arc::new(RemovalAdmissionApi { actual: api, gate })
    } else {
        api
    };
    let tls = ensure_tls_certificate(dir.path()).unwrap();
    let tokens = Arc::new(AsyncTokenStore::new(Arc::new(MemTokenStore::default())));
    tokens.store_token(TOKEN).await.unwrap();
    let info = Arc::new(PairingInfo {
        barrier: Mutex::new(None),
        snapshot: Mutex::new(PairingSnapshot {
            port: None,
            bind_addresses: Some(vec![Ipv4Addr::LOCALHOST.into()]),
            tc_address: Some("tc-pairing-fixture.example".into()),
        }),
        dir: dir.path().to_path_buf(),
        tokens: tokens.clone(),
    });
    let reverse_registry = Arc::new(PrimaryReverseRegistry::new());
    let mut ws = WsApiServer::new_with_reverse(
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
    ws.install_pairing_info(info.clone());
    let port = ws.start().await.unwrap();
    info.snapshot.lock().unwrap().port = Some(port);
    (
        Server {
            ws,
            port,
            cfg: client_config(&tls.fingerprint256),
            api,
            bus,
            store,
            registry,
            reverse_registry,
            dir,
        },
        info,
    )
}

#[tokio::test]
async fn personal_pairing_inflight_revalidation_orders_real_revocation_and_removal() {
    let (srv, info) = start_pairing().await;
    for remove in [false, true] {
        let token = if remove { "a5" } else { "b5" }.repeat(32);
        let mut person = Guest::connect(&srv, &token).await;
        sqlx::query("INSERT INTO host_member (principal_id, added_at) VALUES (?, ?)")
            .bind(&person.principal.id.0)
            .bind(now_iso())
            .execute(srv.store.write_pool())
            .await
            .unwrap();
        let mut second = reconnect(&srv, &person.principal, &token).await;
        let id = person.principal.id.clone();
        let (entered, arrived) = tokio::sync::oneshot::channel();
        let (release, wait) = tokio::sync::oneshot::channel();
        *info.barrier.lock().unwrap() = Some(SnapshotBarrier {
            entered,
            release: wait,
        });
        let pairing = tokio::spawn(async move {
            let result = person.call("pairing.getSelfInfo", json!({})).await;
            (result, person)
        });
        tokio::time::timeout(Duration::from_secs(5), arrived)
            .await
            .unwrap()
            .unwrap();
        // The first credential check succeeded. Commit actual invalidation
        // before permitting the route/profile/final-check portion to complete.
        if remove {
            assert!(srv.store.remove_host_member(&id).await.unwrap().removed);
        } else {
            assert_eq!(
                second.call("principal.revokeSelf", json!({})).await["result"]["revoked"],
                true
            );
            assert_closed(&mut second.ws).await;
        }
        release.send(()).unwrap();
        let (result, mut person) = pairing.await.unwrap();
        assert_eq!(result["error"]["data"]["code"], "access-revoked");
        assert!(result.get("result").is_none());
        assert!(!result.to_string().contains(&token));
        assert_closed(&mut person.ws).await;
    }
    srv.ws.stop().await;
}

async fn reconnect(srv: &Server, principal: &intent_core::Principal, token: &str) -> Guest {
    let url = format!("wss://localhost:{}/ws?token={token}", srv.port);
    Guest {
        principal: principal.clone(),
        ws: common::wss_connect_with_retry(srv.port, srv.cfg.clone(), &url).await,
        next_id: 0,
    }
}

fn assert_pairing(response: &Value, token: &str, person: &intent_core::Principal, role: &str) {
    assert!(
        response.get("error").is_none(),
        "personal pairing must succeed"
    );
    let result = &response["result"];
    assert!(
        result["token"].as_str() == Some(token),
        "must return exactly the admitted bearer"
    );
    assert_eq!(result["principal"]["id"], person.id.0);
    assert_eq!(result["principal"]["hostRole"], role);
    assert_eq!(result["principal"]["isAdministrator"], role == "owner");
    assert_eq!(result["version"], 1);
    let expected = format!(
        "intent://pair?v=1&host=&port={}&fp={}&token={token}&tc=tc-pairing-fixture.example",
        result["port"],
        result["fingerprint"].as_str().unwrap()
    );
    assert!(
        result["uri"].as_str() == Some(&expected),
        "QR and copyable URI use the same v1 payload"
    );
}

async fn assert_closed(ws: &mut common::TlsWs) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Close(Some(frame)))) => {
                    assert_eq!(
                        frame.code,
                        tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Policy
                    );
                    return;
                }
                Some(Ok(Message::Ping(bytes))) => ws.send(Message::Pong(bytes)).await.unwrap(),
                _ => panic!("revoked socket must close without another payload"),
            }
        }
    })
    .await
    .expect("live credential invalidation closes the socket");
}

#[tokio::test]
async fn personal_pairing_returns_only_admitted_person() {
    let (srv, _info) = start_pairing().await;
    let owner = srv.store.get_primary_principal().await.unwrap();
    let mut owner_socket = reconnect(&srv, &owner, TOKEN).await;
    let hello=owner_socket.call("client.hello",json!({"capabilities":{"hostMembership":false,"personalPairing":999,"authenticatedDevices":false}})).await;
    assert_shared_capabilities(&hello);
    assert_pairing(
        &owner_socket.call("pairing.getSelfInfo", json!({})).await,
        TOKEN,
        &owner,
        "owner",
    );
    for (token, member) in [("b1".repeat(32), true), ("c1".repeat(32), false)] {
        let mut person = Guest::connect(&srv, &token).await;
        if member {
            sqlx::query("INSERT INTO host_member (principal_id, added_at) VALUES (?, ?)")
                .bind(&person.principal.id.0)
                .bind(now_iso())
                .execute(srv.store.write_pool())
                .await
                .unwrap();
        }
        let hello=person.call("client.hello",json!({"capabilities":{"hostMembership":false,"personalPairing":999,"authenticatedDevices":false}})).await;
        assert_shared_capabilities(&hello);
        let role = if member { "member" } else { "guest" };
        let response = person.call("pairing.getSelfInfo", json!({})).await;
        assert_pairing(&response, &token, &person.principal, role);
        assert!(
            !response.to_string().contains(TOKEN),
            "never export the owner secret"
        );
        let mut second = reconnect(&srv, &person.principal, &token).await;
        for _ in 0..2 {
            assert_pairing(
                &second.call("pairing.getSelfInfo", json!({})).await,
                &token,
                &person.principal,
                role,
            );
        }
        drop(person.ws);
        assert_pairing(
            &second.call("pairing.getSelfInfo", json!({})).await,
            &token,
            &person.principal,
            role,
        );
        for method in [
            "pairing.getInfo",
            "server.pairingInfo",
            "server.rotateToken",
        ] {
            assert!(second.call(method, json!({})).await.get("error").is_some());
        }
    }
    srv.ws.stop().await;
}

#[tokio::test]
async fn personal_pairing_refuses_override_params() {
    let (srv, _info) = start_pairing().await;
    let mut person = Guest::connect(&srv, &"d1".repeat(32)).await;
    for params in [
        json!({"principalId":"someone-else"}),
        json!({"role":"owner"}),
        json!({"credential":TOKEN}),
        json!({"host":"another-host"}),
        json!(["owner"]),
    ] {
        let result = person.call("pairing.getSelfInfo", params).await;
        assert_eq!(result["error"]["code"], -32602);
        assert_eq!(result["error"]["data"]["code"], "invalid-params");
        assert!(result.get("result").is_none());
        assert!(!result.to_string().contains(TOKEN));
    }
    srv.ws.stop().await;
}

#[tokio::test]
async fn personal_pairing_rotation_closes_legacy_sockets_only() {
    let (srv, info) = start_pairing().await;
    let owner = srv.store.get_primary_principal().await.unwrap();
    let mut first = reconnect(&srv, &owner, TOKEN).await;
    let mut second = reconnect(&srv, &owner, TOKEN).await;
    let mut other = Guest::connect(&srv, &"e1".repeat(32)).await;
    let url = format!("wss://localhost:{}/tunnel?token={TOKEN}", srv.port);
    let mut tunnel = common::wss_connect_with_retry(srv.port, srv.cfg.clone(), &url).await;
    // The same durable token-store mutation used by server.rotateToken.
    info.tokens.store_token(&"f1".repeat(32)).await.unwrap();
    assert_closed(&mut first.ws).await;
    assert_closed(&mut second.ws).await;
    assert_closed(&mut tunnel).await;
    assert_eq!(
        other.call("principal.me", json!({})).await["result"]["id"],
        other.principal.id.0
    );
    srv.ws.stop().await;
}

#[tokio::test]
async fn personal_pairing_owner_device_and_dynamic_role_keep_the_exact_bearer() {
    let (srv, info) = start_pairing().await;
    let owner = srv.store.get_primary_principal().await.unwrap();
    let token = "a2".repeat(32);
    srv.store
        .insert_principal_credential(&owner.id, &sha256_hex(token.as_bytes()))
        .await
        .unwrap();
    let mut device = reconnect(&srv, &owner, &token).await;
    assert_pairing(
        &device.call("pairing.getSelfInfo", json!({})).await,
        &token,
        &owner,
        "owner",
    );
    let personal = "b2".repeat(32);
    let mut person = Guest::connect(&srv, &personal).await;
    let before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM principal_credential")
        .fetch_one(srv.store.read_pool())
        .await
        .unwrap();
    person.call("client.hello", json!({"clientId":"owner","displayName":"Owner","principalId":owner.id.0,"hostRole":"owner","kind":"ios"})).await;
    assert_pairing(
        &person.call("pairing.getSelfInfo", json!({})).await,
        &personal,
        &person.principal,
        "guest",
    );
    sqlx::query("INSERT INTO host_member (principal_id, added_at) VALUES (?, ?)")
        .bind(&person.principal.id.0)
        .bind(now_iso())
        .execute(srv.store.write_pool())
        .await
        .unwrap();
    assert_pairing(
        &person.call("pairing.getSelfInfo", json!({})).await,
        &personal,
        &person.principal,
        "member",
    );
    sqlx::query("DELETE FROM host_member WHERE principal_id = ?")
        .bind(&person.principal.id.0)
        .execute(srv.store.write_pool())
        .await
        .unwrap();
    assert_pairing(
        &person.call("pairing.getSelfInfo", json!({})).await,
        &personal,
        &person.principal,
        "guest",
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM principal_credential")
            .fetch_one(srv.store.read_pool())
            .await
            .unwrap(),
        before
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM workspace WHERE id != '__chief__'")
            .fetch_one(srv.store.read_pool())
            .await
            .unwrap(),
        0
    );
    info.snapshot.lock().unwrap().bind_addresses = Some(vec!["192.0.2.7".parse().unwrap()]);
    let direct = person.call("pairing.getSelfInfo", json!({})).await;
    assert_eq!(direct["result"]["hosts"], json!(["192.0.2.7"]));
    assert!(direct["result"]["token"].as_str() == Some(&personal));
    info.snapshot.lock().unwrap().tc_address = None;
    let direct = person.call("pairing.getSelfInfo", json!({})).await;
    assert!(direct["result"].get("tcAddress").is_none());
    info.snapshot.lock().unwrap().bind_addresses = Some(vec![Ipv4Addr::LOCALHOST.into()]);
    assert!(person
        .call("pairing.getSelfInfo", json!({}))
        .await
        .get("error")
        .is_some());
    info.snapshot.lock().unwrap().port = None;
    assert_eq!(
        person.call("pairing.getSelfInfo", json!({})).await["error"]["data"]["code"],
        "listener-down"
    );
    srv.ws.stop().await;
}

#[tokio::test]
async fn personal_pairing_real_self_revocation_closes_multiple_devices() {
    let (srv, _info) = start_pairing().await;
    let token = "c2".repeat(32);
    let mut person = Guest::connect(&srv, &token).await;
    sqlx::query("INSERT INTO host_member (principal_id, added_at) VALUES (?, ?)")
        .bind(&person.principal.id.0)
        .bind(now_iso())
        .execute(srv.store.write_pool())
        .await
        .unwrap();
    let response = person.call("pairing.getSelfInfo", json!({})).await;
    assert_pairing(&response, &token, &person.principal, "member");
    let mut second = reconnect(&srv, &person.principal, &token).await;
    let mut other = Guest::connect(&srv, &"d2".repeat(32)).await;
    let revoked = person.call("principal.revokeSelf", json!({})).await;
    assert_eq!(revoked["result"]["revoked"], true);
    assert_eq!(revoked["result"]["hostMembershipRemoved"], true);
    assert_eq!(
        srv.store.get_host_role(&person.principal.id).await.unwrap(),
        intent_core::HostRole::Guest
    );
    assert_closed(&mut person.ws).await;
    assert_closed(&mut second.ws).await;
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
    assert_pairing(
        &other.call("pairing.getSelfInfo", json!({})).await,
        &"d2".repeat(32),
        &other.principal,
        "guest",
    );
    srv.ws.stop().await;
}

#[tokio::test]
async fn member_removal_real_owner_operation_closes_paired_devices() {
    let (srv, _info) = start_pairing().await;
    let owner = srv.store.get_primary_principal().await.unwrap();
    let mut administrator = reconnect(&srv, &owner, TOKEN).await;
    let token = "a6".repeat(32);
    let mut member = Guest::connect(&srv, &token).await;
    sqlx::query("INSERT INTO host_member (principal_id, added_at) VALUES (?, ?)")
        .bind(&member.principal.id.0)
        .bind(now_iso())
        .execute(srv.store.write_pool())
        .await
        .unwrap();
    assert_pairing(
        &member.call("pairing.getSelfInfo", json!({})).await,
        &token,
        &member.principal,
        "member",
    );
    let mut phone = reconnect(&srv, &member.principal, &token).await;
    let mut other = Guest::connect(&srv, &"b6".repeat(32)).await;
    assert_eq!(
        administrator
            .call(
                "host.members.remove",
                json!({"principalId":member.principal.id})
            )
            .await["result"],
        json!({"removed":true})
    );
    assert_closed(&mut member.ws).await;
    assert_closed(&mut phone.ws).await;
    assert_eq!(
        other.call("principal.me", json!({})).await["result"]["id"],
        other.principal.id.0
    );
    assert_eq!(
        administrator
            .call(
                "host.members.remove",
                json!({"principalId":member.principal.id})
            )
            .await["result"],
        json!({"removed":false})
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
async fn personal_pairing_durable_removal_and_individual_revocation_refuse_then_close() {
    let (srv, _info) = start_pairing().await;
    for member in [false, true] {
        let token = if member { "e2" } else { "f2" }.repeat(32);
        let mut person = Guest::connect(&srv, &token).await;
        let mut other = Guest::connect(&srv, &format!("{token}other")).await;
        if member {
            sqlx::query("INSERT INTO host_member (principal_id, added_at) VALUES (?, ?)")
                .bind(&person.principal.id.0)
                .bind(now_iso())
                .execute(srv.store.write_pool())
                .await
                .unwrap();
        }
        let mut second = reconnect(&srv, &person.principal, &token).await;
        assert!(person
            .call("pairing.getSelfInfo", json!({}))
            .await
            .get("error")
            .is_none());
        if member {
            let removed = srv
                .store
                .remove_host_member(&person.principal.id)
                .await
                .unwrap();
            assert!(removed.removed && removed.credentials == 1);
        } else {
            assert!(srv
                .store
                .revoke_principal_credential(&sha256_hex(token.as_bytes()))
                .await
                .unwrap());
        }
        // No synthetic broadcast: the read must independently revalidate SQLite.
        for device in [&mut person, &mut second] {
            let refused = device.call("pairing.getSelfInfo", json!({})).await;
            assert_eq!(refused["error"]["code"], -32003);
            assert_eq!(refused["error"]["data"]["code"], "access-revoked");
            assert!(refused.get("result").is_none());
            assert!(!refused.to_string().contains(&token));
            assert_closed(&mut device.ws).await;
        }
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
        assert!(other
            .call("pairing.getSelfInfo", json!({}))
            .await
            .get("error")
            .is_none());
    }
    srv.ws.stop().await;
}

#[tokio::test]
async fn personal_pairing_wrong_host_and_fingerprint_never_substitute_identity() {
    let (srv, _info) = start_pairing().await;
    let (foreign, _foreign_info) = start_pairing().await;
    let token = "a3".repeat(32);
    let mut person = Guest::connect(&srv, &token).await;
    let response = person.call("pairing.getSelfInfo", json!({})).await;
    assert_pairing(&response, &token, &person.principal, "guest");
    assert_eq!(
        status_code(
            &https_request(
                foreign.port,
                foreign.cfg.clone(),
                &upgrade_req("/ws", None, Some(&token))
            )
            .await
        ),
        401
    );
    let tcp = TcpStream::connect(("127.0.0.1", foreign.port))
        .await
        .unwrap();
    assert!(
        tokio_rustls::TlsConnector::from(client_config(&"00:".repeat(32)))
            .connect(ServerName::try_from("localhost").unwrap(), tcp)
            .await
            .is_err()
    );
    assert_eq!(
        status_code(
            &https_request(
                srv.port,
                srv.cfg.clone(),
                &upgrade_req("/ws", Some("https://untrusted.example"), Some(&token))
            )
            .await
        ),
        403
    );
    srv.ws.stop().await;
    foreign.ws.stop().await;
}

#[tokio::test]
async fn personal_pairing_removal_racing_pair_and_upgrade_cannot_restore_access() {
    let (srv, _info) = start_pairing().await;
    for n in 0..4 {
        let token = format!("pairing-race-{n}");
        let mut person = Guest::connect(&srv, &token).await;
        sqlx::query("INSERT INTO host_member (principal_id, added_at) VALUES (?, ?)")
            .bind(&person.principal.id.0)
            .bind(now_iso())
            .execute(srv.store.write_pool())
            .await
            .unwrap();
        let id = person.principal.id.clone();
        let tls = tls_connect(srv.port, srv.cfg.clone()).await;
        let url = format!("wss://localhost:{}/ws?token={token}", srv.port);
        let (paired, upgrade, removed) = tokio::join!(
            person.call("pairing.getSelfInfo", json!({})),
            tokio_tungstenite::client_async(url, tls),
            srv.store.remove_host_member(&id),
        );
        assert!(removed.unwrap().removed);
        if paired.get("result").is_some() {
            assert!(paired["result"]["token"].as_str() == Some(&token));
            assert_eq!(
                person.call("pairing.getSelfInfo", json!({})).await["error"]["data"]["code"],
                "access-revoked"
            );
        } else {
            assert_eq!(paired["error"]["data"]["code"], "access-revoked");
        }
        assert_closed(&mut person.ws).await;
        match upgrade {
            Ok((ws, _)) => {
                let mut admitted_before_remove = Guest {
                    ws,
                    principal: person.principal,
                    next_id: 0,
                };
                assert_eq!(
                    admitted_before_remove
                        .call("pairing.getSelfInfo", json!({}))
                        .await["error"]["data"]["code"],
                    "access-revoked"
                );
                assert_closed(&mut admitted_before_remove.ws).await;
            }
            Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
                assert_eq!(response.status().as_u16(), 401);
            }
            Err(error) => panic!(
                "unexpected upgrade error class: {:?}",
                std::mem::discriminant(&error)
            ),
        }
        assert!(srv
            .store
            .resolve_active_principal_credential(&sha256_hex(token.as_bytes()))
            .await
            .unwrap()
            .is_none());
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
    }
    srv.ws.stop().await;
}

async fn removal_tunnel(srv: &Server, token: &str) -> common::TlsWs {
    let url = format!("wss://localhost:{}/tunnel?token={token}", srv.port);
    common::wss_connect_with_retry(srv.port, srv.cfg.clone(), &url).await
}
async fn removal_frame(ws: &mut common::TlsWs) -> intent_transport::tunnel::Frame {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Binary(b))) => {
                    return intent_transport::tunnel::Frame::decode(&b).unwrap()
                }
                Some(Ok(Message::Ping(b))) => ws.send(Message::Pong(b)).await.unwrap(),
                _ => panic!("expected binary tunnel response"),
            }
        }
    })
    .await
    .unwrap()
}
async fn removal_send(ws: &mut common::TlsWs, f: intent_transport::tunnel::Frame) {
    ws.send(Message::Binary(f.encode().into())).await.unwrap();
}
async fn removal_roundtrip(ws: &mut common::TlsWs, peer: &mut TcpStream) {
    use intent_transport::tunnel::Frame;
    removal_send(
        ws,
        Frame::Data {
            stream_id: 1,
            payload: b"hello".to_vec(),
        },
    )
    .await;
    let mut bytes = [0; 5];
    tokio::time::timeout(Duration::from_secs(5), peer.read_exact(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&bytes, b"hello");
    peer.write_all(b"reply").await.unwrap();
    assert_eq!(
        removal_frame(ws).await,
        Frame::Data {
            stream_id: 1,
            payload: b"reply".to_vec()
        }
    );
}

#[tokio::test]
async fn member_removal_idle_active_tunnels_forwarding_and_final_control() {
    use intent_transport::tunnel::Frame;
    let (srv, _) = start_pairing().await;
    let owner = srv.store.get_primary_principal().await.unwrap();
    let mut admin = reconnect(&srv, &owner, TOKEN).await;
    let token = "removal-personal-member";
    let mut idle = Guest::connect(&srv, token).await;
    sqlx::query("INSERT INTO host_member (principal_id,added_at) VALUES (?,?)")
        .bind(&idle.principal.id.0)
        .bind(now_iso())
        .execute(srv.store.write_pool())
        .await
        .unwrap();
    let second_token = "removal-another-device";
    srv.store
        .insert_principal_credential(&idle.principal.id, &sha256_hex(second_token.as_bytes()))
        .await
        .unwrap();
    let mut active = reconnect(&srv, &idle.principal, second_token).await;
    assert_pairing(
        &active.call("pairing.getSelfInfo", json!({})).await,
        second_token,
        &active.principal,
        "member",
    );
    let subscribed = idle
        .call(
            "events.subscribe",
            json!({"eventTypes":["host:members-changed"]}),
        )
        .await;
    let subscription = subscribed["result"]["subscriptionId"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut unaffected = Guest::connect(&srv, "removal-unaffected").await;
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let forward = active
        .call("forward.create", json!({"remotePort":port}))
        .await;
    let local = u16::try_from(forward["result"]["localPort"].as_u64().unwrap()).unwrap();
    let mut downstream = TcpStream::connect(("127.0.0.1", local)).await.unwrap();
    let (mut upstream, _) = listener.accept().await.unwrap();
    downstream.write_all(b"ok").await.unwrap();
    let mut two = [0; 2];
    upstream.read_exact(&mut two).await.unwrap();
    assert_eq!(&two, b"ok");
    let mut idle_tunnel = removal_tunnel(&srv, token).await;
    let mut active_tunnel = removal_tunnel(&srv, second_token).await;
    removal_send(&mut active_tunnel, Frame::Open { stream_id: 1, port }).await;
    assert_eq!(
        removal_frame(&mut active_tunnel).await,
        Frame::OpenOk { stream_id: 1 }
    );
    let (mut peer, _) = listener.accept().await.unwrap();
    removal_roundtrip(&mut active_tunnel, &mut peer).await;
    let mut kept_tunnel = removal_tunnel(&srv, TOKEN).await;
    removal_send(&mut kept_tunnel, Frame::Open { stream_id: 1, port }).await;
    assert_eq!(
        removal_frame(&mut kept_tunnel).await,
        Frame::OpenOk { stream_id: 1 }
    );
    let (mut kept_peer, _) = listener.accept().await.unwrap();
    assert_eq!(
        admin
            .call(
                "host.members.remove",
                json!({"principalId":idle.principal.id})
            )
            .await["result"],
        json!({"removed":true})
    );
    // A subscribed removed person receives the real durable control event,
    // despite revocation having priority over ordinary bulk traffic.
    let notification = tokio::time::timeout(Duration::from_secs(5), idle.ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Message::Text(frame) = notification else {
        panic!("removal control must precede close")
    };
    let control: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(control["params"]["subscriptionId"], subscription);
    assert_eq!(control["params"]["event"]["type"], "host:members-changed");
    assert_eq!(
        control["params"]["event"]["data"]["principalId"],
        idle.principal.id.0
    );
    assert_eq!(control["params"]["event"]["data"]["action"], "removed");
    assert_closed(&mut idle.ws).await;
    assert_closed(&mut active.ws).await;
    assert_closed(&mut idle_tunnel).await;
    assert_closed(&mut active_tunnel).await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), downstream.read(&mut two))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), peer.read(&mut two))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    assert!(TcpStream::connect(("127.0.0.1", local)).await.is_err());
    for old in [token, second_token] {
        for path in ["/ws", "/tunnel"] {
            assert_eq!(
                status_code(
                    &https_request(
                        srv.port,
                        srv.cfg.clone(),
                        &upgrade_req(path, None, Some(old))
                    )
                    .await
                ),
                401
            );
        }
    }
    assert_eq!(
        unaffected.call("principal.me", json!({})).await["result"]["id"],
        unaffected.principal.id.0
    );
    assert_eq!(
        admin.call("host.members.list", json!({})).await["result"]["members"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    removal_roundtrip(&mut kept_tunnel, &mut kept_peer).await;
    srv.ws.stop().await;
}

#[derive(Default)]
struct RemovalAdmissionGate {
    before: Mutex<Option<SnapshotBarrier>>,
    after: Mutex<Option<SnapshotBarrier>>,
}
struct RemovalAdmissionApi {
    actual: Arc<dyn WorkspaceApi>,
    gate: Arc<RemovalAdmissionGate>,
}
impl WorkspaceApi for RemovalAdmissionApi {
    fn primary_principal_id(
        &self,
    ) -> intent_core::BoxFuture<'_, CoreResult<intent_core::PrincipalId>> {
        self.actual.primary_principal_id()
    }
    fn principal_host_role(
        &self,
        id: intent_core::PrincipalId,
    ) -> intent_core::BoxFuture<'_, CoreResult<intent_core::HostRole>> {
        Box::pin(async move {
            let role = self.actual.principal_host_role(id).await;
            let pause = self.gate.after.lock().unwrap().take();
            if let Some(pause) = pause {
                let _ = pause.entered.send(());
                let _ = pause.release.await;
            }
            role
        })
    }
    fn resolve_principal_credential(
        &self,
        hash: String,
    ) -> intent_core::BoxFuture<'_, CoreResult<Option<intent_core::PrincipalId>>> {
        let pause = self.gate.before.lock().unwrap().take();
        Box::pin(async move {
            if let Some(pause) = pause {
                let _ = pause.entered.send(());
                let _ = pause.release.await;
            }
            self.actual.resolve_principal_credential(hash).await
        })
    }
    fn host_members_remove(
        &self,
        id: intent_core::PrincipalId,
    ) -> intent_core::BoxFuture<'_, CoreResult<Value>> {
        self.actual.host_members_remove(id)
    }
    fn principal_me(&self) -> intent_core::BoxFuture<'_, CoreResult<Value>> {
        self.actual.principal_me()
    }
    fn subscribe_principal_revocations(
        &self,
    ) -> Option<tokio::sync::broadcast::Receiver<intent_core::PrincipalRevocation>> {
        self.actual.subscribe_principal_revocations()
    }
}

#[tokio::test]
async fn member_removal_connection_admission_both_linearizations_cannot_reopen_access() {
    for path in ["/ws", "/tunnel"] {
        for admitted_first in [false, true] {
            let gate = Arc::new(RemovalAdmissionGate::default());
            let (srv, _) = start_pairing_with_admission(Some(gate.clone())).await;
            let owner = srv.store.get_primary_principal().await.unwrap();
            let mut admin = reconnect(&srv, &owner, TOKEN).await;
            let token = "connection-removal-barrier";
            let mut person = Guest::connect(&srv, token).await;
            sqlx::query("INSERT INTO host_member (principal_id,added_at) VALUES (?,?)")
                .bind(&person.principal.id.0)
                .bind(now_iso())
                .execute(srv.store.write_pool())
                .await
                .unwrap();
            let (entered, reached) = tokio::sync::oneshot::channel();
            let (release, wait) = tokio::sync::oneshot::channel();
            let pause = SnapshotBarrier {
                entered,
                release: wait,
            };
            if admitted_first {
                *gate.after.lock().unwrap() = Some(pause);
            } else {
                *gate.before.lock().unwrap() = Some(pause);
            }
            let tls = tls_connect(srv.port, srv.cfg.clone()).await;
            let url = format!("wss://localhost:{}{path}?token={token}", srv.port);
            let connect =
                tokio::spawn(async move { tokio_tungstenite::client_async(url, tls).await });
            tokio::time::timeout(Duration::from_secs(5), reached)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                admin
                    .call(
                        "host.members.remove",
                        json!({"principalId":person.principal.id})
                    )
                    .await["result"]["removed"],
                true
            );
            release.send(()).unwrap();
            let result = connect.await.unwrap();
            if admitted_first {
                let (mut socket, _) = result.expect("the already admitted handshake may finish");
                assert_closed(&mut socket).await;
            } else {
                assert!(
                    matches!(result,Err(tokio_tungstenite::tungstenite::Error::Http(r)) if r.status().as_u16()==401)
                );
            }
            assert_closed(&mut person.ws).await;
            assert_eq!(
                status_code(
                    &https_request(
                        srv.port,
                        srv.cfg.clone(),
                        &upgrade_req(path, None, Some(token))
                    )
                    .await
                ),
                401
            );
            srv.ws.stop().await;
        }
    }
}

#[tokio::test]
async fn member_removal_invalidates_inflight_personal_pairing_through_real_owner_rpc() {
    let (srv, info) = start_pairing().await;
    let owner = srv.store.get_primary_principal().await.unwrap();
    let mut admin = reconnect(&srv, &owner, TOKEN).await;
    let token = "removal-inflight-pairing";
    let mut person = Guest::connect(&srv, token).await;
    sqlx::query("INSERT INTO host_member (principal_id,added_at) VALUES (?,?)")
        .bind(&person.principal.id.0)
        .bind(now_iso())
        .execute(srv.store.write_pool())
        .await
        .unwrap();
    let id = person.principal.id.clone();
    let mut phone = reconnect(&srv, &person.principal, token).await;
    assert_pairing(
        &phone.call("pairing.getSelfInfo", json!({})).await,
        token,
        &phone.principal,
        "member",
    );
    let (entered, reached) = tokio::sync::oneshot::channel();
    let (release, wait) = tokio::sync::oneshot::channel();
    *info.barrier.lock().unwrap() = Some(SnapshotBarrier {
        entered,
        release: wait,
    });
    let pairing = tokio::spawn(async move {
        let response = person.call("pairing.getSelfInfo", json!({})).await;
        (person, response)
    });
    tokio::time::timeout(Duration::from_secs(5), reached)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        admin
            .call("host.members.remove", json!({"principalId":id}))
            .await["result"]["removed"],
        true
    );
    assert_closed(&mut phone.ws).await;
    release.send(()).unwrap();
    let (mut person, response) = pairing.await.unwrap();
    assert_eq!(response["error"]["data"]["code"], "access-revoked");
    assert!(!response.to_string().contains(token));
    assert_closed(&mut person.ws).await;
    srv.ws.stop().await;
}

#[tokio::test]
async fn member_removal_reconciles_roster_directory_presence_and_snapshots() {
    let (srv, _) = start_pairing().await;
    let mut member = Guest::connect(&srv, "removal-roster-member").await;
    sqlx::query("INSERT INTO host_member (principal_id,added_at) VALUES (?,?)")
        .bind(&member.principal.id.0)
        .bind(now_iso())
        .execute(srv.store.write_pool())
        .await
        .unwrap();
    let workspace = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&workspace))
        .await
        .unwrap();
    assert_eq!(
        member.call("principal.me", json!({})).await["result"]["hostRole"],
        "member"
    );
    let hello = member
        .call("client.hello", json!({"clientId":"removal-roster-phone"}))
        .await;
    assert!(hello.get("error").is_none());
    let mut owner = PresenceClient::open(srv.port, srv.cfg.clone(), TOKEN).await;
    assert_eq!(
        owner
            .call(1, "presence.snapshot", json!({"workspaceId":workspace}))
            .await["result"]["members"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let sub = owner.call(2, "workspace.subscribe", json!({})).await;
    let snapshot = owner
        .push(sub["result"]["subscriptionId"].as_str().unwrap())
        .await;
    assert_eq!(snapshot["snapshot"].as_array().unwrap().len(), 1);
    let events = owner
        .call(
            3,
            "events.subscribe",
            json!({"eventTypes":["host:members-changed","workspace:updated","presence:changed"]}),
        )
        .await;
    assert!(events["result"]["subscriptionId"].is_string());
    let before = owner.call(4, "principal.list", json!({})).await;
    assert!(before["result"]["principals"]
        .as_array()
        .unwrap()
        .iter()
        .any(|r| r["principalId"] == member.principal.id.0));
    let removed = owner
        .call(
            5,
            "host.members.remove",
            json!({"principalId":member.principal.id}),
        )
        .await;
    assert_eq!(removed["result"], json!({"removed":true}));
    let event = removal_observed_event(&mut owner, "host:members-changed").await;
    assert_eq!(event["data"]["principalId"], member.principal.id.0);
    assert_eq!(event["data"]["action"], "removed");
    let durable = srv
        .store
        .query_events(&intent_store::EventQuery {
            event_types: vec!["host:members-changed".into()],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(durable
        .iter()
        .any(|row| Some(row.id.as_str()) == event["id"].as_str()));
    let changed = removal_observed_event(&mut owner, "workspace:updated").await;
    assert_eq!(
        changed["data"]["changes"]["removedPrincipalId"],
        member.principal.id.0
    );
    assert_eq!(changed["data"]["changes"]["members"], true);
    let offline = removal_observed_event(&mut owner, "presence:changed").await;
    assert!(offline["data"]["members"].as_array().unwrap().is_empty());
    let directory = owner.call(6, "principal.list", json!({})).await;
    assert!(!directory["result"]["principals"]
        .as_array()
        .unwrap()
        .iter()
        .any(|r| r["principalId"] == member.principal.id.0));
    let roster = owner
        .call(
            7,
            "workspace.members.list",
            json!({"workspaceId":workspace}),
        )
        .await;
    assert_eq!(roster["result"]["members"].as_array().unwrap().len(), 1);
    assert_eq!(roster["result"]["members"][0]["hostRole"], "owner");
    let fresh = owner.call(8, "workspace.subscribe", json!({})).await;
    let snapshot = owner
        .push(fresh["result"]["subscriptionId"].as_str().unwrap())
        .await;
    assert_eq!(snapshot["snapshot"].as_array().unwrap().len(), 1);
    assert_closed(&mut member.ws).await;
    srv.ws.stop().await;
}

async fn removal_observed_event(client: &mut PresenceClient, kind: &str) -> Value {
    if let Some(index) = client
        .skipped
        .iter()
        .position(|v| v["method"] == "events.event" && v["params"]["event"]["type"] == kind)
    {
        return client.skipped.remove(index)["params"]["event"].clone();
    }
    client.event(kind).await
}

fn assert_shared_capabilities(hello: &Value) {
    assert!(hello.get("error").is_none());
    let capabilities = &hello["result"]["server"]["capabilities"];
    assert_eq!(capabilities["hostMembership"], 1);
    assert_eq!(capabilities["personalPairing"], 1);
    assert_eq!(capabilities["authenticatedDevices"], 1);
}

#[path = "member_removal_rejoin.rs"]
mod member_removal_rejoin;
