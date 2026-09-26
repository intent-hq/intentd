//! Personal pairing uses the actual TLS admission, router, services and SQLite store.
use super::*;
use intent_transport::{PairingSnapshot, ServerPairingInfo};
use serde_json::json;
use std::future::Future;
use std::pin::Pin;

struct PairingInfo {
    snapshot: Mutex<PairingSnapshot>,
    dir: std::path::PathBuf,
    tokens: Arc<AsyncTokenStore>,
}

impl ServerPairingInfo for PairingInfo {
    fn pairing_snapshot(&self) -> Pin<Box<dyn Future<Output = PairingSnapshot> + Send + '_>> {
        let snapshot = self.snapshot.lock().unwrap().clone();
        Box::pin(async move { snapshot })
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
    let (api, bus, store, registry, dir) = make_services(None, None).await;
    let tls = ensure_tls_certificate(dir.path()).unwrap();
    let tokens = Arc::new(AsyncTokenStore::new(Arc::new(MemTokenStore::default())));
    tokens.store_token(TOKEN).await.unwrap();
    let info = Arc::new(PairingInfo {
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
    // The same durable token-store mutation used by server.rotateToken.
    info.tokens.store_token(&"f1".repeat(32)).await.unwrap();
    assert_closed(&mut first.ws).await;
    assert_closed(&mut second.ws).await;
    assert_eq!(
        other.call("principal.me", json!({})).await["result"]["id"],
        other.principal.id.0
    );
    srv.ws.stop().await;
}
