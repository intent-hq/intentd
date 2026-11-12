//! `/local-pair` admission and serving (§5.58, §2.7) through the listener's
//! real `handle_conn`, over an in-memory stream so the TCP peer address can
//! be any address: loopback-only peers, the strict `Origin` rule, the
//! connection cap, the single served method, and the grant.

use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::pin::Pin;
use std::sync::Arc;

use futures_util::{SinkExt as _, StreamExt as _};
use intent_core::local_pairing::LocalPairingService;
use intent_core::WorkspaceApi;
use intent_services::EventBus;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _, DuplexStream};
use tokio_tungstenite::tungstenite::protocol::Role;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use crate::local_pairing::{LOCAL_PAIR_ONLY_MESSAGE, MAX_LOCAL_PAIR_CONNECTIONS};
use crate::server::{PairingSnapshot, ServerPairingInfo};
use crate::{AsyncTokenStore, TokenStore, WsApiServer, WsOptions};

const PORT: u16 = 4242;

struct Api {
    pairing: Arc<intent_services::local_pairing::LocalPairing>,
}

impl WorkspaceApi for Api {
    fn local_pairing(&self) -> Option<Arc<dyn LocalPairingService>> {
        Some(self.pairing.clone())
    }
}

struct NoTokens;

impl TokenStore for NoTokens {
    fn load_token(&self) -> Option<String> {
        None
    }
    fn store_token(&self, _token: &str) -> intent_core::Result<()> {
        Ok(())
    }
}

struct Provider {
    data_dir: std::path::PathBuf,
    token_store: AsyncTokenStore,
}

impl ServerPairingInfo for Provider {
    fn pairing_snapshot(&self) -> Pin<Box<dyn Future<Output = PairingSnapshot> + Send + '_>> {
        Box::pin(async move {
            PairingSnapshot {
                port: Some(PORT),
                bind_addresses: None,
                tc_address: None,
            }
        })
    }
    fn host_environment(&self) -> crate::host_env::HostEnvironment {
        crate::host_env::HostEnvironment {
            hostname: "host".into(),
            pretty_hostname: "Host Mac".into(),
            device_kind: None,
            hardware_model: None,
        }
    }
    fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }
    fn token_store(&self) -> &AsyncTokenStore {
        &self.token_store
    }
    fn product_name(&self) -> String {
        "Intent Test".into()
    }
}

struct Fixture {
    server: WsApiServer,
    pairing: Arc<intent_services::local_pairing::LocalPairing>,
    _dir: tempfile::TempDir,
}

async fn fixture(enabled: bool) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let store = intent_store::Store::open(&dir.path().join("pairing.db"))
        .await
        .unwrap();
    let pairing = intent_services::local_pairing::LocalPairing::new(store, None).unwrap();
    let bus = EventBus::new(
        intent_store::Store::open(&dir.path().join("bus.db"))
            .await
            .unwrap(),
    );
    let certificate = crate::ensure_tls_certificate(dir.path()).unwrap();
    let tokens = Arc::new(AsyncTokenStore::new(Arc::new(NoTokens)));
    let options = WsOptions {
        base_port: 0,
        enabled,
        ..WsOptions::default()
    };
    let api = Arc::new(Api {
        pairing: pairing.clone(),
    });
    let mut server = WsApiServer::new(api, bus, &certificate, &tokens, options, None).unwrap();
    server.install_pairing_info(Arc::new(Provider {
        data_dir: dir.path().to_path_buf(),
        token_store: AsyncTokenStore::new(Arc::new(NoTokens)),
    }));
    Fixture {
        server,
        pairing,
        _dir: dir,
    }
}

/// Send a `/local-pair` upgrade from `peer` with `extra` header lines;
/// returns the status and, on `101`, the client end of the stream.
async fn upgrade(f: &Fixture, peer: IpAddr, extra: &str) -> (u16, Option<DuplexStream>) {
    let (mut client, server_end) = tokio::io::duplex(64 * 1024);
    let inner = f.server.inner.clone();
    tokio::spawn(async move {
        let _ = inner.handle_conn(server_end, 0, peer).await;
    });
    let request = format!(
        "GET /local-pair HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n{extra}\r\n"
    );
    client.write_all(request.as_bytes()).await.unwrap();
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        if client.read(&mut byte).await.unwrap() == 0 {
            break;
        }
        head.push(byte[0]);
    }
    let head = String::from_utf8(head).unwrap();
    let status: u16 = head
        .split(' ')
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("no status line: {head:?}"));
    (status, (status == 101).then_some(client))
}

const LOOPBACK: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

async fn call(ws: &mut WebSocketStream<DuplexStream>, frame: Value) -> Value {
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .unwrap();
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(text))) => return serde_json::from_str(&text).unwrap(),
            Some(Ok(_)) => {}
            other => panic!("expected a response, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn non_loopback_peers_are_refused_at_upgrade() {
    let f = fixture(true).await;
    for peer in [
        IpAddr::V4(Ipv4Addr::new(192, 168, 1, 20)),
        IpAddr::V4(Ipv4Addr::new(100, 64, 0, 7)),
        IpAddr::V6("fe80::1".parse::<Ipv6Addr>().unwrap()),
    ] {
        assert_eq!(upgrade(&f, peer, "").await.0, 403, "{peer}");
    }
    for peer in [
        LOOPBACK,
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 9)),
        IpAddr::V6(Ipv6Addr::LOCALHOST),
        IpAddr::V6(Ipv4Addr::LOCALHOST.to_ipv6_mapped()),
    ] {
        assert_eq!(upgrade(&f, peer, "").await.0, 101, "{peer}");
    }
}

#[tokio::test]
async fn only_absent_or_loopback_origins_are_admitted() {
    let f = fixture(true).await;
    for origin in [
        "http://localhost",
        "http://localhost:5173",
        "https://127.0.0.1:443",
        "http://[::1]:3000",
    ] {
        let (status, _) = upgrade(&f, LOOPBACK, &format!("Origin: {origin}\r\n")).await;
        assert_eq!(status, 101, "{origin}");
    }
    let hostname = whoami::hostname()
        .ok()
        .filter(|h| !h.eq_ignore_ascii_case("localhost"))
        .unwrap_or_else(|| "my-mac".into());
    for origin in [
        "null".to_string(),
        "file://".to_string(),
        "https://evil.example".to_string(),
        "http://localhost.evil.example".to_string(),
        format!("http://{hostname}"),
        format!("http://{hostname}.local:8080"),
        "ws://localhost".to_string(),
    ] {
        let (status, _) = upgrade(&f, LOOPBACK, &format!("Origin: {origin}\r\n")).await;
        assert_eq!(status, 403, "{origin}");
    }
    // A present but empty Origin is not an absent one.
    assert_eq!(upgrade(&f, LOOPBACK, "Origin: \r\n").await.0, 403);
}

#[tokio::test]
async fn disabled_listener_refuses_and_connections_are_capped() {
    let off = fixture(false).await;
    assert_eq!(upgrade(&off, LOOPBACK, "").await.0, 403);

    let f = fixture(true).await;
    let mut held = Vec::new();
    for _ in 0..MAX_LOCAL_PAIR_CONNECTIONS {
        let (status, stream) = upgrade(&f, LOOPBACK, "").await;
        assert_eq!(status, 101);
        held.push(stream.unwrap());
    }
    assert_eq!(upgrade(&f, LOOPBACK, "").await.0, 503);
    // Never counted as a client (`/health`, `system.status`).
    assert_eq!(f.server.client_count(), 0);
    drop(held);
    let reopened = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if upgrade(&f, LOOPBACK, "").await.0 == 101 {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(reopened.is_ok(), "a closed connection frees its slot");
}

#[tokio::test]
async fn serves_only_redemption_and_returns_the_grant() {
    let f = fixture(true).await;
    let created = f.pairing.create_code().await.unwrap();
    // A bearer token is ignored: it grants nothing and does not refuse.
    let (status, stream) = upgrade(&f, LOOPBACK, "Authorization: Bearer nope\r\n").await;
    assert_eq!(status, 101);
    let mut ws = WebSocketStream::from_raw_socket(stream.unwrap(), Role::Client, None).await;

    for (id, method) in [
        (1, "workspace.list"),
        (2, "client.hello"),
        (3, "events.subscribe"),
        (4, "pairing.createLocalCode"),
        (5, "pairing.listLocalCredentials"),
    ] {
        let refused = call(
            &mut ws,
            json!({"jsonrpc":"2.0","id":id,"method":method,"params":{}}),
        )
        .await;
        assert_eq!(refused["error"]["code"], -32001, "{method}: {refused}");
        assert_eq!(refused["error"]["message"], LOCAL_PAIR_ONLY_MESSAGE);
    }

    let wrong = if created.code == "AAA-AAA" {
        "BBB-BBB"
    } else {
        "AAA-AAA"
    };
    let invalid = call(
        &mut ws,
        json!({"jsonrpc":"2.0","id":6,"method":"pairing.redeemLocalCode",
            "params":{"code":wrong,"appName":"Intent Alpha"}}),
    )
    .await;
    assert_eq!(invalid["error"]["code"], -32602, "{invalid}");
    assert_eq!(invalid["error"]["data"]["code"], "local-code-invalid");
    assert_eq!(invalid["error"]["data"]["attemptsRemaining"], 4);

    let malformed = call(
        &mut ws,
        json!({"jsonrpc":"2.0","id":7,"method":"pairing.redeemLocalCode",
            "params":{"code":"0O1","appName":"Intent Alpha"}}),
    )
    .await;
    assert_eq!(malformed["error"]["data"]["code"], "invalid-params");

    let typed = created.code.to_lowercase().replace('-', " ");
    let grant = call(
        &mut ws,
        json!({"jsonrpc":"2.0","id":"r","method":"pairing.redeemLocalCode",
            "params":{"code":typed,"appName":"Intent Alpha"}}),
    )
    .await;
    let grant = &grant["result"];
    assert_eq!(grant["version"], 1, "{grant}");
    assert_eq!(grant["host"], "127.0.0.1");
    assert_eq!(grant["port"], PORT);
    assert_eq!(
        grant["fingerprint"],
        f.server.fingerprint().unwrap(),
        "the grant pins the listener's own certificate"
    );
    assert_eq!(grant["token"].as_str().unwrap().len(), 64);
    assert_eq!(grant["displayName"], "Intent Test");
    assert_eq!(grant["prettyHostname"], "Host Mac");

    let listed = f.pairing.list_credentials().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].app_name, "Intent Alpha");

    let reused = call(
        &mut ws,
        json!({"jsonrpc":"2.0","id":8,"method":"pairing.redeemLocalCode",
            "params":{"code":created.code,"appName":"Intent Alpha"}}),
    )
    .await;
    assert_eq!(reused["error"]["data"]["code"], "local-code-expired");
}

#[tokio::test]
async fn a_redemption_notification_never_spends_the_code() {
    let f = fixture(true).await;
    let created = f.pairing.create_code().await.unwrap();
    let (_, stream) = upgrade(&f, LOOPBACK, "").await;
    let mut ws = WebSocketStream::from_raw_socket(stream.unwrap(), Role::Client, None).await;
    ws.send(Message::Text(
        json!({"jsonrpc":"2.0","method":"pairing.redeemLocalCode",
            "params":{"code":created.code,"appName":"Intent Alpha"}})
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    let grant = call(
        &mut ws,
        json!({"jsonrpc":"2.0","id":1,"method":"pairing.redeemLocalCode",
            "params":{"code":created.code,"appName":"Intent Alpha"}}),
    )
    .await;
    assert!(grant["result"]["token"].is_string(), "{grant}");
}
