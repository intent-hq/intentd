//! Real loopback TLS/auth/Store witnesses. Synthetic faults are named explicitly.
mod lifecycle;
use super::*;
use intent_core::{now_iso, Principal, PrincipalId};
use intent_core::{Caller, WorkspaceApi};
use intent_services::prepared_source_bootstrap::Counts;
use intent_store::Store;
use serde_json::{json, Value};
use std::sync::{Condvar, Mutex};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

const TOKEN: &str = "abababababababababababababababababababababababababababababababab";
struct Secret;
impl crate::TokenStore for Secret {
    fn load_token(&self) -> Option<String> {
        Some(TOKEN.into())
    }
    fn store_token(&self, _: &str) -> Result<()> {
        Ok(())
    }
}
struct Fixture {
    _dir: tempfile::TempDir,
    store: Store,
    shared: Arc<Shared>,
    server: PreparedBootstrap,
    config: Arc<rustls::ClientConfig>,
}
impl Fixture {
    async fn new() -> Self {
        Self::with_secret(Arc::new(Secret)).await
    }
    async fn with_secret(secret: Arc<dyn crate::TokenStore>) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("test.db")).await.unwrap();
        let api =
            Arc::new(Services::new(store.clone()).with_assets_root(dir.path().join("assets")));
        let cert = crate::ensure_tls_certificate(dir.path()).unwrap();
        let mut roots = rustls::RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut cert.cert.as_bytes()) {
            roots.add(cert.unwrap()).unwrap();
        }
        let config = Arc::new(
            rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth(),
        );
        let shared = Arc::new(Shared {
            root: api.prepared_source_contexts(),
            api,
            tls: crate::ws::build_acceptor(&cert).unwrap(),
            token: AsyncTokenStore::new(secret),
            guests: SharedGuestLimits::new(crate::GuestConnectionLimits {
                max_guest_connections: 1,
                max_connections_per_guest: 1,
            }),
            observe: Mutex::new(Vec::new()),
            hello_pending: tokio::sync::Notify::new(),
            validation_pending: tokio::sync::Notify::new(),
            partial_hello: std::sync::atomic::AtomicBool::new(false),
            control_pending: tokio::sync::Notify::new(),
            source_pending: std::sync::atomic::AtomicUsize::new(0),
            source_partial: std::sync::atomic::AtomicUsize::new(0),
            partial_control: std::sync::atomic::AtomicBool::new(false),
            auth_io: std::sync::Mutex::new(Vec::new()),
            page_auth_gate: std::sync::Mutex::new(None),
            page_auth_pending: tokio::sync::Notify::new(),
        });
        let server = PreparedBootstrap::start_shared(shared.clone())
            .await
            .unwrap();
        Self {
            _dir: dir,
            store,
            shared,
            server,
            config,
        }
    }
    async fn guest(&self, id: &str, token: &str) {
        let now = now_iso();
        self.store
            .upsert_principal(&Principal {
                id: PrincipalId::from_string(id),
                identity: None,
                github_user_id: None,
                login: None,
                display_name: None,
                avatar_url: None,
                is_primary: false,
                created_at: now.clone(),
                updated_at: now,
            })
            .await
            .unwrap();
        self.store
            .insert_principal_credential(&PrincipalId::from_string(id), &crate::hash_token(token))
            .await
            .unwrap();
    }
    async fn socket(
        &self,
        mode: Mode,
        token: &str,
    ) -> std::result::Result<
        WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>,
        tokio_tungstenite::tungstenite::Error,
    > {
        let address = if mode == Mode::Read {
            self.server.endpoints.read
        } else {
            self.server.endpoints.cleanup
        };
        let mut request = format!("wss://localhost:{}/ws", address.port())
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert("Authorization", format!("Bearer {token}").parse().unwrap());
        request
            .headers_mut()
            .insert("Origin", "https://localhost".parse().unwrap());
        let tcp = TcpStream::connect(address).await?;
        let tls = tokio_rustls::TlsConnector::from(self.config.clone())
            .connect(
                rustls_pki_types::ServerName::try_from("localhost").unwrap(),
                tcp,
            )
            .await?;
        tokio_tungstenite::client_async(request, tls)
            .await
            .map(|r| r.0)
    }
    async fn finish(self) {
        self.server.stop().await;
        assert_eq!(self.shared.root.counts(), Counts::default());
        self.store.close().await;
    }
}
fn hello(mode: Mode, id: &Value) -> String {
    json!({"jsonrpc":"2.0","id":id,"method":"client.hello","params":{"clientId":"bootstrap-test","sourceSession":{"version":1,"mode":mode.as_str()},"capabilities":{"desktopControl":1}}}).to_string()
}
async fn condition(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("observable condition");
}

#[test]
fn prepared_bootstrap_baseline_requires_strict_negotiation() {
    let mut frame: Value = serde_json::from_str(&hello(Mode::Read, &json!(1))).unwrap();
    frame["params"]["sourceSession"]["version"] = json!(2);
    assert!(hello::validate(&frame.to_string(), Mode::Read).is_err());
    assert!(hello::validate(&hello(Mode::Cleanup, &json!(1)), Mode::Read).is_err());
    for id in [json!(-9_007_199_254_740_991_i64), json!("\0".repeat(64))] {
        assert!(hello::validate(&hello(Mode::Read, &id), Mode::Read).is_ok());
    }
    for id in [
        Value::Null,
        json!(9_007_199_254_740_992_u64),
        json!("a".repeat(65)),
    ] {
        assert!(hello::validate(&hello(Mode::Read, &id), Mode::Read).is_err());
    }
    assert!(hello::validate(r#"{"jsonrpc":"2.0","jsonrpc":"2.0"}"#, Mode::Read).is_err());
}

#[tokio::test]
async fn prepared_bootstrap_real_tls_auth_exact_hello_and_root_identity() {
    let f = Fixture::new().await;
    assert!(Arc::ptr_eq(
        &f.shared.root,
        &f.shared.api.as_ref().clone().prepared_source_contexts()
    ));
    let replacement = Services::new(f.store.clone()).prepared_source_contexts();
    assert_ne!(replacement.incarnation(), f.shared.root.incarnation());
    for mode in [Mode::Read, Mode::Cleanup] {
        let mut ws = f.socket(mode, TOKEN).await.unwrap();
        let id = json!("\0".repeat(64));
        ws.send(Message::Text(hello(mode, &id).into()))
            .await
            .unwrap();
        let response = ws.next().await.unwrap().unwrap().into_text().unwrap();
        let value: Value = serde_json::from_str(&response).unwrap();
        assert_eq!(value["id"], id);
        assert_eq!(value["result"]["protocolVersion"], crate::PROTOCOL_VERSION);
        assert_eq!(
            value["result"]["server"]["sourceSession"],
            json!({"version":1,"mode":mode.as_str(),"daemonIncarnation":f.shared.root.incarnation()})
        );
        assert!(response.len() <= 8192);
        ws.close(None).await.unwrap();
        drop(ws);
    }
    condition(|| f.shared.observe.lock().unwrap().len() == 2).await;
    for witness in f.shared.observe.lock().unwrap().iter() {
        assert!(witness.ready && witness.closed);
        assert!(witness.read > 0 && witness.read <= 65536);
        assert!(witness.written > 0 && witness.written <= 65536);
        assert!(witness.http_in > 0 && witness.http_in <= 16384 && witness.http_out <= 16384);
        assert!(witness.hello_in > 0 && witness.hello_out <= 8192);
        eprintln!("actual pinned TLS bootstrap witness {witness:?}");
    }
    f.finish().await;
}

#[tokio::test]
async fn prepared_bootstrap_real_guest_quota_and_cleanup_separation() {
    let f = Fixture::new().await;
    f.guest("\u{feff}guest", "personal-token").await;
    let mut read = f.socket(Mode::Read, "personal-token").await.unwrap();
    read.send(Message::Text(hello(Mode::Read, &json!(-1)).into()))
        .await
        .unwrap();
    let r: Value =
        serde_json::from_str(&read.next().await.unwrap().unwrap().into_text().unwrap()).unwrap();
    assert_eq!(r["result"]["clientId"], "\u{feff}guest:bootstrap-test");
    assert!(f.socket(Mode::Read, "personal-token").await.is_err());
    let mut cleanup = f.socket(Mode::Cleanup, "personal-token").await.unwrap();
    cleanup
        .send(Message::Text(hello(Mode::Cleanup, &json!(2)).into()))
        .await
        .unwrap();
    assert!(cleanup.next().await.unwrap().unwrap().is_text());
    // Cleanup never negotiates read mode, including after a successful hello.
    cleanup
        .send(Message::Text(hello(Mode::Read, &json!(3)).into()))
        .await
        .unwrap();
    assert!(!matches!(cleanup.next().await, Some(Ok(Message::Text(_)))));
    drop(cleanup);
    drop(read);
    f.finish().await;
}

#[tokio::test]
async fn prepared_bootstrap_pre_tls_read_saturation_keeps_cleanup_lane() {
    let f = Fixture::new().await;
    let mut sockets = Vec::new();
    for _ in 0..240 {
        sockets.push(TcpStream::connect(f.server.endpoints.read).await.unwrap());
    }
    condition(|| f.shared.root.counts().read == 240).await;
    let mut cleanup = f.socket(Mode::Cleanup, TOKEN).await.unwrap();
    cleanup
        .send(Message::Text(hello(Mode::Cleanup, &json!(1)).into()))
        .await
        .unwrap();
    assert!(cleanup.next().await.unwrap().unwrap().is_text());
    assert_eq!(f.shared.root.counts().read, 240);
    drop(sockets);
    drop(cleanup);
    f.finish().await;
}

#[tokio::test]
async fn prepared_bootstrap_context_capacity_exact_identity_and_abandoned_owner() {
    let f = Fixture::new().await;
    let root = f.shared.root.clone();
    let mut c = root.try_admit(Mode::Read).unwrap();
    assert!(c.bind(&"p".repeat(3692), [1; 16], None));
    c.retire();
    let mut c = root.try_admit(Mode::Cleanup).unwrap();
    assert!(!c.bind(&"p".repeat(3693), [1; 16], None));
    c.retire();
    let c = root.try_admit(Mode::Read).unwrap();
    drop(c);
    assert_eq!(root.counts().uncertain, 1);
    f.server.stop().await;
    assert_eq!(root.counts().read, 1);
    assert_eq!(root.counts().uncertain, 1);
    f.store.close().await;
}

struct HeldSecret {
    entered: tokio::sync::Notify,
    gate: (Mutex<bool>, Condvar),
    fail: bool,
}
impl crate::TokenStore for HeldSecret {
    fn load_token(&self) -> Option<String> {
        self.entered.notify_one();
        let guard = self.gate.0.lock().unwrap();
        let (guard, timed) = self
            .gate
            .1
            .wait_timeout_while(guard, Duration::from_secs(20), |open| !*open)
            .unwrap();
        assert!(!timed.timed_out());
        drop(guard);
        if self.fail {
            None
        } else {
            Some(TOKEN.into())
        }
    }
    fn store_token(&self, _: &str) -> Result<()> {
        Ok(())
    }
}
#[tokio::test]
async fn prepared_bootstrap_cancel_retains_actual_blocking_auth_until_completion() {
    for fail in [false, true] {
        let secret = Arc::new(HeldSecret {
            entered: tokio::sync::Notify::new(),
            gate: (Mutex::new(false), Condvar::new()),
            fail,
        });
        let f = Fixture::with_secret(secret.clone()).await;
        {
            let connection = f.socket(Mode::Read, TOKEN);
            tokio::pin!(connection);
            tokio::select! { ()=secret.entered.notified()=>{}, value=&mut connection=>panic!("auth returned before release: {}",value.is_ok()) }
            f.server.request_stop();
            tokio::task::yield_now().await;
            assert!(f.shared.root.counts().read > 0);
            assert_eq!(f.shared.root.counts().ready, 0);
            *secret.gate.0.lock().unwrap() = true;
            secret.gate.1.notify_all();
            assert!(connection.await.is_err());
        }
        // Borrowed connect future is gone before fixture shutdown.
        f.server.stop().await;
        assert_eq!(f.shared.root.counts(), Counts::default());
        f.store.close().await;
    }
}

#[tokio::test]
async fn prepared_bootstrap_exact_http_boundary_and_hello_byte_boundary() {
    for n in [16384, 16385] {
        let mut head = vec![b'x'; n - 4];
        head.extend_from_slice(b"\r\n\r\n");
        assert_eq!(
            io::read_head(&mut head.as_slice()).await.is_ok(),
            n == 16384
        );
    }
    let basic = hello(Mode::Read, &json!(1));
    let mut frame: Value = serde_json::from_str(&basic).unwrap();
    frame["params"]["capabilities"]["padding"] = json!("");
    let base = frame.to_string().len();
    frame["params"]["capabilities"]["padding"] = json!("x".repeat(8192 - base));
    assert_eq!(frame.to_string().len(), 8192);
    assert!(hello::validate(&frame.to_string(), Mode::Read).is_ok());
    frame["params"]["capabilities"]["padding"] = json!("x".repeat(8193 - base));
    assert!(hello::validate(&frame.to_string(), Mode::Read).is_err());
}

#[tokio::test]
async fn prepared_bootstrap_actual_auth_deadline_revokes_before_late_result() {
    let secret = Arc::new(HeldSecret {
        entered: tokio::sync::Notify::new(),
        gate: (Mutex::new(false), Condvar::new()),
        fail: false,
    });
    let f = Fixture::with_secret(secret.clone()).await;
    {
        let connection = f.socket(Mode::Read, TOKEN);
        tokio::pin!(connection);
        tokio::select! { () = secret.entered.notified() => {}, _ = &mut connection => panic!("held auth unexpectedly returned") }
        // Inject only the eligibility clock; the blocked TokenStore call is real.
        tokio::time::pause();
        tokio::time::advance(Duration::from_millis(10001)).await;
        tokio::task::yield_now().await;
        assert!(f.shared.root.counts().read > 0);
        assert_eq!(f.shared.root.counts().ready, 0);
        *secret.gate.0.lock().unwrap() = true;
        secret.gate.1.notify_all();
        tokio::time::resume();
        assert!(connection.await.is_err());
    }
    f.finish().await;
}

#[tokio::test]
async fn prepared_bootstrap_actual_tls_http_limit_and_auth_rejections() {
    let f = Fixture::new().await;
    for (length, origin, token, success) in [
        (16384, "https://localhost", TOKEN, true),
        (16385, "https://localhost", TOKEN, false),
        (1024, "https://evil.example", TOKEN, false),
        (1024, "https://localhost", "wrong-token", false),
    ] {
        let tcp = TcpStream::connect(f.server.endpoints.read).await.unwrap();
        let mut tls = tokio_rustls::TlsConnector::from(f.config.clone())
            .connect(
                rustls_pki_types::ServerName::try_from("localhost").unwrap(),
                tcp,
            )
            .await
            .unwrap();
        let prefix=format!("GET /ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nAuthorization: Bearer {token}\r\nOrigin: {origin}\r\nX-Padding: ");
        let head = format!("{prefix}{}\r\n\r\n", "x".repeat(length - prefix.len() - 4));
        assert_eq!(head.len(), length);
        tls.write_all(head.as_bytes()).await.unwrap();
        tls.flush().await.unwrap();
        let response = io::read_head(&mut tls).await;
        assert_eq!(
            response.is_ok(),
            success,
            "HTTP length={length}, origin={origin}"
        );
        if let Ok(response) = response {
            assert!(response.starts_with(b"HTTP/1.1 101"));
        }
        drop(tls);
    }
    f.finish().await;
}

#[tokio::test]
async fn prepared_bootstrap_auth_regression_caller_lookup_internal_retains_uncertainty() {
    let f = Fixture::new().await;
    f.store.close().await;
    assert!(f.socket(Mode::Read, TOKEN).await.is_err());
    f.server.stop().await;
    assert_eq!(
        f.shared.root.counts().uncertain,
        1,
        "failed actual caller lookup cannot become settled Forbidden"
    );
}

#[tokio::test]
async fn prepared_bootstrap_auth_regression_ready_principal_revocation_retires_socket() {
    let f = Fixture::new().await;
    f.guest("revoked-guest", "revocable-token").await;
    let mut ws = f.socket(Mode::Read, "revocable-token").await.unwrap();
    ws.send(Message::Text(hello(Mode::Read, &json!(1)).into()))
        .await
        .unwrap();
    assert!(ws.next().await.unwrap().unwrap().is_text());
    intent_core::with_caller(
        Caller::Wire {
            principal_id: PrincipalId::from_string("revoked-guest"),
            host_role: intent_core::HostRole::Guest,
        },
        f.shared.api.principal_revoke_self(),
    )
    .await
    .unwrap();
    let closed = tokio::time::timeout(Duration::from_millis(250), ws.next()).await;
    drop(ws);
    f.finish().await;
    assert!(
        closed.is_ok(),
        "ready context must observe actual committed credential revocation"
    );
}

struct RotatingHeldSecret {
    entered: tokio::sync::Notify,
    gate: (Mutex<bool>, Condvar),
    token: Mutex<String>,
}
impl crate::TokenStore for RotatingHeldSecret {
    fn load_token(&self) -> Option<String> {
        let captured = self.token.lock().unwrap().clone();
        self.entered.notify_one();
        let guard = self.gate.0.lock().unwrap();
        let (_guard, timeout) = self
            .gate
            .1
            .wait_timeout_while(guard, Duration::from_secs(20), |open| !*open)
            .unwrap();
        assert!(!timeout.timed_out());
        Some(captured)
    }
    fn store_token(&self, value: &str) -> Result<()> {
        *self.token.lock().unwrap() = value.into();
        Ok(())
    }
}

#[tokio::test]
async fn prepared_bootstrap_auth_regression_held_load_rotation_never_upgrades() {
    let secret = Arc::new(RotatingHeldSecret {
        entered: tokio::sync::Notify::new(),
        gate: (Mutex::new(false), Condvar::new()),
        token: Mutex::new(TOKEN.into()),
    });
    let f = Fixture::with_secret(secret.clone()).await;
    let accepted;
    {
        let connection = f.socket(Mode::Read, TOKEN);
        tokio::pin!(connection);
        tokio::select! { ()=secret.entered.notified()=>{}, _=&mut connection=>panic!("held load returned") }
        f.shared
            .token
            .store_token("replacement-token")
            .await
            .unwrap();
        *secret.gate.0.lock().unwrap() = true;
        secret.gate.1.notify_all();
        let outcome = connection.await;
        accepted = outcome.is_ok();
        drop(outcome);
    }
    f.finish().await;
    assert!(
        !accepted,
        "captured legacy rotation must prevent late old-token upgrade"
    );
}

#[tokio::test]
async fn prepared_bootstrap_wire_held_persistence_retains_cancelled_and_rotated_outcomes() {
    for rotate in [false, true] {
        for fail in [false, true] {
            let f = Fixture::new().await;
            let mut ws = f.socket(Mode::Read, TOKEN).await.unwrap();
            condition(|| f.shared.root.counts().read == 2).await;
            let held = f.store.write_pool().acquire().await.unwrap();
            ws.send(Message::Text(hello(Mode::Read, &json!(-1)).into()))
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(5), f.shared.hello_pending.notified())
                .await
                .unwrap();
            if rotate {
                f.shared
                    .token
                    .store_token("rotated-during-persistence")
                    .await
                    .unwrap();
            } else {
                f.server.request_stop();
            }
            // Peer observes closure, but the actual Store write is still pool-blocked.
            assert!(!matches!(ws.next().await, Some(Ok(Message::Text(_)))));
            // Rotation retains both the active owner and pending accept. Stop retires
            // only the pending accept; this exact remaining cell owns persistence.
            let retained = if rotate { 2 } else { 1 };
            condition(|| f.shared.root.counts().read == retained).await;
            assert_eq!(f.shared.root.counts().read, retained);
            assert_eq!(f.shared.root.counts().ready, 0);
            assert!(f.shared.observe.lock().unwrap().is_empty());
            if fail {
                // Closing the real pool causes the waiting SQL acquisition to fail.
                let closed = f.store.write_pool().close();
                tokio::pin!(closed);
                assert!(futures_util::poll!(&mut closed).is_pending());
                drop(held);
                closed.await;
            } else {
                drop(held);
            }
            condition(|| !f.shared.observe.lock().unwrap().is_empty()).await;
            assert!(f
                .shared
                .observe
                .lock()
                .unwrap()
                .iter()
                .all(|o| !o.ready && o.closed));
            drop(ws);
            f.server.stop().await;
            assert_eq!(f.shared.root.counts().uncertain, usize::from(fail));
            if !fail {
                assert_eq!(f.shared.root.counts(), Counts::default());
            }
            f.store.close().await;
        }
    }
}

#[tokio::test]
async fn prepared_bootstrap_wire_hello_exact_limit_and_overlimit() {
    let f = Fixture::new().await;
    for size in [8192, 8193] {
        let mut ws = f.socket(Mode::Read, TOKEN).await.unwrap();
        let mut frame: Value =
            serde_json::from_str(&hello(Mode::Read, &json!("\u{0001}".repeat(64)))).unwrap();
        frame["params"]["capabilities"]["padding"] = json!("");
        let base = frame.to_string().len();
        frame["params"]["capabilities"]["padding"] = json!("x".repeat(size - base));
        let raw = frame.to_string();
        assert_eq!(raw.len(), size);
        ws.send(Message::Text(raw.into())).await.unwrap();
        let outcome = ws.next().await;
        if size == 8192 {
            let response = outcome.unwrap().unwrap().into_text().unwrap();
            assert!(response.len() <= 8192);
            let response: Value = serde_json::from_str(&response).unwrap();
            assert_eq!(response["id"], frame["id"]);
            assert_eq!(
                response["result"]["server"]["sourceSession"]["mode"],
                "read"
            );
        } else {
            assert!(!matches!(outcome, Some(Ok(Message::Text(_)))));
        }
        drop(ws);
    }
    f.finish().await;
}

#[tokio::test]
async fn prepared_bootstrap_wire_cipher_counted_tcp_exact_each_direction() {
    use tokio::io::AsyncReadExt as _;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let connect = TcpStream::connect(listener.local_addr().unwrap());
    let (client, server) = tokio::join!(connect, listener.accept());
    let mut client = client.unwrap();
    let meter = Arc::new(Meter::default());
    let mut server = CountedTcp::new(server.unwrap().0, meter.clone());
    let bytes = vec![23_u8; 65537];
    let mut received = vec![0_u8; 65536];
    let (sent, got) = tokio::join!(client.write_all(&bytes), server.read_exact(&mut received));
    sent.unwrap();
    got.unwrap();
    assert_eq!(received, bytes[..65536]);
    assert!(server.read_u8().await.is_err());
    assert_eq!(meter.read.load(Ordering::Acquire), 65536);
    let (sent, got) = tokio::join!(server.write_all(&bytes), client.read_exact(&mut received));
    assert!(sent.is_err());
    got.unwrap();
    assert_eq!(received, bytes[..65536]);
    assert_eq!(meter.written.load(Ordering::Acquire), 65536);
    assert!(!meter.closed.load(Ordering::Acquire));
    drop(server);
    assert!(meter.closed.load(Ordering::Acquire));
    match client.read(&mut [0_u8; 1]).await {
        Ok(0) => {}
        Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
        other => panic!("closed local socket delivered unexpected bytes: {other:?}"),
    }
    // Real TCP wrapper boundary; these bytes are synthetic, not valid TLS records.
}

#[tokio::test]
async fn prepared_bootstrap_wire_scoped_output_exact_envelope_and_principal_capacity() {
    let f = Fixture::new().await;
    f.guest("probe", "probe-token").await;
    let raw = hello(Mode::Read, &json!("\u{0001}".repeat(64)));
    let mut probe = f.socket(Mode::Read, "probe-token").await.unwrap();
    probe.send(Message::Text(raw.clone().into())).await.unwrap();
    let frame = probe.next().await.unwrap().unwrap().into_text().unwrap();
    let overhead = frame.len() - "probe".len();
    drop(probe);
    condition(|| f.shared.observe.lock().unwrap().len() == 1).await;
    for (i, size) in [8192, 8193].into_iter().enumerate() {
        let encoded = size - overhead;
        let principal = format!(
            "{}{}",
            "\u{0001}".repeat(encoded / 6),
            "x".repeat(encoded % 6)
        );
        assert!(principal.len() <= 3692);
        let credential = format!("output-boundary-{i}");
        f.guest(&principal, &credential).await;
        let mut ws = f.socket(Mode::Read, &credential).await.unwrap();
        ws.send(Message::Text(raw.clone().into())).await.unwrap();
        let result = ws.next().await;
        if size == 8192 {
            let response = result.unwrap().unwrap().into_text().unwrap();
            assert_eq!(response.len(), 8192);
            let value: Value = serde_json::from_str(&response).unwrap();
            assert_eq!(
                value["result"]["clientId"],
                format!("{principal}:bootstrap-test")
            );
            assert_eq!(value["id"], "\u{0001}".repeat(64));
        } else {
            assert!(!matches!(result, Some(Ok(Message::Text(_)))));
        }
        drop(ws);
        condition(|| f.shared.observe.lock().unwrap().len() == i + 2).await;
    }
    let oversized = format!("\u{feff}{}", "p".repeat(3690));
    assert_eq!(oversized.len(), 3693);
    f.guest(&oversized, "large-principal").await;
    // Actual credential resolution succeeds; this composition cannot retain its identity.
    assert!(crate::auth::prepared_validate_token(
        &f.shared.token,
        f.shared.api.as_ref(),
        "large-principal"
    )
    .await
    .unwrap()
    .is_some());
    assert!(f.socket(Mode::Cleanup, "large-principal").await.is_err());
    f.finish().await;
}

struct PanickingSecret;
impl crate::TokenStore for PanickingSecret {
    fn load_token(&self) -> Option<String> {
        panic!("injected blocking credential panic")
    }
    fn store_token(&self, _: &str) -> Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn prepared_bootstrap_wire_aborted_supervisor_and_panicked_auth_keep_root_debt() {
    let secret = Arc::new(HeldSecret {
        entered: tokio::sync::Notify::new(),
        gate: (Mutex::new(false), Condvar::new()),
        fail: false,
    });
    let f = Fixture::with_secret(secret.clone()).await;
    {
        let connection = f.socket(Mode::Read, TOKEN);
        tokio::pin!(connection);
        tokio::select! { ()=secret.entered.notified()=>{}, _=&mut connection=>panic!("held auth returned") }
        f.server.tasks[0].abort(); // Inject accept-loop loss, which aborts its owned JoinSet.
        assert!(connection.await.is_err());
        condition(|| f.shared.root.counts().uncertain > 0).await;
        let retained = f.shared.root.counts().uncertain;
        assert!(retained <= 2); // Accepted auth plus at most one pending accept.
        *secret.gate.0.lock().unwrap() = true;
        secret.gate.1.notify_all();
    }
    f.server.stop().await;
    let debt = f.shared.root.counts();
    assert!(debt.uncertain > 0 && debt.ready == 0);
    let replacement = PreparedBootstrap::start_shared(f.shared.clone())
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&replacement.root, &f.shared.root));
    replacement.stop().await;
    assert_eq!(f.shared.root.counts(), debt);
    f.store.close().await;

    let f = Fixture::with_secret(Arc::new(PanickingSecret)).await;
    assert!(f.socket(Mode::Read, TOKEN).await.is_err());
    f.server.stop().await;
    assert_eq!(f.shared.root.counts().uncertain, 1);
    f.store.close().await;
    // Neither injected task loss nor a caught auth JoinError proves dependent retirement.
}

#[tokio::test]
async fn prepared_bootstrap_owned_revalidation_wait_survives_rotation_cancel_and_errors() {
    for rotate in [false, true] {
        for fail in [false, true] {
            let f = Fixture::new().await;
            let mut ws = f.socket(Mode::Read, TOKEN).await.unwrap();
            condition(|| f.shared.root.counts().read == 2).await;
            // Exhaust the real read pool AFTER initial authentication. Hello writes
            // succeed independently; final primary-principal validation must wait.
            let mut held = Vec::new();
            for _ in 0..32 {
                held.push(f.store.read_pool().acquire().await.unwrap());
            }
            ws.send(Message::Text(hello(Mode::Read, &json!(1)).into()))
                .await
                .unwrap();
            tokio::time::timeout(
                Duration::from_secs(5),
                f.shared.validation_pending.notified(),
            )
            .await
            .unwrap();
            if rotate {
                f.shared
                    .token
                    .store_token("rotated-during-final-validation")
                    .await
                    .unwrap();
            } else {
                f.server.request_stop();
            }
            assert!(!matches!(ws.next().await, Some(Ok(Message::Text(_)))));
            let retained = if rotate { 2 } else { 1 };
            condition(|| f.shared.root.counts().read == retained).await;
            assert_eq!(f.shared.root.counts().ready, 0);
            assert!(f.shared.observe.lock().unwrap().is_empty());
            if fail {
                let closed = f.store.read_pool().close();
                tokio::pin!(closed);
                assert!(futures_util::poll!(&mut closed).is_pending());
                drop(held);
                closed.await;
            } else {
                drop(held);
            }
            condition(|| !f.shared.observe.lock().unwrap().is_empty()).await;
            drop(ws);
            f.server.stop().await;
            assert_eq!(f.shared.root.counts().uncertain, usize::from(fail));
            if !fail {
                assert_eq!(f.shared.root.counts(), Counts::default());
            }
            f.store.close().await;
        }
    }
}

#[tokio::test]
async fn prepared_bootstrap_owned_partial_tls_hello_write_never_becomes_ready() {
    let f = Fixture::new().await;
    f.shared.partial_hello.store(true, Ordering::Release);
    let mut ws = f.socket(Mode::Read, TOKEN).await.unwrap();
    ws.send(Message::Text(hello(Mode::Read, &json!(1)).into()))
        .await
        .unwrap();
    assert!(!matches!(ws.next().await, Some(Ok(Message::Text(_)))));
    condition(|| !f.shared.observe.lock().unwrap().is_empty()).await;
    {
        let observations = f.shared.observe.lock().unwrap();
        let o = &observations[0];
        assert!(o.fail_write_at > 8);
        assert_eq!(o.written, o.fail_write_at);
        assert!(!o.ready && o.closed && o.hello_out > 0);
        eprintln!("injected continuation failure after eight actual TLS hello bytes: {o:?}");
    }
    drop(ws);
    f.finish().await;
}

#[test]
fn prepared_bootstrap_capability_complete_raw_depth_boundary() {
    for depth in [30, 31, 32] {
        let mut value = json!("opaque\0value");
        for _ in 0..depth {
            value = json!({"nested":value});
        }
        let mut frame: Value = serde_json::from_str(&hello(Mode::Read, &json!(-1))).unwrap();
        frame["params"]["capabilities"] = value;
        eprintln!(
            "BOOTSTRAP_RAW_DEPTH {}",
            json!({"raw":frame.to_string(),"relativeLeafDepth":depth,"totalContainerDepth":depth+2,"expectedAccepted":depth==30})
        );
        assert_eq!(
            hello::validate(&frame.to_string(), Mode::Read).is_ok(),
            depth == 30,
            "relative capabilities depth {depth}"
        );
    }
}

#[tokio::test]
async fn prepared_bootstrap_capability_wire_witness_preserves_opaque_depth_and_versions() {
    let f = Fixture::new().await;
    for mode in [Mode::Read, Mode::Cleanup] {
        for depth in [30, 31, 32] {
            let mut ws = f.socket(mode, TOKEN).await.unwrap();
            let mut frame: Value =
                serde_json::from_str(&hello(mode, &json!("\0".repeat(64)))).unwrap();
            let mut capability = json!("opaque\0value");
            for _ in 0..depth {
                capability = json!({"nested":capability});
            }
            frame["params"]["capabilities"] = capability;
            let request_raw = frame.to_string();
            ws.send(Message::Text(request_raw.clone().into()))
                .await
                .unwrap();
            let response = ws.next().await;
            if depth == 30 {
                let response_raw = response.unwrap().unwrap().into_text().unwrap();
                let value: Value = serde_json::from_str(&response_raw).unwrap();
                assert_eq!(value["id"], frame["id"]);
                assert_eq!(value["result"]["protocolVersion"], crate::PROTOCOL_VERSION);
                assert_eq!(
                    value["result"]["server"]["sourceSession"],
                    json!({"version":1,"mode":mode.as_str(),"daemonIncarnation":f.shared.root.incarnation()})
                );
                eprintln!(
                    "BOOTSTRAP_WIRE_CAPTURE {}",
                    json!({"requestRaw":request_raw,"responseRaw":response_raw.as_str(),"requestBytes":request_raw.len(),"responseBytes":response_raw.len(),"mode":mode.as_str(),"authority":"actual isolated TLS + credential + Services Store; bootstrap only"})
                );
            } else {
                assert!(!matches!(response, Some(Ok(Message::Text(_)))));
            }
            drop(ws);
        }
    }
    f.finish().await;
}
