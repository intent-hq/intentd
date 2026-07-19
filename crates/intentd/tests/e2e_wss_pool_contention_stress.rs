//! Stress test: concurrent writes must not starve reads (pool-contention fix).
//!
//! Drives 30+ concurrent note writes over WSS and asserts that a lightweight
//! read RPC (`system.status`) issued mid-load responds within a small bound
//! (< 2s), proving the single-writer/read pool split (fix/sqlite-pool-contention)
//! prevents pool exhaustion and `database is locked` errors.

use std::net::Ipv4Addr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use intent_core::{Result as CoreResult, WorkspaceApi};
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::{
    ensure_tls_certificate, AsyncTokenStore, TokenStore, WsApiServer, WsOptions,
};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::Message;

const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

#[derive(Default)]
struct MemTokenStore(std::sync::Mutex<Option<String>>);

impl TokenStore for MemTokenStore {
    fn load_token(&self) -> Option<String> {
        self.0.lock().unwrap().clone()
    }
    fn store_token(&self, token: &str) -> CoreResult<()> {
        *self.0.lock().unwrap() = Some(token.to_string());
        Ok(())
    }
}

#[derive(Debug)]
struct PinnedVerifier {
    fingerprint: String,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for PinnedVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let fp = Sha256::digest(end_entity.as_ref())
            .iter()
            .map(|b| format!("{b:02X}"))
            .collect::<Vec<_>>()
            .join(":");
        if fp == self.fingerprint {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General("fingerprint mismatch".into()))
        }
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn client_config(fingerprint: &str) -> Arc<ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .unwrap()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedVerifier {
            fingerprint: fingerprint.to_string(),
            provider,
        }))
        .with_no_client_auth();
    Arc::new(config)
}

async fn make_services() -> (Arc<dyn WorkspaceApi>, EventBus, Store, std::path::PathBuf) {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = Path::new("/tmp").join(format!("intentd-wss-stress-{}", &short[..8]));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(&dir.join("intentd.db"))
        .await
        .expect("open store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic workspaces root");
    let services = Services::new(store.clone())
        .with_assets_root(dir.join("assets"))
        .with_workspaces_root(workspaces_root);
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);
    (api, bus, store, dir)
}

struct Server {
    ws: WsApiServer,
    port: u16,
    cfg: Arc<ClientConfig>,
    _dir: std::path::PathBuf,
}

async fn start() -> Server {
    let (api, bus, _store, dir) = make_services().await;
    let tls = ensure_tls_certificate(&dir).expect("cert");
    let token_store_inner = Arc::new(MemTokenStore::default());
    token_store_inner.store_token(TOKEN).unwrap();
    let token_store = Arc::new(AsyncTokenStore::new(token_store_inner));
    let mut opts = WsOptions::default();
    opts.base_port = 0;
    opts.bind_address = Ipv4Addr::LOCALHOST.into();
    let ws =
        WsApiServer::new(api.clone(), bus.clone(), &tls, token_store, opts, None).expect("server");
    let cfg = client_config(&tls.fingerprint256);
    let port = ws.start().await.expect("start");
    Server {
        ws,
        port,
        cfg,
        _dir: dir,
    }
}

async fn tls_connect(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> tokio_rustls::client::TlsStream<TcpStream> {
    let tcp = TcpStream::connect((Ipv4Addr::LOCALHOST, port))
        .await
        .expect("tcp connect");
    let name = ServerName::try_from("localhost").unwrap();
    TlsConnector::from(cfg)
        .connect(name, tcp)
        .await
        .expect("tls connect")
}

async fn connect_ws(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> tokio_tungstenite::WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let tls = tls_connect(port, cfg).await;
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    let (ws, _resp) = tokio_tungstenite::client_async(url, tls)
        .await
        .expect("ws handshake");
    ws
}

async fn wss_call(port: u16, cfg: Arc<ClientConfig>, frame: &str) -> Value {
    let mut ws = connect_ws(port, cfg).await;
    ws.send(Message::Text(frame.to_string()))
        .await
        .expect("send");
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(text))) => return serde_json::from_str(&text).expect("json"),
            Some(Ok(_)) => continue,
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// 30 concurrent note writes + read RPC mid-load must respond within 2s.
/// Proves the single-writer/read pool split prevents pool exhaustion and
/// `database is locked` errors under heavy concurrent write load.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_writes_do_not_starve_reads() {
    let srv = start().await;

    // Create a workspace + 30 notes.
    let created = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"Stress WS"}}"#,
    )
    .await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let mut note_ids = Vec::new();
    for i in 0..30 {
        let resp = wss_call(
            srv.port,
            srv.cfg.clone(),
            &format!(
                r#"{{"jsonrpc":"2.0","id":{},"method":"note.create","params":{{"workspaceId":"{}","title":"Note {}","content":"initial"}}}}"#,
                i + 2,
                ws_id,
                i
            ),
        )
        .await;
        let note_id = resp["result"]["note"]["id"]
            .as_str()
            .expect("note id")
            .to_string();
        note_ids.push(note_id);
    }

    // Spawn 30 concurrent note write tasks.
    let mut write_tasks = Vec::new();
    for (i, note_id) in note_ids.iter().enumerate() {
        let port = srv.port;
        let cfg = srv.cfg.clone();
        let ws_id = ws_id.clone();
        let note_id = note_id.clone();
        let handle = tokio::spawn(async move {
            let frame = format!(
                r#"{{"jsonrpc":"2.0","id":{},"method":"note.setContent","params":{{"workspaceId":"{}","noteId":"{}","content":"edit {i}","confirmReplacement":true}}}}"#,
                100 + i,
                ws_id,
                note_id,
            );
            wss_call(port, cfg, &frame).await
        });
        write_tasks.push(handle);
    }

    // Issue a lightweight read RPC mid-load (workspace.list).
    tokio::time::sleep(Duration::from_millis(50)).await;
    let start = Instant::now();
    let list_resp = wss_call(
        srv.port,
        srv.cfg.clone(),
        r#"{"jsonrpc":"2.0","id":999,"method":"workspace.list"}"#,
    )
    .await;
    let elapsed = start.elapsed();

    // Assert: read responds within 2s (< 2s proves no pool exhaustion).
    assert!(
        elapsed < Duration::from_secs(2),
        "workspace.list took {elapsed:?} — read pool is blocked by writers"
    );
    assert_eq!(list_resp["id"], 999);
    assert!(
        list_resp.get("result").is_some(),
        "workspace.list must succeed: {list_resp}"
    );
    assert!(
        list_resp["result"]["workspaces"].is_array(),
        "workspace.list must return workspaces array: {list_resp}"
    );

    // Assert: no write failures.
    for (i, task) in write_tasks.into_iter().enumerate() {
        let resp = task.await.expect("write task panicked");
        assert!(
            resp.get("result").is_some() && resp.get("error").is_none(),
            "note write {i} failed: {resp}"
        );
    }

    srv.ws.stop().await;
}
