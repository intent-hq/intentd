//! WSS end-to-end for `nextToken` pagination on `sentry.listIssues` /
//! `sentry.searchIssues` (PROTOCOL §5.29): the wire `nextToken` must reach the
//! engine as the raw Sentry page cursor, and the response must carry the
//! paginated `{ issues, nextToken? }` envelope — `nextToken` non-null only
//! when the engine reports another page. Drives a real [`WsApiServer`] over
//! TLS with bearer-token auth and a pinned self-signed fingerprint (the
//! production transport path) with a recording stub engine injected via
//! `with_sentry_engine`.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use intent_core::{Result as CoreResult, WorkspaceApi};
use intent_sentry::{
    FetchIssuesRequest, Result as SentryResult, SentryAuthState, SentryEngine, SentryIssueLevel,
    SentryIssuePage, SentryIssueResult, SentryIssueStatus, SentryProject,
};
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::{
    ensure_tls_certificate, AsyncTokenStore, TokenStore, WsApiServer, WsOptions,
};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

/// A fixed 64-char hex token (valid shape) shared by server + client.
const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

type TlsWs = WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;

struct TempDir(PathBuf);
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// In-memory [`TokenStore`] so tests never touch the real OS keychain.
#[derive(Default)]
struct MemTokenStore(Mutex<Option<String>>);

impl TokenStore for MemTokenStore {
    fn load_token(&self) -> Option<String> {
        self.0.lock().unwrap().clone()
    }
    fn store_token(&self, token: &str) -> CoreResult<()> {
        *self.0.lock().unwrap() = Some(token.to_string());
        Ok(())
    }
}

/// Client cert verifier that pins the server's SHA-256 fingerprint (colon hex)
/// and otherwise validates the handshake signature with the ring provider.
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

fn issue(short_id: &str) -> SentryIssueResult {
    SentryIssueResult {
        id: format!("uuid-{short_id}"),
        short_id: short_id.to_string(),
        title: "boom".into(),
        culprit: None,
        status: SentryIssueStatus::Unresolved,
        level: SentryIssueLevel::Error,
        count: "1".into(),
        user_count: 0,
        first_seen: "2026-01-01T00:00:00Z".into(),
        last_seen: "2026-01-02T00:00:00Z".into(),
        project_name: "Web".into(),
        project_slug: "web".into(),
        url: None,
        r#type: None,
        value: None,
        filename: None,
        function: None,
    }
}

/// A recorded `sentry.searchIssues` engine call:
/// `(query, project, limit, cursor)`.
type SearchCall = (String, Option<String>, Option<u32>, Option<String>);

/// Recording stub engine: captures the typed request / `(query, project,
/// limit, cursor)` each paginated call was made with so tests can assert the
/// wire `nextToken` landed on the engine cursor, and reports a next page
/// (`0:100:0`) only on the first page (no cursor) so the response token
/// round-trips exactly once.
#[derive(Default)]
struct RecordingEngine {
    list_calls: Mutex<Vec<FetchIssuesRequest>>,
    search_calls: Mutex<Vec<SearchCall>>,
}

#[async_trait]
impl SentryEngine for RecordingEngine {
    async fn auth_status(&self) -> SentryResult<SentryAuthState> {
        unimplemented!()
    }

    async fn list_issues(&self, request: FetchIssuesRequest) -> SentryResult<SentryIssuePage> {
        let next_token = match request.cursor {
            None => Some("0:100:0".to_string()),
            Some(_) => None,
        };
        self.list_calls.lock().unwrap().push(request);
        Ok(SentryIssuePage {
            issues: vec![issue("PROJ-1")],
            next_token,
        })
    }

    async fn search_issues(
        &self,
        query: &str,
        project: Option<&str>,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> SentryResult<SentryIssuePage> {
        self.search_calls.lock().unwrap().push((
            query.to_string(),
            project.map(str::to_string),
            limit,
            cursor.map(str::to_string),
        ));
        Ok(SentryIssuePage {
            issues: vec![issue("PROJ-2")],
            next_token: match cursor {
                None => Some("0:100:0".to_string()),
                Some(_) => None,
            },
        })
    }

    async fn list_projects(&self, _: Option<u32>) -> SentryResult<Vec<SentryProject>> {
        unimplemented!()
    }
    async fn get_issue(&self, _: &str) -> SentryResult<SentryIssueResult> {
        unimplemented!()
    }
    async fn resolve_issue(&self, _: &str) -> SentryResult<SentryIssueResult> {
        unimplemented!()
    }
    async fn ignore_issue(&self, _: &str) -> SentryResult<SentryIssueResult> {
        unimplemented!()
    }
    async fn assign_issue(&self, _: &str, _: Option<&str>) -> SentryResult<SentryIssueResult> {
        unimplemented!()
    }
}

struct Fixture {
    _ws: WsApiServer,
    port: u16,
    cfg: Arc<ClientConfig>,
    engine: Arc<RecordingEngine>,
    _dir: TempDir,
}

/// Boot a TLS + bearer-auth WSS listener whose services carry the recording
/// stub engine.
async fn boot() -> Fixture {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = std::env::temp_dir().join(format!("intentd-sentry-page-{}", &short[..8]));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(&dir.join("intentd.db")).await.expect("store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic root");

    let engine = Arc::new(RecordingEngine::default());
    let services = Arc::new(
        Services::new(store)
            .with_workspaces_root(workspaces_root)
            .with_event_bus(bus.clone())
            .with_sentry_engine(engine.clone()),
    );
    let api: Arc<dyn WorkspaceApi> = services;
    let tls = ensure_tls_certificate(&dir).expect("cert");
    let token_store_inner = Arc::new(MemTokenStore::default());
    token_store_inner.store_token(TOKEN).unwrap();
    let token_store = Arc::new(AsyncTokenStore::new(token_store_inner));
    let opts = WsOptions {
        base_port: 0,
        bind_addresses: vec![Ipv4Addr::LOCALHOST.into()],
        ..Default::default()
    };
    let ws_srv = WsApiServer::new(api, bus, &tls, &token_store, opts, None).expect("server");
    let cfg = client_config(&tls.fingerprint256);
    let port = ws_srv.start().await.expect("start");
    Fixture {
        _ws: ws_srv,
        port,
        cfg,
        engine,
        _dir: TempDir(dir),
    }
}

/// Establish an authenticated WSS connection over pinned TLS (token in the
/// query string).
async fn connect(port: u16, cfg: Arc<ClientConfig>) -> TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

async fn wss_rpc(ws: &mut TlsWs, id: i64, method: &str, params: Value) -> Value {
    let req = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(req.to_string().into()))
        .await
        .unwrap();
    timeout(common::rpc_read_timeout(), async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v.get("id") == Some(&json!(id)) {
                        assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
                        return v["result"].clone();
                    }
                }
                Message::Ping(p) => {
                    let _ = ws.send(Message::Pong(p)).await;
                }
                Message::Pong(_) => {}
                _ => panic!("unexpected message"),
            }
        }
    })
    .await
    .expect("response timeout")
}

/// The opaque wire `nextToken` for an engine page cursor: no-pad base64 of
/// `{"c":"<cursor>"}` (mirrors the services-layer §5.5 encoding).
fn wire_next_token(cursor: &str) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD_NO_PAD
        .encode(serde_json::to_vec(&json!({ "c": cursor })).unwrap())
}

/// `sentry.listIssues`: the first page (no `nextToken`) returns the paginated
/// envelope with the engine's cursor wrapped into the opaque wire `nextToken`;
/// passing that token back decodes onto the engine cursor, and the last page
/// carries an explicit `nextToken: null`.
#[tokio::test]
async fn list_issues_next_token_round_trips() {
    let fx = boot().await;
    let mut ws = connect(fx.port, fx.cfg.clone()).await;

    let r = wss_rpc(
        &mut ws,
        1,
        "sentry.listIssues",
        json!({ "project": "web", "limit": 5 }),
    )
    .await;
    assert_eq!(r["issues"][0]["shortId"], "PROJ-1");
    assert_eq!(r["nextToken"], json!(wire_next_token("0:100:0")));

    let r = wss_rpc(
        &mut ws,
        2,
        "sentry.listIssues",
        json!({ "project": "web", "limit": 5, "nextToken": wire_next_token("0:100:0") }),
    )
    .await;
    assert_eq!(r["issues"][0]["shortId"], "PROJ-1");
    assert_eq!(r["nextToken"], json!(null), "last page is nextToken null");

    let calls = fx.engine.list_calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].project.as_deref(), Some("web"));
    assert_eq!(calls[0].limit, Some(5));
    assert_eq!(calls[0].cursor, None);
    assert_eq!(calls[1].project.as_deref(), Some("web"));
    assert_eq!(calls[1].limit, Some(5));
    assert_eq!(calls[1].cursor.as_deref(), Some("0:100:0"));
}

/// `sentry.searchIssues`: same envelope and cursor semantics, with the wire
/// `query` + `project` forwarded alongside the token.
#[tokio::test]
async fn search_issues_next_token_round_trips() {
    let fx = boot().await;
    let mut ws = connect(fx.port, fx.cfg.clone()).await;

    let r = wss_rpc(
        &mut ws,
        1,
        "sentry.searchIssues",
        json!({ "query": "login bug", "project": "web" }),
    )
    .await;
    assert_eq!(r["issues"][0]["shortId"], "PROJ-2");
    assert_eq!(r["nextToken"], json!(wire_next_token("0:100:0")));

    let r = wss_rpc(
        &mut ws,
        2,
        "sentry.searchIssues",
        json!({ "query": "login bug", "project": "web", "nextToken": wire_next_token("0:100:0") }),
    )
    .await;
    assert_eq!(r["issues"][0]["shortId"], "PROJ-2");
    assert_eq!(r["nextToken"], json!(null), "last page is nextToken null");

    let calls = fx.engine.search_calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(
        calls[0],
        ("login bug".to_string(), Some("web".to_string()), None, None)
    );
    assert_eq!(
        calls[1],
        (
            "login bug".to_string(),
            Some("web".to_string()),
            None,
            Some("0:100:0".to_string())
        )
    );
}
