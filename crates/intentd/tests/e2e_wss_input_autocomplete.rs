//! WSS end-to-end for `input.autocomplete` (PROTOCOL §5.59): the settings
//! written over the wire decide whether the daemon asks the upstream, the
//! reply is cleaned before it reaches the client, every upstream failure is
//! a `{ completion: "" }` result, and malformed params reject with `-32602`.
//! Drives a real [`WsApiServer`] over TLS with bearer-token auth and a
//! pinned self-signed fingerprint; the upstream is a local mock server.

#![cfg(unix)]

use crate::common;

use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use intent_core::{Result as CoreResult, WorkspaceApi};
use intent_services::test_support::mock_chat_upstream::{MockChatUpstream, MockReply};
use intent_services::{AutocompleteUpstream, EventBus, Services, SettingsRegistry};
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

/// Establish an authenticated WSS connection over pinned TLS (token in the
/// query string).
async fn connect(port: u16, cfg: Arc<ClientConfig>) -> TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// Send one JSON-RPC request and return the full response envelope.
async fn wss_rpc_raw(ws: &mut TlsWs, id: i64, method: &str, params: Value) -> Value {
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
                        return v;
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

struct Fixture {
    _ws: WsApiServer,
    port: u16,
    cfg: Arc<ClientConfig>,
    kilo: MockChatUpstream,
    _dir: tempfile::TempDir,
}

/// Boot a TLS + bearer-auth WSS listener whose services ask `kilo` (and an
/// unused `OpenRouter` mock) for suggestions, with `upstream_timeout`.
async fn boot(kilo_reply: MockReply, upstream_timeout: Duration) -> Fixture {
    let dir_guard = common::test_tempdir("intentd-autocomplete-");
    let dir = dir_guard.path().to_path_buf();
    let store = Store::open(&dir.join("intentd.db")).await.expect("store");
    let bus = EventBus::new(store.clone());
    let registry =
        Arc::new(SettingsRegistry::load(dir.join("config.toml")).expect("load registry"));
    let kilo = MockChatUpstream::start(kilo_reply).await;
    let services = Arc::new(
        Services::new_isolated(store)
            .with_event_bus(bus.clone())
            .with_settings_registry(registry)
            .with_autocomplete_upstream(AutocompleteUpstream {
                kilo_url: kilo.url(),
                // Unroutable on purpose: these tests never pick OpenRouter.
                openrouter_url: "http://127.0.0.1:9/chat/completions".to_string(),
                timeout: upstream_timeout,
            }),
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
        kilo,
        _dir: dir_guard,
    }
}

fn ok_budget() -> Duration {
    common::test_timeout(Duration::from_secs(5))
}

async fn enable(ws: &mut TlsWs) {
    let resp = wss_rpc_raw(
        ws,
        100,
        "settings.update",
        json!({ "changes": [{ "path": "autocomplete.enabled", "value": true }] }),
    )
    .await;
    assert!(
        resp.get("error").is_none(),
        "settings.update failed: {resp}"
    );
}

/// Enabled on Kilo without a key: the wire request reaches the upstream with
/// the free model id and the documented prompt, and the response envelope is
/// exactly `{ jsonrpc, id, result: { completion } }` with the cleaned reply.
#[intent_test_macros::daemon_test]
async fn autocomplete_round_trips_over_wss() {
    let fx = boot(
        MockReply::content("Fix the failing test. Then ship it."),
        ok_budget(),
    )
    .await;
    let mut ws = connect(fx.port, fx.cfg.clone()).await;
    enable(&mut ws).await;

    let resp = wss_rpc_raw(
        &mut ws,
        1,
        "input.autocomplete",
        json!({ "text": "Fix the ", "context": "assistant: CI is red" }),
    )
    .await;
    assert_eq!(
        resp,
        json!({ "jsonrpc": "2.0", "id": 1, "result": { "completion": "failing test." } })
    );
    let got = fx.kilo.requests();
    assert_eq!(got.len(), 1);
    assert_eq!(
        got[0].body["model"],
        json!("nvidia/nemotron-3.5-lightning:free")
    );
    assert_eq!(got[0].body["reasoning"], json!({ "enabled": false }));
    assert_eq!(
        got[0].body["messages"][1]["content"],
        json!("<conversation>\nassistant: CI is red\n</conversation>\n<typed>Fix the </typed>")
    );
    assert_eq!(got[0].header("authorization"), None);
}

/// Off by default: an empty completion and no upstream request.
#[intent_test_macros::daemon_test]
async fn disabled_answers_empty_without_a_request() {
    let fx = boot(MockReply::content("never"), ok_budget()).await;
    let mut ws = connect(fx.port, fx.cfg.clone()).await;
    let resp = wss_rpc_raw(
        &mut ws,
        1,
        "input.autocomplete",
        json!({ "text": "Fix the ", "context": "" }),
    )
    .await;
    assert_eq!(
        resp,
        json!({ "jsonrpc": "2.0", "id": 1, "result": { "completion": "" } })
    );
    assert!(fx.kilo.requests().is_empty());
}

/// Upstream failures (rate limit, timeout) are results, never errors.
#[intent_test_macros::daemon_test]
async fn upstream_failures_are_empty_results() {
    for (reply, upstream_timeout) in [
        (MockReply::Status(429, "{}".to_string()), ok_budget()),
        (MockReply::Hang, Duration::from_millis(200)),
    ] {
        let fx = boot(reply, upstream_timeout).await;
        let mut ws = connect(fx.port, fx.cfg.clone()).await;
        enable(&mut ws).await;
        let resp = wss_rpc_raw(
            &mut ws,
            1,
            "input.autocomplete",
            json!({ "text": "Fix the ", "context": "" }),
        )
        .await;
        assert_eq!(
            resp,
            json!({ "jsonrpc": "2.0", "id": 1, "result": { "completion": "" } })
        );
        assert_eq!(fx.kilo.requests().len(), 1);
    }
}

/// Bad types or lengths reject with `-32602`.
#[intent_test_macros::daemon_test]
async fn bad_params_reject_with_invalid_params() {
    let fx = boot(MockReply::content("x"), ok_budget()).await;
    let mut ws = connect(fx.port, fx.cfg.clone()).await;
    for (id, params) in [
        json!({}),
        json!({ "text": "hi" }),
        json!({ "text": "", "context": "" }),
        json!({ "text": 3, "context": "" }),
        json!({ "text": "a".repeat(2001), "context": "" }),
        json!({ "text": "hi", "context": "a".repeat(4001) }),
    ]
    .into_iter()
    .enumerate()
    {
        let id = i64::try_from(id).unwrap() + 1;
        let resp = wss_rpc_raw(&mut ws, id, "input.autocomplete", params.clone()).await;
        assert_eq!(resp["error"]["code"], json!(-32602), "{params}: {resp}");
        assert!(resp.get("result").is_none(), "{resp}");
    }
    assert!(fx.kilo.requests().is_empty());
}
