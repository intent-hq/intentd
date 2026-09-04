//! WSS end-to-end for `voice.transcribe`: the wire request (base64 audio +
//! optional context) must reach the injected [`VoiceEngine`] with the merged
//! keyterm vocabulary / composed prompt, and the response must carry
//! `{ text, provider, durationMs? }`. Missing/invalid `audio` rejects with
//! `-32602`. Drives a real [`WsApiServer`] over TLS with bearer-token auth
//! and a pinned self-signed fingerprint (the production transport path) with
//! a recording stub engine injected via `with_voice_engine`.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use base64::Engine as _;
use futures_util::{SinkExt, StreamExt};
use intent_core::{
    now_iso, Result as CoreResult, Workspace, WorkspaceActivity, WorkspaceApi, WorkspaceAttention,
    WorkspaceId, WorkspaceStatus,
};
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::{
    ensure_tls_certificate, AsyncTokenStore, TokenStore, WsApiServer, WsOptions,
};
use intent_voice::{Result as VoiceResult, TranscribeRequest, Transcript, VoiceEngine};
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

/// Recording stub engine: captures each [`TranscribeRequest`] so tests can
/// assert the wire params landed on the engine (decoded audio, merged
/// keyterms, composed prompt) and returns a fixed transcript.
#[derive(Default)]
struct RecordingEngine {
    calls: Mutex<Vec<TranscribeRequest>>,
}

#[async_trait]
impl VoiceEngine for RecordingEngine {
    async fn transcribe(&self, request: TranscribeRequest) -> VoiceResult<Transcript> {
        self.calls.lock().unwrap().push(request);
        Ok(Transcript {
            text: "ship the release".to_string(),
            duration_ms: Some(1500),
        })
    }

    fn provider_name(&self) -> &'static str {
        "elevenlabs"
    }
}

/// Stub engine whose `transcribe` fails with the registry's no-API-key error
/// shape. The real no-key path fails in `VoiceRegistry::from_settings` (which
/// reads the user's secrets store / env, so it cannot be forced hermetically
/// in an e2e); both surfaces route through the same `map_voice_err` →
/// `VoiceNotConfigured` mapping, so this drives the identical wire path.
struct NoKeyEngine;

#[async_trait]
impl VoiceEngine for NoKeyEngine {
    async fn transcribe(&self, _request: TranscribeRequest) -> VoiceResult<Transcript> {
        Err(intent_voice::Error::NotConfigured(
            "voice: no API key found for elevenlabs \
             (set voice.elevenlabs.apiKey or ELEVENLABS_API_KEY)"
                .to_string(),
        ))
    }

    fn provider_name(&self) -> &'static str {
        "elevenlabs"
    }
}

struct Fixture {
    _ws: WsApiServer,
    port: u16,
    cfg: Arc<ClientConfig>,
    engine: Arc<RecordingEngine>,
    store: Store,
    workspaces_root: PathBuf,
    _dir: TempDir,
}

/// Boot a TLS + bearer-auth WSS listener whose services carry `engine`.
async fn boot_with_engine(
    engine: Arc<dyn VoiceEngine>,
) -> (WsApiServer, u16, Arc<ClientConfig>, Store, PathBuf, TempDir) {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = std::env::temp_dir().join(format!("intentd-voice-{}", &short[..8]));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(&dir.join("intentd.db")).await.expect("store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic root");

    let services = Arc::new(
        Services::new(store.clone())
            .with_workspaces_root(workspaces_root.clone())
            .with_event_bus(bus.clone())
            .with_voice_engine(engine),
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
    (ws_srv, port, cfg, store, workspaces_root, TempDir(dir))
}

/// Boot a TLS + bearer-auth WSS listener whose services carry the recording
/// stub engine.
async fn boot() -> Fixture {
    let engine = Arc::new(RecordingEngine::default());
    let (ws_srv, port, cfg, store, workspaces_root, dir) = boot_with_engine(engine.clone()).await;
    Fixture {
        _ws: ws_srv,
        port,
        cfg,
        engine,
        store,
        workspaces_root,
        _dir: dir,
    }
}

/// Seed a workspace row whose worktree points at
/// `<workspaces_root>/<id>/repo` containing a `README.md` with `readme`, and
/// return its id.
async fn seed_vocab_workspace(fx: &Fixture, readme: &str) -> WorkspaceId {
    let ts = now_iso();
    let id = WorkspaceId::new();
    let checkout = fx.workspaces_root.join(id.as_str()).join("repo");
    std::fs::create_dir_all(&checkout).expect("mkdir checkout");
    std::fs::write(checkout.join("README.md"), readme).expect("write README");
    let ws = Workspace {
        id: id.clone(),
        title: "Vocab".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: ts.clone(),
        updated_at: ts,
        last_activity: None,
        tags: vec![],
        path: None,
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: Some(checkout.to_string_lossy().into_owned()),
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        active_pull_request: None,
        pull_requests: None,
        context_links: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        display_status: None,
        waiting: false,
        token_usage: None,
        cow_supported: None,
        checkout_mode: None,
        execution_environment: None,
        disk_usage: None,
        pending_delete_at: None,
    };
    fx.store.insert_workspace(&ws).await.expect("seed ws");
    id
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

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// `voice.transcribe`: the wire request reaches the engine with the decoded
/// audio and merged context, and the response carries the documented
/// `{ text, provider, durationMs }` result.
#[tokio::test]
async fn transcribe_round_trips_over_wss() {
    let fx = boot().await;
    let mut ws = connect(fx.port, fx.cfg.clone()).await;

    let resp = wss_rpc_raw(
        &mut ws,
        1,
        "voice.transcribe",
        json!({
            "audio": b64(b"opus-bytes"),
            "mimeType": "audio/webm",
            "language": "en",
            "context": { "prompt": "Release planning.", "keyterms": ["Endara"] },
        }),
    )
    .await;
    assert!(resp.get("error").is_none(), "unexpected error: {resp}");
    let result = &resp["result"];
    assert_eq!(result["text"], "ship the release");
    assert_eq!(result["provider"], "elevenlabs");
    assert_eq!(result["durationMs"], 1500);

    let calls = fx.engine.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    let req = &calls[0];
    assert_eq!(req.audio, b"opus-bytes");
    assert_eq!(req.mime_type, "audio/webm");
    assert_eq!(req.language.as_deref(), Some("en"));
    assert!(
        req.keyterms.contains(&"Intent".to_string()),
        "default voice.vocabulary merged in: {:?}",
        req.keyterms
    );
    assert!(
        req.keyterms.contains(&"Endara".to_string()),
        "request keyterms merged in: {:?}",
        req.keyterms
    );
    let prompt = req.prompt.as_deref().unwrap();
    assert!(prompt.contains("Vocabulary:"), "composed prompt: {prompt}");
    assert!(
        prompt.ends_with("Release planning."),
        "request prompt appended: {prompt}"
    );
}

/// `context.keyterms` carrying ElevenLabs-rejected characters reach the
/// engine sanitized on the `keyterms` field only — the composed `OpenAI`
/// `prompt` keeps the unsanitized spellings (PROTOCOL §5.41).
#[tokio::test]
async fn keyterms_sanitized_for_elevenlabs_prompt_keeps_unsanitized_spellings() {
    let fx = boot().await;
    let mut ws = connect(fx.port, fx.cfg.clone()).await;

    let resp = wss_rpc_raw(
        &mut ws,
        1,
        "voice.transcribe",
        json!({
            "audio": b64(b"opus-bytes"),
            "context": { "keyterms": ["[fix] task", "C:\\src", "Endara"] },
        }),
    )
    .await;
    assert!(resp.get("error").is_none(), "unexpected error: {resp}");

    let calls = fx.engine.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    let req = &calls[0];
    for sanitized in ["fix task", "C:src", "Endara"] {
        assert!(
            req.keyterms.contains(&sanitized.to_string()),
            "keyterms carry the ElevenLabs-sanitized spellings: {:?}",
            req.keyterms
        );
    }
    assert!(
        !req.keyterms.iter().any(|t| t.contains(['[', ']', '\\'])),
        "no rejected characters reach the keyterms field: {:?}",
        req.keyterms
    );
    let prompt = req.prompt.as_deref().unwrap();
    assert!(
        prompt.contains("[fix] task") && prompt.contains("C:\\src"),
        "OpenAI prompt keeps the unsanitized spellings: {prompt}"
    );
}

/// Missing / empty / invalid-base64 `audio` rejects with `-32602`, and the
/// engine is never called.
#[tokio::test]
async fn invalid_audio_rejects_with_invalid_params() {
    let fx = boot().await;
    let mut ws = connect(fx.port, fx.cfg.clone()).await;

    let missing = wss_rpc_raw(&mut ws, 1, "voice.transcribe", json!({})).await;
    assert_eq!(missing["error"]["code"], -32602, "{missing}");

    let not_b64 = wss_rpc_raw(
        &mut ws,
        2,
        "voice.transcribe",
        json!({ "audio": "!!not-base64!!" }),
    )
    .await;
    assert_eq!(not_b64["error"]["code"], -32602, "{not_b64}");

    let bad_provider = wss_rpc_raw(
        &mut ws,
        3,
        "voice.transcribe",
        json!({ "audio": b64(b"x"), "provider": "whisper" }),
    )
    .await;
    assert_eq!(bad_provider["error"]["code"], -32602, "{bad_provider}");

    assert!(
        fx.engine.calls.lock().unwrap().is_empty(),
        "engine never called on invalid params"
    );
}

/// Language resolution order (PROTOCOL §5.41): per-call `language` >
/// `voice.language` setting > none (provider auto-detection). Drives
/// `settings.update` over the same WSS connection to set the fallback and
/// asserts what actually lands on the engine at each step.
#[tokio::test]
async fn language_falls_back_to_voice_language_setting() {
    let fx = boot().await;
    let mut ws = connect(fx.port, fx.cfg.clone()).await;

    // 1. No per-call language, no setting → auto-detect (None).
    let resp = wss_rpc_raw(
        &mut ws,
        1,
        "voice.transcribe",
        json!({ "audio": b64(b"a") }),
    )
    .await;
    assert!(resp.get("error").is_none(), "unexpected error: {resp}");

    // 2. Set voice.language = "de" → the setting fills the gap.
    let upd = wss_rpc_raw(
        &mut ws,
        2,
        "settings.update",
        json!({ "changes": [{ "path": "voice.language", "value": "de" }] }),
    )
    .await;
    assert!(upd.get("error").is_none(), "unexpected error: {upd}");
    let resp = wss_rpc_raw(
        &mut ws,
        3,
        "voice.transcribe",
        json!({ "audio": b64(b"b") }),
    )
    .await;
    assert!(resp.get("error").is_none(), "unexpected error: {resp}");

    // 3. Per-call language wins over the setting.
    let resp = wss_rpc_raw(
        &mut ws,
        4,
        "voice.transcribe",
        json!({ "audio": b64(b"c"), "language": "en" }),
    )
    .await;
    assert!(resp.get("error").is_none(), "unexpected error: {resp}");

    let calls = fx.engine.calls.lock().unwrap();
    assert_eq!(calls.len(), 3);
    assert_eq!(
        calls[0].language, None,
        "no language anywhere → auto-detect"
    );
    assert_eq!(
        calls[1].language.as_deref(),
        Some("de"),
        "voice.language setting fills a missing per-call language"
    );
    assert_eq!(
        calls[2].language.as_deref(),
        Some("en"),
        "per-call language wins over the setting"
    );
}

/// The injected engine is used regardless of the `provider` override (the
/// injected handle wins, mirroring the linear/sentry test wiring), and the
/// response `provider` reflects the engine that ran.
#[tokio::test]
async fn provider_override_still_uses_injected_engine() {
    let fx = boot().await;
    let mut ws = connect(fx.port, fx.cfg.clone()).await;

    let resp = wss_rpc_raw(
        &mut ws,
        1,
        "voice.transcribe",
        json!({ "audio": b64(b"pcm"), "provider": "openai" }),
    )
    .await;
    assert!(resp.get("error").is_none(), "unexpected error: {resp}");
    assert_eq!(resp["result"]["provider"], "elevenlabs");
    assert_eq!(fx.engine.calls.lock().unwrap().len(), 1);
}

/// The missing-API-key failure surfaces on the wire as `-32603` with the
/// generic `"Internal error"` message plus machine-readable
/// `error.data = { code: "voice-no-api-key", detail }`, the detail text
/// unchanged from the pre-structured shape (PROTOCOL §5.41, monorepo#1448).
#[tokio::test]
async fn missing_api_key_surfaces_structured_error_data() {
    let (_srv, port, cfg, _store, _root, _dir) = boot_with_engine(Arc::new(NoKeyEngine)).await;
    let mut ws = connect(port, cfg).await;

    let resp = wss_rpc_raw(
        &mut ws,
        1,
        "voice.transcribe",
        json!({ "audio": b64(b"pcm") }),
    )
    .await;
    let err = &resp["error"];
    assert_eq!(err["code"], -32603, "{resp}");
    assert_eq!(err["message"], "Internal error");
    assert_eq!(err["data"]["code"], "voice-no-api-key");
    assert_eq!(
        err["data"]["detail"],
        "voice not configured: voice: no API key found for elevenlabs \
         (set voice.elevenlabs.apiKey or ELEVENLABS_API_KEY)"
    );
}

/// A `voice.transcribe` call carrying a known `workspaceId` injects the
/// workspace's auto-derived vocabulary between the user vocabulary and the
/// request keyterms (PROTOCOL §5.41, v4.6: user `voice.vocabulary` →
/// workspace auto-terms → `context.keyterms`).
#[tokio::test]
async fn transcribe_with_workspace_id_injects_derived_vocabulary() {
    let fx = boot().await;
    let ws_id = seed_vocab_workspace(&fx, "# Repo\nZorblatt tooling and the Quuxify pass.").await;
    let mut ws = connect(fx.port, fx.cfg.clone()).await;

    let resp = wss_rpc_raw(
        &mut ws,
        1,
        "voice.transcribe",
        json!({
            "audio": b64(b"opus"),
            "workspaceId": ws_id.as_str(),
            "context": { "keyterms": ["Endara"] },
        }),
    )
    .await;
    assert!(resp.get("error").is_none(), "unexpected error: {resp}");

    let calls = fx.engine.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    let keyterms = &calls[0].keyterms;
    for expected in ["Zorblatt", "Quuxify"] {
        assert!(
            keyterms.contains(&expected.to_string()),
            "workspace auto-terms injected: {keyterms:?}"
        );
    }
    let vocab_pos = keyterms.iter().position(|t| t == "Intent").expect("vocab");
    let derived_pos = keyterms
        .iter()
        .position(|t| t == "Zorblatt")
        .expect("derived");
    let request_pos = keyterms
        .iter()
        .position(|t| t == "Endara")
        .expect("request");
    assert!(
        vocab_pos < derived_pos && derived_pos < request_pos,
        "merge order user vocabulary → workspace auto-terms → request keyterms: {keyterms:?}"
    );
}

/// An unknown/stale `workspaceId` on `voice.transcribe` is tolerated — the
/// call behaves exactly like a no-`workspaceId` call — while a non-string
/// value rejects with `-32602` / `error.data.code: "invalid-params"` before
/// the engine is reached (PROTOCOL §5.41, v4.6).
#[tokio::test]
async fn unknown_workspace_id_tolerated_non_string_rejects() {
    let fx = boot().await;
    let mut ws = connect(fx.port, fx.cfg.clone()).await;

    let resp = wss_rpc_raw(
        &mut ws,
        1,
        "voice.transcribe",
        json!({ "audio": b64(b"pcm"), "workspaceId": "ws-gone-stale" }),
    )
    .await;
    assert!(
        resp.get("error").is_none(),
        "stale workspaceId must never error: {resp}"
    );
    {
        let calls = fx.engine.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert!(
            !calls[0].keyterms.iter().any(|t| t == "Zorblatt"),
            "no injection for an unknown workspace"
        );
    }

    let bad = wss_rpc_raw(
        &mut ws,
        2,
        "voice.transcribe",
        json!({ "audio": b64(b"pcm"), "workspaceId": 42 }),
    )
    .await;
    assert_eq!(bad["error"]["code"], -32602, "{bad}");
    assert_eq!(bad["error"]["data"]["code"], "invalid-params", "{bad}");
    assert_eq!(
        fx.engine.calls.lock().unwrap().len(),
        1,
        "engine never called on a non-string workspaceId"
    );
}

/// `voice.getWorkspaceVocabulary` serves the derived terms only (`{ terms }`,
/// no user vocabulary merged in), respects the
/// `voice.workspaceVocabulary.maxTerms` setting (0 disables), and an unknown
/// `workspaceId` is the standard not-found error (`-32602` with
/// `error.data.code: "not-found"`) (PROTOCOL §5.41, v4.6).
#[tokio::test]
async fn get_workspace_vocabulary_serves_derived_terms_and_not_found() {
    let fx = boot().await;
    let ws_id = seed_vocab_workspace(&fx, "The Zorblatt pipeline needs a Quuxify pass.").await;
    let mut ws = connect(fx.port, fx.cfg.clone()).await;

    let resp = wss_rpc_raw(
        &mut ws,
        1,
        "voice.getWorkspaceVocabulary",
        json!({ "workspaceId": ws_id.as_str() }),
    )
    .await;
    assert!(resp.get("error").is_none(), "unexpected error: {resp}");
    let terms = resp["result"]["terms"].as_array().expect("terms array");
    let terms: Vec<&str> = terms.iter().filter_map(Value::as_str).collect();
    for expected in ["Zorblatt", "Quuxify"] {
        assert!(terms.contains(&expected), "missing {expected}: {terms:?}");
    }
    assert!(
        !terms.contains(&"Intent"),
        "derived terms only — user voice.vocabulary is not merged in: {terms:?}"
    );

    // maxTerms = 0 disables derivation entirely → { terms: [] }.
    let upd = wss_rpc_raw(
        &mut ws,
        2,
        "settings.update",
        json!({ "changes": [{ "path": "voice.workspaceVocabulary.maxTerms", "value": 0 }] }),
    )
    .await;
    assert!(upd.get("error").is_none(), "unexpected error: {upd}");
    let resp = wss_rpc_raw(
        &mut ws,
        3,
        "voice.getWorkspaceVocabulary",
        json!({ "workspaceId": ws_id.as_str() }),
    )
    .await;
    assert!(resp.get("error").is_none(), "unexpected error: {resp}");
    assert_eq!(resp["result"]["terms"], json!([]), "{resp}");

    // Unknown workspace → -32602 with data.code "not-found".
    let missing = wss_rpc_raw(
        &mut ws,
        4,
        "voice.getWorkspaceVocabulary",
        json!({ "workspaceId": "ws-does-not-exist" }),
    )
    .await;
    assert_eq!(missing["error"]["code"], -32602, "{missing}");
    assert_eq!(missing["error"]["data"]["code"], "not-found", "{missing}");

    // Missing workspaceId → -32602 invalid-params.
    let none = wss_rpc_raw(&mut ws, 5, "voice.getWorkspaceVocabulary", json!({})).await;
    assert_eq!(none["error"]["code"], -32602, "{none}");
    assert_eq!(none["error"]["data"]["code"], "invalid-params", "{none}");
}
