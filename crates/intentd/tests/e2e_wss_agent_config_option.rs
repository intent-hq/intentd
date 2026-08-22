//! WSS e2e for post-session model AND reasoning-effort application via
//! `session/set_config_option`.
//!
//! Providers whose adapter exposes the model as a `configOptions[id="model"]`
//! select (`supports_config_option_model`; claude-code's pinned adapter) get
//! the stored model applied right after session establishment via
//! `session/set_config_option { sessionId, configId: "model", value }` —
//! verified live against claude-agent-acp@0.60.0. This suite drives the real
//! daemon over WSS with the mock provider flipped into that mode
//! (`MOCK_AGENT_CONFIG_OPTION_MODEL=1`) and asserts — via the fixture's
//! `MOCK_AGENT_CONFIG_LOG` seam — the exact wire params the provider received:
//!
//! * The call fires once on the fresh session with the bare model id (the
//!   compound `mock:` prefix stripped), BEFORE the first prompt resolves.
//! * A second turn on the same live session does NOT re-issue it.
//!
//! The same seam covers the generic reasoning-effort application (PROTOCOL
//! §5.5): with the mock advertising a `thought_level`-category select
//! (`MOCK_AGENT_THOUGHT_LEVEL=<currentValue>`) under its own config id, the
//! session's `reasoningEffort` is applied at session setup and re-applied on
//! the LIVE session after an `agent.update` change, so it lands before the
//! next prompt without a respawn.
//!
//! It also covers the sibling `session/set_model` path
//! (`supports_set_model`; grok and codex — codex's npx-fallback adapter
//! ignores `-c model=…` argv overrides, so the stored model rides this call):
//! with the mock flipped into that mode (`MOCK_AGENT_SET_MODEL=1`), the
//! daemon issues `session/set_model { sessionId, modelId }` once on the
//! fresh session, with the bare model id.
//!
//! Gated on `node` + the mock script; skips cleanly otherwise.

#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-cfgopt-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, listen: &str, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    let secrets_file = data_dir.join("secrets.json");
    if listen != "uds" {
        common::enable_ws_api(data_dir);
    }
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_SECRETS_FILE", &secrets_file)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd.spawn().expect("spawn intentd serve")
}

async fn await_uds(socket: &Path) -> bool {
    timeout(common::daemon_startup_timeout(), async {
        loop {
            if UnixStream::connect(socket).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .is_ok()
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

async fn connect_ws(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    loop {
        let next = timeout(Duration::from_secs(15), ws.next())
            .await
            .expect("wss rpc timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["id"] == json!(id) {
                    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
                    return v["result"].clone();
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// Read one `events.event` notification from a subscriber connection (bounded).
async fn wss_event<S>(ws: &mut WebSocketStream<S>, secs: u64) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let next = timeout(Duration::from_secs(secs), ws.next())
            .await
            .expect("wss event timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == "events.event" {
                    return v;
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// Drain subscriber events until an `agent:stream:end` for `agent_id` arrives.
async fn await_stream_end<S>(sub: &mut WebSocketStream<S>, agent_id: &str)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    for _ in 0..120 {
        let frame = wss_event(sub, 30).await;
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:stream:end" && ev["data"]["agentId"].as_str() == Some(agent_id) {
            return;
        }
    }
    panic!("no agent:stream:end for {agent_id}");
}

/// Mock-agent gate (parity with the WSS lifecycle suite).
fn gate() -> Option<String> {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping WSS set_config_option E2E: node not on PATH");
        return None;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping WSS set_config_option E2E: mock script missing at {script}");
        return None;
    }
    Some(script)
}

/// Parse the fixture's config-option log: one JSON object per line, the exact
/// `session/set_config_option` params the provider received.
fn read_config_log(path: &Path) -> Vec<Value> {
    let raw = std::fs::read_to_string(path).unwrap_or_default();
    raw.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("config log line json"))
        .collect()
}

/// The stored model reaches a config-option-model provider as
/// `session/set_config_option { configId: "model", value: <bare id> }` once
/// per fresh session — issued post-establishment, not repeated on a second
/// turn over the same live child.
#[tokio::test]
async fn stored_model_applied_via_set_config_option_over_wss() {
    let Some(script) = gate() else {
        return;
    };

    let data_dir = temp_data_dir();
    let config_log = data_dir.join("config-log.jsonl");
    let config_log_str = config_log.to_string_lossy().into_owned();
    let behavior = json!({ "response": "ok" }).to_string();
    let env: [(&str, &str); 6] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_CONFIG_OPTION_MODEL", "1"),
        ("MOCK_AGENT_CONFIG_LOG", &config_log_str),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

    // SUBSCRIBER conn — events.subscribe BEFORE the turns so we miss nothing.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let ws_result = wss_rpc(
        &mut sub,
        1,
        "workspace.create",
        json!({ "title": "CfgOpt E2E", "noPrompt": true }),
    )
    .await;
    let ws_id = ws_result["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let sub_resp = wss_rpc(
        &mut sub,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // RPC conn — create an agent with a compound stored model and run two turns.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({
            "workspaceId": ws_id,
            "name": "CfgOpt",
            "model": "mock:sonnet",
        }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "first turn" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    await_stream_end(&mut sub, &agent_id).await;

    // The daemon issued exactly one session/set_config_option on the fresh
    // session, with the bare model id (compound `mock:` prefix stripped) and
    // the exact live-verified wire shape { sessionId, configId, value }.
    let log = read_config_log(&config_log);
    assert_eq!(log.len(), 1, "one set_config_option after turn 1: {log:?}");
    assert_eq!(log[0]["configId"], "model", "configId: {:?}", log[0]);
    assert_eq!(log[0]["value"], "sonnet", "bare model id: {:?}", log[0]);
    assert!(
        log[0]["sessionId"].is_string(),
        "sessionId present: {:?}",
        log[0]
    );

    // Turn 2 reuses the live session — no re-application.
    let sent2 = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "second turn" }),
    )
    .await;
    assert_eq!(sent2["success"], true, "second sendMessage ok: {sent2}");
    await_stream_end(&mut sub, &agent_id).await;

    let log = read_config_log(&config_log);
    assert_eq!(
        log.len(),
        1,
        "no re-issue on a reused live session: {log:?}"
    );
}

/// The stored model reaches a `set_model` provider (grok/codex-like) as
/// `session/set_model { sessionId, modelId }` once per fresh session — with
/// the bare model id, `{base}/{effort}` suffix intact (codex-acp's
/// ModelId.fromString parses the effort itself) — and is not re-issued on a
/// second turn over the same live child.
#[tokio::test]
async fn stored_model_applied_via_set_model_over_wss() {
    let Some(script) = gate() else {
        return;
    };

    let data_dir = temp_data_dir();
    let config_log = data_dir.join("config-log.jsonl");
    let config_log_str = config_log.to_string_lossy().into_owned();
    let behavior = json!({ "response": "ok" }).to_string();
    let env: [(&str, &str); 6] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_SET_MODEL", "1"),
        ("MOCK_AGENT_CONFIG_LOG", &config_log_str),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

    let mut sub = connect_ws(port, cfg.clone()).await;
    let ws_result = wss_rpc(
        &mut sub,
        1,
        "workspace.create",
        json!({ "title": "SetModel E2E", "noPrompt": true }),
    )
    .await;
    let ws_id = ws_result["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    wss_rpc(
        &mut sub,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;

    // Compound stored model with a `{base}/{effort}` suffix — the daemon
    // strips the `mock:` provider prefix and sends the rest intact.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({
            "workspaceId": ws_id,
            "name": "SetModel",
            "model": "mock:gpt-5.3-codex/high",
        }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "first turn" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    await_stream_end(&mut sub, &agent_id).await;

    // Exactly one session/set_model on the fresh session, with the wire
    // shape { sessionId, modelId } and the effort suffix preserved.
    let log = read_config_log(&config_log);
    assert_eq!(log.len(), 1, "one set_model after turn 1: {log:?}");
    assert_eq!(
        log[0]["modelId"], "gpt-5.3-codex/high",
        "bare id with effort suffix intact: {:?}",
        log[0]
    );
    assert!(
        log[0]["sessionId"].is_string(),
        "sessionId present: {:?}",
        log[0]
    );

    // Turn 2 reuses the live session — no re-application.
    let sent2 = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "second turn" }),
    )
    .await;
    assert_eq!(sent2["success"], true, "second sendMessage ok: {sent2}");
    await_stream_end(&mut sub, &agent_id).await;

    let log = read_config_log(&config_log);
    assert_eq!(
        log.len(),
        1,
        "no re-issue on a reused live session: {log:?}"
    );
}

/// A rejected `session/set_model` (e.g. an unknown model id) is best-effort:
/// the daemon logs a warning, the provider keeps its default model, and the
/// turn still completes.
#[tokio::test]
async fn set_model_failure_does_not_fail_the_turn() {
    let Some(script) = gate() else {
        return;
    };

    let data_dir = temp_data_dir();
    let config_log = data_dir.join("config-log.jsonl");
    let config_log_str = config_log.to_string_lossy().into_owned();
    let behavior = json!({ "response": "ok", "rejectSetModel": true }).to_string();
    let env: [(&str, &str); 6] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_SET_MODEL", "1"),
        ("MOCK_AGENT_CONFIG_LOG", &config_log_str),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

    let mut sub = connect_ws(port, cfg.clone()).await;
    let ws_result = wss_rpc(
        &mut sub,
        1,
        "workspace.create",
        json!({ "title": "SetModel Reject E2E", "noPrompt": true }),
    )
    .await;
    let ws_id = ws_result["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    wss_rpc(
        &mut sub,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "SetModelReject", "model": "mock:bogus-model" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "turn despite rejection" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    // The turn must still complete (stream:end) even though the provider
    // rejected the set_model call.
    await_stream_end(&mut sub, &agent_id).await;

    // The daemon did attempt the call (with the bare stored id) …
    let log = read_config_log(&config_log);
    assert_eq!(log.len(), 1, "set_model was attempted: {log:?}");
    assert_eq!(log[0]["modelId"], "bogus-model", "modelId: {:?}", log[0]);

    // … and the agent is still healthy: transcript has the mock's response.
    let messages = wss_rpc(
        &mut rpc,
        12,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let rendered = messages.to_string();
    assert!(
        rendered.contains("\"ok\""),
        "assistant response landed despite the rejected set_model: {rendered}"
    );
}

/// A rejected `session/set_config_option` (e.g. an unknown model id) is
/// best-effort: the daemon logs a warning, the provider keeps its default
/// model, and the turn still completes.
#[tokio::test]
async fn set_config_option_failure_does_not_fail_the_turn() {
    let Some(script) = gate() else {
        return;
    };

    let data_dir = temp_data_dir();
    let config_log = data_dir.join("config-log.jsonl");
    let config_log_str = config_log.to_string_lossy().into_owned();
    let behavior = json!({ "response": "ok", "rejectSetConfigOption": true }).to_string();
    let env: [(&str, &str); 6] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_CONFIG_OPTION_MODEL", "1"),
        ("MOCK_AGENT_CONFIG_LOG", &config_log_str),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

    let mut sub = connect_ws(port, cfg.clone()).await;
    let ws_result = wss_rpc(
        &mut sub,
        1,
        "workspace.create",
        json!({ "title": "CfgOpt Reject E2E", "noPrompt": true }),
    )
    .await;
    let ws_id = ws_result["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    wss_rpc(
        &mut sub,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "CfgOptReject", "model": "mock:bogus-model" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "turn despite rejection" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    // The turn must still complete (stream:end) even though the provider
    // rejected the config-option call.
    await_stream_end(&mut sub, &agent_id).await;

    // The daemon did attempt the call (with the bare stored id) …
    let log = read_config_log(&config_log);
    assert_eq!(log.len(), 1, "set_config_option was attempted: {log:?}");
    assert_eq!(log[0]["configId"], "model", "configId: {:?}", log[0]);
    assert_eq!(log[0]["value"], "bogus-model", "value: {:?}", log[0]);

    // … and the agent is still healthy: transcript has the mock's response.
    let messages = wss_rpc(
        &mut rpc,
        12,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let rendered = messages.to_string();
    assert!(
        rendered.contains("\"ok\""),
        "assistant response landed despite the rejected set_config_option: {rendered}"
    );
}

/// PROTOCOL §5.5: the session's `reasoningEffort` reaches a provider that
/// advertises a `thought_level`-category config option, under the ADAPTER's
/// own config id (`effort` here — codex-acp names it `reasoning_effort`), and
/// a mid-session `agent.update` change is re-applied on the same live session
/// so it takes effect by the next prompt with no respawn.
#[tokio::test]
async fn reasoning_effort_applied_and_reapplied_over_wss() {
    let Some(script) = gate() else {
        return;
    };

    let data_dir = temp_data_dir();
    let config_log = data_dir.join("config-log.jsonl");
    let config_log_str = config_log.to_string_lossy().into_owned();
    let behavior = json!({ "response": "ok" }).to_string();
    let env: [(&str, &str); 6] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        // The provider opens on "medium" and offers low/medium/high.
        ("MOCK_AGENT_THOUGHT_LEVEL", "medium"),
        ("MOCK_AGENT_CONFIG_LOG", &config_log_str),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

    let mut sub = connect_ws(port, cfg.clone()).await;
    let ws_result = wss_rpc(
        &mut sub,
        1,
        "workspace.create",
        json!({ "title": "Effort E2E", "noPrompt": true }),
    )
    .await;
    let ws_id = ws_result["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    wss_rpc(
        &mut sub,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({
            "workspaceId": ws_id,
            "name": "Effort",
            "model": "mock:sonnet",
            "reasoningEffort": "high",
        }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    assert_eq!(
        created["agent"]["reasoningEffort"], "high",
        "effort persisted on create: {created}"
    );

    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "first turn" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    await_stream_end(&mut sub, &agent_id).await;

    // One call, under the adapter's own config id, with the stored effort.
    let log = read_config_log(&config_log);
    assert_eq!(log.len(), 1, "one effort application: {log:?}");
    assert_eq!(log[0]["configId"], "effort", "adapter's id: {:?}", log[0]);
    assert_eq!(log[0]["value"], "high", "stored effort: {:?}", log[0]);

    // A second turn on the same live session re-applies nothing.
    let sent2 = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "second turn" }),
    )
    .await;
    assert_eq!(sent2["success"], true, "second sendMessage ok: {sent2}");
    await_stream_end(&mut sub, &agent_id).await;
    assert_eq!(
        read_config_log(&config_log).len(),
        1,
        "unchanged effort is not re-sent"
    );

    // Mid-session change → applied on the SAME live session before the turn.
    // Stored with the caller's spelling ("LOW"); the adapter must receive its
    // OWN spelling ("low") since matching is case-insensitive.
    let updated = wss_rpc(
        &mut rpc,
        13,
        "agent.update",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "changes": { "reasoningEffort": "LOW" },
        }),
    )
    .await;
    assert_eq!(updated["agent"]["reasoningEffort"], "LOW", "{updated}");
    let sent3 = wss_rpc(
        &mut rpc,
        14,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "third turn" }),
    )
    .await;
    assert_eq!(sent3["success"], true, "third sendMessage ok: {sent3}");
    await_stream_end(&mut sub, &agent_id).await;

    let log = read_config_log(&config_log);
    assert_eq!(log.len(), 2, "the change was applied: {log:?}");
    assert_eq!(log[1]["configId"], "effort", "{:?}", log[1]);
    assert_eq!(
        log[1]["value"], "low",
        "the adapter's own spelling is sent, not the caller's: {:?}",
        log[1]
    );
}

/// PROTOCOL §5.5 (Option C): the effort levels a provider's `thought_level`
/// select advertises at session open are persisted and served as
/// `effortLevels` on the agent wire payloads — announced by an
/// `agent:updated { effortLevels }` event and carried by `agent.get` — all
/// over the real WSS transport.
#[tokio::test]
async fn effort_levels_persisted_and_served_over_wss() {
    let Some(script) = gate() else {
        return;
    };

    let data_dir = temp_data_dir();
    let behavior = json!({ "response": "ok" }).to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        // The provider advertises a thought_level select (low/medium/high).
        ("MOCK_AGENT_THOUGHT_LEVEL", "medium"),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

    let mut sub = connect_ws(port, cfg.clone()).await;
    let ws_result = wss_rpc(
        &mut sub,
        1,
        "workspace.create",
        json!({ "title": "EffortLevels E2E", "noPrompt": true }),
    )
    .await;
    let ws_id = ws_result["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    wss_rpc(
        &mut sub,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "EffortLevels", "model": "mock:sonnet" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    assert!(
        created["agent"].get("effortLevels").is_none(),
        "no levels before the first session open: {created}"
    );

    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "first turn" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // The session open persisted the discovered levels and announced them
    // with agent:updated { effortLevels } (PROTOCOL §6 events.event shape).
    let mut saw_effort_update = false;
    for _ in 0..120 {
        let frame = wss_event(&mut sub, 30).await;
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:updated"
            && ev["data"]["agentId"].as_str() == Some(&agent_id)
            && ev["data"]["effortLevels"] == json!(["low", "medium", "high"])
        {
            saw_effort_update = true;
        }
        if ev["type"] == "agent:stream:end" && ev["data"]["agentId"].as_str() == Some(&agent_id) {
            break;
        }
    }
    assert!(
        saw_effort_update,
        "agent:updated with effortLevels [low, medium, high] on the wire"
    );

    // agent.get serves the persisted set as `effortLevels` (camelCase).
    let got = wss_rpc(
        &mut rpc,
        12,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        got["agent"]["effortLevels"],
        json!(["low", "medium", "high"]),
        "agent.get carries the session-discovered effortLevels: {got}"
    );
}

/// A provider that advertises no `thought_level` option silently ignores the
/// session's `reasoningEffort` — no `session/set_config_option` is issued and
/// the turn completes normally.
#[tokio::test]
async fn reasoning_effort_is_a_no_op_without_a_thought_level_option() {
    let Some(script) = gate() else {
        return;
    };

    let data_dir = temp_data_dir();
    let config_log = data_dir.join("config-log.jsonl");
    let config_log_str = config_log.to_string_lossy().into_owned();
    let behavior = json!({ "response": "ok" }).to_string();
    // No MOCK_AGENT_THOUGHT_LEVEL and no config-option model: the mock's
    // session results carry no `configOptions` at all.
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_CONFIG_LOG", &config_log_str),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

    let mut sub = connect_ws(port, cfg.clone()).await;
    let ws_result = wss_rpc(
        &mut sub,
        1,
        "workspace.create",
        json!({ "title": "Effort NoOp E2E", "noPrompt": true }),
    )
    .await;
    let ws_id = ws_result["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    wss_rpc(
        &mut sub,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({
            "workspaceId": ws_id,
            "name": "EffortNoOp",
            "model": "mock:sonnet",
            "reasoningEffort": "high",
        }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "turn" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    await_stream_end(&mut sub, &agent_id).await;

    assert!(
        read_config_log(&config_log).is_empty(),
        "no config option advertised → nothing sent: {:?}",
        read_config_log(&config_log)
    );
}
