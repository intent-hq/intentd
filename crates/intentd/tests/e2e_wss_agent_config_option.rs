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
//! (`supports_set_model`; grok today): with the mock flipped into that mode
//! (`MOCK_AGENT_SET_MODEL=1`), the daemon issues
//! `session/set_model { sessionId, modelId }` once on the fresh session,
//! with the bare model id.
//!
//! codex rides the config-option path with an extra wrinkle
//! (`config_option_model_strips_effort`): its stored ids may embed a
//! reasoning effort (`{base}/{effort}`), and the adapter's model select
//! values are bare base ids, so the daemon strips the suffix before sending.
//! Covered here with `MOCK_AGENT_CONFIG_OPTION_MODEL_STRIPS_EFFORT=1`.
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
            "model": "sonnet", "provider": "mock",
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

/// The stored model reaches a `set_model` provider (grok-like) as
/// `session/set_model { sessionId, modelId }` once per fresh session — with
/// the bare model id (compound `mock:` prefix stripped) — and is not
/// re-issued on a second turn over the same live child.
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

    // Compound stored model — the daemon strips the `mock:` provider prefix
    // and sends the bare id.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({
            "workspaceId": ws_id,
            "name": "SetModel",
            "model": "grok-4.5", "provider": "mock",
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
    // shape { sessionId, modelId } and the bare model id.
    let log = read_config_log(&config_log);
    assert_eq!(log.len(), 1, "one set_model after turn 1: {log:?}");
    assert_eq!(
        log[0]["modelId"], "grok-4.5",
        "bare id with the mock: prefix stripped: {:?}",
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

/// A codex-like config-option-model provider whose stored ids embed a
/// reasoning effort (`config_option_model_strips_effort`): the daemon strips
/// the `{base}/{effort}` suffix before sending, because the adapter's
/// `configOptions[id="model"]` select values are bare base ids (and its
/// `session/set_model` handler rejects both bare and `/`-compound ids, so
/// that path is unusable — intent-hq/monorepo#3174).
#[tokio::test]
async fn stored_model_effort_suffix_stripped_for_config_option_over_wss() {
    let Some(script) = gate() else {
        return;
    };

    let data_dir = temp_data_dir();
    let config_log = data_dir.join("config-log.jsonl");
    let config_log_str = config_log.to_string_lossy().into_owned();
    let behavior = json!({ "response": "ok" }).to_string();
    let env: [(&str, &str); 7] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_CONFIG_OPTION_MODEL", "1"),
        ("MOCK_AGENT_CONFIG_OPTION_MODEL_STRIPS_EFFORT", "1"),
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
        json!({ "title": "StripEffort E2E", "noPrompt": true }),
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
    // strips both the `mock:` provider prefix AND the `/high` effort suffix.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({
            "workspaceId": ws_id,
            "name": "StripEffort",
            "model": "gpt-5.3-codex/high", "provider": "mock",
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

    // Exactly one session/set_config_option { configId: "model" } on the
    // fresh session, with the BARE BASE id — no effort suffix.
    let log = read_config_log(&config_log);
    assert_eq!(log.len(), 1, "one set_config_option after turn 1: {log:?}");
    assert_eq!(log[0]["configId"], "model", "configId: {:?}", log[0]);
    assert_eq!(
        log[0]["value"], "gpt-5.3-codex",
        "effort suffix stripped from the config-option value: {:?}",
        log[0]
    );
    assert!(
        log[0]["sessionId"].is_string(),
        "sessionId present: {:?}",
        log[0]
    );
}

/// Reproduces the host adapter's independent model state: it starts/loads on
/// vega-alpha, accepts only base model config values, and reports the model
/// actually used by each prompt. A logged `set_config_option` alone is not proof.
async fn assert_effective_codex_model_selection(
    requested: &str,
    expected: &str,
    explicit_effort: Option<&str>,
    effort: &str,
    resume: bool,
) {
    let Some(script) = gate() else {
        return;
    };
    let data_dir = temp_data_dir();
    let prompt_log = data_dir.join("prompts.jsonl");
    let prompt_log_str = prompt_log.to_string_lossy().into_owned();
    let behavior = json!({
        "advertiseLoadSession": true,
        "modelSelection": {
            "defaultModel": "vega-alpha",
            "models": ["vega-alpha", "gpt-5.6-luna", "gpt-5.5"],
        },
    })
    .to_string();
    let env = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", script.as_str()),
        ("MOCK_AGENT_BEHAVIOR", behavior.as_str()),
        ("MOCK_AGENT_PROMPT_LOG", prompt_log_str.as_str()),
        ("MOCK_AGENT_CONFIG_OPTION_MODEL", "1"),
        ("MOCK_AGENT_CONFIG_OPTION_MODEL_STRIPS_EFFORT", "1"),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let mut daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let cfg = client_config(
        status["result"]["fingerprint"]
            .as_str()
            .expect("fingerprint"),
    );
    let mut sub = connect_ws(port, cfg.clone()).await;
    let workspace = wss_rpc(
        &mut sub,
        1,
        "workspace.create",
        json!({ "title": "Effective Codex model", "noPrompt": true }),
    )
    .await;
    let ws_id = workspace["workspace"]["id"].as_str().expect("workspace id");
    wss_rpc(
        &mut sub,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    let mut rpc = connect_ws(port, cfg).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({
            "workspaceId": ws_id, "name": "Effective model", "provider": "mock",
            "model": if resume { "gpt-5.6-luna" } else { requested },
            "reasoningEffort": explicit_effort,
        }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().expect("agent id");
    if resume {
        wss_rpc(
            &mut rpc,
            11,
            "agent.sendMessage",
            json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "control turn" }),
        )
        .await;
        await_stream_end(&mut sub, agent_id).await;
        let control = wss_rpc(
            &mut rpc,
            12,
            "agent.getConversation",
            json!({ "workspaceId": ws_id, "agentId": agent_id }),
        )
        .await;
        assert!(control.to_string().contains(&format!(
            "effective-model=gpt-5.6-luna effort={} loaded=false",
            explicit_effort.unwrap_or("high")
        )));
        wss_rpc(
            &mut rpc,
            13,
            "agent.setModel",
            json!({
                "workspaceId": ws_id, "agentId": agent_id,
                "modelId": requested, "providerId": "mock",
            }),
        )
        .await;
        // The mock resolver has no spawn model. Restart this isolated daemon
        // to force session/load of the persisted selection. The live Codex
        // reproduction separately verified automatic model-change respawn.
        daemon.child.kill().expect("stop test daemon");
        daemon.child.wait().expect("reap test daemon");
        daemon.child = spawn_serve(&data_dir, "both", &env);
        assert!(await_uds(&socket).await, "daemon did not restart");
        let status = common::await_wss_status(&socket).await;
        let port = u16::try_from(status["result"]["port"].as_u64().expect("port"))
            .expect("value fits in u16");
        let cfg = client_config(
            status["result"]["fingerprint"]
                .as_str()
                .expect("fingerprint"),
        );
        rpc = connect_ws(port, cfg.clone()).await;
        sub = connect_ws(port, cfg).await;
        wss_rpc(
            &mut sub,
            2,
            "events.subscribe",
            json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
        )
        .await;
    }
    wss_rpc(
        &mut rpc,
        20,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "selected model turn" }),
    )
    .await;
    await_stream_end(&mut sub, agent_id).await;
    let conversation = wss_rpc(
        &mut rpc,
        21,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let expected_response = format!("effective-model={expected} effort={effort} loaded={resume}");
    assert!(
        conversation.to_string().contains(&expected_response),
        "requested {requested}, expected execution {expected_response}; actual: {conversation}"
    );

    // Reusing the same child must not reset a suffix-only effort to its
    // opening default. The prompt log records actual accepted state.
    wss_rpc(
        &mut rpc,
        22,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "reuse selected model" }),
    )
    .await;
    await_stream_end(&mut sub, agent_id).await;
    let prompts = read_config_log(&prompt_log);
    assert_eq!(prompts.len(), if resume { 3 } else { 2 });
    for prompt in prompts.iter().rev().take(2) {
        assert_eq!(prompt["effectiveModel"], expected, "{prompt}");
        assert_eq!(prompt["effectiveEffort"], effort, "{prompt}");
    }
    if resume {
        // Return to the initial model on another loaded child. The new
        // accepted-state record distinguishes switch-back from earlier output.
        wss_rpc(
            &mut rpc,
            23,
            "agent.setModel",
            json!({
                "workspaceId": ws_id, "agentId": agent_id,
                "modelId": "gpt-5.6-luna", "providerId": "mock",
            }),
        )
        .await;
        daemon.child.kill().expect("stop test daemon");
        daemon.child.wait().expect("reap test daemon");
        daemon.child = spawn_serve(&data_dir, "both", &env);
        assert!(await_uds(&socket).await, "daemon did not restart");
        let status = common::await_wss_status(&socket).await;
        let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
        let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
        rpc = connect_ws(port, cfg.clone()).await;
        sub = connect_ws(port, cfg).await;
        wss_rpc(
            &mut sub,
            24,
            "events.subscribe",
            json!({
                "eventTypes": ["agent:*"], "workspaceId": ws_id,
            }),
        )
        .await;
        wss_rpc(
            &mut rpc,
            25,
            "agent.sendMessage",
            json!({
                "workspaceId": ws_id, "agentId": agent_id, "content": "switch back",
            }),
        )
        .await;
        await_stream_end(&mut sub, agent_id).await;
        let prompts = read_config_log(&prompt_log);
        assert_eq!(prompts.len(), 4);
        assert_eq!(prompts[3]["effectiveModel"], "gpt-5.6-luna");
        assert_eq!(
            prompts[3]["effectiveEffort"],
            explicit_effort.unwrap_or("high")
        );
    }
}

#[tokio::test]
async fn codex_base_model_is_effective_after_resume() {
    assert_effective_codex_model_selection("gpt-5.5", "gpt-5.5", Some("medium"), "medium", true)
        .await;
}

#[tokio::test]
async fn codex_bracket_model_is_effective_on_fresh_session() {
    assert_effective_codex_model_selection(
        "gpt-5.6-luna[low]",
        "gpt-5.6-luna",
        Some("low"),
        "low",
        false,
    )
    .await;
}

#[tokio::test]
async fn codex_bracket_model_is_effective_after_resume() {
    assert_effective_codex_model_selection(
        "gpt-5.5[medium]",
        "gpt-5.5",
        Some("medium"),
        "medium",
        true,
    )
    .await;
}

#[tokio::test]
async fn codex_embedded_effort_is_used_without_explicit_effort() {
    assert_effective_codex_model_selection("gpt-5.6-luna[low]", "gpt-5.6-luna", None, "low", false)
        .await;
    assert_effective_codex_model_selection("gpt-5.5/medium", "gpt-5.5", None, "medium", true).await;
}

#[tokio::test]
async fn codex_explicit_effort_overrides_embedded_effort() {
    assert_effective_codex_model_selection(
        "gpt-5.6-luna[low]",
        "gpt-5.6-luna",
        Some("medium"),
        "medium",
        false,
    )
    .await;
    assert_effective_codex_model_selection(
        "gpt-5.5/low",
        "gpt-5.5",
        Some("medium"),
        "medium",
        true,
    )
    .await;
}

#[tokio::test]
async fn codex_default_selection_keeps_provider_default() {
    assert_effective_codex_model_selection("default", "vega-alpha", None, "high", false).await;
}

/// A rejected explicit Codex selection cannot execute a default-model prompt,
/// including on retry. Cover fresh, loaded, and recreated session setup.
async fn assert_codex_rejection_and_recovery(advertise_load: bool) {
    let Some(script) = gate() else { return };
    let data_dir = temp_data_dir();
    let config_log = data_dir.join("config.jsonl");
    let session_log = data_dir.join("sessions.jsonl");
    let prompt_log = data_dir.join("prompts.jsonl");
    let config_path = config_log.to_string_lossy().into_owned();
    let session_path = session_log.to_string_lossy().into_owned();
    let prompt_path = prompt_log.to_string_lossy().into_owned();
    let behavior = json!({
        "advertiseLoadSession": advertise_load,
        "modelSelection": {
            "defaultModel": "vega-alpha",
            "models": ["vega-alpha", "gpt-5.6-luna", "gpt-5.5"],
        },
    })
    .to_string();
    let env = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", script.as_str()),
        ("MOCK_AGENT_BEHAVIOR", behavior.as_str()),
        ("MOCK_AGENT_CONFIG_OPTION_MODEL", "1"),
        ("MOCK_AGENT_CONFIG_OPTION_MODEL_STRIPS_EFFORT", "1"),
        ("MOCK_AGENT_CONFIG_LOG", config_path.as_str()),
        ("MOCK_AGENT_SESSION_LOG", session_path.as_str()),
        ("MOCK_AGENT_PROMPT_LOG", prompt_path.as_str()),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().expect("port")).unwrap();
    let cfg = client_config(
        status["result"]["fingerprint"]
            .as_str()
            .expect("fingerprint"),
    );
    let mut sub = connect_ws(port, cfg.clone()).await;
    let workspace = wss_rpc(
        &mut sub,
        1,
        "workspace.create",
        json!({
            "title": "Codex rejection", "noPrompt": true,
        }),
    )
    .await;
    let ws_id = workspace["workspace"]["id"].as_str().unwrap();
    wss_rpc(
        &mut sub,
        2,
        "events.subscribe",
        json!({
            "eventTypes": ["agent:*"], "workspaceId": ws_id,
        }),
    )
    .await;
    let mut rpc = connect_ws(port, cfg).await;
    // Unknown bracket content must not be stripped into a valid base model.
    let rejected = "gpt-5.5[bogus]";
    let created = wss_rpc(
        &mut rpc,
        3,
        "agent.create",
        json!({
            "workspaceId": ws_id, "name": "Rejected model", "provider": "mock",
            "model": rejected, "reasoningEffort": "low",
        }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap();
    let sent = wss_rpc(
        &mut rpc,
        4,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id, "agentId": agent_id, "content": "must not run on default",
        }),
    )
    .await;
    assert_eq!(sent["success"], true);
    for attempt in 0..3 {
        if attempt > 0 {
            let retry = wss_rpc(
                &mut rpc,
                5,
                "agent.retry",
                json!({
                    "workspaceId": ws_id, "agentId": agent_id,
                }),
            )
            .await;
            assert_eq!(retry["ok"], true, "{retry}");
        }
        let mut failed = false;
        let mut ended = false;
        let mut error_status = false;
        for _ in 0..200 {
            let frame = wss_event(&mut sub, 30).await;
            let event = &frame["params"]["event"];
            if event["data"]["agentId"].as_str() != Some(agent_id) {
                continue;
            }
            match event["type"].as_str() {
                Some("agent:failed") => {
                    let error = event["data"]["error"].as_str().unwrap();
                    assert!(
                        error.contains(rejected) && error.contains("Select a supported model"),
                        "{error}"
                    );
                    failed = true;
                }
                Some("agent:stream:end") => {
                    assert!(failed, "failure must precede stream end");
                    ended = true;
                }
                Some("agent:status-changed") if event["data"]["status"] == "error" => {
                    error_status = true;
                }
                _ => {}
            }
            if failed && ended && error_status {
                break;
            }
        }
        assert!(
            failed && ended && error_status,
            "terminal failure on attempt {attempt}"
        );
        let session = wss_rpc(
            &mut rpc,
            6,
            "agent.getSession",
            json!({
                "workspaceId": ws_id, "agentId": agent_id,
            }),
        )
        .await;
        assert_eq!(session["session"]["status"], "error");
        assert!(session["session"]["stopReason"]
            .as_str()
            .unwrap()
            .contains(rejected));
        assert!(
            read_config_log(&prompt_log).is_empty(),
            "no prompt may execute after rejection"
        );
        let configs = read_config_log(&config_log);
        assert_eq!(
            configs.len(),
            attempt + 1,
            "selection must be attempted again on every retry"
        );
        assert!(configs.iter().all(|config| config["value"] == rejected));
        let sessions = read_config_log(&session_log);
        assert_eq!(sessions.len(), attempt + 1);
        assert_eq!(
            sessions[attempt]["method"],
            if attempt > 0 && advertise_load {
                "session/load"
            } else {
                "session/new"
            }
        );
        if attempt > 0 {
            assert_ne!(
                sessions[attempt]["pid"],
                sessions[attempt - 1]["pid"],
                "rejected child must be discarded"
            );
        }
    }
    wss_rpc(
        &mut rpc,
        7,
        "agent.setModel",
        json!({
            "workspaceId": ws_id, "agentId": agent_id,
            "modelId": "gpt-5.6-luna[medium]", "providerId": "mock",
        }),
    )
    .await;
    let retry = wss_rpc(
        &mut rpc,
        8,
        "agent.retry",
        json!({
            "workspaceId": ws_id, "agentId": agent_id,
        }),
    )
    .await;
    assert_eq!(retry["ok"], true, "{retry}");
    await_stream_end(&mut sub, agent_id).await;
    let prompts = read_config_log(&prompt_log);
    assert_eq!(
        prompts.len(),
        1,
        "recovery executes exactly one queued prompt"
    );
    assert_eq!(prompts[0]["effectiveModel"], "gpt-5.6-luna");
    assert_eq!(
        prompts[0]["effectiveEffort"], "low",
        "explicit effort still wins after recovery"
    );
    let conversation = wss_rpc(
        &mut rpc,
        9,
        "agent.getConversation",
        json!({
            "workspaceId": ws_id, "agentId": agent_id,
        }),
    )
    .await;
    assert!(conversation
        .to_string()
        .contains("effective-model=gpt-5.6-luna effort=low"));
    assert!(!conversation
        .to_string()
        .contains("effective-model=vega-alpha"));
}

#[tokio::test]
async fn codex_rejection_invalidates_child_on_fresh_and_loaded_sessions() {
    assert_codex_rejection_and_recovery(true).await;
}

#[tokio::test]
async fn codex_rejection_invalidates_child_on_recreated_sessions() {
    assert_codex_rejection_and_recovery(false).await;
}

/// A real stdout closure during model setup must enter the existing spawn
/// retry path. Only a fresh, successfully configured child may run the prompt.
async fn assert_codex_config_transport_recovery(advertise_load: bool) {
    let Some(script) = gate() else { return };
    let data_dir = temp_data_dir();
    let config_log = data_dir.join("config.jsonl");
    let session_log = data_dir.join("sessions.jsonl");
    let prompt_log = data_dir.join("prompts.jsonl");
    let attempts = data_dir.join("attempts");
    let behavior = json!({
        "advertiseLoadSession": advertise_load,
        "exitOnModelConfigForAttempts": 2,
        "modelSelection": {
            "defaultModel": "vega-alpha",
            "models": ["vega-alpha", "gpt-5.5"],
        },
    })
    .to_string();
    let env = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("INTENTD_SPAWN_RETRY_BACKOFF_MS", "1,1"),
        ("MOCK_AGENT_SCRIPT_PATH", script.as_str()),
        ("MOCK_AGENT_BEHAVIOR", behavior.as_str()),
        ("MOCK_AGENT_CONFIG_OPTION_MODEL", "1"),
        ("MOCK_AGENT_CONFIG_OPTION_MODEL_STRIPS_EFFORT", "1"),
        ("MOCK_AGENT_CONFIG_LOG", config_log.to_str().unwrap()),
        ("MOCK_AGENT_SESSION_LOG", session_log.to_str().unwrap()),
        ("MOCK_AGENT_PROMPT_LOG", prompt_log.to_str().unwrap()),
        ("MOCK_AGENT_ATTEMPT_FILE", attempts.to_str().unwrap()),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    let mut sub = connect_ws(port, cfg.clone()).await;
    let workspace = wss_rpc(
        &mut sub,
        1,
        "workspace.create",
        json!({
            "title": "Codex transport recovery", "noPrompt": true,
        }),
    )
    .await;
    let ws_id = workspace["workspace"]["id"].as_str().unwrap();
    wss_rpc(
        &mut sub,
        2,
        "events.subscribe",
        json!({
            "eventTypes": ["agent:*"], "workspaceId": ws_id,
        }),
    )
    .await;
    let mut rpc = connect_ws(port, cfg).await;
    let created = wss_rpc(
        &mut rpc,
        3,
        "agent.create",
        json!({
            "workspaceId": ws_id, "name": "Transport recovery", "provider": "mock",
            "model": "gpt-5.5[low]",
        }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap();
    let sent = wss_rpc(
        &mut rpc,
        4,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id, "agentId": agent_id, "content": "recover configured model",
        }),
    )
    .await;
    assert_eq!(sent["success"], true);

    let mut retries = 0;
    let mut ended = false;
    let mut idle = false;
    for _ in 0..200 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id) {
            continue;
        }
        match event["type"].as_str() {
            Some("agent:failed") => panic!("transport should recover automatically: {event}"),
            Some("agent:stream:status") if event["data"]["phase"] == "spawn-retry" => {
                retries += 1;
                assert!(
                    event["data"]["message"]
                        .as_str()
                        .unwrap()
                        .contains("stdout closed"),
                    "{event}"
                );
            }
            Some("agent:stream:end") => {
                assert_eq!(retries, 2, "both dropped connections must be retried");
                ended = true;
            }
            Some("agent:status-changed") if event["data"]["status"] == "idle" => idle = true,
            Some("agent:message") => assert!(
                !event.to_string().contains("model_changed"),
                "no successful switch notice for failed setups: {event}"
            ),
            _ => {}
        }
        if ended && idle {
            break;
        }
    }
    assert!(ended && idle, "recovered turn completes and becomes idle");
    let sessions = read_config_log(&session_log);
    assert_eq!(
        sessions.len(),
        3,
        "two failed children and one recovery: {sessions:?}"
    );
    for (i, session) in sessions.iter().enumerate() {
        assert_eq!(
            session["method"],
            if i > 0 && advertise_load {
                "session/load"
            } else {
                "session/new"
            }
        );
        assert!(
            sessions[..i]
                .iter()
                .all(|prior| prior["pid"] != session["pid"]),
            "each retry must use a fresh child: {sessions:?}"
        );
    }
    let configs = read_config_log(&config_log);
    let models: Vec<_> = configs
        .iter()
        .filter(|config| config["configId"] == "model")
        .collect();
    assert_eq!(
        models.len(),
        3,
        "model config must be retried on every child: {configs:?}"
    );
    assert!(models.iter().all(|config| config["value"] == "gpt-5.5"));
    let prompts = read_config_log(&prompt_log);
    assert_eq!(
        prompts.len(),
        1,
        "failed setups must not send a prompt: {prompts:?}"
    );
    assert_eq!(prompts[0]["effectiveModel"], "gpt-5.5");
    assert_eq!(prompts[0]["effectiveEffort"], "low");
    let conversation = wss_rpc(
        &mut rpc,
        5,
        "agent.getConversation",
        json!({
            "workspaceId": ws_id, "agentId": agent_id,
        }),
    )
    .await;
    let rendered = conversation.to_string();
    assert!(
        rendered.contains("effective-model=gpt-5.5 effort=low"),
        "{rendered}"
    );
    assert!(
        !rendered.contains("effective-model=vega-alpha"),
        "{rendered}"
    );
    assert!(!rendered.contains("Select a supported model"), "{rendered}");
}

#[tokio::test]
async fn codex_model_config_transport_failure_retries_loaded_session() {
    assert_codex_config_transport_recovery(true).await;
}

#[tokio::test]
async fn codex_model_config_transport_failure_retries_recreated_session() {
    assert_codex_config_transport_recovery(false).await;
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
        json!({ "workspaceId": ws_id, "name": "SetModelReject", "model": "bogus-model", "provider": "mock" }),
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
        json!({ "workspaceId": ws_id, "name": "CfgOptReject", "model": "bogus-model", "provider": "mock" }),
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
            "model": "sonnet", "provider": "mock",
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
        json!({ "workspaceId": ws_id, "name": "EffortLevels", "model": "sonnet", "provider": "mock" }),
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
            "model": "sonnet", "provider": "mock",
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
