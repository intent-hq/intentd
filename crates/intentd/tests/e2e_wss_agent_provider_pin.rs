//! WSS e2e: every conversation pins its provider, so a later
//! `model.defaultProvider` change never moves it.
//!
//! 1. `agent.create` with only a `model` returns an `AgentLite` whose
//!    `provider` is the configured default (`mock`), and after
//!    `settings.update` switches the default to `grok` the next turn still
//!    runs on the live `mock` child — no grok spawn, no `model_changed` row.
//! 2. A legacy row (NULL provider — seeded by clearing the column while the
//!    daemon is stopped) whose last turn ran `mock` is repaired at its next
//!    turn onto `mock`, not the `grok` default.
//!
//! `grok` resolves hermetically to a shell wrapper around the mock fixture
//! (`providers.paths`), which writes its own session log so a grok spawn is
//! detectable. Gated on `node` + the mock script; skips cleanly otherwise.

#![cfg(unix)]

use crate::common;

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use intentd_test_support::GuardedChild;
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

/// Fixed 64-hex token, adopted by the daemon via the `INTENTD_AUTH_TOKEN` seam.
const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

fn temp_data_dir() -> tempfile::TempDir {
    common::test_tempdir_in("/tmp", "itd-pvpin-")
}

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> GuardedChild {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    common::enable_ws_api(data_dir);
    let mut cmd = common::hermetic_serve_command(data_dir);
    cmd.env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    for (k, v) in env {
        cmd.env(k, v);
    }
    common::hermetic_fixture_identity(&mut cmd, data_dir);
    GuardedChild::spawn(&mut cmd).expect("spawn intentd serve")
}

/// Pin the server's SHA-256 fingerprint (colon-UPPER hex over the DER cert).
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

/// Open an authenticated WSS connection (token in the query string).
async fn connect_ws(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// Send one JSON-RPC frame and return the result whose id matches; any
/// out-of-band notifications (`events.event`) are ignored.
async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    loop {
        let next = timeout(
            intentd_test_support::test_timeout(Duration::from_secs(15)),
            ws.next(),
        )
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
        let next = timeout(
            intentd_test_support::test_timeout(Duration::from_secs(secs)),
            ws.next(),
        )
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
fn gate(test: &str) -> Option<String> {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping {test}: node not on PATH");
        return None;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping {test}: mock script missing at {script}");
        return None;
    }
    Some(script)
}

/// Parse the mock fixture's session-lifecycle log: one
/// `{ method, sessionId, pid }` JSON per `session/new` / `session/load` the
/// child received (`MOCK_AGENT_SESSION_LOG` seam).
fn read_session_log(path: &Path) -> Vec<(String, String, u64)> {
    let raw = std::fs::read_to_string(path).expect("read session log");
    raw.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            let v: Value = serde_json::from_str(l).expect("session log line json");
            (
                v["method"].as_str().expect("method").to_string(),
                v["sessionId"].as_str().unwrap_or_default().to_string(),
                v["pid"].as_u64().expect("pid"),
            )
        })
        .collect()
}

/// Write an executable shell wrapper that execs the mock fixture under `node`,
/// discarding whatever base args the daemon passes for the impersonated
/// provider (grok's `agent stdio`) — the fixture speaks ACP on stdio and
/// ignores argv anyway. The wrapper redirects the fixture's session log to
/// `grok-session-log.jsonl`, so any grok spawn is observable. Returns the
/// wrapper's absolute path.
fn write_provider_wrapper(data_dir: &Path, script: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let node = intent_providers::resolve_on_path("node").expect("node on PATH (gated)");
    let wrapper = data_dir.join("fake-grok");
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\nexport MOCK_AGENT_SESSION_LOG=\"{}\"\nexec \"{}\" \"{}\"\n",
            data_dir.join("grok-session-log.jsonl").display(),
            node.display(),
            script
        ),
    )
    .expect("write wrapper");
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755))
        .expect("chmod wrapper");
    wrapper
}

/// Seed `config.toml` with a `providers.paths` override pinning `grok` to the
/// wrapper — the highest-precedence tier of provider binary resolution, so a
/// real grok install (native `~/.grok/bin` or PATH) can never be picked up.
fn seed_grok_path_override(data_dir: &Path, wrapper: &Path) {
    let path = data_dir.join("config.toml");
    let mut text = std::fs::read_to_string(&path).unwrap_or_default();
    if !text.is_empty() && !text.ends_with('\n') {
        text.push('\n');
    }
    let _ = write!(
        text,
        "\n[providers.paths]\ngrok = \"{}\"\n",
        wrapper.display()
    );
    std::fs::write(&path, text).expect("write config.toml");
}

async fn start(data_dir: &Path, env: &[(&str, &str)]) -> (GuardedChild, u16, Arc<ClientConfig>) {
    let daemon = spawn_serve(data_dir, env);
    let socket = data_dir.join("intentd.sock");
    let status = common::await_wss_status_logged(&socket, &data_dir.join("daemon.log")).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    (daemon, port, client_config(&fingerprint))
}

fn session_log_len(path: &Path) -> usize {
    if path.exists() {
        read_session_log(path).len()
    } else {
        0
    }
}

async fn send_turn<S>(
    rpc: &mut WebSocketStream<S>,
    sub: &mut WebSocketStream<S>,
    id: i64,
    ws_id: &str,
    agent_id: &str,
    text: &str,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let sent = wss_rpc(
        rpc,
        id,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": text }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    await_stream_end(sub, agent_id).await;
}

async fn set_default_provider<S>(rpc: &mut WebSocketStream<S>, id: i64, provider: &str)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    wss_rpc(
        rpc,
        id,
        "settings.update",
        json!({ "changes": [{ "path": "model.defaultProvider", "value": provider }] }),
    )
    .await;
}

async fn model_changed_count<S>(
    rpc: &mut WebSocketStream<S>,
    id: i64,
    ws_id: &str,
    agent_id: &str,
) -> usize
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let full = wss_rpc(
        rpc,
        id,
        "agent.getSession",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    full["session"]["messages"]
        .as_array()
        .expect("session messages")
        .iter()
        .filter(|m| m["metadata"]["type"] == "model_changed")
        .count()
}

#[tokio::test]
async fn default_provider_change_never_moves_an_existing_conversation() {
    let Some(script) = gate("WSS provider pin E2E") else {
        return;
    };
    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let wrapper = write_provider_wrapper(&data_dir, &script);
    seed_grok_path_override(&data_dir, &wrapper);
    let mock_log = data_dir.join("mock-session-log.jsonl");
    let grok_log = data_dir.join("grok-session-log.jsonl");
    let mock_log_str = mock_log.to_string_lossy().into_owned();
    let env: [(&str, &str); 3] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_SESSION_LOG", &mock_log_str),
    ];

    let (daemon, port, cfg) = start(&data_dir, &env).await;
    let mut sub = connect_ws(port, cfg.clone()).await;
    let mut rpc = connect_ws(port, cfg.clone()).await;
    set_default_provider(&mut rpc, 1, "mock").await;
    let ws_result = wss_rpc(
        &mut sub,
        2,
        "workspace.create",
        json!({ "title": "Provider Pin E2E", "noPrompt": true }),
    )
    .await;
    let ws_id = ws_result["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    wss_rpc(
        &mut sub,
        3,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;

    // (1) Model-only create pins the default provider on the wire projection.
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Pinned", "model": "default" }),
    )
    .await;
    assert_eq!(
        created["agent"]["provider"], "mock",
        "model-only create pins the default provider: {created}"
    );
    let pinned = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    // A second conversation becomes the legacy row of case (2).
    let legacy = wss_rpc(
        &mut rpc,
        11,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Legacy", "model": "default" }),
    )
    .await["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    send_turn(&mut rpc, &mut sub, 12, &ws_id, &pinned, "PIN_TURN_ONE").await;
    send_turn(&mut rpc, &mut sub, 13, &ws_id, &legacy, "LEGACY_TURN_ONE").await;
    let mock_sessions = session_log_len(&mock_log);

    set_default_provider(&mut rpc, 14, "grok").await;
    send_turn(&mut rpc, &mut sub, 15, &ws_id, &pinned, "PIN_TURN_TWO").await;
    let got = wss_rpc(
        &mut rpc,
        16,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": pinned }),
    )
    .await;
    assert_eq!(got["agent"]["provider"], "mock", "still pinned: {got}");
    assert_eq!(session_log_len(&grok_log), 0, "grok was never spawned");
    assert_eq!(
        session_log_len(&mock_log),
        mock_sessions,
        "the live mock child served the turn"
    );
    assert_eq!(model_changed_count(&mut rpc, 17, &ws_id, &pinned).await, 0);

    // (2) Turn the second conversation into a legacy row (NULL provider)
    // while the daemon is stopped; its last turn ran on mock.
    drop(sub);
    drop(rpc);
    drop(daemon);
    {
        let store = intent_store::Store::open(&data_dir.join("intentd.db"))
            .await
            .expect("open daemon store");
        let ws = intent_core::WorkspaceId::from(ws_id.as_str());
        let id = intent_core::AgentId::from(legacy.as_str());
        let session = store.get_agent_session(&id).await.expect("legacy session");
        store
            .set_agent_session_model(
                &ws,
                &id,
                session.model.as_deref().expect("model"),
                None,
                &intent_core::now_iso(),
            )
            .await
            .expect("clear provider");
        store.close().await;
    }
    let (_daemon, port, cfg) = start(&data_dir, &env).await;
    let mut sub = connect_ws(port, cfg.clone()).await;
    let mut rpc = connect_ws(port, cfg.clone()).await;
    wss_rpc(
        &mut sub,
        20,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    let before = wss_rpc(
        &mut rpc,
        21,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": legacy }),
    )
    .await;
    assert!(
        before["agent"]["provider"].is_null(),
        "seeded legacy row: {before}"
    );
    send_turn(&mut rpc, &mut sub, 22, &ws_id, &legacy, "LEGACY_TURN_TWO").await;
    let after = wss_rpc(
        &mut rpc,
        23,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": legacy }),
    )
    .await;
    assert_eq!(
        after["agent"]["provider"], "mock",
        "repaired onto its last-turn provider, not the grok default: {after}"
    );
    assert_eq!(session_log_len(&grok_log), 0, "grok was never spawned");
    assert_eq!(model_changed_count(&mut rpc, 24, &ws_id, &legacy).await, 0);
}
