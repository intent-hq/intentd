//! WSS end-to-end: a turn for a session whose provider was disabled in
//! Settings > Agents never spawns that provider (intent-hq/intent#5737).
//!
//! Creates an agent on the mock provider, runs one turn (baseline), flips
//! `providers.enabled.mock` to `false` through `settings.update` — with the
//! settings default ALSO pointing at mock, so no usable re-home target exists
//! — and sends again. Asserts over the wire:
//! - the turn fails BEFORE any spawn: terminal `agent:failed` carrying the
//!   `session/prompt`-labelled "not enabled" rejection + `agent:stream:end`,
//!   with no `agent:stream:activity` chunk in between,
//! - the persisted session carries the same `stopReason`, and its identity is
//!   untouched (`provider` still `mock`),
//! - re-enabling the provider lets the very next turn run normally on the
//!   same session.
//!
//! The re-home branch (usable default → session moved) is covered at unit
//! level in `intent-services`; this harness has a single runnable provider.
//! Gated on `node` + the mock script; skips cleanly otherwise.

#![cfg(unix)]

mod common;

use std::path::Path;
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
use tokio::net::UnixStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

/// Live `intentd serve` process; killed (whole process group) on drop, with
/// the daemon log echoed for post-mortems.
struct Daemon {
    child: GuardedChild,
    data_dir: tempfile::TempDir,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let log_path = self.data_dir.path().join("daemon.log");
        if let Ok(log) = std::fs::read_to_string(&log_path) {
            eprintln!("=== DAEMON LOG ===\n{log}\n=== END LOG ===");
        }
    }
}

fn temp_data_dir() -> tempfile::TempDir {
    common::test_tempdir_in("/tmp", "itd-wss-disabledprov-")
}

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> GuardedChild {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    common::enable_ws_api(data_dir);
    let mut cmd = common::serve_command();
    cmd.env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    for (k, v) in env {
        cmd.env(k, v);
    }
    GuardedChild::spawn(&mut cmd).expect("spawn intentd serve")
}

async fn await_uds(socket: &Path) -> bool {
    timeout(common::daemon_startup_timeout(), async {
        loop {
            if UnixStream::connect(socket).await.is_ok() {
                return;
            }
            // timing-guard: poll interval
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .is_ok()
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
async fn connect_ws(port: u16, cfg: Arc<ClientConfig>) -> common::TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// Send one JSON-RPC frame and return the result whose id matches; any
/// out-of-band notifications (`events.event`) are ignored. Panics on an
/// error envelope.
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

/// Read one `events.event` notification from a subscriber connection.
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
                    return v["params"]["event"].clone();
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

/// Drain the subscriber until `agent_id`'s `agent:stream:end` arrives,
/// returning every event of that agent seen (the terminal one included).
async fn drain_until_stream_end<S>(ws: &mut WebSocketStream<S>, agent_id: &str) -> Vec<Value>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut seen = Vec::new();
    for _ in 0..200 {
        let ev = wss_event(ws, 30).await;
        if ev["data"]["agentId"].as_str() != Some(agent_id) {
            continue;
        }
        let terminal = ev["type"] == "agent:stream:end";
        seen.push(ev);
        if terminal {
            return seen;
        }
    }
    panic!("no agent:stream:end for {agent_id} within the event budget: {seen:?}");
}

fn events_of<'a>(events: &'a [Value], ty: &str) -> Vec<&'a Value> {
    events.iter().filter(|ev| ev["type"] == ty).collect()
}

/// Mock-agent gate (parity with the other WSS E2E suites).
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

/// Regression (intent-hq/intent#5737): with `providers.enabled.mock = false`
/// and no usable re-home target, a turn on a mock-pinned session fails
/// before any spawn with the `session/prompt`-labelled "not enabled"
/// rejection, leaves the session identity untouched, and runs again once the
/// provider is re-enabled.
#[intent_test_macros::daemon_test]
async fn disabled_provider_turn_is_rejected_before_spawn_over_wss() {
    let Some(script) = gate("WSS disabled-provider turn E2E") else {
        return;
    };

    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let behavior = json!({ "response": "mock response" }).to_string();
    let env: [(&str, &str); 3] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
    ];
    let child = spawn_serve(&data_dir, &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir_guard,
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

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let ws_result = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({ "title": "5737 WSS E2E disabled provider", "noPrompt": true }),
    )
    .await;
    let ws_id = ws_result["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // SUBSCRIBER conn — subscribe BEFORE any turn so no event can be missed.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": &ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": &ws_id, "name": "DisabledProvider", "model": "default", "provider": "mock" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    assert_eq!(created["agent"]["provider"], "mock", "{created}");

    // Baseline: with the provider enabled the turn runs on the mock child.
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": &ws_id, "agentId": &agent_id, "content": "baseline turn" }),
    )
    .await;
    assert_eq!(sent["success"], true, "baseline sendMessage ok: {sent}");
    let baseline = drain_until_stream_end(&mut sub, &agent_id).await;
    assert!(
        events_of(&baseline, "agent:failed").is_empty(),
        "baseline turn must not fail: {baseline:?}"
    );
    assert!(
        !events_of(&baseline, "agent:stream:activity").is_empty(),
        "baseline turn streams output from the mock child: {baseline:?}"
    );

    // Disable mock in Settings > Agents, AND make it the settings default so
    // the turn-start re-home finds no usable target (the only runnable
    // provider in this harness is the one being disabled).
    wss_rpc(
        &mut rpc,
        20,
        "settings.update",
        json!({ "changes": [
            { "path": "model.defaultProvider", "value": "mock" },
            { "path": "providers.enabled", "value": { "mock": false } },
        ] }),
    )
    .await;

    // The front door still accepts the message (turns are async); the
    // rejection surfaces as the turn's terminal failure.
    let sent = wss_rpc(
        &mut rpc,
        21,
        "agent.sendMessage",
        json!({ "workspaceId": &ws_id, "agentId": &agent_id, "content": "turn on disabled provider" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage accepted: {sent}");
    let rejected = drain_until_stream_end(&mut sub, &agent_id).await;
    let failed = events_of(&rejected, "agent:failed");
    assert_eq!(
        failed.len(),
        1,
        "exactly one terminal agent:failed: {rejected:?}"
    );
    let error = failed[0]["data"]["error"]
        .as_str()
        .expect("agent:failed carries the error text");
    assert!(
        error.contains("session/prompt: provider \"mock\" (Mock (E2E)) is not enabled"),
        "the turn-start rejection is the not-enabled InvalidParams labelled session/prompt: {error}"
    );
    assert!(
        error.contains("Settings > Agents"),
        "rejection names the remedy: {error}"
    );
    assert!(
        events_of(&rejected, "agent:stream:activity").is_empty(),
        "nothing streamed — the disabled provider must never spawn: {rejected:?}"
    );
    assert!(
        rejected
            .iter()
            .filter(|ev| ev["type"] == "agent:stream:status")
            .all(|ev| !ev["data"]["message"]
                .as_str()
                .unwrap_or("")
                .contains("retrying")),
        "the rejection is non-retryable — no retry hints: {rejected:?}"
    );

    // Durable-before-observable: the Error status + stopReason are persisted
    // before the terminal pair, and the session identity is untouched (no
    // re-home target existed, so provider/model stay as created).
    let session = wss_rpc(
        &mut rpc,
        22,
        "agent.getSession",
        json!({ "workspaceId": &ws_id, "agentId": &agent_id }),
    )
    .await;
    assert_eq!(session["session"]["status"], "error", "{session}");
    let stop_reason = session["session"]["stopReason"]
        .as_str()
        .expect("stopReason persisted");
    assert!(
        stop_reason.contains("is not enabled"),
        "stopReason carries the rejection: {stop_reason}"
    );
    let got = wss_rpc(
        &mut rpc,
        23,
        "agent.get",
        json!({ "workspaceId": &ws_id, "agentId": &agent_id }),
    )
    .await;
    assert_eq!(
        got["agent"]["provider"], "mock",
        "no re-home without a usable target: {got}"
    );

    // Re-enable: the very next turn on the same session runs normally.
    wss_rpc(
        &mut rpc,
        30,
        "settings.update",
        json!({ "changes": [{ "path": "providers.enabled", "value": {} }] }),
    )
    .await;
    let sent = wss_rpc(
        &mut rpc,
        31,
        "agent.sendMessage",
        json!({ "workspaceId": &ws_id, "agentId": &agent_id, "content": "turn after re-enable" }),
    )
    .await;
    assert_eq!(sent["success"], true, "re-enabled sendMessage ok: {sent}");
    let recovered = drain_until_stream_end(&mut sub, &agent_id).await;
    assert!(
        events_of(&recovered, "agent:failed").is_empty(),
        "turn after re-enable must not fail: {recovered:?}"
    );
    assert!(
        !events_of(&recovered, "agent:stream:activity").is_empty(),
        "turn after re-enable streams from the mock child again: {recovered:?}"
    );
}
