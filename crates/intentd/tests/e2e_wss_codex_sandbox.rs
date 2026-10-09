//! Codex enterprise sandbox fallback through authenticated, pinned WSS.

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
    common::test_tempdir_in("/tmp", "itd-wss-codex-sandbox-")
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
        .env_remove("INITIAL_AGENT_MODE")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    for (k, v) in env {
        cmd.env(k, v);
    }
    common::hermetic_fixture_identity(&mut cmd, data_dir);
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
                    assert_eq!(v["jsonrpc"], "2.0");
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

async fn scenario(
    policy: Value,
    provider: &str,
    initial: Option<&str>,
    expected: &[&str],
    succeeds: bool,
) {
    let script = format!(
        "{}/tests/fixtures/mock-acp-agent.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    assert!(
        intent_providers::resolve_on_path("node").is_some(),
        "node prerequisite"
    );
    let dir = temp_data_dir();
    let path = dir.path().to_path_buf();
    let toolchain = common::codex_npx::install(&path, &script);
    let prompts = path.join("prompts.jsonl");
    let modes = path.join("modes.jsonl");
    let behavior = json!({"sandboxPolicy":policy}).to_string();
    let mut env: Vec<(&str, &str)> = toolchain
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    env.extend([
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_PROMPT_LOG", prompts.to_str().unwrap()),
        ("MOCK_AGENT_CONFIG_LOG", modes.to_str().unwrap()),
    ]);
    if let Some(initial) = initial {
        env.push(("INITIAL_AGENT_MODE", initial));
    }
    let _daemon = Daemon {
        child: spawn_serve(&path, &env),
        data_dir: dir,
    };
    assert!(await_uds(&path.join("intentd.sock")).await);
    let status = common::await_wss_status(&path.join("intentd.sock")).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    let mut rpc = common::wss_connect_with_retry(port, cfg.clone(), &url).await;
    let mut sub = common::wss_connect_with_retry(port, cfg, &url).await;
    let created = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({"title":"Sandbox fallback", "noPrompt":true}),
    )
    .await;
    let ws_id = created["workspace"]["id"].as_str().unwrap();
    let subscribed = wss_rpc(
        &mut sub,
        2,
        "events.subscribe",
        json!({"workspaceId":ws_id,"eventTypes":["agent:*"]}),
    )
    .await;
    assert!(subscribed["subscriptionId"].is_string());
    let created = wss_rpc(&mut rpc, 3, "agent.create", json!({"workspaceId":ws_id,"name":"Sandbox regression","provider":provider,"model":"mock-model"})).await;
    let agent_id = created["agent"]["id"].as_str().unwrap();
    for turn in 0..if succeeds { 2 } else { 1 } {
        let sent = wss_rpc(&mut rpc, 4+turn, "agent.sendMessage", json!({"workspaceId":ws_id,"agentId":agent_id,"content":format!("sandbox turn {turn}")})).await;
        assert_eq!(sent["success"], true);
        let mut events = drain_until_stream_end(&mut sub, agent_id).await;
        if !succeeds {
            while events_of(&events, "agent:failed").is_empty() {
                events.push(wss_event(&mut sub, 30).await);
            }
            let failed = events_of(&events, "agent:failed");
            assert!(failed[0]["data"]["error"].as_str().is_some());
        } else {
            assert!(
                events_of(&events, "agent:failed").is_empty(),
                "unexpected failure: {events:?}"
            );
            let conversation = wss_rpc(
                &mut rpc,
                20 + turn,
                "agent.getConversation",
                json!({"workspaceId":ws_id,"agentId":agent_id}),
            )
            .await;
            assert_eq!(
                conversation["messages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|message| message["role"] == "user")
                    .count(),
                usize::try_from(turn + 1).unwrap(),
                "fallback must not duplicate user messages"
            );
            assert!(
                conversation
                    .to_string()
                    .contains(&format!("effective-sandbox={}", expected.last().unwrap())),
                "missing effective mode: {conversation}"
            );
        }
    }
    let log: Vec<Value> = std::fs::read_to_string(prompts)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let actual: Vec<&str> = log
        .iter()
        .map(|v| v["sandboxMode"].as_str().unwrap())
        .collect();
    assert_eq!(
        actual, expected,
        "exact bounded prompt attempts and session persistence"
    );
    let changes: Vec<Value> = std::fs::read_to_string(modes)
        .unwrap_or_default()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let changed: Vec<&str> = changes.iter().filter_map(|v| v["mode"].as_str()).collect();
    let unique: std::collections::HashSet<_> = changed.iter().collect();
    assert_eq!(
        unique.len(),
        changed.len(),
        "each mode set at most once: {changed:?}"
    );
}

#[tokio::test]
async fn codex_policy_falls_back_to_workspace_write_and_retains_it() {
    scenario(
        json!({"allowed":["workspace-write","read-only"]}),
        "codex",
        None,
        &["agent-full-access", "workspace-write", "workspace-write"],
        true,
    )
    .await;
}

#[tokio::test]
async fn codex_full_access_stays_when_permitted() {
    scenario(
        json!({"allowed":["agent-full-access","workspace-write","read-only"]}),
        "codex",
        None,
        &["agent-full-access", "agent-full-access"],
        true,
    )
    .await;
}

#[tokio::test]
async fn codex_unavailable_workspace_falls_back_to_read_only() {
    scenario(json!({"allowed":["workspace-write","read-only"],"advertised":["agent-full-access","read-only"]}), "codex", None,
        &["agent-full-access","read-only","read-only"], true).await;
}

#[tokio::test]
async fn codex_policy_only_permits_read_only() {
    scenario(
        json!({"allowed":["read-only"]}),
        "codex",
        None,
        &["agent-full-access", "read-only", "read-only"],
        true,
    )
    .await;
}

#[tokio::test]
async fn codex_workspace_prompt_rejection_advances_to_read_only() {
    scenario(json!({"allowed":["workspace-write","read-only"],"allowedByMode":{"workspace-write":["read-only"]}}), "codex", None,
        &["agent-full-access","workspace-write","read-only","read-only"], true).await;
}

#[tokio::test]
async fn codex_workspace_mode_change_rejection_advances_to_read_only() {
    scenario(
        json!({"allowed":["workspace-write","read-only"],"rejectSetMode":["workspace-write"]}),
        "codex",
        None,
        &["agent-full-access", "read-only", "read-only"],
        true,
    )
    .await;
}

#[tokio::test]
async fn codex_no_supported_fallback_stops() {
    scenario(
        json!({"allowed":["workspace-write","read-only"],"advertised":["agent-full-access"]}),
        "codex",
        None,
        &["agent-full-access"],
        false,
    )
    .await;
}

#[tokio::test]
async fn codex_both_fallbacks_rejected_stops_without_loop() {
    scenario(json!({"allowed":["workspace-write","read-only"],"allowedByMode":{"workspace-write":["read-only"],"read-only":[]}}), "codex", None,
        &["agent-full-access","workspace-write","read-only"], false).await;
}

#[tokio::test]
async fn codex_unrelated_error_does_not_retry() {
    scenario(
        json!({"allowed":["workspace-write","read-only"],"unrelated":true}),
        "codex",
        None,
        &["agent-full-access"],
        false,
    )
    .await;
}

#[tokio::test]
async fn codex_output_prevents_replay() {
    scenario(
        json!({"allowed":["workspace-write","read-only"],"outputBeforeError":true}),
        "codex",
        None,
        &["agent-full-access"],
        false,
    )
    .await;
}

#[tokio::test]
async fn other_provider_does_not_use_codex_fallback() {
    scenario(
        json!({"allowed":["workspace-write","read-only"]}),
        "mock",
        None,
        &["agent-full-access"],
        false,
    )
    .await;
}

#[tokio::test]
async fn explicit_read_only_remains_read_only() {
    scenario(
        json!({"allowed":["workspace-write","read-only"]}),
        "codex",
        Some("read-only"),
        &["read-only", "read-only"],
        true,
    )
    .await;
}

#[tokio::test]
async fn explicit_workspace_write_never_tries_full_access() {
    scenario(
        json!({"allowed":["workspace-write","read-only"]}),
        "codex",
        Some("workspace-write"),
        &["workspace-write", "workspace-write"],
        true,
    )
    .await;
}

#[tokio::test]
async fn client_request_prevents_replay_even_without_output() {
    scenario(
        json!({"allowed":["workspace-write","read-only"],"clientCallBeforeError":true}),
        "codex",
        None,
        &["agent-full-access"],
        false,
    )
    .await;
}

#[tokio::test]
async fn read_only_fallback_denies_sandbox_escape_and_client_writes() {
    scenario(
        json!({"allowed":["read-only"],"probeRestrictions":true}),
        "codex",
        None,
        &["agent-full-access", "read-only", "read-only"],
        true,
    )
    .await;
}

#[tokio::test]
async fn rejected_explicit_read_only_does_not_escalate() {
    scenario(
        json!({"allowed":["workspace-write"]}),
        "codex",
        Some("read-only"),
        &["read-only"],
        false,
    )
    .await;
}

#[tokio::test]
async fn output_during_mode_change_prevents_replay() {
    scenario(
        json!({"allowed":["workspace-write","read-only"],"outputOnSetMode":true}),
        "codex",
        None,
        &["agent-full-access"],
        false,
    )
    .await;
}
