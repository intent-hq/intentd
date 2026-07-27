//! WSS e2e regression for the cross-provider `agent.setModel` history replay.
//!
//! When `agent.setModel` switches an agent to a model from a DIFFERENT
//! provider (a compound id like `grok:grok-4-fast` while running on `mock`),
//! the next turn must:
//!
//! 1. Tear down the old provider child and spawn the new provider's binary
//!    (`ensure_started`'s provider-change respawn).
//! 2. Open a fresh `session/new` on the new child — the old ACP session
//!    cannot be resumed across providers (the mock advertises
//!    `loadSession: false`, so the resume-impossible recreate path runs).
//! 3. Prepend the prior conversation history as `<supervisor>` XML to the
//!    first prompt of the recreated session, so the new provider has context.
//!
//! The "other" provider is `grok`, resolved hermetically via a
//! `providers.paths` override in `config.toml` pointing at a shell wrapper
//! that execs the same deterministic mock fixture — so no real grok install
//! is ever picked up, and the fixture's `MOCK_AGENT_PROMPT_LOG` seam records
//! the exact prompt text the new child received.
//!
//! Gated on `node` + the mock script; skips cleanly otherwise.
//!
//! Regression for monorepo#882: the store's provider-immutability guard used
//! to reject `agent_set_model_op`'s provider reconciliation with `-32603`
//! once the first turn persisted `acp_session_id`, making this whole path
//! unreachable over the wire. The intentional switch now lands via the
//! narrow `Store::set_agent_session_model` writer.

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

/// Fixed 64-hex token, adopted by the daemon via the `INTENTD_AUTH_TOKEN` seam.
const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

/// Live `intentd serve` process; killed and its data dir removed on drop.
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
    let dir = PathBuf::from("/tmp").join(format!("itd-xprov-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    let secrets_file = data_dir.join("secrets.json");
    common::enable_ws_api(data_dir);
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
    ws.send(Message::Text(frame.to_string()))
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
            Some(Ok(_)) => continue,
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
            Some(Ok(_)) => continue,
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

/// Parse the mock fixture's prompt log: one `{ turn, text }` JSON per line.
fn read_prompt_log(path: &Path) -> Vec<(u64, String)> {
    let raw = std::fs::read_to_string(path).expect("read prompt log");
    raw.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            let v: Value = serde_json::from_str(l).expect("prompt log line json");
            (
                v["turn"].as_u64().expect("turn"),
                v["text"].as_str().expect("text").to_string(),
            )
        })
        .collect()
}

/// Write an executable shell wrapper that execs the mock fixture under `node`,
/// discarding whatever base args the daemon passes for the impersonated
/// provider (grok's `agent stdio`) — the fixture speaks ACP on stdio and
/// ignores argv anyway. Returns the wrapper's absolute path.
fn write_provider_wrapper(data_dir: &Path, script: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let node = intent_providers::resolve_on_path("node").expect("node on PATH (gated)");
    let wrapper = data_dir.join("fake-grok");
    std::fs::write(
        &wrapper,
        format!("#!/bin/sh\nexec \"{}\" \"{}\"\n", node.display(), script),
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
    text.push_str(&format!(
        "\n[providers.paths]\ngrok = \"{}\"\n",
        wrapper.display()
    ));
    std::fs::write(&path, text).expect("write config.toml");
}

/// Cross-provider `agent.setModel` regression: switching a live `mock` agent
/// to `grok:grok-4-fast` must respawn onto the (hermetically wrapped) grok
/// binary, open a fresh `session/new` there, and prepend the prior
/// conversation as `<supervisor>` XML to the first prompt of the recreated
/// session — with the history replay firing exactly once. Regression for
/// monorepo#882 (the switch used to be rejected after the first turn).
#[tokio::test]
async fn cross_provider_set_model_replays_history_as_supervisor_xml() {
    let Some(script) = gate("WSS cross-provider history E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let wrapper = write_provider_wrapper(&data_dir, &script);
    seed_grok_path_override(&data_dir, &wrapper);
    let prompt_log = data_dir.join("prompt-log.jsonl");
    let prompt_log_str = prompt_log.to_string_lossy().into_owned();
    // Distinctive assistant text so the replayed history provably carries the
    // FIRST session's exchange, not just the user message.
    let behavior = json!({ "response": "XPROV_E2E_ASSISTANT_REPLY" }).to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_PROMPT_LOG", &prompt_log_str),
    ];
    let child = spawn_serve(&data_dir, &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
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
        json!({ "title": "XProv History E2E", "noPrompt": true }),
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

    // RPC conn — create the agent on the mock provider and run turn 1.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({
            "workspaceId": ws_id,
            "name": "XProv",
            "model": "mock:default",
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
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "XPROV_FIRST_USER_TURN" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    await_stream_end(&mut sub, &agent_id).await;

    // Cross-provider switch: a compound id from a DIFFERENT provider.
    let set = wss_rpc(
        &mut rpc,
        12,
        "agent.setModel",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "modelId": "grok:grok-4-fast" }),
    )
    .await;
    assert_eq!(set["success"], true, "setModel ok: {set}");

    // Turn 2 lands on the new provider's fresh session.
    let sent2 = wss_rpc(
        &mut rpc,
        13,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "XPROV_SECOND_USER_TURN" }),
    )
    .await;
    assert_eq!(sent2["success"], true, "second sendMessage ok: {sent2}");
    await_stream_end(&mut sub, &agent_id).await;

    // agent.get reflects the switched compound id.
    let got = wss_rpc(
        &mut rpc,
        14,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        got["agent"]["model"].as_str(),
        Some("grok:grok-4-fast"),
        "agent.get shows the cross-provider model: {got}"
    );

    // The switched turn persists the informational model-change transcript
    // notice: a `role: "system"` row with `model_changed` metadata naming
    // both provider identities (mock → grok).
    let full = wss_rpc(
        &mut rpc,
        15,
        "agent.getSession",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let messages = full["session"]["messages"]
        .as_array()
        .expect("session messages");
    let notices: Vec<&Value> = messages
        .iter()
        .filter(|m| m["metadata"]["type"] == "model_changed")
        .collect();
    assert_eq!(
        notices.len(),
        1,
        "exactly one model-change notice: {messages:?}"
    );
    let notice = notices[0];
    assert_eq!(notice["role"], "system", "notice is a system row: {notice}");
    assert_eq!(
        notice["metadata"]["fromProvider"], "mock",
        "notice names the old provider: {notice}"
    );
    assert_eq!(
        notice["metadata"]["toProvider"], "grok",
        "notice names the new provider: {notice}"
    );

    // Both children appended to the same prompt log; each fresh process starts
    // its own turn counter at 1.
    let log = read_prompt_log(&prompt_log);
    assert!(
        log.len() >= 2,
        "expected prompts from both provider children, got {}: {log:?}",
        log.len()
    );

    // Turn 1 (old mock child): plain first prompt, no history replay.
    let (first_turn, first_text) = &log[0];
    assert_eq!(
        *first_turn, 1,
        "first logged prompt is the old child's turn 1"
    );
    assert!(
        first_text.ends_with("XPROV_FIRST_USER_TURN"),
        "turn 1 carries the first user message: {first_text:?}"
    );
    assert!(
        !first_text.contains("<supervisor>"),
        "no history replay before the switch: {first_text:?}"
    );

    // Next prompt (new grok-wrapper child): its OWN turn 1 — proof the switch
    // respawned a fresh process and opened a fresh session — carrying the
    // prior history as <supervisor> XML ahead of the new user content.
    let (second_turn, second_text) = &log[1];
    assert_eq!(
        *second_turn, 1,
        "post-switch prompt is turn 1 of a FRESH child (respawn + session/new): {log:?}"
    );
    assert!(
        second_text.contains("<supervisor>") && second_text.contains("</supervisor>"),
        "recreated session's first prompt must wrap history in <supervisor> XML: {second_text:?}"
    );
    assert!(
        second_text.contains("The previous ACP session was lost"),
        "supervisor preamble present: {second_text:?}"
    );
    assert!(
        second_text.contains("XPROV_FIRST_USER_TURN")
            && second_text.contains("XPROV_E2E_ASSISTANT_REPLY"),
        "replayed history carries BOTH sides of the first exchange: {second_text:?}"
    );
    let close = second_text
        .find("</supervisor>")
        .expect("closing </supervisor>");
    assert!(
        second_text
            .find("XPROV_SECOND_USER_TURN")
            .expect("second user message present")
            > close,
        "new user content follows the </supervisor> block: {second_text:?}"
    );

    // The replay fires exactly once: any later prompt must not repeat it.
    for (turn, text) in &log[2..] {
        assert!(
            !text.contains("<supervisor>"),
            "history replay must fire exactly once; repeated on turn {turn}: {text:?}"
        );
    }
}
