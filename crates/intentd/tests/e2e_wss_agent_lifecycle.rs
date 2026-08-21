//! WSS end-to-end agent lifecycle (WSS-1): the UDS analogue in
//! `uds_agent_runtime.rs` ported to the WebSocket transport.
//!
//! Boots a real `intentd serve` (WSS listener enabled via config) against the mock ACP provider and
//! drives the full agent lifecycle over a pinned TLS WebSocket — one persistent
//! SUBSCRIBER connection (events.event notifications) and one RPC connection
//! (request/response). Mirrors the lifecycle assertions of the UDS suite and
//! the §5.14 `locality == "remote"` guarantee from `wss_integration.rs`.
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
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::{sleep, timeout};
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
        // Kill the whole process group (daemon + any Node.js ACP provider
        // children) BEFORE removing the data dir, so an orphaned child can't
        // re-create files (e.g. node-compile-cache under a redirected TMPDIR)
        // after cleanup. The daemon is spawned with process_group(0).
        #[cfg(unix)]
        {
            use nix::sys::signal::{self, Signal};
            use nix::unistd::Pid;
            let pid = Pid::from_raw(self.child.id().cast_signed());
            let _ = signal::killpg(pid, Signal::SIGKILL);
        }
        #[cfg(not(unix))]
        {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-{}", &id[..8]));
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
    // Group leader so Daemon::drop can killpg the daemon + ACP children.
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd.spawn().expect("spawn intentd serve")
}

/// Wait for the daemon's UDS to accept connections, up to the shared
/// daemon-startup budget (see `common::daemon_startup_timeout`).
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

async fn tls_connect(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> tokio_rustls::client::TlsStream<TcpStream> {
    common::tls_connect_with_retry(port, cfg).await
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

/// Variant of `wss_event` that returns `None` on timeout instead of panicking,
/// for tests that assert an event stream stayed silent (e.g. no-prompt initial
/// agent must not emit any `agent:stream:*` frame). Uses a single deadline so
/// periodic non-`events.event` frames (heartbeat `Ping`, unrelated pushes) do
/// not reset the wait window and hide silence-violations.
async fn wss_event_opt<S>(ws: &mut WebSocketStream<S>, secs: u64) -> Option<Value>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    wss_event_opt_until(ws, deadline).await
}

/// Variant of `wss_event_opt` bounded by an absolute deadline, for loops that
/// share one hard deadline across many reads (no per-call truncation to whole
/// seconds, no stale `remaining` snapshots).
async fn wss_event_opt_until<S>(
    ws: &mut WebSocketStream<S>,
    deadline: tokio::time::Instant,
) -> Option<Value>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let remaining = match deadline.checked_duration_since(tokio::time::Instant::now()) {
            Some(d) if !d.is_zero() => d,
            _ => return None,
        };
        let Ok(next) = timeout(remaining, ws.next()).await else {
            return None;
        };
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == "events.event" {
                    return Some(v);
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

/// Read one `subscription.push` notification from a connection (bounded). Used
/// to read a channel's seq-0 snapshot after `chat.subscribe`.
async fn wss_push<S>(ws: &mut WebSocketStream<S>, secs: u64) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let next = timeout(Duration::from_secs(secs), ws.next())
            .await
            .expect("wss push timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == "subscription.push" {
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

/// Mock-agent gate (parity with the UDS E2E suite).
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

const MARKER: &str = "MCP_TOOL_MARKER_wss_e2e";

/// Full agent lifecycle over WSS (steps 1–9 of the task note): events.subscribe
/// + client.hello → agent.create → agent.sendMessage → assert ≥1 chunk + one
/// terminal stream:end + ≥1 note:updated → note.get sees the MCP-mutated body →
/// agent.list reports an assistant message persisted.
#[tokio::test]
async fn mock_agent_full_turn_over_wss() {
    let Some(script) = gate("WSS full-turn E2E") else {
        return;
    };

    // Pre-seed the daemon's DB with a workspace + target note (the daemon opens
    // this same data dir on launch). The store is closed before the daemon
    // process starts so it gets a clean handle. Mirrors the UDS analogue.
    let data_dir = temp_data_dir();
    let (ws_id, note_id) = seed_workspace_and_note(&data_dir).await;
    // Post-WSAPI-8: agent-supplied JS via `workspace_api` replaces the
    // discrete `add_to_note` tool.
    let js = format!(
        "return await ws.note.add({}, {{ content: {} }});",
        json!(note_id),
        json!(MARKER),
    );
    // The response's first line completes (newline) while its second line
    // never gets one: the mid-turn activity preview must clip at the newline
    // and the terminal stream:end must carry the full-text derivation.
    let behavior = json!({
        "toolCall": {
            "name": "workspace_api",
            "arguments": { "code": js, "summary": "WSS E2E ws.note.add" },
        },
        "response": "first line done\nadded via mcp over wss",
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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

    // SUBSCRIBER conn — events.subscribe BEFORE the turn so we miss nothing.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*", "note:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // RPC conn — assert §5.14 remote locality, then drive the turn.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let hello = wss_rpc(
        &mut rpc,
        10,
        "client.hello",
        json!({ "clientId": "wss-e2e", "name": "WSS E2E" }),
    )
    .await;
    assert_eq!(
        hello["server"]["locality"], "remote",
        "WSS ⇒ remote (§5.14)"
    );

    let created = wss_rpc(
        &mut rpc,
        11,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "WSS", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    let sent = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "please add" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Collect events until terminal: ≥1 chunk, exactly one stream:end, ≥1
    // note:updated (from the MCP tool's domain event). Also record every
    // `agent:stream:status` frame (STAT-1 / PROTOCOL §6.5) with its arrival
    // ordinal so we can assert the pre-first-token status hints land on the
    // real WSS transport AND arrive before the first `agent:stream:activity`.
    let mut chunks = 0u32;
    let mut ends = 0u32;
    let mut saw_note_updated = false;
    let mut first_chunk_at: Option<usize> = None;
    let mut first_activity_frame: Option<Value> = None;
    let mut end_frame: Option<Value> = None;
    let mut status_frames: Vec<(usize, Value)> = Vec::new();
    for i in 0..80 {
        let frame = wss_event(&mut sub, 30).await;
        match frame["params"]["event"]["type"].as_str() {
            Some("agent:stream:activity") => {
                chunks += 1;
                if first_chunk_at.is_none() {
                    first_chunk_at = Some(i);
                    first_activity_frame = Some(frame["params"]["event"].clone());
                }
            }
            Some("agent:stream:end") => {
                ends += 1;
                end_frame = Some(frame["params"]["event"].clone());
                break;
            }
            Some("agent:stream:status") => {
                status_frames.push((i, frame["params"]["event"].clone()));
            }
            Some("note:updated") => saw_note_updated = true,
            _ => {}
        }
    }
    assert!(chunks >= 1, "at least one agent:stream:activity over WSS");
    assert_eq!(ends, 1, "exactly one terminal agent:stream:end over WSS");
    assert!(
        saw_note_updated,
        "tool's note:updated domain event delivered over WSS"
    );

    // Live-preview enrichment: the activity signal carries the server-derived
    // `lastAgentResponse` (the mock streams its text response before the
    // first activity emit) but never raw transcript `content`. Mid-turn the
    // preview is clipped at the last newline — only the completed first line
    // surfaces, never the still-streaming second line; the terminal
    // stream:end carries the final (full-text) preview values.
    let activity = first_activity_frame.expect("first activity frame captured");
    let activity_data = &activity["data"];
    assert_eq!(
        activity_data["agentId"].as_str(),
        Some(agent_id.as_str()),
        "activity carries the agent id: {activity}"
    );
    assert!(
        activity_data.get("content").is_none(),
        "activity payload never carries transcript content: {activity}"
    );
    assert_eq!(
        activity_data["lastAgentResponse"].as_str(),
        Some("first line done"),
        "mid-turn activity preview clips at the last newline: {activity}"
    );
    let end = end_frame.expect("terminal stream:end frame captured");
    assert_eq!(
        end["data"]["lastAgentResponse"].as_str(),
        Some("added via mcp over wss"),
        "terminal stream:end carries the final lastAgentResponse: {end}"
    );

    // STAT-1 — pre-first-token status hints must reach the FE over the real
    // WSS transport and MUST all arrive before the first `agent:stream:activity`
    // (that's the whole point: they populate the spinner *before* streaming
    // starts, then the chunk-reducer clears them). The daemon emits at four
    // real turn-startup transitions for a brand-new agent's first turn:
    // `launch` (spawn), `init` (handshake), `session-create` (session/new),
    // `prompt` (session/prompt). At least the last one — `prompt` — MUST land
    // before the first chunk on the same subscription.
    let first_chunk_at =
        first_chunk_at.expect("at least one agent:stream:activity observed to anchor ordering");
    assert!(
        !status_frames.is_empty(),
        "expected >=1 agent:stream:status frame over WSS before the first chunk"
    );
    for (ord, ev) in &status_frames {
        assert!(
            *ord < first_chunk_at,
            "agent:stream:status at ordinal {ord} arrived AT/AFTER first agent:stream:activity \
             (ordinal {first_chunk_at}) -- startup hints MUST precede streaming: {ev}"
        );
        let data = &ev["data"];
        assert_eq!(
            data["agentId"].as_str(),
            Some(agent_id.as_str()),
            "agent:stream:status.agentId must match: {ev}"
        );
        assert_eq!(
            data["workspaceId"].as_str(),
            Some(ws_id.as_str()),
            "agent:stream:status.workspaceId must match (self-sufficient payload sec 6.7): {ev}"
        );
        assert!(
            data["phase"].as_str().is_some(),
            "agent:stream:status.phase required: {ev}"
        );
        assert!(
            data["message"].as_str().is_some(),
            "agent:stream:status.message required: {ev}"
        );
        assert!(
            data["level"].as_str().is_some(),
            "agent:stream:status.level required: {ev}"
        );
        assert!(
            data["timestamp"].as_u64().is_some(),
            "agent:stream:status.timestamp (epoch-ms) required: {ev}"
        );
    }
    let phases: Vec<&str> = status_frames
        .iter()
        .filter_map(|(_, e)| e["data"]["phase"].as_str())
        .collect();
    assert!(
        phases.contains(&"prompt"),
        "expected a 'prompt' status hint (\"Sent prompt\u{2026}\") before the first chunk; \
         observed phases: {phases:?}"
    );

    // BE state mutated via the real agent→BE MCP loop.
    let note = wss_rpc(
        &mut rpc,
        13,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    assert!(
        note["note"]["content"]
            .as_str()
            .unwrap_or_default()
            .contains(MARKER)
            || note["content"]
                .as_str()
                .unwrap_or_default()
                .contains(MARKER),
        "note mutated by the daemon-spawned MCP tool call over WSS: {note}"
    );

    // Assistant message persisted (AgentLite messageCount ≥ 1).
    let list = wss_rpc(&mut rpc, 14, "agent.list", json!({ "workspaceId": ws_id })).await;
    let agents = list["agents"].as_array().expect("agents array");
    let listed = agents
        .iter()
        .find(|a| a["id"] == json!(agent_id))
        .expect("created agent listed");
    let mc = listed["messageCount"].as_u64().unwrap_or(0);
    assert!(mc >= 1, "assistant message persisted (messageCount={mc})");
}

/// Abnormal finish reason (PROTOCOL §7): a turn resolving with a
/// non-`end_turn` stop reason (`refusal` here) durably tags the assistant row
/// with `metadata.finishReason` — visible on the `agent.getConversation`
/// transcript after the turn — and the terminal `agent:stream:end` carries
/// the same `finishReason` live. The turn stays a completion: `agent:idle`
/// fires (carrying its existing `finishReason` lifecycle field) and
/// `agent:failed` never does.
#[tokio::test]
async fn abnormal_finish_reason_persists_on_transcript_over_wss() {
    let Some(script) = gate("WSS abnormal finishReason E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let behavior = json!({
        "response": "refusing to continue",
        "stopReason": "refusal",
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "WSS-FINISH", "model": "mock:default" }),
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
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "do something" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Terminal surface: stream:end carries finishReason, agent:idle follows,
    // agent:failed never fires — an abnormal stop reason is a completion.
    let mut end_frame: Option<Value> = None;
    let mut idle_frame: Option<Value> = None;
    for _ in 0..80 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        match event["type"].as_str() {
            Some("agent:stream:end") => {
                end_frame = Some(event.clone());
            }
            Some("agent:idle") => {
                idle_frame = Some(event.clone());
                break;
            }
            Some("agent:failed") => {
                panic!("agent:failed emitted for an abnormal (refusal) completion: {frame}");
            }
            _ => {}
        }
    }
    let end = end_frame.expect("terminal agent:stream:end observed");
    assert_eq!(
        end["data"]["finishReason"].as_str(),
        Some("refusal"),
        "terminal stream:end carries the abnormal finishReason: {end}"
    );
    let message_id = end["data"]["messageId"]
        .as_str()
        .expect("stream:end carries the persisted messageId")
        .to_string();
    let idle = idle_frame.expect("agent:idle observed");
    assert_eq!(
        idle["data"]["finishReason"].as_str(),
        Some("refusal"),
        "agent:idle lifecycle finishReason: {idle}"
    );

    // Durable half: the transcript row carries metadata.finishReason so a
    // reloading client can render the ending without having seen the event.
    let convo = wss_rpc(
        &mut rpc,
        12,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let messages = convo["messages"].as_array().expect("messages array");
    let assistant = messages
        .iter()
        .find(|m| m["id"] == json!(message_id))
        .expect("assistant row from the turn present in the transcript");
    assert_eq!(assistant["role"], json!("assistant"));
    assert_eq!(
        assistant["metadata"]["finishReason"].as_str(),
        Some("refusal"),
        "assistant row durably tagged with the abnormal finishReason: {assistant}"
    );
}

/// intent-hq/monorepo#2669 over the real WSS wire: a turn that resolves a
/// clean `end_turn` after a sustained stream-silence tail (the incident
/// signature of a silently-truncated turn under session bloat) gets the
/// advisory annotation — the terminal `agent:idle` carries `silentTailMs` +
/// `suspectedTruncated: true`, `agent.diagnostics` reports the row's
/// `lastTurnSilentTailMs` and raises the `long-silent-tail` stuck-risk — while
/// a healthy quick turn carries none of it. Also asserts the field stays off
/// the hot `agent.get` / `agent.list` payloads (diagnostics-only by design).
/// The suspect threshold is lowered via `INTENTD_SILENT_TAIL_SUSPECT_MS` so
/// the mock's parked tail crosses it without a minutes-long test.
#[tokio::test]
async fn silent_tail_annotation_and_diagnostics_over_wss() {
    let Some(script) = gate("WSS silent-tail annotation E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    // The stalled prompt streams a chunk, then parks in total silence before
    // resolving `end_turn`; the quick prompt resolves immediately. Margins
    // are ~1 s of tolerance each way for saturated CI runners: the stalled
    // side parks 1 s past the threshold, the quick side must merely resolve
    // in under 2 s.
    let behavior = json!({
        "response": "stalled reply",
        "silentTailBeforeResultMs": 3000,
        "rules": [
            { "ifPromptContains": "quick", "response": "quick reply" },
        ],
    })
    .to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("INTENTD_SILENT_TAIL_SUSPECT_MS", "2000"),
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
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let stalled = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Stalled", "model": "mock:default" }),
    )
    .await;
    let stalled_id = stalled["agent"]["id"]
        .as_str()
        .expect("stalled id")
        .to_string();
    let quick = wss_rpc(
        &mut rpc,
        11,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Quick", "model": "mock:default" }),
    )
    .await;
    let quick_id = quick["agent"]["id"].as_str().expect("quick id").to_string();

    // Drive the stalled turn: chunk → 1.5s silent tail → clean end_turn.
    let sent = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": stalled_id, "content": "go stall" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    let mut stalled_idle: Option<Value> = None;
    for _ in 0..80 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        if event["type"] == json!("agent:idle")
            && event["data"]["agentId"].as_str() == Some(stalled_id.as_str())
        {
            stalled_idle = Some(event.clone());
            break;
        }
    }
    let idle = stalled_idle.expect("stalled agent:idle observed");
    assert_eq!(
        idle["data"]["finishReason"].as_str(),
        Some("end_turn"),
        "the truncation-suspect turn still completes normally: {idle}"
    );
    assert_eq!(
        idle["data"]["suspectedTruncated"],
        json!(true),
        "agent:idle carries suspectedTruncated: {idle}"
    );
    let tail_ms = idle["data"]["silentTailMs"]
        .as_u64()
        .expect("agent:idle carries silentTailMs");
    assert!(
        tail_ms >= 2000,
        "tail past the lowered threshold: {tail_ms}"
    );

    // Drive the healthy quick turn: no annotation on its idle.
    let sent = wss_rpc(
        &mut rpc,
        13,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": quick_id, "content": "quick please" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    let mut quick_idle: Option<Value> = None;
    for _ in 0..80 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        if event["type"] == json!("agent:idle")
            && event["data"]["agentId"].as_str() == Some(quick_id.as_str())
        {
            quick_idle = Some(event.clone());
            break;
        }
    }
    let idle = quick_idle.expect("quick agent:idle observed");
    assert!(
        idle["data"].get("suspectedTruncated").is_none()
            && idle["data"].get("silentTailMs").is_none(),
        "healthy turn's idle omits the annotation (absent, never false): {idle}"
    );

    // Diagnostics: both rows carry lastTurnSilentTailMs; only the stalled
    // agent raises the long-silent-tail stuck-risk.
    let diag = wss_rpc(
        &mut rpc,
        20,
        "agent.diagnostics",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    let agents = diag["diagnostics"]["agents"].as_array().expect("agents");
    let row_for = |id: &str| {
        agents
            .iter()
            .find(|a| a["id"].as_str() == Some(id))
            .expect("agent row in diagnostics")
    };
    let stalled_tail = row_for(&stalled_id)["lastTurnSilentTailMs"]
        .as_u64()
        .expect("stalled row carries lastTurnSilentTailMs");
    assert!(stalled_tail >= 2000, "stalled tail: {stalled_tail}");
    let quick_tail = row_for(&quick_id)["lastTurnSilentTailMs"]
        .as_u64()
        .expect("quick row carries lastTurnSilentTailMs");
    assert!(quick_tail < 2000, "quick tail stays short: {quick_tail}");
    let risks = diag["diagnostics"]["stuckRisks"]
        .as_array()
        .expect("stuckRisks");
    let long: Vec<&Value> = risks
        .iter()
        .filter(|r| r["type"] == json!("long-silent-tail"))
        .collect();
    assert_eq!(
        long.len(),
        1,
        "exactly one long-silent-tail risk: {risks:?}"
    );
    assert_eq!(long[0]["agentId"], json!(stalled_id), "risk: {:?}", long[0]);
    assert_eq!(long[0]["silentTailMs"], json!(stalled_tail));
    assert_eq!(long[0]["thresholdMs"], json!(2000));

    // Diagnostics-only by design: the hot payloads never carry the field.
    let got = wss_rpc(&mut rpc, 30, "agent.get", json!({ "agentId": stalled_id })).await;
    assert!(
        got["agent"].get("lastTurnSilentTailMs").is_none(),
        "agent.get omits lastTurnSilentTailMs: {}",
        got["agent"]
    );
    let list = wss_rpc(&mut rpc, 31, "agent.list", json!({ "workspaceId": ws_id })).await;
    for row in list["agents"].as_array().expect("agents array") {
        assert!(
            row.get("lastTurnSilentTailMs").is_none(),
            "agent.list omits lastTurnSilentTailMs: {row}"
        );
    }
}

/// STAB-156 — workspace-MCP delivery via ACP session setup (`session/new`
/// `mcpServers`), the wire path claude-code/codex/droid/grok use. Same full
/// turn as
/// [`mock_agent_full_turn_over_wss`], but `MOCK_AGENT_SESSION_MCP=1` flips the
/// mock provider to `supports_session_mcp_servers` with NO `--mcp-config`
/// flag: the only way the mock child can reach the workspace bridge is the
/// `mcpServers` array the daemon put in the `session/new` request. The mock
/// spawns the bridge command from that entry and mutates a note through it, so
/// a successful marker assertion proves the field rode the real WSS→daemon→
/// ACP wire and the bridge endpoint it carried actually works.
#[tokio::test]
async fn mock_agent_full_turn_over_wss_with_session_mcp_servers() {
    let Some(script) = gate("WSS session-mcpServers E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let (ws_id, note_id) = seed_workspace_and_note(&data_dir).await;
    let js = format!(
        "return await ws.note.add({}, {{ content: {} }});",
        json!(note_id),
        json!(MARKER),
    );
    let behavior = json!({
        "toolCall": {
            "name": "workspace_api",
            "arguments": { "code": js, "summary": "WSS session-mcp E2E ws.note.add" },
        },
        "response": "added via session/new mcpServers",
    })
    .to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_SESSION_MCP", "1"),
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
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*", "note:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        11,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "WSS-SessionMCP", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    let sent = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "please add" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Wait for the terminal stream:end; the note:updated event proves the MCP
    // tool call went through the bridge the session/new request delivered.
    let mut ends = 0u32;
    let mut saw_note_updated = false;
    for _ in 0..80 {
        let frame = wss_event(&mut sub, 30).await;
        match frame["params"]["event"]["type"].as_str() {
            Some("agent:stream:end") => {
                ends += 1;
                break;
            }
            Some("note:updated") => saw_note_updated = true,
            _ => {}
        }
    }
    assert_eq!(ends, 1, "exactly one terminal agent:stream:end over WSS");
    assert!(
        saw_note_updated,
        "tool's note:updated domain event delivered over WSS"
    );

    // The note mutated — reachable ONLY through the session/new-delivered
    // bridge entry (the mock provider got no --mcp-config in this mode).
    let note = wss_rpc(
        &mut rpc,
        13,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    assert!(
        note["note"]["content"]
            .as_str()
            .unwrap_or_default()
            .contains(MARKER)
            || note["content"]
                .as_str()
                .unwrap_or_default()
                .contains(MARKER),
        "note mutated via the session/new-delivered workspace-MCP bridge: {note}"
    );
}

#[allow(clippy::similar_names)] // deliberate parallel naming across the scenario's instances
/// Session-status lifecycle persistence (P0 — chat-spinner clear). A normal
/// `agent.sendMessage` turn must drive the persisted `agent_session.status`
/// through `Idle → active → idle` and emit the matching
/// `agent:status-changed` self-sufficient events (PROTOCOL §6.5/§6.7), so a
/// hydrated/reloaded chat reflects the post-turn idle state. A freshly
/// created session persists the legacy `AgentStatus::Idle` (wire form
/// `"Idle"`) for reference parity with `agent-factory.ts:435`; end-of-turn
/// then rewrites to `AgentStatus::RuntimeIdle` (wire form `"idle"`).
/// Co-emitted with `agent:idle` at turn end. The `agent:idle` payload also
/// carries `isBackground` from the session row: `false` for this normal
/// (foreground) agent, and `true` for a second agent created with
/// `isBackground: true`.
#[tokio::test]
async fn agent_session_status_persists_idle_active_idle_over_wss() {
    let Some(script) = gate("WSS status-lifecycle E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let behavior = json!({ "response": "status lifecycle ok" }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "WSS-Status", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // A brand-new agent persists as `AgentStatus::Idle` — the legacy
    // capitalized wire value `"Idle"` — for reference parity with
    // `agent-factory.ts:435`. The runtime then transitions the session
    // through `active` and rewrites the terminal state to `RuntimeIdle`
    // (lowercase `"idle"`) as the turn ends.
    let pre = wss_rpc(
        &mut rpc,
        11,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        pre["agent"]["status"], "Idle",
        "fresh agent persisted with status=Idle (legacy capitalized value, reference parity): {pre}"
    );

    let sent = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "drive a turn" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Collect status-changed transitions alongside the terminal agent:idle
    // (PROTOCOL §6.5 / §7). agent:idle is emitted from `run_prompt_turn`; the
    // matching status-changed → idle lands shortly after when the worker
    // releases the in-flight slot via `end_turn`, so we keep draining events
    // until both transitions are observed.
    let mut transitions: Vec<(String, bool)> = Vec::new();
    let mut idle_payload: Option<Value> = None;
    for _ in 0..160 {
        let frame = wss_event(&mut sub, 30).await;
        let ev = &frame["params"]["event"];
        match ev["type"].as_str() {
            Some("agent:status-changed")
                if ev["data"]["agentId"].as_str() == Some(agent_id.as_str()) =>
            {
                let s = ev["data"]["status"]
                    .as_str()
                    .expect("status string in agent:status-changed data")
                    .to_string();
                let active = ev["data"]["isActive"].as_bool().unwrap_or(false);
                transitions.push((s, active));
            }
            Some("agent:idle") if ev["data"]["agentId"].as_str() == Some(agent_id.as_str()) => {
                idle_payload = Some(ev["data"].clone());
            }
            _ => {}
        }
        if idle_payload.is_some() && transitions.contains(&("idle".to_string(), false)) {
            break;
        }
    }
    let idle = idle_payload.expect(
        "terminal agent:idle emitted at turn end (no idle observed within the event window)",
    );
    // The idle payload carries `isBackground` from the session row — `false`
    // for a normal (foreground) agent.
    assert_eq!(
        idle["isBackground"],
        json!(false),
        "agent:idle carries isBackground=false for a foreground agent: {idle}"
    );
    // The emit-time waiting flag: this agent parents no pending completion
    // watches, so the idle payload reports `false`.
    assert_eq!(
        idle["isWaitingForOtherAgents"],
        json!(false),
        "agent:idle carries isWaitingForOtherAgents=false with no pending watches: {idle}"
    );
    // Idle-visibility: `waitingOnHooks` is stamped only when the idle agent
    // owns active background hooks — this agent owns none, so the field is
    // omitted entirely (absent, never `[]`).
    assert!(
        idle.get("waitingOnHooks").is_none(),
        "agent:idle omits waitingOnHooks when the agent owns no active hook: {idle}"
    );
    // Idle-visibility (unified external-wait): same omission rule for
    // `waitingOnPrMonitors` — this agent owns no active PR monitor.
    assert!(
        idle.get("waitingOnPrMonitors").is_none(),
        "agent:idle omits waitingOnPrMonitors when the agent owns no active monitor: {idle}"
    );
    assert!(
        transitions.contains(&("active".to_string(), true)),
        "saw active/isActive=true transition (got {transitions:?})"
    );
    assert!(
        transitions.contains(&("idle".to_string(), false)),
        "saw idle/isActive=false transition (got {transitions:?})"
    );

    // The persisted row reflects the post-turn idle state (hydration parity).
    let post = wss_rpc(
        &mut rpc,
        13,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        post["agent"]["status"], "idle",
        "agent_session.status persisted idle after turn: {post}"
    );
    assert_eq!(
        post["agent"]["isActive"], false,
        "agent_session.is_active cleared after turn: {post}"
    );

    // Background counterpart: an agent created with `isBackground: true`
    // (G-A1/P3-1.2c) must emit its terminal `agent:idle` with
    // `isBackground: true` so subscribers (e.g. notification routing) can
    // branch without a follow-up `agent.get`.
    let bg_created = wss_rpc(
        &mut rpc,
        14,
        "agent.create",
        json!({
            "workspaceId": ws_id,
            "name": "WSS-Status-BG",
            "model": "mock:default",
            "isBackground": true,
        }),
    )
    .await;
    let bg_agent_id = bg_created["agent"]["id"]
        .as_str()
        .expect("background agent id")
        .to_string();
    let bg_sent = wss_rpc(
        &mut rpc,
        15,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": bg_agent_id, "content": "drive a turn" }),
    )
    .await;
    assert_eq!(
        bg_sent["success"], true,
        "background sendMessage ok: {bg_sent}"
    );

    let mut bg_idle_payload: Option<Value> = None;
    for _ in 0..160 {
        let frame = wss_event(&mut sub, 30).await;
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:idle"
            && ev["data"]["agentId"].as_str() == Some(bg_agent_id.as_str())
        {
            bg_idle_payload = Some(ev["data"].clone());
            break;
        }
    }
    let bg_idle = bg_idle_payload.expect("background agent emitted terminal agent:idle");
    assert_eq!(
        bg_idle["isBackground"],
        json!(true),
        "agent:idle carries isBackground=true for a background agent: {bg_idle}"
    );
}

/// `agent.stop` keep-alive + resume over WSS (step 10). The first turn streams
/// "streaming-before-cancel" and parks at `session/cancel`; `agent.stop`
/// interrupts (terminal stream:end emitted, child kept alive); a follow-up
/// `agent.sendMessage` resumes the SAME child and the mock reports `turn=2`
/// (per-process counter), proving interrupt-not-kill keep-alive semantics.
#[tokio::test]
async fn agent_stop_keep_alive_resume_over_wss() {
    let Some(script) = gate("WSS agent.stop keep-alive E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let behavior = json!({ "blockUntilCancel": true, "response": "resumed" }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*", "chat:stream:delta"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "WSS", "model": "mock:default" }),
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
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "first" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // First turn streams a chunk and parks at session/cancel.
    let mut saw_block_chunk = false;
    for _ in 0..50 {
        let frame = wss_event(&mut sub, 30).await;
        if frame["params"]["event"]["type"] == "chat:stream:delta"
            && frame["params"]["event"]["data"]["content"]
                .as_str()
                .unwrap_or_default()
                .contains("streaming-before-cancel")
        {
            saw_block_chunk = true;
            break;
        }
    }
    assert!(saw_block_chunk, "first turn streamed a chunk and parked");

    // STAB-125 turn-liveness over the wire: while the turn is parked mid-flight
    // (nothing persisted for it yet), `agent.get` and `agent.getConversation`
    // must report `turnInFlight: true` with an RFC-3339 `lastStreamActivityAt`
    // — the additive fields that let a poller tell a long-but-alive turn from
    // a wedged agent.
    let got = wss_rpc(
        &mut rpc,
        20,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        got["agent"]["turnInFlight"], true,
        "mid-turn agent.get reports turnInFlight: {got}"
    );
    assert!(
        got["agent"]["lastStreamActivityAt"].is_string(),
        "mid-turn agent.get carries lastStreamActivityAt: {got}"
    );
    let conv = wss_rpc(
        &mut rpc,
        21,
        "agent.getConversation",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        conv["turnInFlight"], true,
        "mid-turn getConversation reports turnInFlight: {conv}"
    );
    assert!(
        conv["lastStreamActivityAt"].is_string(),
        "mid-turn getConversation carries lastStreamActivityAt: {conv}"
    );

    // Stop the agent mid-turn — interrupt (not kill); terminal stream:end fires
    // carrying `stopReason: "interrupted"` + the interrupted row's `messageId`
    // (distinguishable from a normal turn end, which carries neither).
    let stopped = wss_rpc(&mut rpc, 12, "agent.stop", json!({ "agentId": agent_id })).await;
    assert_eq!(stopped["success"], true, "stop ok: {stopped}");
    let mut interrupted_message_id = None;
    for _ in 0..50 {
        let frame = wss_event(&mut sub, 30).await;
        if frame["params"]["event"]["type"] == "agent:stream:end" {
            let data = &frame["params"]["event"]["data"];
            assert_eq!(
                data["agentId"].as_str().unwrap_or_default(),
                agent_id,
                "terminal stream:end carries the agent id"
            );
            assert_eq!(
                data["stopReason"], "interrupted",
                "interrupt stream:end carries stopReason: {data}"
            );
            assert_eq!(
                data["interruptReason"], "user_stop",
                "agent.stop stream:end carries interruptReason: {data}"
            );
            interrupted_message_id = Some(
                data["messageId"]
                    .as_str()
                    .unwrap_or_else(|| panic!("interrupt stream:end carries messageId: {data}"))
                    .to_string(),
            );
            break;
        }
    }
    let interrupted_message_id =
        interrupted_message_id.expect("terminal agent:stream:end emitted on stop");

    // The interrupted row persisted with `metadata.interrupted: true` under the
    // messageId the event carried.
    let conv = wss_rpc(
        &mut rpc,
        22,
        "agent.getConversation",
        json!({ "agentId": agent_id }),
    )
    .await;
    let row = conv["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .find(|m| m["id"] == interrupted_message_id.as_str())
        .unwrap_or_else(|| panic!("interrupted row persisted: {conv}"));
    assert_eq!(
        row["role"], "assistant",
        "interrupted row is assistant: {row}"
    );
    assert_eq!(
        row["metadata"]["interrupted"], true,
        "interrupted row tagged metadata.interrupted: {row}"
    );
    assert_eq!(
        row["metadata"]["interruptReason"], "user_stop",
        "interrupted row carries the machine-readable reason: {row}"
    );

    // Keep-alive: a follow-up resumes the SAME child (mock reports turn=2).
    let resumed = wss_rpc(
        &mut rpc,
        13,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "second" }),
    )
    .await;
    assert_eq!(resumed["success"], true, "resume sendMessage ok: {resumed}");

    let mut saw_resume_chunk = false;
    let mut saw_resume_end = false;
    for _ in 0..50 {
        let frame = wss_event(&mut sub, 30).await;
        match frame["params"]["event"]["type"].as_str() {
            Some("chat:stream:delta") => {
                if frame["params"]["event"]["data"]["content"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("turn=2")
                {
                    saw_resume_chunk = true;
                }
            }
            Some("agent:stream:end") => {
                // A NORMAL turn end stays `{ agentId }` — no stopReason.
                assert!(
                    frame["params"]["event"]["data"].get("stopReason").is_none(),
                    "normal stream:end carries no stopReason: {frame}"
                );
                saw_resume_end = true;
                break;
            }
            _ => {}
        }
    }
    assert!(
        saw_resume_chunk,
        "follow-up turn resumed the SAME process (mock reported turn=2)"
    );
    assert!(
        saw_resume_end,
        "resumed turn emits its own terminal stream:end"
    );

    // STAB-125: once the turn ends the liveness fields reset — turnInFlight
    // false, lastStreamActivityAt absent. Poll briefly: the worker releases
    // the busy slot just after the terminal stream:end.
    let mut reset = false;
    for i in 0..40 {
        let got = wss_rpc(
            &mut rpc,
            30 + i,
            "agent.get",
            json!({ "workspaceId": ws_id, "agentId": agent_id }),
        )
        .await;
        if got["agent"]["turnInFlight"] == false {
            assert!(
                got["agent"].get("lastStreamActivityAt").is_none(),
                "idle agent omits lastStreamActivityAt: {got}"
            );
            reset = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(reset, "turn-liveness fields reset after the turn ends");
}

/// Live-turn preview overlay over WSS: while a turn is in flight, `agent.get`
/// and `agent.list` derive `lastAgentResponse` from the live-turn slot's
/// streamed text instead of the persisted preview. The mock's first turn
/// streams "streaming-before-cancel" and parks with NOTHING persisted for the
/// turn, so a non-null `lastAgentResponse` mid-turn can only come from the
/// overlay. After the interrupted flush + a resumed turn completes, the
/// projection falls back to the newest persisted preview.
#[tokio::test]
async fn agent_lite_live_turn_preview_overlay_over_wss() {
    let Some(script) = gate("WSS live-turn preview overlay E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let behavior = json!({ "blockUntilCancel": true, "response": "resumed" }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*", "chat:stream:delta"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "WSS-Overlay", "model": "mock:default" }),
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
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "first" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // First turn streams its chunk and parks at session/cancel. The broadcast
    // carrying content is `chat:stream:delta` (the `agent:stream:activity`
    // rename left the agent:* family content-free). Hard deadline:
    // `wss_event`'s per-read window resets on every frame (heartbeat pings
    // included), which can spin past the runner's test budget on a slow
    // coverage machine instead of failing fast. On timeout, surface every
    // event observed while waiting plus the daemon log tail so a CI-only
    // failure is diagnosable from the runner output alone. No RPC in the
    // diagnostic path: after 120s of not reading `rpc` its heartbeat pongs
    // stopped, so the server may have dropped that connection already.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    let mut seen_events: Vec<Value> = Vec::new();
    loop {
        let Some(frame) = wss_event_opt_until(&mut sub, deadline).await else {
            let log = std::fs::read_to_string(data_dir.join("daemon.log")).unwrap_or_default();
            let tail = &log[log.len().saturating_sub(4000)..];
            panic!(
                "first turn did not stream its chunk within deadline;\n\
                 events seen while waiting: {seen_events:#?}\n\
                 daemon.log tail:\n{tail}"
            );
        };
        if frame["params"]["event"]["type"] == "chat:stream:delta"
            && frame["params"]["event"]["data"]["content"]
                .as_str()
                .unwrap_or_default()
                .contains("streaming-before-cancel")
        {
            break;
        }
        seen_events.push(frame["params"]["event"].clone());
    }

    // Mid-turn, nothing is persisted for this turn (and no previous turn
    // exists), so the streamed text can only be served by the live-turn
    // overlay. Poll briefly: the slot is stamped on the chunk path.
    let mut mid_turn: Option<Value> = None;
    for i in 0..40 {
        let got = wss_rpc(
            &mut rpc,
            20 + i,
            "agent.get",
            json!({ "workspaceId": ws_id, "agentId": agent_id }),
        )
        .await;
        if got["agent"]["lastAgentResponse"].as_str() == Some("streaming-before-cancel") {
            mid_turn = Some(got["agent"].clone());
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mid_turn = mid_turn.expect("mid-turn agent.get served the live-turn overlay");
    assert_eq!(
        mid_turn["isResponding"], true,
        "overlay only applies while responding: {mid_turn}"
    );
    // No digest streamed and none persisted → omitted.
    assert!(
        mid_turn.get("digest").is_none(),
        "no digest mid-turn: {mid_turn}"
    );

    // `agent.list` serves the same overlay.
    let list = wss_rpc(&mut rpc, 60, "agent.list", json!({ "workspaceId": ws_id })).await;
    let row = list["agents"]
        .as_array()
        .expect("agents array")
        .iter()
        .find(|a| a["id"] == agent_id.as_str())
        .unwrap_or_else(|| panic!("agent row in list: {list}"));
    assert_eq!(
        row["lastAgentResponse"].as_str(),
        Some("streaming-before-cancel"),
        "agent.list serves the live-turn overlay: {row}"
    );

    // Interrupt the parked turn. Hard-deadline event reads (wss_event_opt_until)
    // so a missing event fails fast instead of hanging — heartbeat pings would
    // otherwise keep resetting a per-read window.
    let stopped = wss_rpc(&mut rpc, 12, "agent.stop", json!({ "agentId": agent_id })).await;
    assert_eq!(stopped["success"], true, "stop ok: {stopped}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let frame = wss_event_opt_until(&mut sub, deadline)
            .await
            .expect("interrupt stream:end within deadline");
        if frame["params"]["event"]["type"] == "agent:stream:end" {
            break;
        }
    }

    // Wait for the worker to release the busy slot BEFORE resuming, so the
    // follow-up send takes the direct path (a send racing into the busy
    // window would be queued instead).
    let mut idle = false;
    for i in 0..100 {
        let got = wss_rpc(
            &mut rpc,
            100 + i,
            "agent.get",
            json!({ "workspaceId": ws_id, "agentId": agent_id }),
        )
        .await;
        if got["agent"]["isResponding"] == false {
            idle = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(idle, "agent released the busy slot after the interrupt");

    // Resume; the second turn completes normally with "resumed turn=2".
    let resumed = wss_rpc(
        &mut rpc,
        13,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "second" }),
    )
    .await;
    assert_eq!(resumed["success"], true, "resume sendMessage ok: {resumed}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let frame = wss_event_opt_until(&mut sub, deadline)
            .await
            .expect("resumed turn stream:end within deadline");
        if frame["params"]["event"]["type"] == "agent:stream:end" {
            break;
        }
    }

    // Once the turn ends (slot cleared, worker released) the projection is
    // back to persisted semantics: the newest persisted assistant row wins.
    let mut settled = false;
    for i in 0..100 {
        let got = wss_rpc(
            &mut rpc,
            300 + i,
            "agent.get",
            json!({ "workspaceId": ws_id, "agentId": agent_id }),
        )
        .await;
        if got["agent"]["isResponding"] == false
            && got["agent"]["lastAgentResponse"].as_str() == Some("resumed turn=2")
        {
            settled = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        settled,
        "idle agent.get serves the persisted preview of the resumed turn"
    );
}

/// Interrupt-priority delivery (PROTOCOL §5.5): `agent.sendMessage` with
/// `priority: "interrupt"` preempts a mid-turn agent instead of queueing —
/// the current turn is cancelled keep-alive (terminal `agent:stream:end`,
/// child NEVER killed) and the message streams immediately as a fresh turn on
/// the SAME session (mock reports `turn=2`; a killed/restarted child would
/// report `turn=1`). A follow-up interrupt-priority send to the then-idle
/// agent falls through to the plain send path (`turn=3`), proving the agent
/// keeps processing across interrupts without failing or restarting.
#[tokio::test]
async fn interrupt_priority_send_preempts_turn_keep_alive_over_wss() {
    let Some(script) = gate("WSS interrupt-priority sendMessage E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let behavior = json!({ "blockUntilCancel": true, "response": "resumed" }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*", "chat:stream:delta"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "WSS", "model": "mock:default" }),
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
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "first" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // First turn streams a chunk and parks at session/cancel.
    let mut saw_block_chunk = false;
    for _ in 0..50 {
        let frame = wss_event(&mut sub, 30).await;
        if frame["params"]["event"]["type"] == "chat:stream:delta"
            && frame["params"]["event"]["data"]["content"]
                .as_str()
                .unwrap_or_default()
                .contains("streaming-before-cancel")
        {
            saw_block_chunk = true;
            break;
        }
    }
    assert!(saw_block_chunk, "first turn streamed a chunk and parked");

    // Interrupt-priority send while mid-turn: NOT queued — the response shape
    // is the immediate-stream `{ success, queued: false, messageId }` (a
    // normal-priority send here would return `queued: true`).
    let interrupted = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "content": "urgent interrupt",
            "priority": "interrupt",
        }),
    )
    .await;
    assert_eq!(interrupted["success"], true, "interrupt ok: {interrupted}");
    assert_eq!(
        interrupted["queued"], false,
        "interrupt priority streams immediately, never queues: {interrupted}"
    );
    assert!(
        interrupted["messageId"].is_string(),
        "immediate delivery carries a messageId: {interrupted}"
    );

    // Preemption ordering: terminal stream:end for the cancelled first turn →
    // the interrupt message streams `turn=2` on the SAME child → its own end.
    // Regression guard: the preemption must NOT emit the STAB-28 synthetic
    // `agent:idle` (reason: "interrupted") on the wire — an interrupt that
    // carries a follow-up message is a preemption, not a settlement, and the
    // synthetic idle would wake parent completion watches mid-preemption.
    let mut saw_preempt_end = false;
    let mut saw_interrupt_chunk = false;
    let mut saw_interrupt_end = false;
    let mut interrupted_idles = 0usize;
    for _ in 0..80 {
        let frame = wss_event(&mut sub, 30).await;
        match frame["params"]["event"]["type"].as_str() {
            Some("agent:idle")
                if frame["params"]["event"]["data"]["reason"].as_str() == Some("interrupted") =>
            {
                interrupted_idles += 1;
            }
            Some("agent:stream:end") if !saw_preempt_end => {
                let data = &frame["params"]["event"]["data"];
                assert_eq!(
                    data["agentId"].as_str().unwrap_or_default(),
                    agent_id,
                    "terminal stream:end carries the agent id"
                );
                assert_eq!(
                    data["interruptReason"], "preempted_by_message",
                    "preemption stream:end carries interruptReason: {data}"
                );
                assert_eq!(
                    data["interruptedBy"],
                    json!({ "kind": "user" }),
                    "FE-originated interrupt send stamps user attribution: {data}"
                );
                saw_preempt_end = true;
            }
            Some("chat:stream:delta") => {
                if frame["params"]["event"]["data"]["content"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("turn=2")
                {
                    assert!(
                        saw_preempt_end,
                        "the interrupt turn starts only after the preempted turn's stream:end"
                    );
                    saw_interrupt_chunk = true;
                }
            }
            Some("agent:stream:end") => {
                saw_interrupt_end = true;
                break;
            }
            _ => {}
        }
    }
    assert!(
        saw_preempt_end,
        "preemption emitted the terminal stream:end"
    );
    assert!(
        saw_interrupt_chunk,
        "interrupt message ran on the SAME process (mock reported turn=2, not a turn=1 respawn)"
    );
    assert!(
        saw_interrupt_end,
        "interrupt turn emits its own terminal stream:end"
    );
    assert_eq!(
        interrupted_idles, 0,
        "interrupt-with-message preemption must not emit the synthetic \
         agent:idle (reason: interrupted) on the wire"
    );

    // Idle fall-through + liveness: another interrupt-priority send now behaves
    // like a plain send and the SAME child answers turn=3 — the agent survived
    // both interrupts (never killed, never failed, never restarted).
    let idle_send = wss_rpc(
        &mut rpc,
        13,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "content": "after interrupt",
            "priority": "interrupt",
        }),
    )
    .await;
    assert_eq!(idle_send["success"], true, "idle interrupt ok: {idle_send}");
    assert_eq!(
        idle_send["queued"], false,
        "idle interrupt streams: {idle_send}"
    );

    let mut saw_third_chunk = false;
    let mut saw_third_end = false;
    for _ in 0..80 {
        let frame = wss_event(&mut sub, 30).await;
        match frame["params"]["event"]["type"].as_str() {
            Some("chat:stream:delta") => {
                if frame["params"]["event"]["data"]["content"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("turn=3")
                {
                    saw_third_chunk = true;
                }
            }
            Some("agent:stream:end") if saw_third_chunk => {
                saw_third_end = true;
                break;
            }
            _ => {}
        }
    }
    assert!(
        saw_third_chunk,
        "post-interrupt send still reaches the SAME live child (turn=3)"
    );
    assert!(saw_third_end, "post-interrupt turn completes cleanly");
}

/// Interrupt-priority `agent.sendToTask` (PROTOCOL §5.5): a message addressed
/// to the task note's assignee with `priority: "interrupt"` preempts the
/// assignee's mid-turn stream keep-alive and delivers immediately — the same
/// never-kill semantics as `agent.sendMessage`, resolved through the task
/// assignment (`task.markAsTask` + `task.assignAgent`).
#[tokio::test]
async fn interrupt_priority_send_to_task_over_wss() {
    let Some(script) = gate("WSS interrupt-priority sendToTask E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let (ws_id, note_id) = seed_workspace_and_note(&data_dir).await;
    let behavior = json!({ "blockUntilCancel": true, "response": "resumed" }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*", "chat:stream:delta"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "WSS", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // Make the seeded note a task and assign the agent to it.
    let marked = wss_rpc(
        &mut rpc,
        11,
        "task.markAsTask",
        json!({ "workspaceId": ws_id, "noteId": note_id, "status": "in_progress" }),
    )
    .await;
    assert_eq!(marked["ok"], true, "markAsTask ok: {marked}");
    let assigned = wss_rpc(
        &mut rpc,
        12,
        "task.assignAgent",
        json!({ "workspaceId": ws_id, "noteId": note_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(assigned["ok"], true, "assignAgent ok: {assigned}");

    // Park the assignee mid-turn.
    let sent = wss_rpc(
        &mut rpc,
        13,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "first" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    let mut saw_block_chunk = false;
    for _ in 0..50 {
        let frame = wss_event(&mut sub, 30).await;
        if frame["params"]["event"]["type"] == "chat:stream:delta"
            && frame["params"]["event"]["data"]["content"]
                .as_str()
                .unwrap_or_default()
                .contains("streaming-before-cancel")
        {
            saw_block_chunk = true;
            break;
        }
    }
    assert!(saw_block_chunk, "assignee streamed a chunk and parked");

    // Interrupt via the task note: resolves the assignee and preempts its turn.
    let result = wss_rpc(
        &mut rpc,
        14,
        "agent.sendToTask",
        json!({
            "workspaceId": ws_id,
            "taskNoteId": note_id,
            "message": "interrupt via task",
            "priority": "interrupt",
        }),
    )
    .await;
    assert_eq!(result["ok"], true, "sendToTask ok: {result}");
    assert_eq!(
        result["agentId"].as_str().unwrap_or_default(),
        agent_id,
        "resolved the task assignee"
    );
    assert_eq!(
        result["result"]["queued"], false,
        "interrupt priority delivered immediately, not queued: {result}"
    );

    // Same keep-alive preemption as sendMessage: terminal stream:end, then the
    // interrupt message runs turn=2 on the SAME (never-killed) child.
    let mut saw_preempt_end = false;
    let mut saw_interrupt_chunk = false;
    let mut saw_interrupt_end = false;
    for _ in 0..80 {
        let frame = wss_event(&mut sub, 30).await;
        match frame["params"]["event"]["type"].as_str() {
            Some("agent:stream:end") if !saw_preempt_end => saw_preempt_end = true,
            Some("chat:stream:delta") => {
                if frame["params"]["event"]["data"]["content"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("turn=2")
                {
                    saw_interrupt_chunk = true;
                }
            }
            Some("agent:stream:end") => {
                saw_interrupt_end = true;
                break;
            }
            _ => {}
        }
    }
    assert!(
        saw_preempt_end,
        "preemption emitted the terminal stream:end"
    );
    assert!(
        saw_interrupt_chunk,
        "task interrupt ran on the SAME process (turn=2)"
    );
    assert!(saw_interrupt_end, "interrupt turn completes cleanly");
}

/// Duplicate interrupt delivery (PROTOCOL §5.5): the SAME interrupt-priority
/// message (same `messageId`) delivered twice in quick succession — the exact
/// race that transitioned agents to `failed` in the reference app — preempts
/// exactly ONE turn. The duplicate is acknowledged idempotently
/// (`deduplicated: true`) without cancelling the interrupt turn it raced; the
/// message is persisted once (not double-persisted); and the agent survives:
/// the follow-up send runs `turn=3` on the SAME child (a double delivery
/// would have burned a turn and reported `turn=4`; a killed/restarted child
/// would report `turn=1`) and `agent.get` never shows an `error` status.
#[tokio::test]
async fn duplicate_interrupt_priority_send_delivered_once_over_wss() {
    let Some(script) = gate("WSS duplicate interrupt-priority E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let behavior = json!({ "blockUntilCancel": true, "response": "resumed" }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*", "chat:stream:delta"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "WSS", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // Park the first turn mid-stream.
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "first" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    let mut saw_block_chunk = false;
    for _ in 0..50 {
        let frame = wss_event(&mut sub, 30).await;
        if frame["params"]["event"]["type"] == "chat:stream:delta"
            && frame["params"]["event"]["data"]["content"]
                .as_str()
                .unwrap_or_default()
                .contains("streaming-before-cancel")
        {
            saw_block_chunk = true;
            break;
        }
    }
    assert!(saw_block_chunk, "first turn streamed a chunk and parked");

    // The SAME interrupt delivered twice back-to-back (stable messageId).
    let dup_payload = json!({
        "workspaceId": ws_id,
        "agentId": agent_id,
        "content": "duplicate interrupt payload",
        "messageId": "user-msg-dup-e2e",
        "priority": "interrupt",
    });
    let first = wss_rpc(&mut rpc, 12, "agent.sendMessage", dup_payload.clone()).await;
    assert_eq!(first["success"], true, "first delivery ok: {first}");
    assert_eq!(
        first["queued"], false,
        "first delivery preempts and streams immediately: {first}"
    );
    assert_eq!(first["messageId"], "user-msg-dup-e2e");

    let second = wss_rpc(&mut rpc, 13, "agent.sendMessage", dup_payload).await;
    assert_eq!(
        second["success"], true,
        "duplicate is not an error: {second}"
    );
    assert_eq!(
        second["deduplicated"], true,
        "duplicate is acknowledged idempotently, no second preemption: {second}"
    );
    assert_eq!(second["messageId"], "user-msg-dup-e2e");

    // Exactly one preemption: terminal stream:end for the parked turn, then
    // the interrupt runs turn=2 on the SAME child and completes.
    let mut saw_preempt_end = false;
    let mut saw_interrupt_chunk = false;
    let mut saw_interrupt_end = false;
    for _ in 0..80 {
        let frame = wss_event(&mut sub, 30).await;
        match frame["params"]["event"]["type"].as_str() {
            Some("agent:stream:end") if !saw_preempt_end => saw_preempt_end = true,
            Some("chat:stream:delta") => {
                if frame["params"]["event"]["data"]["content"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("turn=2")
                {
                    saw_interrupt_chunk = true;
                }
            }
            Some("agent:stream:end") => {
                saw_interrupt_end = true;
                break;
            }
            _ => {}
        }
    }
    assert!(
        saw_preempt_end,
        "the single preemption emitted the terminal stream:end"
    );
    assert!(
        saw_interrupt_chunk,
        "the interrupt ran once on the SAME process (turn=2)"
    );
    assert!(saw_interrupt_end, "the interrupt turn completes cleanly");

    // Not double-persisted: the conversation carries the interrupt content in
    // exactly ONE user message.
    let convo = wss_rpc(
        &mut rpc,
        14,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let dup_count = convo["messages"]
        .as_array()
        .expect("messages")
        .iter()
        .filter(|m| {
            m["role"] == "user"
                && serde_json::to_string(&m["contentBlocks"])
                    .unwrap_or_default()
                    .contains("duplicate interrupt payload")
        })
        .count();
    assert_eq!(
        dup_count, 1,
        "duplicate delivery must not double-persist: {convo}"
    );

    // Liveness + turn accounting: the follow-up send runs turn=3 on the SAME
    // child. A double-delivered interrupt would have burned an extra turn
    // (turn=4 here); a killed/restarted child would report turn=1.
    let after = wss_rpc(
        &mut rpc,
        15,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "after dup" }),
    )
    .await;
    assert_eq!(after["success"], true, "post-dup send ok: {after}");
    let mut saw_third_chunk = false;
    let mut saw_third_end = false;
    for _ in 0..80 {
        let frame = wss_event(&mut sub, 30).await;
        match frame["params"]["event"]["type"].as_str() {
            Some("chat:stream:delta") => {
                if frame["params"]["event"]["data"]["content"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("turn=3")
                {
                    saw_third_chunk = true;
                }
            }
            Some("agent:stream:end") if saw_third_chunk => {
                saw_third_end = true;
                break;
            }
            _ => {}
        }
    }
    assert!(
        saw_third_chunk,
        "exactly one interrupt turn ran — follow-up is turn=3 on the SAME live child"
    );
    assert!(saw_third_end, "post-dup turn completes cleanly");

    // The agent never transitioned to a failed status across the duplicate.
    let got = wss_rpc(
        &mut rpc,
        16,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_ne!(
        got["agent"]["status"].as_str().unwrap_or_default(),
        "error",
        "the agent never reaches a failed status: {got}"
    );
}

/// AUDIT-P1-3: the daemon-owned activity flags (`isResponding`/`isWaitingOnTool`/
/// `isWaitingForOtherAgents`, PROTOCOL §5.5/§7.1) reflect a genuinely-active
/// worker over the WSS wire. A `blockUntilCancel` agent parks mid-turn (a live
/// worker draining a turn) so its `AgentLite` + chat snapshot report
/// `isResponding: true`; a freshly-created idle agent reports every flag `false`.
#[tokio::test]
async fn agent_activity_flags_active_vs_idle_over_wss() {
    let Some(script) = gate("WSS agent activity flags E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let behavior = json!({ "blockUntilCancel": true, "response": "parked" }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*", "chat:stream:delta"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;

    // An idle agent: created but never prompted — no worker, no watches.
    let idle = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Idle", "model": "mock:default" }),
    )
    .await;
    let idle_id = idle["agent"]["id"].as_str().expect("idle id").to_string();

    // An active worker: prompt the agent and let it park mid-turn.
    let busy = wss_rpc(
        &mut rpc,
        11,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Busy", "model": "mock:default" }),
    )
    .await;
    let busy_id = busy["agent"]["id"].as_str().expect("busy id").to_string();
    let sent = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": busy_id, "content": "go" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Wait until the turn has streamed its chunk and parked — the worker is now
    // genuinely in-flight.
    let mut parked = false;
    for _ in 0..50 {
        let frame = wss_event(&mut sub, 30).await;
        if frame["params"]["event"]["type"] == "chat:stream:delta"
            && frame["params"]["event"]["data"]["content"]
                .as_str()
                .unwrap_or_default()
                .contains("streaming-before-cancel")
        {
            parked = true;
            break;
        }
    }
    assert!(parked, "busy agent streamed a chunk and parked mid-turn");

    // agent.get: the active worker is responding (not waiting on a tool — the
    // parked turn's only block is text — and parents no other agents). The
    // `waitingForAgentIds` list (PROTOCOL §5.5/§7.1) is always present and
    // mirrors `isWaitingForOtherAgents`: empty array when no watches are
    // pending (never null/omitted).
    let busy_lite = wss_rpc(&mut rpc, 13, "agent.get", json!({ "agentId": busy_id })).await;
    let busy_lite = &busy_lite["agent"];
    assert_eq!(busy_lite["isResponding"], true, "busy lite: {busy_lite}");
    assert_eq!(
        busy_lite["isWaitingOnTool"], false,
        "busy lite: {busy_lite}"
    );
    assert_eq!(
        busy_lite["isWaitingForOtherAgents"], false,
        "busy lite: {busy_lite}"
    );
    assert_eq!(
        busy_lite["waitingForAgentIds"],
        json!([]),
        "busy lite: {busy_lite}"
    );

    // agent.get: the idle agent reports every flag false and an empty
    // `waitingForAgentIds`.
    let idle_lite = wss_rpc(&mut rpc, 14, "agent.get", json!({ "agentId": idle_id })).await;
    let idle_lite = &idle_lite["agent"];
    assert_eq!(idle_lite["isResponding"], false, "idle lite: {idle_lite}");
    assert_eq!(
        idle_lite["isWaitingOnTool"], false,
        "idle lite: {idle_lite}"
    );
    assert_eq!(
        idle_lite["isWaitingForOtherAgents"], false,
        "idle lite: {idle_lite}"
    );
    assert_eq!(
        idle_lite["waitingForAgentIds"],
        json!([]),
        "idle lite: {idle_lite}"
    );

    // agent.list carries the same per-agent flags + waiting-on id list.
    let list = wss_rpc(&mut rpc, 15, "agent.list", json!({ "workspaceId": ws_id })).await;
    let agents = list["agents"].as_array().expect("agents array");
    let busy_row = agents
        .iter()
        .find(|a| a["id"] == json!(busy_id))
        .expect("busy in list");
    let idle_row = agents
        .iter()
        .find(|a| a["id"] == json!(idle_id))
        .expect("idle in list");
    assert_eq!(busy_row["isResponding"], true, "busy row: {busy_row}");
    assert_eq!(idle_row["isResponding"], false, "idle row: {idle_row}");
    assert_eq!(
        busy_row["waitingForAgentIds"],
        json!([]),
        "busy row: {busy_row}"
    );
    assert_eq!(
        idle_row["waitingForAgentIds"],
        json!([]),
        "idle row: {idle_row}"
    );

    // chat.subscribe's seq-0 snapshot for the busy agent carries the flags too.
    let chat = wss_rpc(
        &mut rpc,
        16,
        "chat.subscribe",
        json!({ "agentId": busy_id }),
    )
    .await;
    assert!(
        chat["subscriptionId"].is_string(),
        "chat subscribed: {chat}"
    );
    let push = wss_push(&mut rpc, 15).await;
    assert_eq!(push["params"]["kind"], "snapshot", "push: {push}");
    assert_eq!(
        push["params"]["snapshot"]["isResponding"], true,
        "snapshot: {}",
        push["params"]["snapshot"]
    );
    assert_eq!(
        push["params"]["snapshot"]["waitingForAgentIds"],
        json!([]),
        "snapshot: {}",
        push["params"]["snapshot"]
    );

    // Release the parked worker so the daemon tears down cleanly.
    let stopped = wss_rpc(&mut rpc, 17, "agent.stop", json!({ "agentId": busy_id })).await;
    assert_eq!(stopped["success"], true, "stop ok: {stopped}");
}

/// monorepo#2063 A2 over the real WSS wire: `agent.diagnostics` rows carry
/// `subtreeMemoryBytes` for an agent whose spawned subtree the live sampler
/// has attributed, and omit it for an agent that was never spawned — proving
/// the composition-root `set_tree_probe` wiring in `cmd_serve`, the sampler's
/// per-agent bucketing, and the wire shape end to end (not just the
/// service-level fake). Also asserts the field stays off the hot `agent.get` /
/// `agent.list` payloads (diagnostics-only by design).
///
/// Timing: the sampler publishes a full attribution sweep at boot and then on
/// its ~5s baseline cadence, so the parked agent's bucket lands within one
/// baseline period of the spawn — the poll loop below bounds that wait.
#[tokio::test]
async fn agent_diagnostics_reports_subtree_memory_over_wss() {
    let Some(script) = gate("WSS diagnostics subtreeMemoryBytes E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    // Park the prompted agent mid-turn so its node child stays alive across
    // sampler sweeps and keeps a registered agent-root pid.
    let behavior = json!({ "blockUntilCancel": true, "response": "parked" }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["chat:stream:delta"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;

    // Never prompted — no worker child, so no bucket and no field.
    let idle = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Unspawned", "model": "mock:default" }),
    )
    .await;
    let idle_id = idle["agent"]["id"].as_str().expect("idle id").to_string();

    // Prompted and parked — a live node subtree for the sampler to attribute.
    let busy = wss_rpc(
        &mut rpc,
        11,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Spawned", "model": "mock:default" }),
    )
    .await;
    let busy_id = busy["agent"]["id"].as_str().expect("busy id").to_string();
    let sent = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": busy_id, "content": "go" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Wait until the turn has streamed its pre-cancel chunk — the worker child
    // is now live and registered as an agent root.
    let mut parked = false;
    for _ in 0..50 {
        let frame = wss_event(&mut sub, 30).await;
        if frame["params"]["event"]["type"] == "chat:stream:delta" {
            parked = true;
            break;
        }
    }
    assert!(parked, "busy agent streamed a chunk and parked mid-turn");

    // Poll diagnostics until the live sampler's next full sweep (≤ ~5s away)
    // attributes the parked subtree; the budget leaves slack for slow CI.
    let deadline = std::time::Instant::now() + common::test_timeout(Duration::from_secs(30));
    let mut req_id = 20i64;
    let mut busy_bytes: Option<u64> = None;
    let mut last_diag = Value::Null;
    while std::time::Instant::now() < deadline {
        req_id += 1;
        let diag = wss_rpc(
            &mut rpc,
            req_id,
            "agent.diagnostics",
            json!({ "workspaceId": ws_id }),
        )
        .await;
        let agents = diag["diagnostics"]["agents"]
            .as_array()
            .expect("agents array")
            .clone();
        last_diag = diag;
        let busy_row = agents
            .iter()
            .find(|a| a["id"] == json!(busy_id))
            .expect("busy row in diagnostics");
        if let Some(bytes) = busy_row["subtreeMemoryBytes"].as_u64() {
            busy_bytes = Some(bytes);
            // The never-spawned agent's row omits the field entirely (absent,
            // never 0/null) — asserted on the same snapshot that proved the
            // sweep has attribution data.
            let idle_row = agents
                .iter()
                .find(|a| a["id"] == json!(idle_id))
                .expect("idle row in diagnostics");
            assert!(
                idle_row.get("subtreeMemoryBytes").is_none(),
                "unspawned agent row omits subtreeMemoryBytes: {idle_row}"
            );
            break;
        }
        sleep(Duration::from_millis(250)).await;
    }
    let busy_bytes = busy_bytes.unwrap_or_else(|| {
        panic!("sampler never attributed the parked subtree: {last_diag}");
    });
    assert!(busy_bytes > 0, "a live node subtree has resident bytes");

    // Diagnostics-only by design: the hot list payloads never carry the field.
    let got = wss_rpc(&mut rpc, 90, "agent.get", json!({ "agentId": busy_id })).await;
    assert!(
        got["agent"].get("subtreeMemoryBytes").is_none(),
        "agent.get omits subtreeMemoryBytes: {}",
        got["agent"]
    );
    let list = wss_rpc(&mut rpc, 91, "agent.list", json!({ "workspaceId": ws_id })).await;
    for row in list["agents"].as_array().expect("agents array") {
        assert!(
            row.get("subtreeMemoryBytes").is_none(),
            "agent.list omits subtreeMemoryBytes: {row}"
        );
    }

    // Release the parked worker so the daemon tears down cleanly.
    let stopped = wss_rpc(&mut rpc, 92, "agent.stop", json!({ "agentId": busy_id })).await;
    assert_eq!(stopped["success"], true, "stop ok: {stopped}");
}

/// AUDIT-P2-1b: when an agent has a real pending completion watch (its
/// MCP-delegated child is still running), the daemon emits the specific
/// `waitingForAgentIds: [childId]` alongside `isWaitingForOtherAgents: true`
/// over the WSS wire (PROTOCOL §5.5/§7.1) — proving the id list reflects the
/// genuine parent→child watch registered by the MCP `delegate_task` tool.
/// Drives the full MCP loop (mock ACP fires `delegate_task`)
/// and parks the child so the watch persists for observation.
#[tokio::test]
async fn agent_waiting_for_agent_ids_reflects_pending_watch_over_wss() {
    // Parent fires `delegate_task` with instructions carrying a marker; the
    // delegated child sees the marker in its first prompt and parks. The
    // parent then returns end_turn and goes idle — the watch persists because
    // the child never completes.
    const CHILD_MARK: &str = "AUDIT_P2_1B_PARK_CHILD";
    let Some(script) = gate("WSS waitingForAgentIds E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    // Post-WSAPI-8: replace discrete `delegate_task` with the unified
    // `workspace_api` tool routing through `ws.agent.delegate`.
    let delegate_js = format!(
        "return await ws.agent.delegate({{ agentInstructions: {}, model: 'mock:default' }});",
        json!(CHILD_MARK),
    );
    let behavior = json!({
        "toolCall": {
            "name": "workspace_api",
            "arguments": {
                "code": delegate_js,
                "summary": "AUDIT P2.1B parent delegates via ws.agent.delegate",
            },
        },
        "parkIfPromptContains": CHILD_MARK,
        "response": "parent delegated and is now waiting",
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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

    // SUBSCRIBER conn — subscribe BEFORE the turn so we miss no events.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let parent = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Parent", "model": "mock:default" }),
    )
    .await;
    let parent_id = parent["agent"]["id"]
        .as_str()
        .expect("parent id")
        .to_string();
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": parent_id, "content": "go" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Wait for the parent to go idle (its turn finished — the MCP delegate
    // tool returned and the parent emitted `end_turn`). The child it spawned
    // is parked, so the parent→child completion watch is still pending.
    let mut parent_idle_payload: Option<Value> = None;
    for _ in 0..120 {
        let frame = wss_event(&mut sub, 60).await;
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:idle" && ev["data"]["agentId"] == json!(parent_id) {
            parent_idle_payload = Some(ev["data"].clone());
            break;
        }
    }
    let parent_idle = parent_idle_payload.expect("parent went idle after firing delegate tool");
    // The idle payload itself carries the emit-time waiting flag — computed
    // from the parent's pending completion watches at publish time, so
    // notification clients can suppress the alert without a follow-up
    // `agent.list` read (which can race the child's completion consuming
    // the watch).
    assert_eq!(
        parent_idle["isWaitingForOtherAgents"],
        json!(true),
        "parent agent:idle carries isWaitingForOtherAgents=true while the child watch is pending: {parent_idle}"
    );

    // The parent's `AgentLite` carries the BE-owned waiting-on id list (PROTOCOL
    // §5.5/§7.1): the bool is true, the array is the SINGLE distinct child id
    // the watch is registered against — proving the FE no longer needs to fall
    // back to `metadata.waitingForAgentIds` to resolve the waiting-on agent.
    let parent_lite = wss_rpc(&mut rpc, 12, "agent.get", json!({ "agentId": parent_id })).await;
    let parent_lite = &parent_lite["agent"];
    assert_eq!(
        parent_lite["isWaitingForOtherAgents"], true,
        "parent lite: {parent_lite}"
    );
    let waiting = parent_lite["waitingForAgentIds"]
        .as_array()
        .expect("waitingForAgentIds array");
    assert_eq!(
        waiting.len(),
        1,
        "exactly one waiting-on child: {parent_lite}"
    );
    let child_id = waiting[0].as_str().expect("child id string").to_string();
    assert!(
        child_id.starts_with("agent-"),
        "child id shape: {parent_lite}"
    );
    assert_ne!(child_id, parent_id, "watching a DIFFERENT agent");

    // The child agent the parent is waiting on really exists in the store and
    // its `AgentLite` reports an empty `waitingForAgentIds` (it parents none).
    let child_lite = wss_rpc(&mut rpc, 13, "agent.get", json!({ "agentId": child_id })).await;
    let child_lite = &child_lite["agent"];
    assert_eq!(
        child_lite["isWaitingForOtherAgents"], false,
        "child lite: {child_lite}"
    );
    assert_eq!(
        child_lite["waitingForAgentIds"],
        json!([]),
        "child lite: {child_lite}"
    );

    // Release the parked child so the daemon tears down cleanly.
    let stopped = wss_rpc(&mut rpc, 14, "agent.stop", json!({ "agentId": child_id })).await;
    assert_eq!(stopped["success"], true, "stop ok: {stopped}");
}

/// Agent delegation over WSS: `agent.delegate` with `agentInstructions` must
/// create the child AND start its turn, with every `agent:stream:*` event keyed
/// by the CHILD `agentId` (PROTOCOL §5.5/§6.5). Drives the RPC front door
/// (caller-less) so the child is the only agent that ever runs — proving the
/// streamed output belongs to the child's session, not a parent's. Also passes
/// `taskText` so the child gets a task-derived `name` (NAME-1); the wire
/// contract exposes the derived name in the `agent.delegate` result. Asserts:
/// a non-empty `agentId` in the result, `name == taskText` (task-derived, not
/// the generic `Agent xxxxxx` fallback), ≥1 `agent:stream:activity` + exactly one
/// terminal `agent:stream:end` + an `agent:idle` all carrying the child id, and
/// the child transcript carrying the delivered instructions + an assistant reply.
#[tokio::test]
async fn delegate_starts_child_turn_scoped_to_child_over_wss() {
    let Some(script) = gate("WSS delegate child-turn E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let behavior = json!({ "response": "delegated child ran" }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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

    // SUBSCRIBER conn — subscribe BEFORE delegating so we miss no child events.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // RPC conn — delegate with instructions; the child runs the mock provider.
    // `taskText` exercises the NAME-1 task-derived naming path over the wire.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let task_text = "delegated child task title";
    let delegated = wss_rpc(
        &mut rpc,
        10,
        "agent.delegate",
        json!({
            "workspaceId": ws_id,
            "agentInstructions": "do the delegated work",
            "taskText": task_text,
            "model": "mock:default",
        }),
    )
    .await;
    assert_eq!(delegated["ok"], true, "delegate ok: {delegated}");
    let child_id = delegated["agentId"]
        .as_str()
        .expect("child agent id")
        .to_string();
    assert!(!child_id.is_empty(), "non-empty child agentId");
    // NAME-1: the wire result carries the task-derived name, not the generic
    // `Agent xxxxxx` uuid-suffix fallback that used to leak into the FE.
    assert_eq!(
        delegated["name"].as_str(),
        Some(task_text),
        "delegated child name is task-derived over WSS: {delegated}"
    );
    // NAME-1: `agent.get` must expose `nameExplicitlySet == false` so a later
    // `agent.rename` with `skipIfExplicitlySet: true` (the opening-turn
    // `ws.workspace.setAgentName` self-rename) still applies. Asserting this
    // over the wire guards the rename-guard behavior against regressions where
    // the derived name is correct but the flag flips to `true`.
    let got = wss_rpc(
        &mut rpc,
        12,
        "agent.get",
        json!({ "agentId": child_id, "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(
        got["agent"]["nameExplicitlySet"].as_bool(),
        Some(false),
        "delegated child stays renameable-with-guard over WSS: {got}"
    );

    // Every stream event must carry the CHILD id; collect past the terminal
    // stream:end to the trailing agent:idle (idle is emitted AFTER stream:end
    // in `run_prompt_turn`, §6.5/§7).
    let mut chunks = 0u32;
    let mut ends = 0u32;
    let mut saw_idle = false;
    for _ in 0..120 {
        let frame = wss_event(&mut sub, 30).await;
        let ev = &frame["params"]["event"];
        let ev_agent = ev["data"]["agentId"].as_str().unwrap_or_default();
        match ev["type"].as_str() {
            Some("agent:stream:activity") => {
                assert_eq!(ev_agent, child_id, "chunk scoped to the child: {ev}");
                chunks += 1;
            }
            Some("agent:stream:end") => {
                assert_eq!(ev_agent, child_id, "stream:end scoped to the child: {ev}");
                ends += 1;
            }
            Some("agent:idle") if ev_agent == child_id => saw_idle = true,
            _ => {}
        }
        if ends >= 1 && saw_idle {
            break;
        }
    }
    assert!(chunks >= 1, "child streamed ≥1 chunk keyed by its own id");
    assert_eq!(ends, 1, "exactly one terminal stream:end for the child");
    assert!(saw_idle, "child emitted agent:idle on completion");

    // The child transcript carries the delivered instructions (user) and the
    // mock's assistant reply — observable proof the turn actually ran.
    let conv = wss_rpc(
        &mut rpc,
        11,
        "agent.getConversation",
        json!({ "agentId": child_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    assert!(
        messages.iter().any(|m| m["role"] == "user"
            && serde_json::to_string(&m["contentBlocks"])
                .unwrap_or_default()
                .contains("do the delegated work")),
        "child first message carries the delegated instructions: {conv}"
    );
    assert!(
        messages.iter().any(|m| m["role"] == "assistant"),
        "child produced an assistant reply: {conv}"
    );
}

#[allow(clippy::similar_names)] // deliberate parallel naming across the scenario's instances
/// WAKE-1: `after_all` delegation fan-in over WSS, end to end. A parent fires
/// TWO MCP `delegate_task` calls with `waitMode: "after_all"`; each child
/// reports via `report_to_parent` (suppressed — no immediate parent message)
/// and completes. Asserts (PROTOCOL §5.5/§6.5):
/// - the parent transcript carries EXACTLY ONE `[WORKSPACE EVENTS]` wake with
///   BOTH child reports aggregated, and zero individual report deliveries;
/// - the wake runs a REAL parent turn — `agent:stream:activity` + one
///   `agent:stream:end` + a trailing `agent:idle`, all keyed by the parent;
/// - `isWaitingForOtherAgents` is true (with both child ids) while waiting and
///   false after delivery, with `agent:subscriptions-changed` watch-change
///   events observed on the wire for both transitions.
#[tokio::test]
async fn after_all_group_delivers_single_aggregated_wake_over_wss() {
    const CHILD_A: &str = "WAKE1_CHILD_ALPHA";
    const CHILD_B: &str = "WAKE1_CHILD_BETA";
    const REPORT_A: &str = "REPORT_ALPHA finished the alpha task";
    const REPORT_B: &str = "REPORT_BETA finished the beta task";
    const PARENT_GO: &str = "WAKE1_PARENT_GO";
    let Some(script) = gate("WSS after_all aggregated wake E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    // Post-WSAPI-8: agents drive the workspace through the unified
    // `workspace_api` tool + `ws.*` bindings; the discrete
    // `delegate_task` / `report_to_parent` tools are gone.
    let report_a_js = format!("return await ws.agent.reportToParent({});", json!(REPORT_A));
    let report_b_js = format!("return await ws.agent.reportToParent({});", json!(REPORT_B));
    let delegate_a_js = format!(
        "return await ws.agent.delegate({{ agentInstructions: {}, waitMode: 'after_all', model: 'mock:default' }});",
        json!(CHILD_A),
    );
    let delegate_b_js = format!(
        "return await ws.agent.delegate({{ agentInstructions: {}, waitMode: 'after_all', model: 'mock:default' }});",
        json!(CHILD_B),
    );
    // One behavior, prompt-matched rules: children (matched by their delegated
    // instructions) delay so the parent's waiting window is observable, then
    // report + finish; the parent delegates both children after_all; the wake
    // turn (matched on the [WORKSPACE EVENTS] framing) just acknowledges.
    let behavior = json!({
        "rules": [
            {
                "ifPromptContains": CHILD_A,
                "delayMs": 8000,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": report_a_js, "summary": "alpha reportToParent" }
                },
                "response": "alpha child done",
            },
            {
                "ifPromptContains": CHILD_B,
                "delayMs": 8000,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": report_b_js, "summary": "beta reportToParent" }
                },
                "response": "beta child done",
            },
            {
                "ifPromptContains": "[WORKSPACE EVENTS]",
                "response": "parent acknowledged the aggregated wake",
            },
            {
                "ifPromptContains": PARENT_GO,
                "toolCalls": [
                    {
                        "name": "workspace_api",
                        "arguments": { "code": delegate_a_js, "summary": "delegate alpha after_all" }
                    },
                    {
                        "name": "workspace_api",
                        "arguments": { "code": delegate_b_js, "summary": "delegate beta after_all" }
                    },
                ],
                "response": "parent delegated two after_all children",
            },
        ],
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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

    // SUBSCRIBER conn — subscribe BEFORE the turn so we miss no events.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let parent = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Parent", "model": "mock:default" }),
    )
    .await;
    let parent_id = parent["agent"]["id"]
        .as_str()
        .expect("parent id")
        .to_string();
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": parent_id, "content": PARENT_GO }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Phase 1 — the parent's delegating turn: both delegate_task registrations
    // push agent:subscriptions-changed with the waiting flags; the turn ends
    // with the parent's first agent:idle (which seals the group).
    let mut saw_waiting_true_event = false;
    let mut parent_idle = false;
    for _ in 0..200 {
        let frame = wss_event(&mut sub, 60).await;
        let ev = &frame["params"]["event"];
        let ev_agent = ev["data"]["agentId"].as_str().unwrap_or_default();
        if ev["type"] == "agent:subscriptions-changed"
            && ev_agent == parent_id
            && ev["data"]["isWaitingForOtherAgents"] == json!(true)
        {
            saw_waiting_true_event = true;
        }
        if ev["type"] == "agent:idle" && ev_agent == parent_id {
            parent_idle = true;
            break;
        }
    }
    assert!(parent_idle, "parent went idle after delegating");
    assert!(
        saw_waiting_true_event,
        "watch registration pushed agent:subscriptions-changed with isWaitingForOtherAgents=true"
    );

    // While the (delayed) children are still running, the parent's AgentLite
    // reports the waiting flags with BOTH child ids (PROTOCOL §5.5/§7.1).
    let lite = wss_rpc(&mut rpc, 12, "agent.get", json!({ "agentId": parent_id })).await;
    let lite = &lite["agent"];
    assert_eq!(lite["isWaitingForOtherAgents"], true, "waiting: {lite}");
    let waiting = lite["waitingForAgentIds"]
        .as_array()
        .expect("waitingForAgentIds");
    assert_eq!(waiting.len(), 2, "waiting on both children: {lite}");
    assert!(
        waiting
            .iter()
            .all(|id| id.as_str().unwrap_or_default() != parent_id),
        "waiting ids are the children, not the parent: {lite}"
    );

    // Phase 2 — children finish; the group fires ONE aggregated wake that runs
    // a real parent turn (stream lifecycle keyed by the parent, trailing idle)
    // and the group clear pushes the refreshed (false/empty) waiting flags.
    let mut saw_waiting_false_event = false;
    let mut wake_chunks = 0u32;
    let mut wake_ends = 0u32;
    let mut parent_idle_again = false;
    for _ in 0..400 {
        let frame = wss_event(&mut sub, 90).await;
        let ev = &frame["params"]["event"];
        let ev_agent = ev["data"]["agentId"].as_str().unwrap_or_default();
        if ev_agent != parent_id {
            continue;
        }
        match ev["type"].as_str() {
            Some("agent:subscriptions-changed")
                if ev["data"]["isWaitingForOtherAgents"] == json!(false) =>
            {
                saw_waiting_false_event = true;
            }
            Some("agent:stream:activity") => wake_chunks += 1,
            Some("agent:stream:end") => wake_ends += 1,
            Some("agent:idle") => {
                parent_idle_again = true;
            }
            _ => {}
        }
        if parent_idle_again && wake_ends >= 1 {
            break;
        }
    }
    assert!(
        saw_waiting_false_event,
        "group clear pushed agent:subscriptions-changed with isWaitingForOtherAgents=false"
    );
    assert!(
        wake_chunks >= 1,
        "wake turn streamed ≥1 chunk for the parent"
    );
    assert_eq!(
        wake_ends, 1,
        "exactly one wake-turn stream:end for the parent"
    );
    assert!(parent_idle_again, "parent idled again after the wake turn");

    // After delivery the waiting flags are cleared on the projection too.
    let lite = wss_rpc(&mut rpc, 13, "agent.get", json!({ "agentId": parent_id })).await;
    let lite = &lite["agent"];
    assert_eq!(lite["isWaitingForOtherAgents"], false, "cleared: {lite}");
    assert_eq!(lite["waitingForAgentIds"], json!([]), "cleared: {lite}");

    // The parent transcript carries EXACTLY ONE [WORKSPACE EVENTS] wake with
    // both reports aggregated — and the reports appear NOWHERE else (the
    // individual reportToParent sends were suppressed).
    let conv = wss_rpc(
        &mut rpc,
        14,
        "agent.getConversation",
        json!({ "agentId": parent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let texts: Vec<String> = messages
        .iter()
        .map(|m| serde_json::to_string(&m["contentBlocks"]).unwrap_or_default())
        .collect();
    let wakes: Vec<&String> = texts
        .iter()
        .filter(|t| t.contains("[WORKSPACE EVENTS]"))
        .collect();
    assert_eq!(wakes.len(), 1, "exactly one wake message: {conv}");
    let wake = wakes[0];
    assert!(
        wake.contains("All 2 delegated child agent(s) settled"),
        "aggregated wake header: {wake}"
    );

    // The wake user-message row carries FE `event_notification` metadata so
    // `EventWakeupBanner` reads a real `eventCount` / `eventTypes` / `events`
    // payload instead of the fallback "Subscription update — 0 events".
    let wake_msg = messages
        .iter()
        .find(|m| {
            serde_json::to_string(&m["contentBlocks"])
                .unwrap_or_default()
                .contains("[WORKSPACE EVENTS]")
        })
        .expect("wake message present");
    let metadata = &wake_msg["metadata"];
    assert_eq!(
        metadata["type"], "event_notification",
        "wake metadata type: {wake_msg}"
    );
    assert_eq!(
        metadata["eventCount"], 2,
        "wake metadata eventCount: {wake_msg}"
    );
    let event_types = metadata["eventTypes"].as_array().expect("eventTypes array");
    assert!(
        event_types.iter().any(|t| t == "agent:idle"),
        "eventTypes contains agent:idle: {metadata}"
    );
    let events = metadata["events"].as_array().expect("events array");
    assert_eq!(events.len(), 2, "wake metadata events length: {metadata}");
    for event in events {
        assert!(event["id"].is_string(), "event.id string: {event}");
        assert!(event["type"].is_string(), "event.type string: {event}");
        assert!(event["timestamp"].is_string(), "event.timestamp: {event}");
        assert!(event["actor"].is_object(), "event.actor object: {event}");
    }
    assert!(
        wake.contains(REPORT_A),
        "wake carries the alpha report: {wake}"
    );
    assert!(
        wake.contains(REPORT_B),
        "wake carries the beta report: {wake}"
    );
    assert_eq!(
        texts.iter().filter(|t| t.contains(REPORT_A)).count(),
        1,
        "alpha report appears only inside the wake: {conv}"
    );
    assert_eq!(
        texts.iter().filter(|t| t.contains(REPORT_B)).count(),
        1,
        "beta report appears only inside the wake: {conv}"
    );
}

/// SUB-2 (Copilot #104) end-to-end over WSS: `agent.reportToParent` is
/// metadata-only — it MUST NOT deliver an immediate parent wake — and the
/// single parent wake is driven by the child's `reportToParent` (report-time
/// wake), carrying the persisted `completionReport` with `Report:` framing.
/// The child's subsequent `agent:idle` does NOT deliver a second wake (idle
/// suppression). Exercised on the real WSS wire (not just the `intent-services`
/// unit tests) per the repo's e2e requirement.
///
/// A parent's opening turn delegates one child (immediate, ungrouped —
/// `waitMode: "immediate"`), the child calls `ws.agent.reportToParent` and
/// then finishes, and the parent's wake turn acknowledges. Asserts:
/// - the parent transcript carries EXACTLY ONE `[WORKSPACE EVENTS]` wake
///   message (proving the report-time wake delivered, and idle was suppressed);
/// - that wake carries the `Report: <report>` framing and does NOT fall
///   through to the `Summary:` branch (report-preferred formatting).
///
/// NOTE on ordering: the report-time wake fires DURING the child's turn (the
/// `reportToParent` tool call), so the parent's wake turn runs concurrently
/// with the child finishing its own turn — the parent's wake `stream:end` /
/// second `agent:idle` and the child's terminal `agent:idle` can arrive on
/// the wire in EITHER order. The event loop below must therefore track each
/// milestone independently and never gate one on the other.
#[tokio::test]
async fn report_to_parent_metadata_only_then_idle_delivers_single_wake_over_wss() {
    const CHILD_TAG: &str = "SUB2_WSS_CHILD";
    const REPORT: &str = "SUB2_WSS_REPORT shipped the thing";
    const PARENT_GO: &str = "SUB2_WSS_PARENT_GO";
    let Some(script) = gate("WSS reportToParent SUB-2 E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    // The child reports via the unified `workspace_api` tool + `ws.*` binding
    // (post-WSAPI-8: discrete `report_to_parent` MCP tool is gone).
    let report_js = format!("return await ws.agent.reportToParent({});", json!(REPORT));
    let delegate_js = format!(
        "return await ws.agent.delegate({{ agentInstructions: {}, model: 'mock:default' }});",
        json!(CHILD_TAG),
    );
    // One behavior, prompt-matched rules:
    // - child (matched on its delegated instructions): reportToParent then finish;
    // - parent's wake turn (matched on the [WORKSPACE EVENTS] framing): ack;
    // - parent's opening turn (matched on PARENT_GO): delegate the single child.
    let behavior = json!({
        "rules": [
            {
                "ifPromptContains": CHILD_TAG,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": report_js, "summary": "child reportToParent" }
                },
                "response": "child finished after reportToParent",
            },
            {
                "ifPromptContains": "[WORKSPACE EVENTS]",
                "response": "parent acknowledged the wake",
            },
            {
                "ifPromptContains": PARENT_GO,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": delegate_js, "summary": "delegate SUB-2 child" }
                },
                "response": "parent delegated one immediate child",
            },
        ],
    })
    .to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("WORKSPACE_IDLE_DEBOUNCE_TEST_MS", "50"),
    ];
    let child_proc = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child: child_proc,
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

    // SUBSCRIBER conn — subscribe BEFORE the turn so we miss no events.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let parent = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "SUB2 Parent", "model": "mock:default" }),
    )
    .await;
    let parent_id = parent["agent"]["id"]
        .as_str()
        .expect("parent id")
        .to_string();
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": parent_id, "content": PARENT_GO }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Track the observable milestones ORDER-INSENSITIVELY:
    // - parent goes idle after the delegating turn (first parent agent:idle);
    // - the child streams and its reportToParent triggers the parent's wake
    //   turn (parent stream:end after that first idle) → parent idles again;
    // - the child emits its terminal agent:idle.
    //
    // ROOT CAUSE of the historical 180s-hang flake: the wake is report-time
    // driven — it fires DURING the child's turn — so the parent's wake
    // `stream:end` / second `agent:idle` race the child's terminal
    // `agent:idle` on the wire. The old loop only counted the parent's wake
    // events once `child_idle` was already true; when the wake events won the
    // race, the break condition could never be satisfied and the loop blocked
    // forever in `wss_event` (heartbeat pings reset its per-frame timeout)
    // until the 180s terminate guard. Each milestone below is tracked
    // independently, and the whole wait sits under one hard deadline so a
    // regression fails fast with a diagnostic instead of hanging.
    let mut child_id: Option<String> = None;
    let mut parent_idle_count = 0u32;
    let mut child_idle = false;
    let mut child_idle_data: Option<serde_json::Value> = None;
    let mut parent_wake_ends = 0u32;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while !(parent_idle_count >= 2 && parent_wake_ends >= 1 && child_idle) {
        let Some(frame) = wss_event_opt_until(&mut sub, deadline).await else {
            panic!(
                "timed out waiting for wake milestones: parent_idle_count={parent_idle_count} \
                 parent_wake_ends={parent_wake_ends} child_idle={child_idle} child_id={child_id:?}"
            )
        };
        let ev = &frame["params"]["event"];
        let ev_agent = ev["data"]["agentId"].as_str().unwrap_or_default();
        let ev_type = ev["type"].as_str().unwrap_or_default();
        // Learn the child id from the first non-parent stream chunk (the
        // child's own turn keys every stream event by its agent id).
        if child_id.is_none()
            && ev_type == "agent:stream:activity"
            && !ev_agent.is_empty()
            && ev_agent != parent_id
        {
            child_id = Some(ev_agent.to_string());
        }
        if ev_agent == parent_id && ev_type == "agent:idle" {
            parent_idle_count += 1;
        }
        if let Some(cid) = child_id.as_deref() {
            if ev_agent == cid && ev_type == "agent:idle" {
                child_idle = true;
                child_idle_data = Some(ev["data"].clone());
            }
        }
        // Any parent stream:end after the first parent idle belongs to the
        // wake turn (the delegating turn's stream:end precedes that idle).
        if ev_agent == parent_id && ev_type == "agent:stream:end" && parent_idle_count >= 1 {
            parent_wake_ends += 1;
        }
    }
    assert!(child_id.is_some(), "child agent id observed on the wire");
    assert!(child_idle, "child emitted agent:idle after reportToParent");
    // The child's terminal `agent:idle` data carries the persisted report
    // under BOTH `completionReport` (canonical) and `report` (back-compat).
    let idle_data = child_idle_data.expect("child agent:idle data captured");
    assert_eq!(
        idle_data["completionReport"].as_str(),
        Some(REPORT),
        "child agent:idle carries completionReport: {idle_data}"
    );
    assert_eq!(
        idle_data["report"].as_str(),
        Some(REPORT),
        "child agent:idle carries legacy report: {idle_data}"
    );
    assert_eq!(
        parent_wake_ends, 1,
        "exactly one wake-turn stream:end on the parent (single wake driven by reportToParent, idle suppressed)"
    );
    assert!(
        parent_idle_count >= 2,
        "parent idled after the delegating turn and again after the wake turn"
    );

    // The parent transcript carries EXACTLY ONE `[WORKSPACE EVENTS]` wake
    // (proof that `reportToParent` didn't emit its own), and that wake
    // carries the persisted report via `format_completion_wake`'s
    // `Report:` branch — the `Summary:` fallback MUST NOT appear.
    let conv = wss_rpc(
        &mut rpc,
        12,
        "agent.getConversation",
        json!({ "agentId": parent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let wake_texts: Vec<String> = messages
        .iter()
        .filter_map(|m| {
            let text = serde_json::to_string(&m["contentBlocks"]).unwrap_or_default();
            if text.contains("[WORKSPACE EVENTS]") {
                Some(text)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(
        wake_texts.len(),
        1,
        "exactly one [WORKSPACE EVENTS] wake on the parent: {conv}"
    );
    let wake = &wake_texts[0];
    assert!(
        wake.contains(&format!("Report: {REPORT}")),
        "wake carries the persisted report via the Report: branch: {wake}"
    );
    assert!(
        !wake.contains("Summary:"),
        "wake MUST prefer the persisted report over lastResponseSummary: {wake}"
    );

    // The child's persisted `metadata.completionReport` is the same text
    // (the write persisted by `agent.reportToParent`) — the source of truth
    // that `format_completion_wake` folded into the wake bytes above.
    let cid = child_id.expect("child id captured");
    let child_got = wss_rpc(
        &mut rpc,
        13,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": cid }),
    )
    .await;
    assert_eq!(
        child_got["agent"]["metadata"]["completionReport"].as_str(),
        Some(REPORT),
        "child's persisted completionReport matches: {child_got}"
    );
}

/// Agent attention requests over WSS — discussion kind, task-linked caller.
/// A parentless delegated agent linked to an `in_progress` task note calls
/// `ws.agent.requestDiscussion(reason)` mid-turn. Asserts over the real wire
/// (PROTOCOL §5.5/§6.5):
///  - the self-sufficient `agent:attention-requested` event carries
///    `{ workspaceId, agentId, agentName, kind: "discussion", reason }`;
///  - the raise `agent:updated` carries `attentionRequestKind` +
///    `attentionRequestTimestamp`, and the system-role transcript notice's
///    persist emits `agent:message` with `role: "system"`;
///  - the linked task moves to `discussion_needed` (`task:status-changed`
///    attributed to the caller, read back via `task.get`);
///  - `agent.getSession` serves the pending `attentionRequest*` session
///    fields and the persisted notice with `meta.kind = "discussion-request"`,
///    and the agent's status is NOT `error` (the turn ended normally);
///  - the next USER message (`agent.sendMessage` front door) retires the
///    request: `agent:updated` with `attentionRequestCleared: true`, and the
///    session fields are gone;
///  - after a re-raise, an AUTOMATIC delivery (`agent.sendToTask`, the
///    A2A/system path) ALSO retires the request for THIS agent — the
///    delegate is a BACKGROUND session (`agent.delegate` persists
///    `isBackground: true`), and the child/background retire rule
///    (PROTOCOL §5.5) makes the coordinator's follow-up the acknowledgement.
///    (Top-level foreground agents keep the user-only dismissal — covered by
///    the `attention_request_clear_gates` unit tests and, over the wire, by
///    `attention_request_foreground_automatic_delivery_negative_over_wss`.)
#[tokio::test]
async fn attention_request_discussion_over_wss() {
    const CHILD_MARKER: &str = "ATTN_DISCUSS_CHILD";
    const REASON: &str = "ATTN_WSS need a decision on the migration approach";
    let Some(script) = gate("WSS attention-request discussion E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let (ws_id, note_id) = seed_workspace_and_note(&data_dir).await;
    let request_js = format!(
        "return await ws.agent.requestDiscussion({});",
        json!(REASON)
    );
    let behavior = json!({
        "rules": [{
            "ifPromptContains": CHILD_MARKER,
            "toolCall": {
                "name": "workspace_api",
                "arguments": { "code": request_js, "summary": "raise discussion request" }
            },
            "response": "turn ended after requestDiscussion",
        }],
        "response": "follow-up acknowledged",
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
    ];
    let child_proc = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child: child_proc,
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

    // SUBSCRIBER conn — agent + task events, registered BEFORE the turn.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*", "task:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let marked = wss_rpc(
        &mut rpc,
        10,
        "task.markAsTask",
        json!({ "workspaceId": ws_id, "noteId": note_id, "status": "in_progress" }),
    )
    .await;
    assert_eq!(marked["ok"], true, "markAsTask ok: {marked}");

    // Task-linked parentless delegate (router front door): sets the child
    // session's `taskNoteId` so the attention op can transition the task.
    let delegated = wss_rpc(
        &mut rpc,
        11,
        "agent.delegate",
        json!({
            "workspaceId": ws_id,
            "taskNoteId": note_id,
            "agentInstructions": format!("{CHILD_MARKER} raise a discussion request"),
            "model": "mock:default",
        }),
    )
    .await;
    assert_eq!(delegated["ok"], true, "delegate ok: {delegated}");
    let agent_id = delegated["agentId"].as_str().expect("agent id").to_string();
    let agent_name = delegated["name"].as_str().expect("agent name").to_string();

    // Order-insensitive milestones under one hard deadline (the attention
    // events fire DURING the child's turn, racing its terminal idle).
    let mut attention: Option<Value> = None;
    let mut raise_updated = false;
    let mut system_message = false;
    let mut task_changed: Option<Value> = None;
    let mut idle = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while !(attention.is_some()
        && raise_updated
        && system_message
        && task_changed.is_some()
        && idle)
    {
        let Some(frame) = wss_event_opt_until(&mut sub, deadline).await else {
            panic!(
                "timed out: attention={a} raise_updated={raise_updated} \
                 system_message={system_message} task_changed={t} idle={idle}",
                a = attention.is_some(),
                t = task_changed.is_some(),
            )
        };
        let ev = &frame["params"]["event"];
        let data = &ev["data"];
        match ev["type"].as_str().unwrap_or_default() {
            "agent:attention-requested" if data["agentId"] == json!(agent_id) => {
                attention = Some(data.clone());
            }
            "agent:updated"
                if data["agentId"] == json!(agent_id)
                    && data["attentionRequestKind"].is_string() =>
            {
                assert_eq!(
                    data["attentionRequestKind"], "discussion",
                    "raise agent:updated kind: {data}"
                );
                assert!(
                    data["attentionRequestTimestamp"].is_string(),
                    "raise agent:updated carries the timestamp: {data}"
                );
                raise_updated = true;
            }
            "agent:message" if data["agentId"] == json!(agent_id) && data["role"] == "system" => {
                assert!(
                    data["messageId"].is_string(),
                    "system notice agent:message carries messageId: {data}"
                );
                system_message = true;
            }
            "task:status-changed" if data["noteId"] == json!(note_id) => {
                task_changed = Some(data.clone());
            }
            "agent:idle" if data["agentId"] == json!(agent_id) => idle = true,
            _ => {}
        }
    }
    let attention = attention.expect("attention event captured");
    assert_eq!(
        attention["workspaceId"],
        json!(ws_id),
        "attention event carries workspaceId: {attention}"
    );
    // The RPC front-door delegate is parentless, so the optional
    // `parentAgentId` must be OMITTED entirely — never `null`.
    assert!(
        attention.get("parentAgentId").is_none(),
        "parentAgentId omitted for a parentless caller: {attention}"
    );
    assert_eq!(
        attention["agentName"],
        json!(agent_name),
        "attention event carries agentName: {attention}"
    );
    assert_eq!(
        attention["kind"], "discussion",
        "attention event kind: {attention}"
    );
    assert_eq!(
        attention["reason"], REASON,
        "attention event reason: {attention}"
    );
    let task_changed = task_changed.expect("task:status-changed captured");
    assert_eq!(
        task_changed["previousStatus"], "in_progress",
        "task transition source: {task_changed}"
    );
    assert_eq!(
        task_changed["newStatus"], "discussion_needed",
        "task transition target: {task_changed}"
    );
    assert_eq!(
        task_changed["agentId"],
        json!(agent_id),
        "task transition attributed to the caller: {task_changed}"
    );

    // task.get reads the transitioned status back over the wire.
    let got_task = wss_rpc(
        &mut rpc,
        12,
        "task.get",
        json!({ "workspaceId": ws_id, "taskNoteId": note_id }),
    )
    .await;
    assert_eq!(
        got_task["task"]["status"], "discussion_needed",
        "linked task persisted at discussion_needed: {got_task}"
    );

    // agent.getSession serves the pending attentionRequest* fields, the
    // persisted meta.kind notice, and a non-error status.
    let got = wss_rpc(
        &mut rpc,
        13,
        "agent.getSession",
        json!({ "agentId": agent_id, "workspaceId": ws_id }),
    )
    .await;
    let session = &got["session"];
    assert_eq!(
        session["attentionRequestKind"], "discussion",
        "session attentionRequestKind"
    );
    assert_eq!(
        session["attentionRequestReason"], REASON,
        "session attentionRequestReason"
    );
    assert!(
        session["attentionRequestTimestamp"].is_string(),
        "session attentionRequestTimestamp present"
    );
    assert_ne!(
        session["status"], "error",
        "turn ended normally, status is NOT error: {}",
        session["status"]
    );
    let messages = session["messages"].as_array().expect("messages array");
    let notice = messages
        .iter()
        .find(|m| {
            m["role"] == "system" && m["contentBlocks"][0]["meta"]["kind"] == "discussion-request"
        })
        .expect("persisted discussion-request notice");
    assert_eq!(
        notice["contentBlocks"][0]["type"], "text",
        "notice block is a text block: {notice}"
    );
    assert_eq!(
        notice["contentBlocks"][0]["text"], REASON,
        "notice carries the reason: {notice}"
    );

    // The next USER message retires the pending request: agent:updated
    // with attentionRequestCleared, and the session fields are gone.
    let sent = wss_rpc(
        &mut rpc,
        14,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "follow up" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let Some(frame) = wss_event_opt_until(&mut sub, deadline).await else {
            panic!("timed out waiting for attentionRequestCleared")
        };
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:updated"
            && ev["data"]["agentId"] == json!(agent_id)
            && ev["data"]["attentionRequestCleared"] == true
        {
            break;
        }
    }
    let got = wss_rpc(
        &mut rpc,
        15,
        "agent.getSession",
        json!({ "agentId": agent_id, "workspaceId": ws_id }),
    )
    .await;
    let session = got["session"].as_object().expect("session object");
    assert!(
        !session.contains_key("attentionRequestKind"),
        "attentionRequestKind cleared on next message"
    );
    assert!(
        !session.contains_key("attentionRequestReason"),
        "attentionRequestReason cleared on next message"
    );
    assert!(
        !session.contains_key("attentionRequestTimestamp"),
        "attentionRequestTimestamp cleared on next message"
    );

    // Re-raise: a user message carrying the behavior marker drives another
    // requestDiscussion turn (the turn-begin clear is a no-op — nothing is
    // pending), leaving a fresh pending request on the session.
    let sent = wss_rpc(
        &mut rpc,
        16,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "content": format!("{CHILD_MARKER} raise it again"),
        }),
    )
    .await;
    assert_eq!(sent["success"], true, "re-raise sendMessage ok: {sent}");
    let mut re_raised = false;
    let mut idle = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while !(re_raised && idle) {
        let Some(frame) = wss_event_opt_until(&mut sub, deadline).await else {
            panic!("timed out waiting for the re-raise: re_raised={re_raised} idle={idle}")
        };
        let ev = &frame["params"]["event"];
        let data = &ev["data"];
        match ev["type"].as_str().unwrap_or_default() {
            "agent:updated"
                if data["agentId"] == json!(agent_id)
                    && data["attentionRequestKind"].is_string() =>
            {
                re_raised = true;
            }
            "agent:idle" if data["agentId"] == json!(agent_id) => idle = true,
            _ => {}
        }
    }

    // An AUTOMATIC delivery (agent.sendToTask — same default-origin path as
    // A2A sends and system wakes) ALSO retires the pending request for THIS
    // agent: the delegate is a BACKGROUND session (`agent.delegate` persists
    // `isBackground: true`), so the child/background retire rule applies
    // (PROTOCOL §5.5) — the coordinator's follow-up is the acknowledgement.
    let auto_sent = wss_rpc(
        &mut rpc,
        17,
        "agent.sendToTask",
        json!({ "workspaceId": ws_id, "taskNoteId": note_id, "message": "automatic nudge" }),
    )
    .await;
    assert_eq!(auto_sent["ok"], true, "sendToTask ok: {auto_sent}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let Some(frame) = wss_event_opt_until(&mut sub, deadline).await else {
            panic!("timed out waiting for the automatic delivery's attentionRequestCleared")
        };
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:updated"
            && ev["data"]["agentId"] == json!(agent_id)
            && ev["data"]["attentionRequestCleared"] == true
        {
            break;
        }
    }
    let got = wss_rpc(
        &mut rpc,
        18,
        "agent.getSession",
        json!({ "agentId": agent_id, "workspaceId": ws_id }),
    )
    .await;
    let session = got["session"].as_object().expect("session object");
    assert!(
        !session.contains_key("attentionRequestKind"),
        "attentionRequestKind cleared by the automatic delivery (background delegate)"
    );
    assert!(
        !session.contains_key("attentionRequestReason"),
        "attentionRequestReason cleared by the automatic delivery (background delegate)"
    );
    assert!(
        !session.contains_key("attentionRequestTimestamp"),
        "attentionRequestTimestamp cleared by the automatic delivery (background delegate)"
    );
}

/// The preserved NEGATIVE case of the attention-clear gate over the wire
/// (PROTOCOL §5.5, monorepo#1237): an AUTOMATIC delivery to a TOP-LEVEL
/// FOREGROUND agent — created via `agent.create` (not a delegate), so no
/// parent linkage and `isBackground` defaults to false — must NOT retire a
/// pending `requestDiscussion` attention request. Only the user may dismiss
/// a top-level foreground agent's request, so the `agent.sendToTask` nudge
/// (the same default-origin path as A2A sends and system wakes) completes
/// its turn WITHOUT emitting `attentionRequestCleared`, and
/// `agent.getSession` still serves the pending `attentionRequest*` fields
/// afterwards. Complements the child/background clear path in
/// `attention_request_discussion_over_wss` and the unit-level
/// `attention_request_clear_gates` suite.
#[tokio::test]
async fn attention_request_foreground_automatic_delivery_negative_over_wss() {
    const RAISE_MARKER: &str = "ATTN_FG_RAISE";
    const REASON: &str = "ATTN_WSS foreground needs the user's decision";
    let Some(script) = gate("WSS attention-request foreground negative E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let (ws_id, note_id) = seed_workspace_and_note(&data_dir).await;
    let request_js = format!(
        "return await ws.agent.requestDiscussion({});",
        json!(REASON)
    );
    let behavior = json!({
        "rules": [{
            "ifPromptContains": RAISE_MARKER,
            "toolCall": {
                "name": "workspace_api",
                "arguments": { "code": request_js, "summary": "raise discussion request" }
            },
            "response": "turn ended after requestDiscussion",
        }],
        "response": "automatic nudge acknowledged",
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
    ];
    let child_proc = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child: child_proc,
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

    // SUBSCRIBER conn — agent events, registered BEFORE any turn.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let marked = wss_rpc(
        &mut rpc,
        10,
        "task.markAsTask",
        json!({ "workspaceId": ws_id, "noteId": note_id, "status": "in_progress" }),
    )
    .await;
    assert_eq!(marked["ok"], true, "markAsTask ok: {marked}");

    // TOP-LEVEL FOREGROUND agent (`agent.create` front door — contrast the
    // background delegate in `attention_request_discussion_over_wss`),
    // assigned to the task note so `agent.sendToTask` resolves it as the
    // assignee.
    let created = wss_rpc(
        &mut rpc,
        11,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "FG-Attn", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let assigned = wss_rpc(
        &mut rpc,
        12,
        "task.assignAgent",
        json!({ "workspaceId": ws_id, "noteId": note_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(assigned["ok"], true, "assignAgent ok: {assigned}");

    // Raise: a user message carrying the behavior marker drives the
    // requestDiscussion turn, leaving a pending request on the session.
    let sent = wss_rpc(
        &mut rpc,
        13,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "content": format!("{RAISE_MARKER} raise a discussion request"),
        }),
    )
    .await;
    assert_eq!(sent["success"], true, "raise sendMessage ok: {sent}");
    let mut raised = false;
    let mut idle = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while !(raised && idle) {
        let Some(frame) = wss_event_opt_until(&mut sub, deadline).await else {
            panic!("timed out waiting for the raise: raised={raised} idle={idle}")
        };
        let ev = &frame["params"]["event"];
        let data = &ev["data"];
        match ev["type"].as_str().unwrap_or_default() {
            "agent:updated"
                if data["agentId"] == json!(agent_id)
                    && data["attentionRequestKind"].is_string() =>
            {
                raised = true;
            }
            "agent:idle" if data["agentId"] == json!(agent_id) => idle = true,
            _ => {}
        }
    }
    let got = wss_rpc(
        &mut rpc,
        14,
        "agent.getSession",
        json!({ "agentId": agent_id, "workspaceId": ws_id }),
    )
    .await;
    let session = &got["session"];
    assert_eq!(
        session["attentionRequestKind"], "discussion",
        "pending attentionRequestKind before the automatic delivery: {session}"
    );
    assert_eq!(
        session["attentionRequestReason"], REASON,
        "pending attentionRequestReason before the automatic delivery: {session}"
    );
    assert!(
        session["attentionRequestTimestamp"].is_string(),
        "pending attentionRequestTimestamp before the automatic delivery: {session}"
    );

    // The AUTOMATIC delivery (agent.sendToTask — same default-origin path
    // as A2A sends and system wakes) must NOT retire the request for this
    // top-level foreground agent. Were the clear to fire, its
    // `agent:updated` would be published at the nudge turn's begin, BEFORE
    // the turn runs — so the turn's terminal `agent:idle` bounds the
    // negative wait deterministically (no sleeps).
    let auto_sent = wss_rpc(
        &mut rpc,
        15,
        "agent.sendToTask",
        json!({ "workspaceId": ws_id, "taskNoteId": note_id, "message": "automatic nudge" }),
    )
    .await;
    assert_eq!(auto_sent["ok"], true, "sendToTask ok: {auto_sent}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let Some(frame) = wss_event_opt_until(&mut sub, deadline).await else {
            panic!("timed out waiting for the automatic nudge turn's agent:idle")
        };
        let ev = &frame["params"]["event"];
        let data = &ev["data"];
        if data["agentId"] != json!(agent_id) {
            continue;
        }
        assert!(
            !(ev["type"] == "agent:updated" && data["attentionRequestCleared"] == json!(true)),
            "automatic delivery must NOT emit attentionRequestCleared for a \
             top-level foreground agent: {ev}"
        );
        if ev["type"] == "agent:idle" {
            break;
        }
    }
    let got = wss_rpc(
        &mut rpc,
        16,
        "agent.getSession",
        json!({ "agentId": agent_id, "workspaceId": ws_id }),
    )
    .await;
    let session = &got["session"];
    assert_eq!(
        session["attentionRequestKind"], "discussion",
        "attentionRequestKind survives the automatic delivery: {session}"
    );
    assert_eq!(
        session["attentionRequestReason"], REASON,
        "attentionRequestReason survives the automatic delivery: {session}"
    );
    assert!(
        session["attentionRequestTimestamp"].is_string(),
        "attentionRequestTimestamp survives the automatic delivery: {session}"
    );
}

/// Agent attention requests over WSS — blocker kind + the taskless-caller
/// path. Phase 1: a task-linked delegated agent calls
/// `ws.agent.reportBlocker(reason)` → `agent:attention-requested` with
/// `kind: "blocker"`, the linked task moves to `blocked`, the transcript
/// notice persists with `meta.kind = "blocker-report"`, and the agent's
/// status is NOT `error`. Phase 2: a plain user-created agent (non-delegated,
/// no linked task) calls `ws.agent.requestDiscussion(reason)` — the call
/// succeeds, the session fields persist, and NO `task:status-changed` fires
/// (no linked task = the transition is skipped).
#[tokio::test]
async fn attention_request_blocker_and_taskless_caller_over_wss() {
    const BLOCKER_MARKER: &str = "ATTN_BLOCKER_CHILD";
    const TASKLESS_MARKER: &str = "ATTN_TASKLESS_AGENT";
    const BLOCK_REASON: &str = "ATTN_WSS sandbox filesystem is read-only";
    const TASKLESS_REASON: &str = "ATTN_WSS which provider should I target?";
    let Some(script) = gate("WSS attention-request blocker/taskless E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let (ws_id, note_id) = seed_workspace_and_note(&data_dir).await;
    let blocker_js = format!(
        "return await ws.agent.reportBlocker({});",
        json!(BLOCK_REASON)
    );
    let taskless_js = format!(
        "return await ws.agent.requestDiscussion({});",
        json!(TASKLESS_REASON)
    );
    let behavior = json!({
        "rules": [
            {
                "ifPromptContains": BLOCKER_MARKER,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": blocker_js, "summary": "report blocker" }
                },
                "response": "turn ended after reportBlocker",
            },
            {
                "ifPromptContains": TASKLESS_MARKER,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": taskless_js, "summary": "taskless requestDiscussion" }
                },
                "response": "turn ended after taskless requestDiscussion",
            },
        ],
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
    ];
    let child_proc = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child: child_proc,
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
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*", "task:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // --- Phase 1: task-linked blocker ------------------------------------
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let marked = wss_rpc(
        &mut rpc,
        10,
        "task.markAsTask",
        json!({ "workspaceId": ws_id, "noteId": note_id, "status": "in_progress" }),
    )
    .await;
    assert_eq!(marked["ok"], true, "markAsTask ok: {marked}");
    let delegated = wss_rpc(
        &mut rpc,
        11,
        "agent.delegate",
        json!({
            "workspaceId": ws_id,
            "taskNoteId": note_id,
            "agentInstructions": format!("{BLOCKER_MARKER} report an environment blocker"),
            "model": "mock:default",
        }),
    )
    .await;
    assert_eq!(delegated["ok"], true, "delegate ok: {delegated}");
    let blocker_id = delegated["agentId"].as_str().expect("agent id").to_string();

    let mut attention: Option<Value> = None;
    let mut task_changed: Option<Value> = None;
    let mut idle = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while !(attention.is_some() && task_changed.is_some() && idle) {
        let Some(frame) = wss_event_opt_until(&mut sub, deadline).await else {
            panic!(
                "timed out: attention={a} task_changed={t} idle={idle}",
                a = attention.is_some(),
                t = task_changed.is_some(),
            )
        };
        let ev = &frame["params"]["event"];
        let data = &ev["data"];
        match ev["type"].as_str().unwrap_or_default() {
            "agent:attention-requested" if data["agentId"] == json!(blocker_id) => {
                attention = Some(data.clone());
            }
            "task:status-changed" if data["noteId"] == json!(note_id) => {
                task_changed = Some(data.clone());
            }
            "agent:idle" if data["agentId"] == json!(blocker_id) => idle = true,
            _ => {}
        }
    }
    let attention = attention.expect("blocker attention event");
    assert_eq!(
        attention["kind"], "blocker",
        "attention event kind: {attention}"
    );
    assert_eq!(
        attention["reason"], BLOCK_REASON,
        "attention event reason: {attention}"
    );
    let task_changed = task_changed.expect("task:status-changed captured");
    assert_eq!(
        task_changed["newStatus"], "blocked",
        "task transition target: {task_changed}"
    );
    let got_task = wss_rpc(
        &mut rpc,
        12,
        "task.get",
        json!({ "workspaceId": ws_id, "taskNoteId": note_id }),
    )
    .await;
    assert_eq!(
        got_task["task"]["status"], "blocked",
        "linked task persisted at blocked: {got_task}"
    );
    let got = wss_rpc(
        &mut rpc,
        13,
        "agent.getSession",
        json!({ "agentId": blocker_id, "workspaceId": ws_id }),
    )
    .await;
    let session = &got["session"];
    assert_eq!(
        session["attentionRequestKind"], "blocker",
        "session attentionRequestKind"
    );
    assert_eq!(
        session["attentionRequestReason"], BLOCK_REASON,
        "session attentionRequestReason"
    );
    assert_ne!(
        session["status"], "error",
        "blocker turn ended normally: {}",
        session["status"]
    );
    let messages = session["messages"].as_array().expect("messages array");
    let notice = messages
        .iter()
        .find(|m| {
            m["role"] == "system" && m["contentBlocks"][0]["meta"]["kind"] == "blocker-report"
        })
        .expect("persisted blocker-report notice");
    assert_eq!(
        notice["contentBlocks"][0]["text"], BLOCK_REASON,
        "notice carries the reason: {notice}"
    );

    // --- Phase 2: taskless caller -----------------------------------------
    let created = wss_rpc(
        &mut rpc,
        20,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Taskless", "model": "mock:default" }),
    )
    .await;
    let taskless_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let sent = wss_rpc(
        &mut rpc,
        21,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": taskless_id, "content": TASKLESS_MARKER }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    let mut attention: Option<Value> = None;
    let mut idle = false;
    let mut task_events = 0u32;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while !(attention.is_some() && idle) {
        let Some(frame) = wss_event_opt_until(&mut sub, deadline).await else {
            panic!(
                "timed out: attention={a} idle={idle}",
                a = attention.is_some(),
            )
        };
        let ev = &frame["params"]["event"];
        let data = &ev["data"];
        match ev["type"].as_str().unwrap_or_default() {
            "agent:attention-requested" if data["agentId"] == json!(taskless_id) => {
                attention = Some(data.clone());
            }
            "task:status-changed" => task_events += 1,
            "agent:idle" if data["agentId"] == json!(taskless_id) => idle = true,
            _ => {}
        }
    }
    let attention = attention.expect("taskless attention event");
    assert_eq!(
        attention["kind"], "discussion",
        "taskless attention event kind: {attention}"
    );
    assert_eq!(
        attention["reason"], TASKLESS_REASON,
        "taskless attention event reason: {attention}"
    );
    assert_eq!(
        task_events, 0,
        "no task:status-changed for a taskless caller"
    );
    let got = wss_rpc(
        &mut rpc,
        22,
        "agent.getSession",
        json!({ "agentId": taskless_id, "workspaceId": ws_id }),
    )
    .await;
    let session = &got["session"];
    assert_eq!(
        session["attentionRequestKind"], "discussion",
        "taskless session attentionRequestKind"
    );
    assert_eq!(
        session["attentionRequestReason"], TASKLESS_REASON,
        "taskless session attentionRequestReason"
    );
    assert_ne!(
        session["status"], "error",
        "taskless turn ended normally: {}",
        session["status"]
    );
}

/// Delegated (parented) children carry the optional `parentAgentId` on both
/// `agent:attention-requested` and `agent:failed` over WSS. A parent agent
/// delegates TWO children through the MCP front door (`ws.agent.delegate`,
/// which records the caller as `parent_agent_id`):
///  - child A raises `ws.agent.requestDiscussion(reason)` →
///    `agent:attention-requested` with `parentAgentId` == the parent's id;
///  - child B's prompt carries the mock's `exitIfPromptContains` marker, so
///    every attempt dies mid-`session/prompt` — the one-shot silent redrive
///    (monorepo#764) is spent and the terminal `agent:failed` fires with
///    `parentAgentId` == the parent's id (enriched centrally in
///    `publish_agent_event`).
/// The parentless-omission halves are covered by
/// `attention_request_discussion_over_wss` (attention) and the MIDTURN-1
/// suite in `e2e_wss_agent_midturn_failure.rs` (failed).
#[tokio::test]
async fn delegated_child_attention_and_failure_carry_parent_agent_id_over_wss() {
    const PARENT_GO: &str = "PARENTID_PARENT_GO";
    const CHILD_ATTN: &str = "PARENTID_CHILD_ATTN";
    const CHILD_DIE: &str = "PARENTID_CHILD_DIE";
    const REASON: &str = "PARENTID need a decision from the coordinator";
    let Some(script) = gate("WSS parented attention/failed parentAgentId E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let request_js = format!(
        "return await ws.agent.requestDiscussion({});",
        json!(REASON)
    );
    let delegate_attn_js = format!(
        "return await ws.agent.delegate({{ agentInstructions: {}, model: 'mock:default' }});",
        json!(format!("{CHILD_ATTN} raise a discussion request")),
    );
    let delegate_die_js = format!(
        "return await ws.agent.delegate({{ agentInstructions: {}, model: 'mock:default' }});",
        json!(format!("{CHILD_DIE} this child dies mid-prompt")),
    );
    let behavior = json!({
        // Child B: every prompt carrying the marker dies mid-`session/prompt`
        // (checked before rule selection), so the silent redrive's fresh
        // child dies again and the failure goes terminal.
        "exitIfPromptContains": CHILD_DIE,
        "rules": [
            {
                "ifPromptContains": CHILD_ATTN,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": request_js, "summary": "raise discussion request" }
                },
                "response": "child A ended after requestDiscussion",
            },
            {
                "ifPromptContains": "[WORKSPACE EVENTS]",
                "response": "parent acknowledged the wake",
            },
            {
                "ifPromptContains": PARENT_GO,
                "toolCalls": [
                    {
                        "name": "workspace_api",
                        "arguments": { "code": delegate_attn_js, "summary": "delegate attention child" }
                    },
                    {
                        "name": "workspace_api",
                        "arguments": { "code": delegate_die_js, "summary": "delegate dying child" }
                    },
                ],
                "response": "parent delegated two children",
            },
        ],
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
    ];
    let child_proc = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child: child_proc,
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

    // SUBSCRIBER conn — registered BEFORE the parent's delegating turn.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let parent = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "ParentId-Parent", "model": "mock:default" }),
    )
    .await;
    let parent_id = parent["agent"]["id"]
        .as_str()
        .expect("parent id")
        .to_string();
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": parent_id, "content": PARENT_GO }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Both milestones under one hard deadline, order-insensitive: child A's
    // attention event and child B's terminal agent:failed. The failed path
    // includes a full silent-redrive cycle (kill + respawn + re-prompt), so
    // the window is generous.
    let mut attention: Option<Value> = None;
    let mut failed: Option<Value> = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    while !(attention.is_some() && failed.is_some()) {
        let Some(frame) = wss_event_opt_until(&mut sub, deadline).await else {
            panic!(
                "timed out: attention={a} failed={f}",
                a = attention.is_some(),
                f = failed.is_some(),
            )
        };
        let ev = &frame["params"]["event"];
        let data = &ev["data"];
        match ev["type"].as_str().unwrap_or_default() {
            "agent:attention-requested" if data["reason"] == json!(REASON) => {
                attention = Some(data.clone());
            }
            "agent:failed" if data["agentId"] != json!(parent_id) => {
                failed = Some(data.clone());
            }
            _ => {}
        }
    }

    // Child A's attention event names the delegating parent.
    let attention = attention.expect("attention event captured");
    let attn_child = attention["agentId"].as_str().expect("attention agentId");
    assert_ne!(attn_child, parent_id, "attention came from the child");
    assert_eq!(
        attention["parentAgentId"],
        json!(parent_id),
        "delegated child's attention event carries parentAgentId: {attention}"
    );

    // Child B's terminal agent:failed names the delegating parent too.
    let failed = failed.expect("failed event captured");
    let failed_child = failed["agentId"].as_str().expect("failed agentId");
    assert_ne!(
        failed_child, attn_child,
        "the dying child is a distinct agent"
    );
    let err = failed["error"].as_str().unwrap_or("");
    assert!(
        err.contains("agent stdout closed"),
        "agent:failed carries the mid-turn prompt error, got: {err}"
    );
    assert_eq!(
        failed["parentAgentId"],
        json!(parent_id),
        "delegated child's agent:failed carries parentAgentId: {failed}"
    );
}

/// Pre-seed the daemon's `SQLite` store with a workspace + target note for the
/// MCP tool call (the daemon opens the same data dir on launch).
async fn seed_workspace_and_note(data_dir: &Path) -> (String, String) {
    use intent_core::{NoteCreate, WorkspaceApi, WorkspaceId};
    use intent_services::Services;
    use intent_store::Store;
    let db_path = data_dir.join("intentd.db");
    let store = Store::open(&db_path).await.expect("open store");
    let ws_root = common::hermetic_workspaces_root();
    let services = Services::new(store.clone()).with_workspaces_root(ws_root.path().to_path_buf());
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace_seed(&ws))
        .await
        .expect("insert ws");
    let note = services
        .create_note(
            ws.clone(),
            NoteCreate {
                title: "Target".into(),
                content: Some("# Target\n".into()),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create note")
        .note;
    (ws.0, note.id.0)
}

async fn seed_workspace_only(data_dir: &Path) -> String {
    use intent_core::WorkspaceId;
    use intent_store::Store;
    let db_path = data_dir.join("intentd.db");
    let store = Store::open(&db_path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace_seed(&ws))
        .await
        .expect("insert ws");
    ws.0
}

fn workspace_seed(id: &intent_core::WorkspaceId) -> intent_core::Workspace {
    use intent_core::{now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus};
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "WSS-E2E".to_string(),
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
        worktree_path: None,
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
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
    }
}

// ---------------------------------------------------------------------------
// WSS-2: broaden WSS-driven coverage for untested router read/lifecycle arms
// and transport edges. New helpers below build on the WSS-1 plumbing above
// (`Daemon`, `spawn_serve`, `connect_ws`, `wss_rpc`, `wss_event`, ...).
// ---------------------------------------------------------------------------

/// Like [`wss_rpc`] but returns the FULL JSON-RPC envelope (`result` OR
/// `error`) so a caller can assert error envelopes for arms whose normal
/// outcome over WSS on a fresh workspace is a clean `-32xxx` (e.g. `pr.*`).
async fn wss_rpc_envelope<S>(
    ws: &mut WebSocketStream<S>,
    id: i64,
    method: &str,
    params: Value,
) -> Value
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
            .expect("wss rpc envelope timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["id"] == json!(id) {
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

/// Read one `events.event` notification with a deadline; returns `None` on
/// timeout / connection close so callers can assert "no event of type X
/// arrived" without panicking.
async fn try_wss_event<S>(ws: &mut WebSocketStream<S>, dur: Duration) -> Option<Value>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let Ok(next) = timeout(dur, ws.next()).await else {
            return None;
        };
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == "events.event" {
                    return Some(v);
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            None | Some(Err(_)) => return None,
        }
    }
}

/// Boot a hermetic `intentd serve` with WSS enabled (no mock-agent env), seed a
/// workspace + note, and return `(daemon, ws_id, note_id, port, fingerprint)`.
/// Used by the no-node read-arm sweep below.
async fn boot_daemon_with_seeded_note() -> (Daemon, String, String, u16, String) {
    let data_dir = temp_data_dir();
    let (ws_id, note_id) = seed_workspace_and_note(&data_dir).await;
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
    let child = spawn_serve(&data_dir, "both", &env);
    let daemon = Daemon {
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
    (daemon, ws_id, note_id, port, fingerprint)
}

/// WSS-2 (router): drive a broad slice of untested-over-WSS read/lifecycle
/// router arms — `note.*` (list/get/create/update/listTasks), `mcp.servers.list`,
/// `script.*` (list/create/status/remove), `terminal.*` (create/list/kill),
/// `primitive.*` (addReference/addCli + note mutation), and the `pr.status`
/// error-envelope arm — all over ONE pinned WSS RPC connection so each match
/// arm in `intent-transport::router::dispatch` is exercised through
/// `conn::process_frame` (which is uncounted over WSS in the COV-1 baseline).
/// No agent turn → no `node` dependency.
#[tokio::test]
async fn router_read_lifecycle_arms_over_wss() {
    let (_daemon, ws_id, note_id, port, fingerprint) = boot_daemon_with_seeded_note().await;
    let cfg = client_config(&fingerprint);
    let mut rpc = connect_ws(port, cfg.clone()).await;

    // --- note.* read/lifecycle arms ----------------------------------------
    let list = wss_rpc(&mut rpc, 1, "note.list", json!({ "workspaceId": ws_id })).await;
    let notes = list["notes"].as_array().expect("notes array");
    assert!(
        notes.iter().any(|n| n["id"] == json!(note_id)),
        "seeded note found via note.list: {list}"
    );

    let got = wss_rpc(
        &mut rpc,
        2,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    assert_eq!(got["note"]["id"], json!(note_id));

    let updated = wss_rpc(
        &mut rpc,
        3,
        "note.update",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "content": "# Target\nrouter-wss-update\n",
        }),
    )
    .await;
    assert!(updated["note"]["content"]
        .as_str()
        .unwrap_or_default()
        .contains("router-wss-update"));

    let got2 = wss_rpc(
        &mut rpc,
        4,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    assert!(got2["note"]["content"]
        .as_str()
        .unwrap_or_default()
        .contains("router-wss-update"));

    let created = wss_rpc(
        &mut rpc,
        5,
        "note.create",
        json!({
            "workspaceId": ws_id,
            "title": "Created via WSS",
            "content": "- [ ] Todo via wss\n",
        }),
    )
    .await;
    let created_id = created["note"]["id"]
        .as_str()
        .expect("created id")
        .to_string();
    let list2 = wss_rpc(&mut rpc, 6, "note.list", json!({ "workspaceId": ws_id })).await;
    assert!(list2["notes"]
        .as_array()
        .expect("notes")
        .iter()
        .any(|n| n["id"] == json!(created_id)));

    // `note.listTasks` returns a bare array (TS parity).
    let tasks = wss_rpc(
        &mut rpc,
        7,
        "note.listTasks",
        json!({ "workspaceId": ws_id, "noteId": created_id }),
    )
    .await;
    assert!(tasks.is_array(), "note.listTasks bare array: {tasks}");

    // --- mcp.servers.list — empty on a fresh workspace ----------------------
    let mcp = wss_rpc(&mut rpc, 8, "mcp.servers.list", json!({})).await;
    let servers = mcp["servers"].as_array().expect("servers array");
    assert!(
        servers.is_empty(),
        "no mcp servers configured on a fresh workspace"
    );

    // --- script.* lifecycle reads -------------------------------------------
    let scripts0 = wss_rpc(&mut rpc, 9, "script.list", json!({ "workspaceId": ws_id })).await;
    assert!(scripts0["scripts"].as_array().expect("scripts").is_empty());
    let created_script = wss_rpc(
        &mut rpc,
        10,
        "script.create",
        json!({
            "workspaceId": ws_id,
            "name": "service-wss",
            "command": "sleep 3600",
            "mode": "service",
        }),
    )
    .await;
    let script_id = created_script["id"]
        .as_str()
        .expect("script id")
        .to_string();
    let scripts1 = wss_rpc(&mut rpc, 11, "script.list", json!({ "workspaceId": ws_id })).await;
    assert_eq!(
        scripts1["scripts"].as_array().expect("scripts").len(),
        1,
        "one script after create: {scripts1}"
    );
    let status = wss_rpc(
        &mut rpc,
        12,
        "script.status",
        json!({ "workspaceId": ws_id, "scriptId": script_id }),
    )
    .await;
    assert!(status.is_object(), "script.status object: {status}");
    let started = wss_rpc(
        &mut rpc,
        101,
        "script.start",
        json!({ "workspaceId": ws_id, "scriptId": script_id }),
    )
    .await;
    assert_eq!(started["ok"], json!(true));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let mut poll_id = 102;
    loop {
        let runtime = wss_rpc(
            &mut rpc,
            poll_id,
            "script.status",
            json!({ "workspaceId": ws_id, "scriptId": script_id }),
        )
        .await;
        poll_id += 1;
        if runtime["status"] == "running" {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "script did not reach running state: {runtime}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let script_terminals = wss_rpc(
        &mut rpc,
        poll_id,
        "terminal.list",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    poll_id += 1;
    assert_eq!(
        script_terminals["terminals"],
        json!([]),
        "running script PTY must not be exposed as a terminal tab"
    );
    // `terminal.list` responds with the `{ terminals, daemonBootId }` envelope
    // (PROTOCOL §5.13; monorepo#1334) — even for an empty workspace.
    let script_boot_id = script_terminals["daemonBootId"]
        .as_str()
        .expect("daemonBootId string")
        .to_string();
    let script_output = wss_rpc(
        &mut rpc,
        poll_id,
        "script.output",
        json!({ "workspaceId": ws_id, "scriptId": script_id }),
    )
    .await;
    poll_id += 1;
    assert!(script_output.is_string(), "script.output remains available");
    let stopped = wss_rpc(
        &mut rpc,
        poll_id,
        "script.stop",
        json!({ "workspaceId": ws_id, "scriptId": script_id }),
    )
    .await;
    assert_eq!(stopped["ok"], json!(true));
    let removed = wss_rpc(
        &mut rpc,
        13,
        "script.remove",
        json!({ "workspaceId": ws_id, "scriptId": script_id }),
    )
    .await;
    assert_eq!(removed["ok"], json!(true));

    // --- terminal.* hermetic lifecycle --------------------------------------
    let term = wss_rpc(
        &mut rpc,
        14,
        "terminal.create",
        json!({ "workspaceId": ws_id, "cols": 80, "rows": 24 }),
    )
    .await;
    let terminal_id = term["terminalId"]
        .as_str()
        .expect("terminal id")
        .to_string();
    let term_list = wss_rpc(
        &mut rpc,
        15,
        "terminal.list",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    let terms = term_list["terminals"].as_array().expect("terminals array");
    assert!(
        terms.iter().any(|t| t["id"] == json!(terminal_id)),
        "created terminal listed: {term_list}"
    );
    assert_eq!(
        term_list["daemonBootId"].as_str(),
        Some(script_boot_id.as_str()),
        "daemonBootId stable across calls within one daemon boot"
    );
    // `terminal.write` accepts base64-encoded stdin bytes (PROTOCOL §5.13).
    // "ZWNobyBoaQo=" is base64 for "echo hi\n" — short and stable.
    let written = wss_rpc(
        &mut rpc,
        16,
        "terminal.write",
        json!({ "terminalId": terminal_id, "data": "ZWNobyBoaQo=" }),
    )
    .await;
    assert_eq!(written["ok"], json!(true));
    let killed = wss_rpc(
        &mut rpc,
        17,
        "terminal.kill",
        json!({ "terminalId": terminal_id }),
    )
    .await;
    assert_eq!(killed["ok"], json!(true));

    // --- primitive.* mutation arms (asserted via note.get content) ----------
    let added_ref = wss_rpc(
        &mut rpc,
        18,
        "primitive.addReference",
        json!({
            "workspaceId": ws_id,
            "noteId": created_id,
            "semanticId": "src/lib.rs#L1-10",
            "description": "wss ref",
        }),
    )
    .await;
    assert_eq!(added_ref["ok"], json!(true));
    let added_cli = wss_rpc(
        &mut rpc,
        19,
        "primitive.addCli",
        json!({
            "workspaceId": ws_id,
            "noteId": created_id,
            "command": "echo hello",
            "description": "wss cli",
        }),
    )
    .await;
    assert_eq!(added_cli["ok"], json!(true));
    let got_created = wss_rpc(
        &mut rpc,
        20,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": created_id }),
    )
    .await;
    let body = got_created["note"]["content"].as_str().unwrap_or_default();
    assert!(
        body.contains("wss ref") && body.contains("echo hello"),
        "primitive.* appended to note body: {body}"
    );

    // --- pr.status: error-envelope arm on a fresh workspace -----------------
    // The seeded workspace has no `repository_owner`/`repository_name`/`pr_number`,
    // so `pr.status` returns the well-defined "no active PR" envelope — still a
    // valid hit on the router arm via the WSS path.
    let pr_env = wss_rpc_envelope(&mut rpc, 21, "pr.status", json!({ "workspaceId": ws_id })).await;
    assert!(
        pr_env.get("error").is_some(),
        "pr.status returns an error envelope on a fresh workspace: {pr_env}"
    );
    assert_eq!(
        pr_env["error"]["code"],
        json!(-32603),
        "pr.status `Error::Internal` → -32603 (§9): {pr_env}"
    );
}

/// WSS-2 (terminal.create env, §5.13 gap): the `env` param is layered onto the
/// daemon's inherited environment before the PTY spawn, so an agent-supplied
/// variable is visible to the spawned child. Runs the `env` binary as the
/// terminal command, subscribes to `terminal:data`, and asserts the decoded
/// output carries `MY_TEST_VAR=PROT_MARKER_env_wss`.
#[tokio::test]
async fn terminal_create_env_over_wss() {
    use base64::Engine as _;

    let (_daemon, ws_id, _note_id, port, fingerprint) = boot_daemon_with_seeded_note().await;
    let cfg = client_config(&fingerprint);

    // SUBSCRIBER conn — subscribe BEFORE spawning so no chunk is missed.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["terminal:data", "terminal:exit"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // RPC conn — spawn `env` with a per-terminal env overlay.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        2,
        "terminal.create",
        json!({
            "workspaceId": ws_id,
            "cols": 80,
            "rows": 24,
            "command": "env",
            "env": { "MY_TEST_VAR": "PROT_MARKER_env_wss" },
        }),
    )
    .await;
    let terminal_id = created["terminalId"]
        .as_str()
        .expect("terminalId in terminal.create result")
        .to_string();

    // Collect `terminal:data` chunks (base64) until we've seen both the env
    // marker AND the child's `terminal:exit`, or a single 30s total budget
    // elapses. On slow CI runners the `env` dump can dribble in as many small
    // chunks, so a fixed iteration cap can trip the assert with a truncated
    // buffer — track one overall deadline and recompute the remaining wait
    // each loop, matching the `try_read_text` pattern in
    // `e2e_wss_sticky_reverse.rs`.
    let mut acc: Vec<u8> = Vec::new();
    let mut saw_exit = false;
    let mut saw_marker = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !(saw_marker && saw_exit) {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        let Ok(next) = timeout(remaining, sub.next()).await else {
            break;
        };
        match next {
            Some(Ok(Message::Text(text))) => {
                let frame: Value = serde_json::from_str(&text).expect("json frame");
                if frame["method"] != "events.event" {
                    continue;
                }
                let event = &frame["params"]["event"];
                if event["data"]["terminalId"].as_str() != Some(&terminal_id) {
                    continue;
                }
                match event["type"].as_str() {
                    Some("terminal:data") => {
                        if let Some(chunk) = event["data"]["chunk"].as_str() {
                            let bytes = base64::engine::general_purpose::STANDARD
                                .decode(chunk)
                                .expect("valid base64 in terminal:data.chunk");
                            acc.extend_from_slice(&bytes);
                        }
                    }
                    Some("terminal:exit") => {
                        saw_exit = true;
                    }
                    _ => {}
                }
                if !saw_marker
                    && String::from_utf8_lossy(&acc).contains("MY_TEST_VAR=PROT_MARKER_env_wss")
                {
                    saw_marker = true;
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = sub.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
    let text = String::from_utf8_lossy(&acc);
    // `saw_exit` is a required post-condition — the child prints its env and
    // exits, so a missed `terminal:exit` means the loop bailed on the 30s
    // deadline with a partial buffer, not a successful drain. Assert it
    // explicitly so a truncated marker never silently passes.
    assert!(
        saw_exit,
        "terminal.create must emit `terminal:exit` after the child prints its env \
         (PROTOCOL §5.13); output was: {text:?}"
    );
    assert!(
        text.contains("MY_TEST_VAR=PROT_MARKER_env_wss"),
        "terminal.create must overlay the caller's env onto the spawned child \
         (PROTOCOL §5.13); output was: {text:?}, saw_exit={saw_exit}"
    );

    let listed = wss_rpc(
        &mut rpc,
        3,
        "terminal.list",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert!(
        listed["terminals"]
            .as_array()
            .is_some_and(|terms| terms.iter().all(|term| term["id"] != terminal_id)),
        "naturally exited terminal must be omitted from terminal.list: {listed}"
    );
    assert!(
        listed["daemonBootId"].is_string(),
        "terminal.list envelope carries daemonBootId: {listed}"
    );

    let buffer = wss_rpc(
        &mut rpc,
        4,
        "terminal.getBuffer",
        json!({ "terminalId": terminal_id }),
    )
    .await;
    assert_eq!(buffer["terminalId"], json!(terminal_id));
    assert!(buffer["data"].is_string(), "retained scrollback: {buffer}");

    let released = wss_rpc(
        &mut rpc,
        5,
        "terminal.kill",
        json!({ "terminalId": terminal_id }),
    )
    .await;
    assert_eq!(released["ok"], json!(true));
}

/// Regression (paste/echo throughput): `terminal:data` is transient /
/// broadcast-only (PROTOCOL §5.10 retention note, §6.5), so a PTY producing
/// many small output chunks must (a) deliver every chunk to a live WSS
/// subscriber, in order, before `terminal:exit`, and (b) leave zero
/// `terminal:data` rows behind for `event.query` — while `terminal:exit`
/// stays durable. Before the fix each chunk awaited a durable `SQLite` commit,
/// serializing paste echo behind the writer batch window.
#[tokio::test]
async fn terminal_data_many_chunks_transient_over_wss() {
    // 200 fixed-width markers; the command echo carries the literal
    // `CHUNK-%03d-END` template, which never collides with an expanded marker.
    const CHUNKS: usize = 200;
    use base64::Engine as _;

    let (_daemon, ws_id, _note_id, port, fingerprint) = boot_daemon_with_seeded_note().await;
    let cfg = client_config(&fingerprint);

    // SUBSCRIBER conn — subscribe BEFORE spawning so no chunk is missed.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["terminal:data", "terminal:exit"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // RPC conn — spawn a bare `sh` and drive it via terminal.write, so the
    // loop output streams as many small live chunks (paste-echo shape).
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        2,
        "terminal.create",
        json!({ "workspaceId": ws_id, "cols": 80, "rows": 24, "command": "sh" }),
    )
    .await;
    let terminal_id = created["terminalId"]
        .as_str()
        .expect("terminalId in terminal.create result")
        .to_string();

    let script =
        format!("for i in $(seq 1 {CHUNKS}); do printf 'CHUNK-%03d-END\\n' \"$i\"; done; exit\n");
    let written = wss_rpc(
        &mut rpc,
        3,
        "terminal.write",
        json!({
            "terminalId": terminal_id,
            "data": base64::engine::general_purpose::STANDARD.encode(script.as_bytes()),
        }),
    )
    .await;
    assert_eq!(written["ok"], json!(true));

    // Accumulate decoded chunks until `terminal:exit`; deadline-driven like
    // `terminal_create_env_over_wss` so slow CI dribble never truncates. The
    // in-order marker scan below runs against ONLY the bytes accumulated
    // before the exit frame, so it doubles as the exit-never-overtakes-data
    // assertion.
    let mut acc: Vec<u8> = Vec::new();
    let mut saw_exit = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while !saw_exit {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        let Ok(next) = timeout(remaining, sub.next()).await else {
            break;
        };
        match next {
            Some(Ok(Message::Text(text))) => {
                let frame: Value = serde_json::from_str(&text).expect("json frame");
                if frame["method"] != "events.event" {
                    continue;
                }
                let event = &frame["params"]["event"];
                if event["data"]["terminalId"].as_str() != Some(&terminal_id) {
                    continue;
                }
                match event["type"].as_str() {
                    Some("terminal:data") => {
                        if let Some(chunk) = event["data"]["chunk"].as_str() {
                            let bytes = base64::engine::general_purpose::STANDARD
                                .decode(chunk)
                                .expect("valid base64 in terminal:data.chunk");
                            acc.extend_from_slice(&bytes);
                        }
                    }
                    Some("terminal:exit") => saw_exit = true,
                    _ => {}
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = sub.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
    let text = String::from_utf8_lossy(&acc);
    assert!(
        saw_exit,
        "terminal:exit must arrive after the loop finishes; output was: {text:?}"
    );
    // Every marker delivered, in order, before exit (exit never overtakes
    // data: only pre-exit bytes are in `acc`).
    let mut cursor = 0usize;
    for i in 1..=CHUNKS {
        let marker = format!("CHUNK-{i:03}-END");
        let pos = text[cursor..].find(&marker).unwrap_or_else(|| {
            panic!("marker {marker} missing or out of order (cursor {cursor}); output: {text:?}")
        });
        cursor += pos + marker.len();
    }

    // Transient: zero persisted terminal:data rows; durable: terminal:exit
    // committed before its broadcast, so it is already queryable here.
    let data_rows = wss_rpc(
        &mut rpc,
        4,
        "event.query",
        json!({ "workspaceId": ws_id, "eventType": "terminal:data" }),
    )
    .await;
    assert_eq!(
        data_rows,
        json!([]),
        "terminal:data must not be persisted (PROTOCOL §5.10 / §6.5)"
    );
    let exit_rows = wss_rpc(
        &mut rpc,
        5,
        "event.query",
        json!({ "workspaceId": ws_id, "eventType": "terminal:exit" }),
    )
    .await;
    assert!(
        !exit_rows.as_array().expect("exit rows array").is_empty(),
        "terminal:exit stays durable: {exit_rows}"
    );
}

/// Regression (monorepo#1538): `event.query`'s `eventType` accepts
/// subscribe-style globs over the wire — `note:*` matches note-category
/// events (it previously compiled to an exact `event_type = 'note:*'` match
/// and silently returned `[]`), exact types are unchanged, and bare `*`
/// behaves like no type filter.
#[tokio::test]
async fn event_query_event_type_glob_over_wss() {
    let (_daemon, ws_id, note_id, port, fingerprint) = boot_daemon_with_seeded_note().await;
    let cfg = client_config(&fingerprint);
    let mut rpc = connect_ws(port, cfg).await;

    // Drive a note mutation so a `note:updated` event is persisted.
    let updated = wss_rpc(
        &mut rpc,
        1,
        "note.update",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "content": "# Target\nevent-query-glob-marker\n",
        }),
    )
    .await;
    assert_eq!(updated["note"]["id"], json!(note_id));

    // Category glob matches the persisted note event(s). Poll briefly: the
    // event write is committed asynchronously relative to the RPC response.
    let mut id = 2;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let glob_rows = loop {
        let rows = wss_rpc(
            &mut rpc,
            id,
            "event.query",
            json!({ "workspaceId": ws_id, "eventType": "note:*" }),
        )
        .await;
        id += 1;
        if !rows.as_array().expect("glob rows array").is_empty() {
            break rows;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "note:* never matched the persisted note event: {rows}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(
        glob_rows
            .as_array()
            .expect("glob rows array")
            .iter()
            .all(|e| e["type"].as_str().unwrap_or_default().starts_with("note:")),
        "note:* must return only note-category events: {glob_rows}"
    );

    // Exact-type query is unchanged and agrees with the glob.
    let exact_rows = wss_rpc(
        &mut rpc,
        id,
        "event.query",
        json!({ "workspaceId": ws_id, "eventType": "note:updated" }),
    )
    .await;
    assert!(
        !exact_rows.as_array().expect("exact rows array").is_empty(),
        "exact note:updated query regressed: {exact_rows}"
    );

    // Bare `*` behaves like no type filter (previously an exact match on the
    // literal `*` → silent `[]`), so the note event is visible through it.
    let star_rows = wss_rpc(
        &mut rpc,
        id + 1,
        "event.query",
        json!({ "workspaceId": ws_id, "eventType": "*" }),
    )
    .await;
    assert!(
        star_rows
            .as_array()
            .expect("star rows array")
            .iter()
            .any(|e| e["type"].as_str().unwrap_or_default().starts_with("note:")),
        "bare * must behave like no type filter: {star_rows}"
    );
}

/// WSS-2 (subscriptions): narrow `eventTypes` filters route only matching
/// events to a subscriber. Two pinned WSS subscribers — one scoped to
/// `["note:*"]`, one to `["agent:*"]` — observe a single mock agent turn and
/// each receive ONLY their category. Exercises the filter+forward arms in
/// `subscriptions.rs` / `forward.rs` over WSS.
#[tokio::test]
async fn subscription_filter_branches_over_wss() {
    let Some(script) = gate("WSS subscription filter branches") else {
        return;
    };

    let data_dir = temp_data_dir();
    let (ws_id, note_id) = seed_workspace_and_note(&data_dir).await;
    // Post-WSAPI-8: agents drive `add_to_note` via `workspace_api` +
    // `ws.note.add`; the discrete tool is gone.
    let filter_js = format!(
        "return await ws.note.add({}, {{ content: 'filter-branch-marker' }});",
        json!(note_id),
    );
    let behavior = json!({
        "toolCall": {
            "name": "workspace_api",
            "arguments": { "code": filter_js, "summary": "WSS-2 filter branches ws.note.add" },
        },
        "response": "filter-branch-response",
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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

    // Subscriber A — `note:*` only.
    let mut sub_notes = connect_ws(port, cfg.clone()).await;
    let n_resp = wss_rpc(
        &mut sub_notes,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["note:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(n_resp["subscriptionId"].is_string());

    // Subscriber B — `agent:*` only.
    let mut sub_agents = connect_ws(port, cfg.clone()).await;
    let a_resp = wss_rpc(
        &mut sub_agents,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(a_resp["subscriptionId"].is_string());

    // RPC — drive a single turn (will produce both agent:* and note:updated).
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let _ = wss_rpc(
        &mut rpc,
        10,
        "client.hello",
        json!({ "clientId": "wss-filter", "name": "WSS Filter" }),
    )
    .await;
    let created = wss_rpc(
        &mut rpc,
        11,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "WSS-Filter", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let sent = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "go" }),
    )
    .await;
    assert_eq!(sent["success"], json!(true));

    // Agent-subscriber: collect until terminal stream:end. Assert no note:*.
    let mut agent_chunks = 0u32;
    let mut agent_end = false;
    let mut agent_saw_note = false;
    for _ in 0..80 {
        let frame = wss_event(&mut sub_agents, 30).await;
        match frame["params"]["event"]["type"].as_str() {
            Some("agent:stream:activity") => agent_chunks += 1,
            Some("agent:stream:end") => {
                agent_end = true;
                break;
            }
            Some(t) if t.starts_with("note:") => agent_saw_note = true,
            _ => {}
        }
    }
    assert!(agent_chunks >= 1, "agent:* subscriber got ≥1 chunk");
    assert!(agent_end, "agent:* subscriber got terminal stream:end");
    assert!(
        !agent_saw_note,
        "agent:* subscriber received NO note:* events (filter rejected note:*)"
    );

    // Note-subscriber: drain until silence; assert ≥1 note:updated and NO
    // agent:*. The agent turn is already done (stream:end above) so any
    // pending note:updated has been published; bound with a short timeout.
    let mut note_updated = false;
    let mut note_saw_agent = false;
    while let Some(frame) = try_wss_event(&mut sub_notes, Duration::from_millis(1500)).await {
        match frame["params"]["event"]["type"].as_str() {
            Some(t) if t.starts_with("agent:") => note_saw_agent = true,
            Some("note:updated") => note_updated = true,
            _ => {}
        }
    }
    assert!(
        note_updated,
        "note:* subscriber received ≥1 note:updated from the MCP tool call"
    );
    assert!(
        !note_saw_agent,
        "note:* subscriber received NO agent:* events (filter rejected agent:*)"
    );
}

/// WSS-2 (conn): a subscriber dropping its WSS connection mid-stream tears
/// down its forwarder cleanly without poisoning the daemon. After the drop:
/// (a) an existing RPC conn still answers `system.status` / `agent.list`, and
/// (b) a FRESH subscriber observes a subsequent turn's terminal `stream:end`.
#[tokio::test]
async fn mid_stream_subscriber_disconnect_over_wss() {
    let Some(script) = gate("WSS mid-stream subscriber disconnect") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let behavior = json!({ "blockUntilCancel": true, "response": "resumed" }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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

    // Subscriber A — gets the first chunk, then drops mid-stream while the
    // mock is parked at `session/cancel`.
    let mut sub_a = connect_ws(port, cfg.clone()).await;
    let _ = wss_rpc(
        &mut sub_a,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let _ = wss_rpc(
        &mut rpc,
        10,
        "client.hello",
        json!({ "clientId": "wss-disc", "name": "WSS Disc" }),
    )
    .await;
    let created = wss_rpc(
        &mut rpc,
        11,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "WSS-Disc", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let _ = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "first" }),
    )
    .await;

    // Wait for the parked chunk to arrive on sub_a, then DROP sub_a.
    let mut saw_chunk = false;
    for _ in 0..50 {
        let frame = wss_event(&mut sub_a, 30).await;
        if frame["params"]["event"]["type"] == "agent:stream:activity" {
            saw_chunk = true;
            break;
        }
    }
    assert!(saw_chunk, "sub_a got a chunk before disconnect");
    drop(sub_a);
    // Give the daemon a moment to observe the close and tear down the
    // forwarder + bus subscription (§6.1 disconnect cleanup).
    sleep(Duration::from_millis(200)).await;

    // Daemon still healthy: the existing RPC conn still answers a router
    // arm (`agent.list`) after the peer subscriber's mid-stream close.
    let list = wss_rpc(&mut rpc, 13, "agent.list", json!({ "workspaceId": ws_id })).await;
    assert!(list["agents"]
        .as_array()
        .expect("agents")
        .iter()
        .any(|a| a["id"] == json!(agent_id)));

    // Fresh subscriber: observes the next turn's terminal stream:end. We use
    // `agent.stop` to unpark the agent (proven terminal-end emitter, mirrors
    // `agent_stop_keep_alive_resume_over_wss` above).
    let mut sub_b = connect_ws(port, cfg.clone()).await;
    let _ = wss_rpc(
        &mut sub_b,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    let stopped = wss_rpc(&mut rpc, 14, "agent.stop", json!({ "agentId": agent_id })).await;
    assert_eq!(stopped["success"], json!(true));
    let mut sub_b_end = false;
    for _ in 0..50 {
        let frame = wss_event(&mut sub_b, 30).await;
        if frame["params"]["event"]["type"] == "agent:stream:end" {
            assert_eq!(
                frame["params"]["event"]["data"]["agentId"]
                    .as_str()
                    .unwrap_or_default(),
                agent_id,
            );
            sub_b_end = true;
            break;
        }
    }
    assert!(
        sub_b_end,
        "fresh subscriber after mid-stream drop observes terminal stream:end"
    );
}

/// WSS-2 (ws.rs upgrade edge): an oversized HTTP request head on the upgrade
/// path is rejected and the connection is dropped. The daemon caps the head
/// at `MAX_HEAD_BYTES` (16 KiB) and never writes a response in this case
/// (`read_request_head` returns `InvalidData`), so the client sees an EOF —
/// not a 4xx. We assert that no upgrade response arrives by reading until
/// EOF or timeout. No node required.
#[tokio::test]
async fn oversized_request_head_rejected_over_wss() {
    let (_daemon, _ws_id, _note_id, port, fingerprint) = boot_daemon_with_seeded_note().await;
    let cfg = client_config(&fingerprint);
    let mut tls = tls_connect(port, cfg).await;

    // Write a partial HTTP request head that never terminates with \r\n\r\n
    // and exceeds the 16 KiB cap, padded via a giant fake header value.
    let mut req = String::with_capacity(20_000);
    req.push_str("GET /ws HTTP/1.1\r\nHost: localhost\r\nX-Pad: ");
    req.push_str(&"a".repeat(20_000));
    // No trailing CRLFs — keep the head "unterminated" so the cap fires.
    tls.write_all(req.as_bytes()).await.expect("write head");
    tls.flush().await.expect("flush head");

    // Drain until EOF or timeout. The daemon must NOT respond with a 1xx/2xx
    // upgrade success; it either drops silently or returns a non-101 status.
    let mut buf = [0u8; 1024];
    let mut total = Vec::new();
    let deadline = Duration::from_secs(2);
    loop {
        match timeout(deadline, tls.read(&mut buf)).await {
            Ok(Ok(0) | Err(_)) | Err(_) => break,
            Ok(Ok(n)) => total.extend_from_slice(&buf[..n]),
        }
        if total.len() > 4096 {
            break;
        }
    }
    let resp = String::from_utf8_lossy(&total);
    assert!(
        !resp.contains("101 Switching Protocols"),
        "oversized head must NOT upgrade to a websocket: {resp:?}"
    );
}

// ---------------------------------------------------------------------------
// Queue self-drain (P0): `agent.queueMessage` on an IDLE agent must auto-send
// (drive a turn, persist the assistant reply, emit terminal `agent:stream:end`)
// with NO follow-up `agent.sendMessage`. `agent.removeQueuedMessage` must be
// idempotent (always succeeds), and `agent:queue:updated` must fire on every
// queue mutation carrying `{ agentId, queue }` (PROTOCOL §5.5/§6).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn queue_message_self_drains_on_idle_agent_over_wss() {
    let Some(script) = gate("WSS queue self-drain E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let behavior = json!({ "response": "queued drain ok" }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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

    // SUBSCRIBER conn — subscribe to queue + stream events BEFORE the enqueue.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_resp["subscriptionId"].is_string());

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "QDrain", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_string();

    // Enqueue with NO prior sendMessage — the BE must self-drain.
    let queued = wss_rpc(
        &mut rpc,
        11,
        "agent.queueMessage",
        json!({ "agentId": agent_id, "content": "drain me" }),
    )
    .await;
    assert_eq!(queued["success"], true);
    assert!(queued["queuedMessage"]["id"].is_string());

    // Collect events until terminal `agent:stream:end`; assert at least one
    // `agent:queue:updated` carrying `{ agentId, queue }` arrived along the way.
    let mut saw_queue_updated_enqueue = false;
    let mut saw_stream_end = false;
    for _ in 0..120 {
        let frame = wss_event(&mut sub, 30).await;
        let evt = &frame["params"]["event"];
        match evt["type"].as_str() {
            Some("agent:queue:updated") => {
                assert_eq!(evt["data"]["agentId"].as_str(), Some(agent_id.as_str()));
                assert!(evt["data"]["queue"].is_array(), "queue array present");
                if evt["data"]["queue"]
                    .as_array()
                    .is_some_and(|q| !q.is_empty())
                {
                    saw_queue_updated_enqueue = true;
                }
            }
            Some("agent:stream:end") => {
                saw_stream_end = true;
                break;
            }
            _ => {}
        }
    }
    assert!(
        saw_queue_updated_enqueue,
        "agent:queue:updated emitted on enqueue"
    );
    assert!(
        saw_stream_end,
        "self-drain drove a turn to terminal agent:stream:end with NO explicit sendMessage"
    );

    // Assistant message persisted in the transcript: proves the queued message
    // actually got flipped to in-flight and a turn ran end-to-end.
    let list = wss_rpc(&mut rpc, 12, "agent.list", json!({ "workspaceId": ws_id })).await;
    let listed = list["agents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["id"] == json!(agent_id))
        .expect("agent listed");
    let mc = listed["messageCount"].as_u64().unwrap_or(0);
    assert!(
        mc >= 1,
        "queued message produced an assistant reply (mc={mc})"
    );

    // Queue is empty post-drain.
    let q = wss_rpc(
        &mut rpc,
        13,
        "agent.getQueue",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert!(q["queue"].as_array().unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// STAB-4 regression: dequeued messages must publish agent:message events
// so live subscribers see the user message in the transcript without a full
// re-read. Subscribes to agent:* events, sends a message while the agent is
// busy (queues it), waits for the queue to drain, and asserts an agent:message
// event was published for the dequeued user message.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dequeued_message_publishes_agent_message_event_over_wss() {
    let Some(script) = gate("WSS dequeued message event E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    // First turn is slow to keep the agent busy while we queue the second message.
    let behavior = json!({
        "response": "mock reply",
        "firstTurnDelayMs": 2000
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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

    // SUBSCRIBER conn — subscribe to agent:* events BEFORE sending messages.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:queue:updated", "agent:message", "agent:stream:end"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_resp["subscriptionId"].is_string());

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "StabFour", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_string();

    // Send first message — agent will be busy for 2000ms (firstTurnDelayMs in mock config).
    let send1 = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "first message" }),
    )
    .await;
    assert_eq!(send1["success"], true);
    assert_eq!(send1["queued"], false);

    // Give the agent a moment to start processing the first message.
    sleep(Duration::from_millis(200)).await;

    // Send second message while agent is busy — this will queue.
    let send2 = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "queued message" }),
    )
    .await;
    assert_eq!(send2["success"], true);
    assert_eq!(send2["queued"], true, "second message should be queued");

    // Collect events under one shared deadline (30s, STAB-128 precedent).
    // The daemon does NOT guarantee the relative order of the empty-queue
    // `agent:queue:updated`, the dequeued user `agent:message`, and the
    // `agent:stream:end` frames — the end-of-turn drain and the stream:end
    // publication run on independent async paths — so track each signal
    // independently, filtered by agent id, with no cross-signal ordering
    // gates (STAB-34/36 pattern).
    let mut saw_queue_drain = false;
    let mut user_message_event_ids: Vec<String> = Vec::new();
    let mut stream_end_count = 0;

    // Fast-path exit note: `user_message_event_ids.len() >= 2` relies on the
    // direct-send branch of `agent.sendMessage` also emitting `agent:message`
    // for "first message" (PROTOCOL §5.5 step 6). If that emit ever went away
    // the loop would still be correct — it would just run to the deadline and
    // let the id-match assertion below decide.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !(saw_queue_drain && stream_end_count >= 2 && user_message_event_ids.len() >= 2) {
        let Some(frame) = wss_event_opt_until(&mut sub, deadline).await else {
            break;
        };
        let evt = &frame["params"]["event"];
        if evt["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        match evt["type"].as_str() {
            Some("agent:queue:updated") => {
                // The queue drains to empty once the first turn completes.
                if evt["data"]["queue"]
                    .as_array()
                    .is_some_and(std::vec::Vec::is_empty)
                {
                    saw_queue_drain = true;
                }
            }
            Some("agent:message") => {
                if evt["data"]["role"].as_str() == Some("user") {
                    if let Some(mid) = evt["data"]["messageId"].as_str() {
                        user_message_event_ids.push(mid.to_string());
                    }
                }
            }
            Some("agent:stream:end") => {
                stream_end_count += 1;
            }
            _ => {}
        }
    }

    assert!(
        saw_queue_drain,
        "queue should have drained after first turn"
    );
    assert!(
        stream_end_count >= 2,
        "both turns reached terminal agent:stream:end (saw {stream_end_count})"
    );

    // The `agent:message` payload carries `{ agentId, messageId, role }` (no
    // content, PROTOCOL §6.5) — resolve the persisted row for the dequeued
    // "queued message" content and match the collected event ids against it,
    // instead of relying on event arrival order.
    let convo = wss_rpc(
        &mut rpc,
        13,
        "agent.getConversation",
        json!({ "agentId": agent_id }),
    )
    .await;
    let dequeued_row_id = convo["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .find(|m| {
            m["role"] == "user"
                && m["contentBlocks"][0]["text"]
                    .as_str()
                    .is_some_and(|t| t.starts_with("queued message"))
        })
        .and_then(|m| m["id"].as_str())
        .expect("dequeued user message row present in transcript")
        .to_string();
    assert!(
        user_message_event_ids.contains(&dequeued_row_id),
        "agent:message event for dequeued user message — STAB-4 fix \
         (row id {dequeued_row_id}, event ids {user_message_event_ids:?})"
    );
}

// ---------------------------------------------------------------------------
// messageMetadata through the queued-message path: a send that arrives while
// the agent is busy must (a) surface the caller's `messageMetadata` on the
// queued entry wire shape (`queuedMessage.messageMetadata`, PROTOCOL §5.5) and
// (b) persist it on the drained user message row so `agent.getConversation`
// returns the same `metadata` a directly-delivered send would have — e.g. a
// parent wake's `event_notification` tag survives the busy-parent enqueue —
// plus (c) the drain-time `queueInfo` stamp ({ queuedAt, waitedMs }, PROTOCOL
// §5.5 dequeue-wait annotation) alongside the caller's fields.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn queued_message_metadata_survives_drain_over_wss() {
    let Some(script) = gate("WSS queued messageMetadata E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    // First turn is slow to open a deterministic window where the second send
    // (carrying messageMetadata) lands on a busy agent and queues.
    let behavior = json!({ "response": "mock reply", "firstTurnDelayMs": 2000 }).to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        // The 2s busy window sits below the 5s dequeue-wait annotation
        // threshold (monorepo#2353); drop it so the queueInfo assertions
        // exercise the stamp without slowing the suite.
        ("INTENTD_DEQUEUE_WAIT_MIN_MS", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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

    // SUBSCRIBER conn — watch for stream ends so we know when both turns ran.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:stream:end"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_resp["subscriptionId"].is_string());

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "QMeta", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_string();

    // First send — the agent goes busy for 2000ms.
    let send1 = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "first message" }),
    )
    .await;
    assert_eq!(send1["success"], true);
    assert_eq!(send1["queued"], false);
    sleep(Duration::from_millis(200)).await;

    // Second send while busy, tagged like a parent wake's event notification.
    let metadata = json!({
        "type": "event_notification",
        "eventType": "task_completion",
        "taskNoteId": "note-meta-1",
    });
    let send2 = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "content": "tagged queued message",
            "messageMetadata": metadata,
        }),
    )
    .await;
    assert_eq!(send2["success"], true);
    assert_eq!(send2["queued"], true, "second send should queue: {send2}");
    // (a) The queued entry wire shape carries messageMetadata verbatim.
    assert_eq!(
        send2["queuedMessage"]["messageMetadata"], metadata,
        "queued entry must carry messageMetadata: {send2}"
    );

    // The live queue snapshot agrees with the send response.
    let q = wss_rpc(
        &mut rpc,
        13,
        "agent.getQueue",
        json!({ "agentId": agent_id }),
    )
    .await;
    let queue = q["queue"].as_array().expect("queue array");
    assert_eq!(queue.len(), 1);
    assert_eq!(queue[0]["messageMetadata"], metadata);

    // Wait for both turns to finish (first send + drained queued send).
    let mut stream_end_count = 0;
    for _ in 0..120 {
        let frame = wss_event(&mut sub, 30).await;
        if frame["params"]["event"]["type"] == "agent:stream:end" {
            stream_end_count += 1;
            if stream_end_count >= 2 {
                break;
            }
        }
    }
    assert_eq!(stream_end_count, 2, "both turns must complete");

    // (b) The drained user message row persists the caller's metadata fields
    // verbatim, PLUS the drain-time queueInfo stamp.
    let convo = wss_rpc(
        &mut rpc,
        14,
        "agent.getConversation",
        json!({ "agentId": agent_id }),
    )
    .await;
    let tagged = convo["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .find(|m| {
            m["role"] == "user"
                && m["contentBlocks"][0]["text"]
                    .as_str()
                    .is_some_and(|t| t.starts_with("tagged queued message"))
        })
        .expect("drained user message row present");
    for (key, want) in metadata.as_object().unwrap() {
        assert_eq!(
            &tagged["metadata"][key], want,
            "drained user row must persist messageMetadata field {key}: {tagged}"
        );
    }
    // (c) queueInfo carries the entry's enqueue timestamp (byte-identical to
    // the queued entry's `queuedAt`) and an integer wait in millis.
    let queue_info = &tagged["metadata"]["queueInfo"];
    assert_eq!(
        queue_info["queuedAt"], send2["queuedMessage"]["queuedAt"],
        "queueInfo.queuedAt is the queue entry's queuedAt: {tagged}"
    );
    assert!(
        queue_info["waitedMs"].as_u64().is_some(),
        "queueInfo.waitedMs is a non-negative integer: {tagged}"
    );
    // Both direct-delivery placements are covered: the row-level `metadata`
    // column (direct `agent.sendMessage` parity) and the in-block fold
    // (`deliver_wake_message` parity) — the fold carries queueInfo too.
    assert_eq!(
        tagged["contentBlocks"][0]["messageMetadata"], tagged["metadata"],
        "drained user block must fold the same messageMetadata: {tagged}"
    );
}

// ---------------------------------------------------------------------------
// Production-default threshold gate (monorepo#2353): with NO env override, a
// queued entry that waited only ~2s (below the 5s threshold) drains WITHOUT
// the dequeue-wait [SYSTEM NOTE] and WITHOUT the queueInfo metadata stamp —
// the sub-threshold hop reads exactly like an immediate delivery (PROTOCOL
// §5.5), while caller-supplied messageMetadata still persists verbatim.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sub_threshold_queued_message_drains_without_annotation_over_wss() {
    let Some(script) = gate("WSS sub-threshold dequeue-wait E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    // 2s busy window < 5s threshold; INTENTD_DEQUEUE_WAIT_MIN_MS deliberately
    // NOT set — this test exercises the production default.
    let behavior = json!({ "response": "mock reply", "firstTurnDelayMs": 2000 }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:stream:end"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_resp["subscriptionId"].is_string());

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "SubThresh", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_string();

    // First send — the agent goes busy for 2000ms.
    let send1 = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "first message" }),
    )
    .await;
    assert_eq!(send1["success"], true);
    assert_eq!(send1["queued"], false);
    sleep(Duration::from_millis(200)).await;

    // Second send while busy — queues, waits ~2s, drains sub-threshold.
    let metadata = json!({ "type": "event_notification" });
    let send2 = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "content": "sub-threshold queued message",
            "messageMetadata": metadata,
        }),
    )
    .await;
    assert_eq!(send2["success"], true);
    assert_eq!(send2["queued"], true, "second send should queue: {send2}");

    // Wait for both turns to finish (first send + drained queued send).
    let mut stream_end_count = 0;
    for _ in 0..120 {
        let frame = wss_event(&mut sub, 30).await;
        if frame["params"]["event"]["type"] == "agent:stream:end" {
            stream_end_count += 1;
            if stream_end_count >= 2 {
                break;
            }
        }
    }
    assert_eq!(stream_end_count, 2, "both turns must complete");

    let convo = wss_rpc(
        &mut rpc,
        13,
        "agent.getConversation",
        json!({ "agentId": agent_id }),
    )
    .await;
    let drained = convo["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .find(|m| {
            m["role"] == "user"
                && m["contentBlocks"][0]["text"]
                    .as_str()
                    .is_some_and(|t| t.contains("sub-threshold queued message"))
        })
        .expect("drained user message row present");
    // No dequeue-wait system note on the persisted content.
    let text = drained["contentBlocks"][0]["text"].as_str().unwrap();
    assert!(
        !text.contains("This message was queued at"),
        "sub-threshold drain must not carry the dequeue-wait note: {drained}"
    );
    // No queueInfo stamp — neither on the row metadata nor the in-block fold.
    assert!(
        drained["metadata"]["queueInfo"].is_null(),
        "sub-threshold drain must not stamp queueInfo on row metadata: {drained}"
    );
    assert!(
        drained["contentBlocks"][0]["messageMetadata"]["queueInfo"].is_null(),
        "sub-threshold drain must not fold queueInfo into the block: {drained}"
    );
    // Caller-supplied messageMetadata still persists verbatim.
    assert_eq!(
        drained["metadata"]["type"], "event_notification",
        "caller messageMetadata must persist unchanged: {drained}"
    );
}

// ---------------------------------------------------------------------------
// userAppMessageId round-trip (PROTOCOL §5.5, activates the FE dedup guard):
// a direct `agent.sendMessage` carrying the client-minted id must (a) echo it
// as `appMessageId` on the `agent:message` (role=user) event so a live FE can
// match its optimistic insert, (b) persist it inside the row metadata and
// surface it as `appMessageId` on `agent.getConversation`, and (c) survive
// the busy-agent queue fallback so the drained row and its `agent:message`
// event carry the same id.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn user_app_message_id_round_trips_over_wss() {
    let Some(script) = gate("WSS userAppMessageId E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    // First turn is slow so the SECOND tagged send lands on a busy agent and
    // exercises the queue fallback path.
    let behavior = json!({ "response": "mock reply", "firstTurnDelayMs": 2000 }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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

    // SUBSCRIBER conn — collect `agent:message` echoes and turn boundaries.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:message", "agent:stream:end"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_resp["subscriptionId"].is_string());

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "AppIds", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_string();

    // (a) Direct send with a userAppMessageId — the agent is idle so this
    // takes the direct-delivery path and must echo on agent:message.
    let send1 = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "content": "first tagged message",
            "userAppMessageId": "app-msg-direct-1",
        }),
    )
    .await;
    assert_eq!(send1["success"], true);
    assert_eq!(send1["queued"], false);
    sleep(Duration::from_millis(200)).await;

    // (c) Second tagged send while the slow first turn is running → queues.
    let send2 = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "content": "queued tagged message",
            "userAppMessageId": "app-msg-queued-2",
        }),
    )
    .await;
    assert_eq!(send2["success"], true);
    assert_eq!(send2["queued"], true, "second send should queue: {send2}");
    // The queue entry captured the id inside messageMetadata so the drained
    // re-persist keeps it.
    assert_eq!(
        send2["queuedMessage"]["messageMetadata"]["userAppMessageId"], "app-msg-queued-2",
        "queued entry must capture userAppMessageId: {send2}"
    );

    // Watch the event stream until both turns finish, collecting the
    // user-role agent:message echoes along the way.
    let mut user_echoes: Vec<Value> = Vec::new();
    let mut stream_end_count = 0;
    for _ in 0..200 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        if event["type"] == "agent:message" && event["data"]["role"] == "user" {
            user_echoes.push(event["data"].clone());
        }
        if event["type"] == "agent:stream:end" {
            stream_end_count += 1;
            if stream_end_count >= 2 {
                break;
            }
        }
    }
    assert_eq!(stream_end_count, 2, "both turns must complete");
    // (a) The direct send's echo carries its appMessageId.
    let direct = user_echoes
        .iter()
        .find(|d| d["appMessageId"] == "app-msg-direct-1")
        .unwrap_or_else(|| panic!("direct send must echo appMessageId: {user_echoes:?}"));
    assert!(direct["messageId"].is_string());
    // (c) The drained queued send's echo carries its appMessageId too.
    assert!(
        user_echoes
            .iter()
            .any(|d| d["appMessageId"] == "app-msg-queued-2"),
        "drained queued send must echo appMessageId: {user_echoes:?}"
    );

    // (b) Both persisted rows surface `appMessageId` on conversation reads.
    let convo = wss_rpc(
        &mut rpc,
        14,
        "agent.getConversation",
        json!({ "agentId": agent_id }),
    )
    .await;
    let messages = convo["messages"].as_array().expect("messages array");
    let row1 = messages
        .iter()
        .find(|m| m["role"] == "user" && m["contentBlocks"][0]["text"] == "first tagged message")
        .expect("direct user row present");
    assert_eq!(row1["appMessageId"], "app-msg-direct-1");
    assert_eq!(row1["metadata"]["userAppMessageId"], "app-msg-direct-1");
    let row2 = messages
        .iter()
        .find(|m| {
            m["role"] == "user"
                && m["contentBlocks"][0]["text"]
                    .as_str()
                    .is_some_and(|t| t.starts_with("queued tagged message"))
        })
        .expect("drained user row present");
    assert_eq!(row2["appMessageId"], "app-msg-queued-2");
    assert_eq!(row2["metadata"]["userAppMessageId"], "app-msg-queued-2");
}

#[tokio::test]
async fn remove_queued_message_is_idempotent_over_wss() {
    // No mock-agent needed — `agent.removeQueuedMessage` is a pure router arm
    // when the message id is unknown. The FE's seeded mirror diverges from the
    // BE's in-memory queue after a daemon restart; the BE must return success
    // (not an error) so the FE's optimistic delete sticks.
    let (_daemon, ws_id, _note_id, port, fingerprint) = boot_daemon_with_seeded_note().await;
    let cfg = client_config(&fingerprint);
    let mut rpc = connect_ws(port, cfg.clone()).await;

    let created = wss_rpc(
        &mut rpc,
        1,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "QIdempotent", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_string();

    // Unknown message id on a known agent → success.
    let r = wss_rpc(
        &mut rpc,
        2,
        "agent.removeQueuedMessage",
        json!({ "agentId": agent_id, "messageId": "msg-does-not-exist" }),
    )
    .await;
    assert_eq!(r["success"], json!(true));

    // Unknown agent → also success.
    let r2 = wss_rpc(
        &mut rpc,
        3,
        "agent.removeQueuedMessage",
        json!({
            "agentId": "agent-00000000-0000-0000-0000-000000000000",
            "messageId": "anything"
        }),
    )
    .await;
    assert_eq!(r2["success"], json!(true));
}

/// `agent.sendQueuedMessageNow` over WSS (PROTOCOL §5.5): with the agent
/// mid-turn (parked at session/cancel) and TWO entries queued, sending the
/// SECOND entry now atomically dequeues it — `agent:queue:updated` carries the
/// shrunk snapshot with the FIRST entry preserved — preempts the in-flight
/// turn keep-alive (terminal stream:end, then the entry streams `turn=2` on
/// the SAME child), and the response mirrors sendMessage:
/// `{ success, queued: false, messageId: <entry id> }`. An unknown entry id
/// is `-32602` with no side effects (deliberately NOT idempotent).
#[tokio::test]
async fn send_queued_message_now_over_wss() {
    let Some(script) = gate("WSS sendQueuedMessageNow E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let behavior = json!({ "blockUntilCancel": true, "response": "resumed" }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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

    // SUBSCRIBER conn — subscribe to agent:* BEFORE any queue mutation.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*", "chat:stream:delta"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_resp["subscriptionId"].is_string());

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "QSendNow", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_string();

    // Engage a turn that parks at session/cancel so the queue entries below
    // stay queued (no self-drain race) and the send-now must PREEMPT.
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "first" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    let mut saw_block_chunk = false;
    for _ in 0..50 {
        let frame = wss_event(&mut sub, 30).await;
        if frame["params"]["event"]["type"] == "chat:stream:delta"
            && frame["params"]["event"]["data"]["content"]
                .as_str()
                .unwrap_or_default()
                .contains("streaming-before-cancel")
        {
            saw_block_chunk = true;
            break;
        }
    }
    assert!(saw_block_chunk, "first turn streamed a chunk and parked");

    // Two entries queue up behind the parked turn.
    let q_first = wss_rpc(
        &mut rpc,
        12,
        "agent.queueMessage",
        json!({ "agentId": agent_id, "content": "stays queued" }),
    )
    .await;
    let first_id = q_first["queuedMessage"]["id"].as_str().unwrap().to_string();
    let q_second = wss_rpc(
        &mut rpc,
        13,
        "agent.queueMessage",
        json!({ "agentId": agent_id, "content": "send me now" }),
    )
    .await;
    let second_id = q_second["queuedMessage"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Send the SECOND entry now: response mirrors sendMessage and echoes the
    // ENTRY id as the delivered messageId.
    let now = wss_rpc(
        &mut rpc,
        14,
        "agent.sendQueuedMessageNow",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "messageId": second_id }),
    )
    .await;
    assert_eq!(now["success"], true, "send now ok: {now}");
    assert_eq!(
        now["queued"], false,
        "send-now preempts and streams immediately, never queues: {now}"
    );
    assert_eq!(
        now["messageId"].as_str(),
        Some(second_id.as_str()),
        "the delivered messageId is the queue entry's own id: {now}"
    );

    // Wire ordering: `agent:queue:updated` carries the SHRUNK snapshot (the
    // first entry alone — atomic dequeue preserved the rest of the queue),
    // the preempted turn emits its terminal stream:end, then the entry
    // streams `turn=2` on the SAME child (keep-alive, not a respawn). The
    // subscriber also buffered the ENQUEUE-time snapshots ([first] then
    // [first, second]), so the shrunk [first] snapshot only counts once the
    // two-entry snapshot has been observed.
    let mut saw_two_entry_queue = false;
    let mut saw_shrunk_queue = false;
    let mut saw_preempt_end = false;
    let mut saw_turn2_chunk = false;
    for _ in 0..80 {
        let frame = wss_event(&mut sub, 30).await;
        let evt = &frame["params"]["event"];
        match evt["type"].as_str() {
            Some("agent:queue:updated")
                if evt["data"]["agentId"].as_str() == Some(agent_id.as_str()) =>
            {
                let queue = evt["data"]["queue"].as_array().expect("queue array");
                if queue.len() == 2 {
                    saw_two_entry_queue = true;
                } else if saw_two_entry_queue
                    && queue.len() == 1
                    && queue[0]["id"].as_str() == Some(first_id.as_str())
                {
                    saw_shrunk_queue = true;
                }
            }
            Some("agent:stream:end") if !saw_preempt_end => {
                saw_preempt_end = true;
            }
            Some("chat:stream:delta")
                if evt["data"]["content"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("turn=2") =>
            {
                assert!(
                    saw_preempt_end,
                    "the send-now turn starts only after the preempted turn's stream:end"
                );
                saw_turn2_chunk = true;
            }
            _ => {}
        }
        if saw_shrunk_queue && saw_preempt_end && saw_turn2_chunk {
            break;
        }
    }
    assert!(
        saw_shrunk_queue,
        "agent:queue:updated republished the shrunk snapshot with the rest of the queue preserved"
    );
    assert!(
        saw_preempt_end,
        "preemption emitted the terminal stream:end"
    );
    assert!(
        saw_turn2_chunk,
        "the dequeued entry ran on the SAME process (mock reported turn=2, not a turn=1 respawn)"
    );

    // The delivered user row persists under the ENTRY id.
    let convo = wss_rpc(
        &mut rpc,
        15,
        "agent.getConversation",
        json!({ "agentId": agent_id }),
    )
    .await;
    let row = convo["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .find(|m| m["id"].as_str() == Some(second_id.as_str()))
        .expect("dequeued user row persisted under the entry id")
        .clone();
    assert_eq!(row["role"], "user");

    // Unknown entry id → -32602, no side effects (NOT idempotent — contrast
    // `agent.removeQueuedMessage`).
    let err_env = wss_rpc_envelope(
        &mut rpc,
        16,
        "agent.sendQueuedMessageNow",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "messageId": "no-such-entry" }),
    )
    .await;
    assert_eq!(
        err_env["error"]["code"],
        json!(-32602),
        "absent entry is invalid params: {err_env}"
    );
}

// ---------------------------------------------------------------------------
// Mixed-case drain with an under-edit message (PROTOCOL §5.5/§6.5 invariant):
// when the queue contains ready-to-send messages alongside one marked
// `editing: true`, the agent MUST drain the ready-to-send ones and must NOT
// emit `agent:idle` until the under-edit entry is the only thing left.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn queue_drain_skips_under_edit_message_and_suppresses_idle_over_wss() {
    let Some(script) = gate("WSS mixed-case queue drain E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    // First turn delays 1.2s so we have a deterministic setup window to enqueue
    // + toggle editing + enqueue again while the agent is busy. Subsequent
    // queue-drained turns proceed at full mock speed.
    let behavior = json!({ "response": "drained ok", "firstTurnDelayMs": 1200 }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_resp["subscriptionId"].is_string());

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "QMixed", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_string();

    // Engage the agent in a slow first turn — the mock parks ~1.2s before
    // replying — so the queue mutations below land while the worker is busy
    // and don't race with the self-drain.
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "kick-off" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Enqueue msg_edit (agent busy → goes onto the queue, no self-drain race).
    let q_edit = wss_rpc(
        &mut rpc,
        12,
        "agent.queueMessage",
        json!({ "agentId": agent_id, "content": "under-edit-draft" }),
    )
    .await;
    assert_eq!(q_edit["success"], true);
    let edit_mid = q_edit["queuedMessage"]["id"].as_str().unwrap().to_string();

    // Flip msg_edit to `editing: true` BEFORE the second enqueue, so the
    // drain that fires when the kick-off turn ends sees it as under-edit.
    let toggled = wss_rpc(
        &mut rpc,
        13,
        "agent.editQueuedMessage",
        json!({
            "agentId": agent_id,
            "messageId": edit_mid,
            "content": "under-edit-draft",
            "editing": true,
        }),
    )
    .await;
    assert_eq!(toggled["success"], true);
    assert_eq!(
        toggled["queuedMessage"]["editing"], true,
        "editing flag surfaced on the wire shape"
    );

    // Enqueue msg_drain — the ready-to-send entry the worker MUST drain.
    let q_drain = wss_rpc(
        &mut rpc,
        14,
        "agent.queueMessage",
        json!({ "agentId": agent_id, "content": "drain-me" }),
    )
    .await;
    assert_eq!(q_drain["success"], true);

    // Confirm the pre-drain queue snapshot: [msg_edit(editing), msg_drain].
    let pre = wss_rpc(
        &mut rpc,
        15,
        "agent.getQueue",
        json!({ "agentId": agent_id }),
    )
    .await;
    let pre_q = pre["queue"].as_array().expect("queue");
    assert_eq!(pre_q.len(), 2, "queue mid-turn: {pre_q:?}");
    assert_eq!(pre_q[0]["id"].as_str(), Some(edit_mid.as_str()));
    assert_eq!(pre_q[0]["editing"], true);
    assert_eq!(pre_q[1]["content"], "drain-me");
    assert!(pre_q[1].get("editing").is_none());

    // Collect events until we have observed TWO terminal `agent:stream:end`s:
    //   1. kick-off turn ends (drain still has msg_drain ready → idle SUPPRESSED)
    //   2. msg_drain turn ends (only msg_edit(editing) remains → idle FIRES)
    // The single `agent:idle` for this agent MUST appear AFTER the second
    // stream:end — never between the two (PROTOCOL §5.5/§6.5 invariant).
    let mut stream_ends = 0usize;
    let mut idles_before_drain_done = 0usize;
    let mut idle_after_drain_done = 0usize;
    let mut saw_drain_done = false;
    for _ in 0..240 {
        let frame = wss_event(&mut sub, 30).await;
        let evt = &frame["params"]["event"];
        let agent_match = evt["data"]["agentId"].as_str() == Some(agent_id.as_str());
        match evt["type"].as_str() {
            Some("agent:idle") if agent_match => {
                if saw_drain_done {
                    idle_after_drain_done += 1;
                } else {
                    idles_before_drain_done += 1;
                }
            }
            Some("agent:stream:end") if agent_match => {
                stream_ends += 1;
                if stream_ends >= 2 {
                    saw_drain_done = true;
                }
            }
            _ => {}
        }
        if saw_drain_done && idle_after_drain_done >= 1 {
            break;
        }
    }
    assert!(
        stream_ends >= 2,
        "kick-off + msg_drain both reached terminal stream:end (saw {stream_ends})",
    );
    assert_eq!(
        idles_before_drain_done, 0,
        "agent:idle MUST be suppressed while a ready-to-send message exists; \
         only the under-edit entry is allowed to remain mid-drain",
    );
    assert_eq!(
        idle_after_drain_done, 1,
        "agent:idle fires exactly once when the queue is editing-only",
    );

    // Post-drain snapshot: only the under-edit entry remains.
    let mid = wss_rpc(
        &mut rpc,
        16,
        "agent.getQueue",
        json!({ "agentId": agent_id }),
    )
    .await;
    let mid_q = mid["queue"].as_array().expect("queue");
    assert_eq!(mid_q.len(), 1, "under-edit entry alone: {mid_q:?}");
    assert_eq!(mid_q[0]["id"].as_str(), Some(edit_mid.as_str()));
    assert_eq!(mid_q[0]["editing"], true);

    // Clearing the editing flag (FE "save edit" path) re-includes the entry in
    // the ready-to-send queue and self-drains — final terminal stream:end +
    // a fresh agent:idle (queue now genuinely empty).
    let saved = wss_rpc(
        &mut rpc,
        17,
        "agent.editQueuedMessage",
        json!({
            "agentId": agent_id,
            "messageId": edit_mid,
            "content": "saved-edit",
            "editing": false,
        }),
    )
    .await;
    assert_eq!(saved["success"], true);

    let mut saw_save_end = false;
    let mut saw_save_idle = false;
    for _ in 0..240 {
        let frame = wss_event(&mut sub, 30).await;
        let evt = &frame["params"]["event"];
        let agent_match = evt["data"]["agentId"].as_str() == Some(agent_id.as_str());
        match evt["type"].as_str() {
            Some("agent:idle") if agent_match => saw_save_idle = true,
            Some("agent:stream:end") if agent_match => saw_save_end = true,
            _ => {}
        }
        if saw_save_end && saw_save_idle {
            break;
        }
    }
    assert!(
        saw_save_end,
        "saved edit self-drained to a terminal agent:stream:end",
    );
    assert!(
        saw_save_idle,
        "agent:idle fires once the ready-to-send queue is truly empty",
    );

    let final_q = wss_rpc(
        &mut rpc,
        18,
        "agent.getQueue",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert!(
        final_q["queue"].as_array().unwrap().is_empty(),
        "queue is empty post-save-drain",
    );
}

/// Daemon-owned initial-agent orchestration over WSS (PROTOCOL §5.1):
/// `workspace.create` with an `initialAgent` creates the workspace AND the
/// agent, delivers the prompt exactly once, and starts the first turn — the
/// subscriber sees `workspace:created` → `agent:created` → stream frames keyed
/// to the agent. A replay with the same `idempotencyKey` returns the stored
/// result without re-sending (still exactly one user message).
#[tokio::test]
async fn workspace_create_orchestrates_initial_agent_over_wss() {
    let Some(script) = gate("WSS workspace.create initial-agent E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let behavior = json!({ "response": "initial agent ran" }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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

    // SUBSCRIBER conn — the workspace id is minted by the create, so subscribe
    // unfiltered across both families BEFORE creating.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:*", "agent:*"] }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // RPC conn — one workspace.create carrying the full initialAgent payload
    // (agent id is server-assigned and read back from the result).
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "workspace.create",
        json!({
            "title": "Orchestrated WS",
            "branch": "feat/initial-agent-e2e",
            "idempotencyKey": "wss-create-idem-1",
            "initialAgent": {
                "prompt": "build the initial feature",
                "name": "Initial agent",
                "model": "mock:default",
                "specialist": "implementor",
            },
        }),
    )
    .await;
    let ws_id = created["workspace"]["id"].as_str().expect("workspace id");
    let agent_id = created["initialAgent"]["id"]
        .as_str()
        .expect("result carries the created agent")
        .to_string();
    assert!(
        agent_id.starts_with("agent-"),
        "server-minted agent-{{uuid}} id: {created}"
    );
    assert_eq!(created["initialAgent"]["name"], "Initial agent");
    // The initialAgent result is the AgentLite projection and surfaces the
    // daemon-stamped initial-agent flag (PROTOCOL §5.5, presence-detected).
    assert_eq!(
        created["initialAgent"]["metadata"]["isInitialAgent"], true,
        "initialAgent result carries metadata.isInitialAgent: {created}"
    );

    // Event flow: workspace:created for the new id, agent:created for the
    // initial agent, ≥1 stream chunk keyed to it, exactly one stream:end.
    let mut saw_ws_created = false;
    let mut saw_agent_created = false;
    let mut chunks = 0u32;
    let mut ends = 0u32;
    for _ in 0..120 {
        let frame = wss_event(&mut sub, 30).await;
        let ev = &frame["params"]["event"];
        match ev["type"].as_str() {
            Some("workspace:created") => {
                assert_eq!(ev["data"]["workspaceId"], ws_id, "created for the new ws");
                assert_eq!(ev["data"]["workspace"]["id"], ws_id);
                saw_ws_created = true;
            }
            Some("agent:created") => {
                assert!(saw_ws_created, "workspace:created precedes agent:created");
                assert_eq!(ev["data"]["agentId"], agent_id.as_str());
                saw_agent_created = true;
            }
            Some("agent:stream:activity") => {
                assert_eq!(
                    ev["data"]["agentId"],
                    agent_id.as_str(),
                    "chunk scoped to the initial agent: {ev}"
                );
                chunks += 1;
            }
            Some("agent:stream:end") => {
                assert_eq!(ev["data"]["agentId"], agent_id.as_str());
                ends += 1;
            }
            _ => {}
        }
        if saw_ws_created && saw_agent_created && ends >= 1 {
            break;
        }
    }
    assert!(saw_ws_created, "workspace:created observed");
    assert!(saw_agent_created, "agent:created observed");
    assert!(chunks >= 1, "initial agent streamed ≥1 chunk");
    assert_eq!(ends, 1, "exactly one terminal stream:end");

    // Spec seed: workspace.create seeded the well-known `spec` note and it is
    // addressable through the wire (`note.get`) with the reference-parity
    // defaults (title "Spec", empty body, `spec` tag, pinned + default).
    let spec = wss_rpc(
        &mut rpc,
        14,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": "spec" }),
    )
    .await;
    let note = &spec["note"];
    assert_eq!(note["id"], "spec", "spec note addressable by id: {spec}");
    assert_eq!(note["workspaceId"], ws_id);
    assert_eq!(note["title"], "Spec");
    assert_eq!(note["content"], "");
    assert_eq!(note["tags"], json!(["spec"]));
    assert_eq!(note["isPinned"], true);
    assert_eq!(note["isDefault"], true);

    // Transcript: exactly ONE user message (the prompt, delivered once) and an
    // assistant reply — the double-send bug class is structurally impossible.
    let conv = wss_rpc(
        &mut rpc,
        11,
        "agent.getConversation",
        json!({ "agentId": agent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let user_count = messages.iter().filter(|m| m["role"] == "user").count();
    assert_eq!(user_count, 1, "exactly one delivered prompt: {conv}");
    assert!(
        messages.iter().any(|m| m["role"] == "user"
            && serde_json::to_string(&m["contentBlocks"])
                .unwrap_or_default()
                .contains("build the initial feature")),
        "the user message carries the prompt: {conv}"
    );
    assert!(
        messages.iter().any(|m| m["role"] == "assistant"),
        "initial agent produced an assistant reply: {conv}"
    );

    // The persisted flag survives to later AgentLite reads: `agent.get`
    // serves `metadata.isInitialAgent: true` for the initial agent, and a
    // plain `agent.create`d sibling OMITS the key entirely (never `false` —
    // presence-detected, PROTOCOL §5.5). `agent.list` agrees per row.
    let got = wss_rpc(
        &mut rpc,
        15,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        got["agent"]["metadata"]["isInitialAgent"], true,
        "agent.get serves metadata.isInitialAgent for the initial agent: {got}"
    );
    let sibling = wss_rpc(
        &mut rpc,
        16,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Sibling", "model": "mock:default" }),
    )
    .await;
    let sibling_id = sibling["agent"]["id"]
        .as_str()
        .expect("sibling agent id")
        .to_string();
    assert!(
        sibling["agent"]["metadata"].get("isInitialAgent").is_none(),
        "non-initial agent omits metadata.isInitialAgent: {sibling}"
    );
    let listed = wss_rpc(&mut rpc, 17, "agent.list", json!({ "workspaceId": ws_id })).await;
    let agents = listed["agents"].as_array().expect("agents array");
    let by_id = |id: &str| {
        agents
            .iter()
            .find(|a| a["id"] == id)
            .unwrap_or_else(|| panic!("agent {id} in list: {listed}"))
    };
    assert_eq!(
        by_id(&agent_id)["metadata"]["isInitialAgent"],
        true,
        "agent.list serves metadata.isInitialAgent for the initial agent: {listed}"
    );
    assert!(
        by_id(&sibling_id)["metadata"]
            .get("isInitialAgent")
            .is_none(),
        "agent.list omits metadata.isInitialAgent for the sibling: {listed}"
    );

    // Replay with the same idempotencyKey: the stored result comes back (same
    // workspace + agent) and no second prompt is delivered.
    let replay = wss_rpc(
        &mut rpc,
        12,
        "workspace.create",
        json!({
            "title": "Different title",
            "branch": "feat/other-branch",
            "idempotencyKey": "wss-create-idem-1",
            "initialAgent": {
                "prompt": "some other prompt",
                "model": "mock:default",
            },
        }),
    )
    .await;
    assert_eq!(replay["workspace"]["id"], ws_id, "replay returns original");
    assert_eq!(replay["initialAgent"]["id"], agent_id.as_str());
    let conv = wss_rpc(
        &mut rpc,
        13,
        "agent.getConversation",
        json!({ "agentId": agent_id }),
    )
    .await;
    let user_count = conv["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .filter(|m| m["role"] == "user")
        .count();
    assert_eq!(user_count, 1, "replay delivered no second prompt: {conv}");
}

#[allow(clippy::similar_names)] // deliberate parallel naming across the scenario's instances
/// Regression for the composite `(id, workspace_id)` note PK (migration 0030
/// + `feat(services): workspace-scope note lookups + seed spec per workspace`):
/// two `workspace.create` calls each seed their own `spec` note. Over the
/// real WSS transport the client can call `note.get {noteId: "spec"}` against
/// either workspace and receive a distinct row scoped to that workspace, with
/// no cross-workspace bleed of body, title, or `workspaceId`.
#[tokio::test]
async fn workspace_create_seeds_per_workspace_spec_over_wss() {
    let data_dir = temp_data_dir();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
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
    let mut rpc = connect_ws(port, cfg.clone()).await;

    let ws_a = wss_rpc(
        &mut rpc,
        10,
        "workspace.create",
        json!({ "title": "Alpha", "branch": "feat/spec-ws-a" }),
    )
    .await;
    let ws_a_id = ws_a["workspace"]["id"]
        .as_str()
        .expect("ws_a id")
        .to_string();
    let ws_b = wss_rpc(
        &mut rpc,
        11,
        "workspace.create",
        json!({ "title": "Beta", "branch": "feat/spec-ws-b" }),
    )
    .await;
    let ws_b_id = ws_b["workspace"]["id"]
        .as_str()
        .expect("ws_b id")
        .to_string();
    assert_ne!(ws_a_id, ws_b_id, "two distinct workspaces created");

    let spec_a = wss_rpc(
        &mut rpc,
        12,
        "note.get",
        json!({ "workspaceId": ws_a_id, "noteId": "spec" }),
    )
    .await;
    let spec_b = wss_rpc(
        &mut rpc,
        13,
        "note.get",
        json!({ "workspaceId": ws_b_id, "noteId": "spec" }),
    )
    .await;
    let note_a = &spec_a["note"];
    let note_b = &spec_b["note"];
    assert_eq!(note_a["id"], "spec", "ws_a spec addressable: {spec_a}");
    assert_eq!(note_b["id"], "spec", "ws_b spec addressable: {spec_b}");
    assert_eq!(note_a["workspaceId"], ws_a_id, "ws_a spec scoped to ws_a");
    assert_eq!(note_b["workspaceId"], ws_b_id, "ws_b spec scoped to ws_b");
    for note in [note_a, note_b] {
        assert_eq!(note["title"], "Spec");
        assert_eq!(note["content"], "");
        assert_eq!(note["tags"], json!(["spec"]));
        assert_eq!(note["isPinned"], true);
        assert_eq!(note["isDefault"], true);
    }
}

/// Init a small local git repo (one commit) and return its on-disk path. Used
/// by the WSS clone-orchestration e2e as a `file://` source. Skips the test
/// when `git` is unavailable on `PATH` by returning `None`.
fn seed_local_repo(prefix: &str) -> Option<PathBuf> {
    intent_providers::resolve_on_path("git")?;
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("{prefix}-{}", &id[..8]));
    std::fs::create_dir_all(&dir).ok()?;
    let run = |args: &[&str]| -> bool {
        Command::new("git")
            .args(args)
            .current_dir(&dir)
            .env("GIT_AUTHOR_NAME", "Tester")
            .env("GIT_AUTHOR_EMAIL", "t@e.dev")
            .env("GIT_COMMITTER_NAME", "Tester")
            .env("GIT_COMMITTER_EMAIL", "t@e.dev")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|s| s.success())
    };
    if !run(&["init", "--quiet"]) {
        return None;
    }
    std::fs::write(dir.join("README.md"), "init\n").ok()?;
    if !run(&["add", "README.md"]) || !run(&["commit", "-q", "-m", "chore: init"]) {
        return None;
    }
    Some(dir)
}

/// `workspace.create { githubUrl }` clones the URL inside the idempotent op
/// and streams `git:clone:progress` + `git:clone:done` under the new workspace
/// id before `workspace:created` publishes. The result's `workspace` carries
/// the clone target as `repositoryPath`.
#[tokio::test]
async fn workspace_create_clones_github_url_over_wss() {
    let Some(source) = seed_local_repo("itd-wss-clone-src") else {
        eprintln!("skipping WSS clone E2E: git not available");
        return;
    };
    let data_dir = temp_data_dir();
    let clone_target = data_dir.join("cloned-checkout");
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
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

    // SUBSCRIBER conn — subscribe unfiltered before creating so the clone
    // frames land in the buffer.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:*", "git:*"] }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "workspace.create",
        json!({
            "title": "Cloned via WSS",
            "branch": "feat/wss-clone",
            "githubUrl": format!("file://{}", source.display()),
            "clonePath": clone_target.to_string_lossy(),
        }),
    )
    .await;
    let ws_id = created["workspace"]["id"].as_str().expect("workspace id");
    assert_eq!(
        created["workspace"]["repositoryPath"],
        clone_target.to_string_lossy().as_ref(),
        "repositoryPath set from clone target: {created}"
    );
    assert!(
        clone_target.join(".git").exists(),
        "checkout materialized at {clone_target:?}"
    );

    let mut saw_progress = false;
    let mut saw_done_ok = false;
    let mut ws_created_after_clone = false;
    let mut clone_done_first = false;
    for _ in 0..60 {
        let frame = wss_event(&mut sub, 15).await;
        let ev = &frame["params"]["event"];
        match ev["type"].as_str() {
            Some("git:clone:progress") => {
                assert_eq!(ev["workspaceId"], ws_id, "progress scoped to new ws: {ev}");
                saw_progress = true;
            }
            Some("git:clone:done") => {
                assert_eq!(ev["workspaceId"], ws_id);
                assert_eq!(ev["data"]["ok"], true, "clone succeeded: {ev}");
                saw_done_ok = true;
                clone_done_first = !ws_created_after_clone;
            }
            Some("workspace:created") => {
                assert_eq!(ev["data"]["workspaceId"], ws_id);
                ws_created_after_clone = true;
            }
            _ => {}
        }
        if saw_progress && saw_done_ok && ws_created_after_clone {
            break;
        }
    }
    assert!(saw_progress, "git:clone:progress observed");
    assert!(saw_done_ok, "git:clone:done ok observed");
    assert!(
        clone_done_first,
        "git:clone:done precedes workspace:created"
    );

    // Cleanup the seed source (best-effort).
    let _ = std::fs::remove_dir_all(&source);
}

/// DELIV-1 regression: neither `agent.wakeOrCreate`'s wake-message delivery
/// nor `agent.sendToTask`'s default-priority delivery can bypass the runtime
/// `AgentManager`. Before the fix, both entry points persisted the user
/// message directly to the store without spawning a turn worker — the agent
/// row stayed `pending`, no `agent:idle` fired, and the transcript projection
/// showed a wake-tagged user block with no matching assistant response.
///
/// This e2e drives the fixed path end-to-end over the real WSS transport:
///   1. `agent.wakeOrCreate` creates + delivers the first message via
///      `Services::deliver_wake_message` (now routed through
///      `AgentManager::try_spawn_turn_for_prepersisted`), producing an
///      `agent:idle` enriched with `agentName` + first assistant response.
///   2. `agent.sendToTask` (default priority) delivers a follow-up via
///      `Services::agent_send_to_task_op` (now routed through
///      `AgentManager::send_message`), producing a second `agent:idle`.
///   3. The persisted transcript shows BOTH user prompts AND both matching
///      assistant responses — no lost messages across the cycle.
#[tokio::test]
async fn deliv1_no_lost_messages_wake_or_create_then_send_to_task_over_wss() {
    let Some(script) = gate("WSS DELIV-1 no-lost-messages E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let (ws_id, note_id) = seed_workspace_and_note(&data_dir).await;
    // Distinct responses per turn keyed on prompt text so the transcript
    // assertion below can prove BOTH turns actually ran (not just one).
    let behavior = json!({
        "rules": [
            { "ifPromptContains": "kickoff", "response": "first-turn-ok" },
            { "ifPromptContains": "follow-up", "response": "second-turn-ok" },
        ],
        "response": "fallback"
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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

    // Subscribe BEFORE any RPC that could publish `agent:idle` so no event
    // races past the subscription window.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let marked = wss_rpc(
        &mut rpc,
        10,
        "task.markAsTask",
        json!({ "workspaceId": ws_id, "noteId": note_id, "status": "in_progress" }),
    )
    .await;
    assert_eq!(marked["ok"], true, "markAsTask ok: {marked}");

    // Step 1 — `agent.wakeOrCreate` MUST drive a real turn. The `contextMessage`
    // reaches the mock agent's prompt text, matches the first rule, and streams
    // `first-turn-ok` back through `agent:stream:*` culminating in `agent:idle`.
    let wake = wss_rpc(
        &mut rpc,
        11,
        "agent.wakeOrCreate",
        json!({
            "workspaceId": ws_id,
            "taskNoteId": note_id,
            "contextMessage": "kickoff",
            "create": { "model": "mock:default" },
        }),
    )
    .await;
    assert_eq!(wake["ok"], true, "wakeOrCreate ok: {wake}");
    assert_eq!(
        wake["created"], true,
        "created a fresh agent for the task: {wake}"
    );
    let agent_id = wake["agentId"].as_str().expect("agentId").to_string();

    // Drain events until the first `agent:idle` for our agent lands.
    let mut first_idle_payload: Option<Value> = None;
    for _ in 0..160 {
        let frame = wss_event(&mut sub, 30).await;
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:idle" && ev["data"]["agentId"].as_str() == Some(agent_id.as_str()) {
            first_idle_payload = Some(ev.clone());
            break;
        }
    }
    let first_idle = first_idle_payload
        .expect("first agent:idle fired after wakeOrCreate (DELIV-1: turn was driven)");
    // Enrichment (DELIV-1 payload fix): `agentName` MUST be present so
    // subscribers don't fall back to a generic label.
    assert!(
        first_idle["data"]["agentName"]
            .as_str()
            .is_some_and(|n| !n.is_empty()),
        "agent:idle carries non-empty agentName: {first_idle}"
    );

    // Step 2 — follow-up via `agent.sendToTask` with DEFAULT priority. This
    // is the DELIV-1 fix under test: before, the non-interrupt branch persisted
    // the user message to the store WITHOUT spawning a turn; now it routes
    // through `AgentManager::send_message`, so a second `agent:idle` fires.
    let follow_up = wss_rpc(
        &mut rpc,
        12,
        "agent.sendToTask",
        json!({
            "workspaceId": ws_id,
            "taskNoteId": note_id,
            "message": "follow-up",
        }),
    )
    .await;
    assert_eq!(follow_up["ok"], true, "sendToTask ok: {follow_up}");
    assert_eq!(
        follow_up["agentId"].as_str().unwrap_or_default(),
        agent_id,
        "sendToTask resolved the task assignee"
    );

    let mut second_idle_payload: Option<Value> = None;
    for _ in 0..160 {
        let frame = wss_event(&mut sub, 30).await;
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:idle" && ev["data"]["agentId"].as_str() == Some(agent_id.as_str()) {
            second_idle_payload = Some(ev.clone());
            break;
        }
    }
    let second_idle = second_idle_payload
        .expect("second agent:idle fired after sendToTask (DELIV-1: follow-up drove a turn)");
    assert!(
        second_idle["data"]["agentName"]
            .as_str()
            .is_some_and(|n| !n.is_empty()),
        "second agent:idle carries agentName: {second_idle}"
    );

    // Step 3 — the transcript holds BOTH user prompts + BOTH assistant
    // responses. Zero-length or missing rows would mean a lost message.
    let conv = wss_rpc(
        &mut rpc,
        13,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    assert!(
        !messages.is_empty(),
        "transcript populated after wake+follow-up: {conv}"
    );
    let concat_text: String = messages
        .iter()
        .map(|m| serde_json::to_string(&m["contentBlocks"]).unwrap_or_default())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        concat_text.contains("kickoff"),
        "first user prompt persisted (transcript: {concat_text})"
    );
    assert!(
        concat_text.contains("first-turn-ok"),
        "first assistant response persisted -- NOT lost (transcript: {concat_text})"
    );
    assert!(
        concat_text.contains("follow-up"),
        "second user prompt persisted (transcript: {concat_text})"
    );
    assert!(
        concat_text.contains("second-turn-ok"),
        "second assistant response persisted -- NOT lost (transcript: {concat_text})"
    );
}

/// SUB-1: a coordinator that sends its context message to a working agent over
/// the wire (`agent.wakeOrCreate` + `callerAgentId`) is auto-subscribed to the
/// target's completion — when the target's turn finishes (`agent:idle`), the
/// completion delivery worker wakes the SENDER. Asserts the widened response
/// (`subscriptionId` + notification text, PROTOCOL §5.5) and the delivered
/// `[WORKSPACE EVENTS]` completion wake in the coordinator's transcript.
#[tokio::test]
async fn wake_with_caller_delivers_completion_wake_to_sender_over_wss() {
    let Some(script) = gate("WSS SUB-1 sender completion wake E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let behavior = json!({ "response": "target finished the follow-up" }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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

    // SUBSCRIBER conn — subscribe BEFORE the wake so we miss no events.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // RPC conn — a coordinator (the sender) plus a target agent assigned to a
    // task note.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let coordinator = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Coordinator", "model": "mock:default" }),
    )
    .await;
    let coordinator_id = coordinator["agent"]["id"]
        .as_str()
        .expect("coordinator id")
        .to_string();
    let target = wss_rpc(
        &mut rpc,
        11,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Target", "model": "mock:default" }),
    )
    .await;
    let target_id = target["agent"]["id"]
        .as_str()
        .expect("target id")
        .to_string();

    let note = wss_rpc(
        &mut rpc,
        12,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "SUB-1 task" }),
    )
    .await;
    let note_id = note["note"]["id"].as_str().expect("note id").to_string();
    wss_rpc(
        &mut rpc,
        13,
        "task.markAsTask",
        json!({ "workspaceId": ws_id, "noteId": note_id, "status": "in_progress" }),
    )
    .await;
    wss_rpc(
        &mut rpc,
        14,
        "task.assignAgent",
        json!({ "workspaceId": ws_id, "noteId": note_id, "agentId": target_id }),
    )
    .await;

    // The coordinator sends its context message to the working agent via the
    // widened wake composite, carrying its own id as `callerAgentId` (SUB-1).
    let wake = wss_rpc(
        &mut rpc,
        15,
        "agent.wakeOrCreate",
        json!({
            "workspaceId": ws_id,
            "taskNoteId": note_id,
            "contextMessage": "please follow up",
            "callerAgentId": coordinator_id,
        }),
    )
    .await;
    assert_eq!(wake["ok"], true, "wake ok: {wake}");
    assert_eq!(
        wake["agentId"],
        json!(target_id),
        "woke the assignee: {wake}"
    );
    let action = wake["action"].as_str().unwrap_or_default();
    assert!(
        action == "woke_existing" || action == "message_queued_to_active_agent",
        "live-assignee action: {wake}"
    );
    assert!(
        wake["subscriptionId"].is_string(),
        "sender auto-subscribed: {wake}"
    );
    assert!(
        wake["message"]
            .as_str()
            .unwrap_or_default()
            .contains("You will be notified when the agent responds."),
        "notification text parity: {wake}"
    );

    // The target completes its woken turn (mock provider) → agent:idle.
    let mut target_idle = false;
    for _ in 0..120 {
        let frame = wss_event(&mut sub, 60).await;
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:idle" && ev["data"]["agentId"] == json!(target_id) {
            target_idle = true;
            break;
        }
    }
    assert!(target_idle, "target completed its woken turn");

    // The completion delivery worker wakes the SENDER: poll the coordinator's
    // transcript for the `[WORKSPACE EVENTS]` completion message naming the
    // target.
    let mut delivered = false;
    for attempt in 0..40i64 {
        let conv = wss_rpc(
            &mut rpc,
            100 + attempt,
            "agent.getConversation",
            json!({ "agentId": coordinator_id }),
        )
        .await;
        let text = serde_json::to_string(&conv["messages"]).unwrap_or_default();
        if text.contains("[WORKSPACE EVENTS] Child agent") && text.contains(&target_id) {
            delivered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(delivered, "coordinator received the completion wake");

    // Let the coordinator's wake turn wind down before teardown.
    let _ = wss_rpc(
        &mut rpc,
        200,
        "agent.stop",
        json!({ "agentId": coordinator_id }),
    )
    .await;
}

#[allow(clippy::similar_names)] // deliberate parallel naming across the scenario's instances
/// STAB-118 (SUB-1 `after_all` duplicate wake): when a coordinator delegates
/// two `after_all` children, sends follow-up messages to both via
/// `agent.sendMessage` (which triggers SUB-1 auto-watch), and both children
/// complete, the parent's client-visible transcript MUST carry exactly ONE
/// aggregated `[WORKSPACE EVENTS]` wake (listing both children), NOT separate
/// individual wakes triggered by the SUB-1 auto-watch from each send. This is
/// the WSS end-to-end test covering the real wire flow and transcript delivery
/// (the Services-level regression test in `agent_ops/tests.rs` validates the
/// internal completion-delivery logic; this test proves it over the full WSS
/// stack including JSON-RPC routing and client-visible transcript reads).
#[tokio::test]
async fn sub1_sendmessage_after_all_no_duplicate_wake_wss() {
    const CHILD_A_TAG: &str = "SUB1_WSS_CHILD_A";
    const CHILD_B_TAG: &str = "SUB1_WSS_CHILD_B";
    const PARENT_GO: &str = "SUB1_WSS_PARENT_GO";
    const FOLLOWUP_A: &str = "SUB1_WSS_FOLLOWUP_A";
    const FOLLOWUP_B: &str = "SUB1_WSS_FOLLOWUP_B";
    let Some(script) = gate("WSS SUB-1 after_all E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;

    // The parent delegates two after_all children, sends follow-ups to each.
    let delegate_js = format!(
        "const a = await ws.agent.delegate({{ agentInstructions: {}, model: 'mock:default', waitMode: 'after_all' }}); \
         const b = await ws.agent.delegate({{ agentInstructions: {}, model: 'mock:default', waitMode: 'after_all' }}); \
         return {{ a, b }};",
        json!(CHILD_A_TAG),
        json!(CHILD_B_TAG),
    );
    let behavior = json!({
        "rules": [
            {
                "ifPromptContains": CHILD_A_TAG,
                "response": "child A done",
            },
            {
                "ifPromptContains": CHILD_B_TAG,
                "response": "child B done",
            },
            {
                "ifPromptContains": "[WORKSPACE EVENTS]",
                "response": "parent wake acknowledged",
            },
            {
                "ifPromptContains": PARENT_GO,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": delegate_js, "summary": "delegate two after_all" }
                },
                "response": "parent delegated both",
            },
        ],
    })
    .to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("WORKSPACE_IDLE_DEBOUNCE_TEST_MS", "50"),
    ];
    let child_proc = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child: child_proc,
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

    // SUBSCRIBER conn — subscribe BEFORE the turn so we miss no events.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let parent = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "SUB1 Parent", "model": "mock:default" }),
    )
    .await;
    let parent_id = parent["agent"]["id"]
        .as_str()
        .expect("parent id")
        .to_string();
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": parent_id, "content": PARENT_GO }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Wait for the delegation to complete: parent goes idle, and both children
    // complete their first turns (stream:end + agent:idle).
    let mut parent_idle = false;
    let mut child_a_id: Option<String> = None;
    let mut child_b_id: Option<String> = None;
    let mut child_a_idle = false;
    let mut child_b_idle = false;
    for _ in 0..300 {
        let Some(frame) = wss_event_opt(&mut sub, 30).await else {
            break;
        };
        let ev = &frame["params"]["event"];
        let ev_agent = ev["data"]["agentId"].as_str().unwrap_or_default();
        let ev_type = ev["type"].as_str().unwrap_or_default();
        // Learn child IDs from stream chunks.
        if ev_type == "agent:stream:activity" && !ev_agent.is_empty() && ev_agent != parent_id {
            if child_a_id.is_none() {
                child_a_id = Some(ev_agent.to_string());
            } else if child_b_id.is_none() && ev_agent != child_a_id.as_deref().unwrap() {
                child_b_id = Some(ev_agent.to_string());
            }
        }
        if ev_agent == parent_id && ev_type == "agent:idle" {
            parent_idle = true;
        }
        if let Some(cid_a) = child_a_id.as_deref() {
            if ev_agent == cid_a && ev_type == "agent:idle" {
                child_a_idle = true;
            }
        }
        if let Some(cid_b) = child_b_id.as_deref() {
            if ev_agent == cid_b && ev_type == "agent:idle" {
                child_b_idle = true;
            }
        }
        if parent_idle && child_a_idle && child_b_idle {
            break;
        }
    }
    assert!(parent_idle, "parent went idle after delegation");
    assert!(
        child_a_idle && child_b_idle,
        "both children completed first turn"
    );
    let child_a = child_a_id.expect("child A id");
    let child_b = child_b_id.expect("child B id");

    // Send follow-ups to both children via agent.sendMessage.
    let sent_a = wss_rpc(
        &mut rpc,
        20,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": child_a.clone(), "content": FOLLOWUP_A }),
    )
    .await;
    assert_eq!(sent_a["success"], true, "sendMessage A ok: {sent_a}");
    let sent_b = wss_rpc(
        &mut rpc,
        21,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": child_b.clone(), "content": FOLLOWUP_B }),
    )
    .await;
    assert_eq!(sent_b["success"], true, "sendMessage B ok: {sent_b}");

    // Wait for both children to complete their follow-up turns (stream:end +
    // agent:idle after the sendMessage follow-ups).
    let mut child_a_idle_again = false;
    let mut child_b_idle_again = false;
    for _ in 0..200 {
        let Some(frame) = wss_event_opt(&mut sub, 30).await else {
            break;
        };
        let ev = &frame["params"]["event"];
        let ev_agent = ev["data"]["agentId"].as_str().unwrap_or_default();
        if ev_agent == child_a && ev["type"] == "agent:idle" {
            child_a_idle_again = true;
        }
        if ev_agent == child_b && ev["type"] == "agent:idle" {
            child_b_idle_again = true;
        }
        if child_a_idle_again && child_b_idle_again {
            break;
        }
    }
    assert!(
        child_a_idle_again && child_b_idle_again,
        "both children completed follow-up turns"
    );

    // The parent's wake turn MUST fire: poll the parent's transcript until we
    // see the aggregated `[WORKSPACE EVENTS]` completion message naming both
    // children. Once we see it, wait an additional grace period (500ms with a
    // single deadline) to confirm no late duplicate individual wake messages
    // arrive. The fix ensures that the two SUB-1 auto-watches (triggered by
    // the sendMessage calls) do NOT fire individual wakes because both children
    // are already covered by the undelivered after_all delegation group.
    let mut aggregated_wake_seen = false;
    for attempt in 0..60i64 {
        let conv = wss_rpc(
            &mut rpc,
            100 + attempt,
            "agent.getConversation",
            json!({ "agentId": parent_id }),
        )
        .await;
        // Check per-message contentBlocks (not substring of the entire messages array).
        if let Some(messages) = conv["messages"].as_array() {
            let has_aggregated = messages.iter().any(|m| {
                let blocks_text = serde_json::to_string(&m["contentBlocks"]).unwrap_or_default();
                blocks_text.contains("[WORKSPACE EVENTS]")
                    && blocks_text.contains(&child_a)
                    && blocks_text.contains(&child_b)
            });
            if has_aggregated {
                aggregated_wake_seen = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(aggregated_wake_seen, "parent received aggregated wake");

    // Grace period: wait 500ms (single deadline) to ensure no duplicate
    // individual wakes arrive. Count how many `[WORKSPACE EVENTS]` wake
    // messages appear in the final transcript — MUST be exactly ONE.
    let grace_start = tokio::time::Instant::now();
    let grace_deadline = grace_start + Duration::from_millis(500);
    loop {
        let remaining = match grace_deadline.checked_duration_since(tokio::time::Instant::now()) {
            Some(d) if !d.is_zero() => d,
            _ => break,
        };
        tokio::time::sleep(remaining.min(Duration::from_millis(100))).await;
    }

    // Final assertion: count wake messages in the parent's transcript.
    // Use per-message contentBlocks scanning (consistent with the pattern at
    // line 2211-2230) to avoid over/under-counting if the marker appears
    // multiple times in a single message's metadata.
    let final_conv = wss_rpc(
        &mut rpc,
        200,
        "agent.getConversation",
        json!({ "agentId": parent_id }),
    )
    .await;
    let final_messages = final_conv["messages"].as_array().expect("messages array");
    let wake_messages: Vec<String> = final_messages
        .iter()
        .map(|m| serde_json::to_string(&m["contentBlocks"]).unwrap_or_default())
        .filter(|t| t.contains("[WORKSPACE EVENTS]"))
        .collect();
    assert_eq!(
        wake_messages.len(),
        1,
        "parent transcript MUST have exactly ONE aggregated wake (after_all group), not {} — \
         the SUB-1 auto-watch from sendMessage MUST NOT fire duplicate individual wakes",
        wake_messages.len()
    );

    // Let the parent's wake turn wind down before teardown.
    let _ = wss_rpc(&mut rpc, 201, "agent.stop", json!({ "agentId": parent_id })).await;
}

/// SP-1 (Suggested Next Steps): the `--rules` file assembled by
/// `agent_manager::create_agent` for a top-level (non-sub-agent) interactive
/// agent MUST contain the `## Suggested Next Steps` heading — the directive
/// that tells the model to emit a `<!-- suggested-prompts ... -->` block at
/// the end of user-facing responses. The daemon writes the file into
/// `<data_dir>/agent-configs` (monorepo#1302) and keeps it alive for the
/// lifetime of the agent handle, so we scan that directory after the first
/// turn kicks off spawning.
#[tokio::test]
async fn assembled_rules_file_contains_suggested_next_steps_over_wss() {
    let Some(script) = gate("WSS SP-1 rules-file E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    // Dedicated TMPDIR keeps the daemon's residual temp usage hermetic and
    // lets this test assert the generated rules file no longer lands there
    // (monorepo#1302 moved it under `<data_dir>/agent-configs`).
    let tmp_dir = data_dir.join("tmp");
    std::fs::create_dir_all(&tmp_dir).expect("mkdir tmp dir");
    let tmp_dir_s = tmp_dir.to_string_lossy().into_owned();

    // Any behavior works — we don't care what the mock does after spawn,
    // only that the daemon actually reached the rules-file assembly path.
    let behavior = json!({ "response": "ok" }).to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("TMPDIR", &tmp_dir_s),
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

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        11,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "SP-1 WSS", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // sendMessage triggers `ensure_started` → `create_agent`, which writes the
    // assembled rules file into `std::env::temp_dir()` (redirected TMPDIR).
    let sent = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "hi" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Poll `<data_dir>/agent-configs` for the `intentd-rules-*.md` the daemon
    // writes during spawn (the directory is created on demand at first spawn,
    // so tolerate it not existing yet). Bounded wait so a hung spawn fails
    // loudly.
    let rules_dir = data_dir.join("agent-configs");
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut rules_body: Option<String> = None;
    while std::time::Instant::now() < deadline {
        let mut hit: Option<PathBuf> = None;
        if let Ok(entries) = std::fs::read_dir(&rules_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_s = name.to_string_lossy();
                if name_s.starts_with("intentd-rules-") && name_s.ends_with(".md") {
                    hit = Some(entry.path());
                    break;
                }
            }
        }
        if let Some(path) = hit {
            if let Ok(body) = std::fs::read_to_string(&path) {
                if !body.trim().is_empty() {
                    rules_body = Some(body);
                    break;
                }
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    let body = rules_body.expect(
        "expected `intentd-rules-*.md` to be written under <data_dir>/agent-configs \
         during agent spawn",
    );
    // Regression guard for monorepo#1302: the generated per-agent files must
    // no longer land in the (redirected) OS temp dir.
    let leaked_in_tmp = std::fs::read_dir(&tmp_dir)
        .expect("read TMPDIR")
        .flatten()
        .any(|e| {
            let name = e.file_name();
            let name_s = name.to_string_lossy();
            name_s.starts_with("intentd-rules-") || name_s.starts_with("intentd-mcp-")
        });
    assert!(
        !leaked_in_tmp,
        "generated agent config files must not be written into the OS temp dir"
    );
    // Debug tail walks forward to the next char boundary so the slice never
    // lands mid-multi-byte (the rules text includes non-ASCII like "2–4").
    let tail_from = body.len().saturating_sub(400);
    let tail_start = (tail_from..=body.len())
        .find(|i| body.is_char_boundary(*i))
        .unwrap_or(0);
    assert!(
        body.contains("## Suggested Next Steps"),
        "SP-1: assembled rules file must contain the Suggested Next Steps directive; \
         body tail: {:?}",
        &body[tail_start..]
    );
    assert!(
        body.contains("<!-- suggested-prompts"),
        "SP-1: rules file must embed the suggested-prompts template"
    );

    // Clean teardown so `AgentHandle::drop` reaps the child + temp file.
    let _ = wss_rpc(&mut rpc, 13, "agent.stop", json!({ "agentId": agent_id })).await;
}

/// No-prompt initial-agent parity over WSS (PROTOCOL §5.1): `workspace.create`
/// with an `initialAgent` but no prompt persists the agent row and returns the
/// `AgentLite` — the subscriber sees `workspace:created` → `agent:created`,
/// but never a `stream:*` frame because no first turn was started. The
/// transcript stays empty until the FE sends its first message.
#[tokio::test]
async fn workspace_create_no_prompt_creates_agent_over_wss() {
    let Some(script) = gate("WSS workspace.create no-prompt initial-agent E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let env: [(&str, &str); 3] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
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
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:*", "agent:*"] }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "workspace.create",
        json!({
            "title": "No-prompt WS",
            "branch": "feat/initial-agent-no-prompt-e2e",
            "initialAgent": {
                "name": "Coordinator",
                "model": "mock:default",
                "specialist": "implementor",
            },
        }),
    )
    .await;
    let ws_id = created["workspace"]["id"].as_str().expect("workspace id");
    let agent_id = created["initialAgent"]["id"]
        .as_str()
        .expect("no-prompt create still returns the agent")
        .to_string();
    assert_eq!(created["initialAgent"]["name"], "Coordinator");

    // Event flow: workspace:created → agent:created, NO stream frames.
    let mut saw_ws_created = false;
    let mut saw_agent_created = false;
    let mut saw_stream = false;
    for _ in 0..40 {
        let Some(frame) = wss_event_opt(&mut sub, 2).await else {
            break;
        };
        let ev = &frame["params"]["event"];
        match ev["type"].as_str() {
            Some("workspace:created") => {
                assert_eq!(ev["data"]["workspaceId"], ws_id);
                saw_ws_created = true;
            }
            Some("agent:created") => {
                assert!(saw_ws_created, "workspace:created precedes agent:created");
                assert_eq!(ev["data"]["agentId"], agent_id.as_str());
                saw_agent_created = true;
            }
            Some(t)
                if t.starts_with("agent:stream:") && ev["data"]["agentId"] == agent_id.as_str() =>
            {
                saw_stream = true;
            }
            _ => {}
        }
    }
    assert!(saw_ws_created, "workspace:created observed");
    assert!(saw_agent_created, "agent:created observed");
    assert!(!saw_stream, "no first turn started without a prompt");

    // Transcript is empty — the FE's first send will start the turn.
    let conv = wss_rpc(
        &mut rpc,
        11,
        "agent.getConversation",
        json!({ "agentId": agent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    assert!(
        messages.is_empty(),
        "no messages persisted without a prompt: {conv}"
    );
}

/// Specialist-derived default name over WSS (PROTOCOL §5.1/§5.5):
/// `workspace.create` with a name-less `initialAgent` carrying a specialist
/// derives the agent's name from the specialist's resolved display name
/// (frontmatter `name` — "Coordinator" for the embedded `spec-writer`) and
/// marks it explicitly set, so the opening-turn `setAgentName`
/// (`skipIfExplicitlySet: true`) cannot rename it away.
#[tokio::test]
async fn workspace_create_nameless_initial_agent_derives_specialist_name_over_wss() {
    let Some(script) = gate("WSS workspace.create specialist-derived initial-agent name E2E")
    else {
        return;
    };

    let data_dir = temp_data_dir();
    let env: [(&str, &str); 3] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
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

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "workspace.create",
        json!({
            "title": "Specialist-name WS",
            "branch": "feat/initial-agent-specialist-name-e2e",
            "initialAgent": {
                "model": "mock:default",
                "specialist": "spec-writer",
            },
        }),
    )
    .await;
    let agent_id = created["initialAgent"]["id"]
        .as_str()
        .expect("initial agent returned")
        .to_string();
    assert_eq!(
        created["initialAgent"]["name"], "Coordinator",
        "name derived from spec-writer's display name: {created}"
    );
    assert_eq!(
        created["initialAgent"]["nameExplicitlySet"], true,
        "specialist-derived default counts as explicitly set: {created}"
    );

    // The derived name is persisted, not just projected into the create result.
    let got = wss_rpc(&mut rpc, 11, "agent.get", json!({ "agentId": agent_id })).await;
    assert_eq!(got["agent"]["name"], "Coordinator");
    assert_eq!(got["agent"]["nameExplicitlySet"], true);
}

/// When a delegated agent calls `report_to_parent`, the report persists and is
/// visible via `agent.get` metadata. When the parent sends the agent NEW WORK
/// (a follow-up message), a new turn begins and clears the persisted
/// completion report, emitting `agent:updated` with `completionReportCleared:
/// true`. A subsequent `agent.get` shows no completion report in metadata. The
/// original `agent:idle` wake that delivered the report is unaffected (it
/// fires at turn-end before the next turn begins).
#[tokio::test]
async fn completion_report_cleared_when_new_turn_begins_over_wss() {
    const CHILD_TAG: &str = "CLEAR_REPORT_CHILD";
    const REPORT: &str = "CLEAR_REPORT shipped the thing";
    const SECOND_WORK: &str = "CLEAR_REPORT_SECOND do more work";
    const PARENT_GO: &str = "CLEAR_REPORT_PARENT_GO";
    let Some(script) = gate("WSS clear completion report on new turn") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    // Child behavior: first turn reports back, second turn acknowledges.
    let report_js = format!("return await ws.agent.reportToParent({});", json!(REPORT));
    let delegate_js = format!(
        "return await ws.agent.delegate({{ agentInstructions: {}, model: 'mock:default' }});",
        json!(CHILD_TAG),
    );
    let behavior = json!({
        "rules": [
            {
                "ifPromptContains": CHILD_TAG,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": report_js, "summary": "child reportToParent" }
                },
                "response": "child finished first task",
            },
            {
                "ifPromptContains": SECOND_WORK,
                "response": "child working on second task",
            },
            {
                "ifPromptContains": PARENT_GO,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": delegate_js, "summary": "parent delegates child" }
                },
                "response": "parent delegated child",
            },
        ],
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
    ];
    let child_proc = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child: child_proc,
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

    // Subscribe to agent events.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    // Create a parent agent.
    let parent = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Clear Parent", "model": "mock:default" }),
    )
    .await;
    let parent_id = parent["agent"]["id"]
        .as_str()
        .expect("parent id")
        .to_string();
    // Send the parent a message to trigger delegation.
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": parent_id, "content": PARENT_GO }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Wait for the parent to go idle (delegation complete). Use wss_event_opt
    // with a single deadline so heartbeat Pings don't extend the wait forever.
    let mut parent_idle = false;
    for _ in 0..100 {
        let Some(frame) = wss_event_opt(&mut sub, 30).await else {
            break;
        };
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:idle" && ev["data"]["agentId"] == parent_id {
            parent_idle = true;
            break;
        }
    }
    assert!(parent_idle, "parent agent went idle after delegation");

    // The child ID is in the parent's waitingForAgentIds (delegation metadata).
    let parent_get = wss_rpc(&mut rpc, 15, "agent.get", json!({ "agentId": parent_id })).await;
    let waiting = parent_get["agent"]["waitingForAgentIds"]
        .as_array()
        .expect("parent has waitingForAgentIds");
    assert_eq!(waiting.len(), 1, "parent waiting on exactly one child");
    let child_id = waiting[0].as_str().expect("child id").to_string();

    // Wait for the child to report and idle. Use wss_event_opt with a single
    // deadline so heartbeat Pings don't extend the wait forever.
    let mut child_idle = false;
    for _ in 0..100 {
        let Some(frame) = wss_event_opt(&mut sub, 30).await else {
            break;
        };
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:idle" && ev["data"]["agentId"] == child_id {
            child_idle = true;
            break;
        }
    }
    assert!(child_idle, "child went idle after reportToParent");

    // Assert the report is present in metadata.
    let get_before = wss_rpc(&mut rpc, 12, "agent.get", json!({ "agentId": child_id })).await;
    assert_eq!(
        get_before["agent"]["metadata"]["completionReport"],
        json!(REPORT),
        "completion report persisted after reportToParent"
    );

    // Send the child a new message (starts a new turn).
    let sent = wss_rpc(
        &mut rpc,
        13,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": child_id, "content": SECOND_WORK }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Wait for the agent:updated event with completionReportCleared. Use
    // wss_event_opt with a single deadline so heartbeat Pings don't extend
    // the wait forever.
    let mut saw_cleared_event = false;
    for _ in 0..100 {
        let Some(frame) = wss_event_opt(&mut sub, 30).await else {
            break;
        };
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:updated"
            && ev["data"]["agentId"] == child_id
            && ev["data"]["completionReportCleared"] == true
        {
            saw_cleared_event = true;
            break;
        }
    }
    assert!(
        saw_cleared_event,
        "agent:updated with completionReportCleared must fire when new turn begins"
    );

    // Wait for child to go idle after the second turn. Use wss_event_opt with
    // a single deadline so heartbeat Pings don't extend the wait forever.
    let mut second_idle = false;
    for _ in 0..100 {
        let Some(frame) = wss_event_opt(&mut sub, 30).await else {
            break;
        };
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:idle" && ev["data"]["agentId"] == child_id {
            second_idle = true;
            break;
        }
    }
    assert!(second_idle, "child went idle after second turn");

    // Assert the completion report is now absent.
    let get_after = wss_rpc(&mut rpc, 14, "agent.get", json!({ "agentId": child_id })).await;
    assert!(
        get_after["agent"]["metadata"]["completionReport"].is_null(),
        "completion report cleared after new turn begins: {:?}",
        get_after["agent"]["metadata"]["completionReport"]
    );
    assert!(
        get_after["agent"]["metadata"]["completionReportTimestamp"].is_null(),
        "completion report timestamp cleared after new turn begins"
    );
}

/// Stale queued-message redrive over WSS (#576). A message queued to a
/// delegated child while it is mid-turn — BEFORE the child persists its
/// completion report — drains only after that report was already delivered
/// to the parent (`queued_at < completion_report_timestamp`). The redrive
/// must NOT look like fresh work:
/// - (a) the redriven user message carries the deterministic `[SYSTEM NOTE]`
///   annotation telling the child its report was already delivered;
/// - (b) NO `agent:updated` with `completionReportCleared: true` fires for
///   the stale turn — the delivered report stays queryable via `agent.get`;
/// - (c) the parent receives exactly ONE wake for the report (the child
///   never re-reports, so no duplicate wake).
/// Fresh messages keep today's clear-on-new-turn behavior (covered by
/// `completion_report_cleared_when_new_turn_begins_over_wss` above).
#[tokio::test]
async fn stale_queued_redrive_annotated_and_report_kept_over_wss() {
    const CHILD_TAG: &str = "STALE576_CHILD";
    const REPORT: &str = "STALE576_REPORT shipped the thing";
    const STALE_MSG: &str = "STALE576_QUEUED follow-up sent while the child was mid-turn";
    const PARENT_GO: &str = "STALE576_PARENT_GO";
    // Stable prefix of the daemon's stale-redrive annotation (#576) — see
    // `STALE_REDRIVE_NOTE_PREFIX` in `intent-services`'s agent_manager.
    const NOTE_PREFIX: &str = "[SYSTEM NOTE] This message was queued before you completed";
    let Some(script) = gate("WSS stale queued-message redrive (#576)") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let report_js = format!("return await ws.agent.reportToParent({});", json!(REPORT));
    let delegate_js = format!(
        "return await ws.agent.delegate({{ agentInstructions: {}, model: 'mock:default' }});",
        json!(CHILD_TAG),
    );
    // Prompt-matched rules; the STALE_MSG rule comes FIRST so the redriven
    // turn matches it (and never re-reports). The child's report turn delays
    // 8s BEFORE its reportToParent tool call so the test can queue STALE_MSG
    // mid-turn, strictly before the report timestamp is persisted.
    let behavior = json!({
        "rules": [
            {
                "ifPromptContains": STALE_MSG,
                "response": "child acknowledged the stale message without re-reporting",
            },
            {
                "ifPromptContains": CHILD_TAG,
                "delayMs": 8000,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": report_js, "summary": "child reportToParent" }
                },
                "response": "child finished after reportToParent",
            },
            {
                "ifPromptContains": "[WORKSPACE EVENTS]",
                "response": "parent acknowledged the wake",
            },
            {
                "ifPromptContains": PARENT_GO,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": delegate_js, "summary": "delegate stale-redrive child" }
                },
                "response": "parent delegated one immediate child",
            },
        ],
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
    ];
    let child_proc = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child: child_proc,
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

    // SUBSCRIBER conn — subscribe BEFORE the turn so we miss no events.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let parent = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Stale Parent", "model": "mock:default" }),
    )
    .await;
    let parent_id = parent["agent"]["id"]
        .as_str()
        .expect("parent id")
        .to_string();
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": parent_id, "content": PARENT_GO }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Phase 1 — learn the child id and wait until its report turn is
    // verifiably in flight (busy slot claimed), so the follow-up send below
    // QUEUES instead of delivering. The child id is any non-parent agent id
    // on the wire (only one child exists); mid-turn evidence is its active
    // `agent:status-changed` or a turn-startup `agent:stream:status` frame —
    // both fire well inside the child rule's 8s pre-report delay.
    let mut child_id: Option<String> = None;
    let mut child_mid_turn = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while !child_mid_turn {
        let Some(frame) = wss_event_opt_until(&mut sub, deadline).await else {
            panic!("timed out waiting for the child's report turn to begin: child_id={child_id:?}")
        };
        let ev = &frame["params"]["event"];
        let ev_agent = ev["data"]["agentId"].as_str().unwrap_or_default();
        let ev_type = ev["type"].as_str().unwrap_or_default();
        if ev_agent.is_empty() || ev_agent == parent_id {
            continue;
        }
        if child_id.is_none() {
            child_id = Some(ev_agent.to_string());
        }
        if (ev_type == "agent:status-changed" && ev["data"]["isActive"] == json!(true))
            || ev_type == "agent:stream:status"
        {
            child_mid_turn = true;
        }
    }
    let child_id = child_id.expect("child agent id observed on the wire");

    // Queue the follow-up while the child is mid-turn and its report is NOT
    // yet persisted (the mock delays 8s before reportToParent): `queued_at`
    // therefore predates `completion_report_timestamp` — the exact #576
    // staleness condition. `queued: true` is the deterministic proof the
    // message parked behind the in-flight turn instead of starting a fresh
    // one; the NORMAL (non-requeue) stale path is the one under test, so the
    // redriven row gets the annotation (a persisted terminal-failure requeue
    // would suppress the clear WITHOUT annotating).
    let queued = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": child_id, "content": STALE_MSG }),
    )
    .await;
    assert_eq!(queued["success"], true, "queued send ok: {queued}");
    assert_eq!(
        queued["queued"], true,
        "message must QUEUE behind the child's in-flight turn: {queued}"
    );

    // Phase 2 — drive to completion ORDER-INSENSITIVELY (the report-time
    // wake races the child's own turn end on the wire, see SUB-2 above):
    // the child ends its report turn AND its redriven stale turn (two
    // stream:ends) then idles; the parent idles after delegating, runs
    // exactly ONE wake turn, and idles again. Meanwhile count every
    // `agent:updated` carrying `completionReportCleared: true` for the
    // child — the stale redrive must NOT clear the delivered report, and
    // the subscription's ordered delivery guarantees any turn-begin clear
    // event would land before the stale turn's stream:end.
    let mut parent_idle_count = 0u32;
    let mut parent_wake_ends = 0u32;
    let mut child_stream_ends = 0u32;
    let mut child_idle = false;
    let mut child_cleared_events = 0u32;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    while !(parent_idle_count >= 2 && parent_wake_ends >= 1 && child_stream_ends >= 2 && child_idle)
    {
        let Some(frame) = wss_event_opt_until(&mut sub, deadline).await else {
            panic!(
                "timed out waiting for redrive milestones: parent_idle_count={parent_idle_count} \
                 parent_wake_ends={parent_wake_ends} child_stream_ends={child_stream_ends} \
                 child_idle={child_idle}"
            )
        };
        let ev = &frame["params"]["event"];
        let ev_agent = ev["data"]["agentId"].as_str().unwrap_or_default();
        let ev_type = ev["type"].as_str().unwrap_or_default();
        if ev_agent == parent_id {
            if ev_type == "agent:idle" {
                parent_idle_count += 1;
            }
            // Any parent stream:end after the first parent idle belongs to a
            // wake turn (the delegating turn's stream:end precedes that idle).
            if ev_type == "agent:stream:end" && parent_idle_count >= 1 {
                parent_wake_ends += 1;
            }
        } else if ev_agent == child_id {
            match ev_type {
                "agent:stream:end" => child_stream_ends += 1,
                "agent:idle" => child_idle = true,
                "agent:updated" if ev["data"]["completionReportCleared"] == json!(true) => {
                    child_cleared_events += 1;
                }
                _ => {}
            }
        }
    }
    // (b) the stale redrive suppressed the turn-begin report clear.
    assert_eq!(
        child_cleared_events, 0,
        "NO agent:updated with completionReportCleared:true may fire for the stale turn"
    );
    assert_eq!(
        parent_wake_ends, 1,
        "exactly one wake-turn stream:end on the parent"
    );

    // (a) the redriven user message carries the [SYSTEM NOTE] annotation —
    // and ONLY that message (the delegated-instructions row is untouched).
    let conv = wss_rpc(
        &mut rpc,
        13,
        "agent.getConversation",
        json!({ "agentId": child_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("child messages array");
    let texts: Vec<String> = messages
        .iter()
        .map(|m| serde_json::to_string(&m["contentBlocks"]).unwrap_or_default())
        .collect();
    let stale_rows: Vec<&String> = texts.iter().filter(|t| t.contains(STALE_MSG)).collect();
    assert_eq!(
        stale_rows.len(),
        1,
        "the queued message persisted exactly once in the child transcript: {conv}"
    );
    assert!(
        stale_rows[0].contains(NOTE_PREFIX),
        "the redriven stale message carries the [SYSTEM NOTE] annotation: {}",
        stale_rows[0]
    );
    assert_eq!(
        texts.iter().filter(|t| t.contains(NOTE_PREFIX)).count(),
        1,
        "only the stale redrive is annotated: {conv}"
    );

    // The delivered report stays queryable after the stale turn (the clear
    // was suppressed, not deferred).
    let child_got = wss_rpc(&mut rpc, 14, "agent.get", json!({ "agentId": child_id })).await;
    assert_eq!(
        child_got["agent"]["metadata"]["completionReport"],
        json!(REPORT),
        "completion report still queryable after the stale redrive: {child_got}"
    );

    // (c) the parent received exactly ONE wake for the report: one
    // [WORKSPACE EVENTS] message, and the report text appears nowhere else.
    let conv = wss_rpc(
        &mut rpc,
        15,
        "agent.getConversation",
        json!({ "agentId": parent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("parent messages array");
    let texts: Vec<String> = messages
        .iter()
        .map(|m| serde_json::to_string(&m["contentBlocks"]).unwrap_or_default())
        .collect();
    assert_eq!(
        texts
            .iter()
            .filter(|t| t.contains("[WORKSPACE EVENTS]"))
            .count(),
        1,
        "exactly one wake message in the parent transcript: {conv}"
    );
    assert_eq!(
        texts.iter().filter(|t| t.contains(REPORT)).count(),
        1,
        "the report reached the parent exactly once (inside the single wake): {conv}"
    );
}

/// Emit `agent:message` on daemon-side user-row appends: verify that the
/// direct-send, queue-drain (`persist_user`), and wake-delivery
/// (`deliver_wake_message` runtime) paths all publish `agent:message` with the
/// persisted row's id. The direct-send branch of `AgentManager::send_message`
/// emits too (PROTOCOL §5.5 — previously it was silent, which left an
/// `agent.editAndRegenerate` regenerated user message invisible until reload).
/// This test covers: (1) direct send to an idle agent, (2) dequeued message
/// after a busy turn, (3) wake delivery to an idle agent.
#[tokio::test]
async fn agent_message_event_emitted_for_queue_drain_and_wake_over_wss() {
    // Dequeue-wait note: the drained entry's delivered content (persisted
    // user row == provider prompt) carries the enqueue-time annotation;
    // the direct send was never queued, so its row stays untouched. Stable
    // prefix of `DEQUEUE_WAIT_NOTE_PREFIX` in `intent-services`'s
    // agent_manager.
    const DEQUEUE_NOTE_PREFIX: &str = "[SYSTEM NOTE] This message was queued at";
    let Some(script) = gate("WSS agent:message queue+wake E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let (ws_id, note_id) = seed_workspace_and_note(&data_dir).await;
    // Slow first turn to keep agent busy while we queue the second message.
    let behavior = json!({
        "response": "reply",
        "firstTurnDelayMs": 2000
    })
    .to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        // The 2s busy window sits below the 5s dequeue-wait annotation
        // threshold (monorepo#2353); drop it so the wait-note assertions
        // exercise the annotation without slowing the suite.
        ("INTENTD_DEQUEUE_WAIT_MIN_MS", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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

    // Subscribe to agent:message + stream:end events.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:message", "agent:stream:end"], "workspaceId": &ws_id }),
    )
    .await;
    assert!(sub_resp["subscriptionId"].is_string());

    let mut rpc = connect_ws(port, cfg.clone()).await;

    // Part 1: Queue-drain path (persist_user in agent_manager.rs).
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": &ws_id, "name": "QueueTest", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_string();

    // Send first message — agent will be busy for 2000ms.
    let send1 = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": &ws_id, "agentId": &agent_id, "content": "first" }),
    )
    .await;
    assert_eq!(send1["success"], true);
    assert_eq!(send1["queued"], false);
    // The direct-send branch returns the PERSISTED row id (PROTOCOL §5.5) and
    // emits agent:message for it — collected and asserted below.
    let direct_message_id = send1["messageId"].as_str().expect("messageId").to_string();

    // Give the agent a moment to start processing.
    sleep(Duration::from_millis(200)).await;

    // Send second message while busy — this will queue.
    let send2 = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": &ws_id, "agentId": &agent_id, "content": "queued" }),
    )
    .await;
    assert_eq!(send2["success"], true);
    assert_eq!(send2["queued"], true, "second message should queue");

    // Collect events: wait for agent:message role=user for BOTH the direct
    // send (emitted immediately by the send_message direct branch) and the
    // dequeued message (emitted by persist_user after the first turn ends).
    // Use wss_event_opt with a single 30s deadline per event (parity with the
    // sibling suites) so contention from parallel e2e tests — daemon + node
    // mock-agent spawns easily exceeding a short silence window — doesn't
    // flake the test (STAB-128).
    let mut user_message_event_ids: Vec<String> = Vec::new();
    let mut stream_end_count = 0;
    let drain_wait_started = std::time::Instant::now();
    for _ in 0..100 {
        let Some(frame) = wss_event_opt(&mut sub, 30).await else {
            break;
        };
        let evt = &frame["params"]["event"];
        match evt["type"].as_str() {
            Some("agent:message") => {
                if evt["data"]["agentId"].as_str() == Some(agent_id.as_str())
                    && evt["data"]["role"] == "user"
                {
                    if let Some(mid) = evt["data"]["messageId"].as_str() {
                        user_message_event_ids.push(mid.to_string());
                    }
                    // Both turns ended and both user events seen — done.
                    if stream_end_count >= 2 && user_message_event_ids.len() >= 2 {
                        break;
                    }
                }
            }
            Some("agent:stream:end") => {
                stream_end_count += 1;
                // After 2 turns and both user message events, we're done.
                if stream_end_count >= 2 && user_message_event_ids.len() >= 2 {
                    break;
                }
            }
            _ => {}
        }
    }
    assert!(
        user_message_event_ids.len() >= 2,
        "agent:message events emitted for BOTH the direct send and the dequeued \
         user message (got ids {user_message_event_ids:?}); \
         stream_end_count={stream_end_count}, elapsed={:?}",
        drain_wait_started.elapsed()
    );
    assert_eq!(
        user_message_event_ids[0], direct_message_id,
        "direct-send agent:message event carries the persisted row id returned by the RPC"
    );
    let dequeued_message_id = user_message_event_ids[1].clone();

    // Verify the messageIds match the transcript rows.
    let conv = wss_rpc(
        &mut rpc,
        13,
        "agent.getConversation",
        json!({ "workspaceId": &ws_id, "agentId": &agent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().unwrap();
    // Count user messages - we expect the dequeued message to be the second one.
    let user_messages: Vec<_> = messages.iter().filter(|m| m["role"] == "user").collect();
    assert!(
        user_messages.len() >= 2,
        "should have at least 2 user messages (first + queued)"
    );
    // The direct send's RPC messageId is the first user row's id.
    assert_eq!(
        user_messages[0]["id"].as_str(),
        Some(direct_message_id.as_str()),
        "direct-send RPC messageId matches the first user message row"
    );
    // The dequeued event messageId should match the second user message.
    let second_user_id = user_messages[1]["id"].as_str();
    assert_eq!(
        Some(dequeued_message_id.as_str()),
        second_user_id,
        "dequeued agent:message event ID matches the second (queued) user message"
    );

    let direct_text = serde_json::to_string(&user_messages[0]["contentBlocks"]).unwrap_or_default();
    assert!(
        !direct_text.contains(DEQUEUE_NOTE_PREFIX),
        "immediate delivery is NOT annotated: {direct_text}"
    );
    let queued_text = serde_json::to_string(&user_messages[1]["contentBlocks"]).unwrap_or_default();
    assert!(
        queued_text.contains(DEQUEUE_NOTE_PREFIX),
        "the drained message carries the dequeue-wait note: {queued_text}"
    );
    assert!(
        queued_text.contains("before delivery."),
        "the note names the wait duration: {queued_text}"
    );

    // Part 2: Wake delivery path (deliver_wake_message runtime).
    let marked = wss_rpc(
        &mut rpc,
        14,
        "task.markAsTask",
        json!({ "workspaceId": &ws_id, "noteId": &note_id, "status": "in_progress" }),
    )
    .await;
    assert_eq!(marked["ok"], true);

    let wake_result = wss_rpc(
        &mut rpc,
        15,
        "agent.wakeOrCreate",
        json!({
            "workspaceId": &ws_id,
            "taskNoteId": &note_id,
            "contextMessage": "wake test",
            "create": { "model": "mock:default" },
        }),
    )
    .await;
    assert_eq!(wake_result["ok"], true);
    let task_agent_id = wake_result["agentId"].as_str().unwrap().to_string();

    // Collect events: wait for agent:message role=user for the wake delivery.
    // Same 30s-per-event deadline as above (STAB-128 contention hardening).
    let mut saw_wake_message_event = false;
    let mut wake_message_id: Option<String> = None;
    let mut wake_stream_end_count = 0;
    let wake_wait_started = std::time::Instant::now();
    for _ in 0..100 {
        let Some(frame) = wss_event_opt(&mut sub, 30).await else {
            break;
        };
        let evt = &frame["params"]["event"];
        match evt["type"].as_str() {
            Some("agent:message") => {
                if evt["data"]["agentId"].as_str() == Some(task_agent_id.as_str())
                    && evt["data"]["role"] == "user"
                {
                    wake_message_id = evt["data"]["messageId"].as_str().map(String::from);
                    saw_wake_message_event = true;
                    // The wake turn may have already ended; don't wait for another frame.
                    if wake_stream_end_count >= 1 {
                        break;
                    }
                }
            }
            Some("agent:stream:end") => {
                wake_stream_end_count += 1;
                if saw_wake_message_event && wake_stream_end_count >= 1 {
                    break;
                }
            }
            _ => {}
        }
    }
    assert!(
        saw_wake_message_event,
        "agent:message event emitted for wake delivery (deliver_wake_message path); \
         wake_stream_end_count={wake_stream_end_count}, elapsed={:?}",
        wake_wait_started.elapsed()
    );

    // Verify the wake messageId matches the first (and only) user message in the task agent's transcript.
    let wake_conv = wss_rpc(
        &mut rpc,
        16,
        "agent.getConversation",
        json!({ "workspaceId": &ws_id, "agentId": &task_agent_id }),
    )
    .await;
    let wake_messages = wake_conv["messages"].as_array().unwrap();
    // The wake-delivery agent should have exactly one user message (the wake contextMessage).
    let wake_user_messages: Vec<_> = wake_messages
        .iter()
        .filter(|m| m["role"] == "user")
        .collect();
    assert!(
        !wake_user_messages.is_empty(),
        "wake agent should have at least one user message"
    );
    // The event messageId should match the first user message.
    let first_wake_user_id = wake_user_messages[0]["id"].as_str();
    assert_eq!(
        wake_message_id.as_deref(),
        first_wake_user_id,
        "wake agent:message event ID matches the first user message (wake contextMessage)"
    );
}

/// STAB-114 / monorepo#1014 regression: When an interrupt lands BEFORE any
/// assistant output, the preempted user message is NOT re-queued — it is
/// delivered TOGETHER with the interrupt message in ONE combined prompt
/// (original first), so both messages are honored in order and the queue
/// stays empty.
///
/// Uses `parkBeforeFirstChunk` mock behavior + deterministic wait for
/// agent:stream:status phase="prompt" to ensure the ACP session is established
/// (making the turn cancellable) before sending the interrupt. The combined
/// outbound prompt is asserted via the fixture's `MOCK_AGENT_PROMPT_LOG` seam.
#[tokio::test]
async fn stab_114_interrupt_zero_output_delivers_combined_prompt_over_wss() {
    let Some(script) = gate("STAB-114 zero-output combined delivery E2E") else {
        eprintln!("[STAB114-TEST] Gate returned None, test skipped");
        return;
    };
    eprintln!("[STAB114-TEST] Test body running");

    let data_dir = temp_data_dir();
    let (ws_id, _note_id) = seed_workspace_and_note(&data_dir).await;
    let prompt_log = data_dir.join("prompts.jsonl");
    let prompt_log_str = prompt_log.to_string_lossy().into_owned();
    // parkBeforeFirstChunk parks immediately without streaming any chunks
    let behavior = json!({ "parkBeforeFirstChunk": true, "response": "resumed" }).to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_PROMPT_LOG", &prompt_log_str),
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
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": &ws_id }),
    )
    .await;
    assert!(sub_resp["subscriptionId"].is_string());

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": &ws_id, "name": "STAB114", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_string();

    // Send first message — agent will park without streaming
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": &ws_id, "agentId": &agent_id, "content": "first" }),
    )
    .await;
    assert_eq!(sent["success"], true);

    // Deterministic wait: wait for agent:stream:status with phase="prompt".
    // This ensures the ACP session is established and acp_session_id is persisted,
    // making the turn cancellable. Waiting for isResponding alone is insufficient
    // because it becomes true when the worker starts, before acp_session_id is set.
    let mut saw_prompt_phase = false;
    for _ in 0..50 {
        if let Some(frame) = wss_event_opt(&mut sub, 3).await {
            if frame["params"]["event"]["type"] == "agent:stream:status" {
                let phase = frame["params"]["event"]["data"]["phase"]
                    .as_str()
                    .unwrap_or("");
                if phase == "prompt" {
                    saw_prompt_phase = true;
                    break;
                }
            }
        }
    }
    assert!(
        saw_prompt_phase,
        "STAB-114: must see prompt phase before interrupt (ensures session established)"
    );

    // Interrupt before any output
    let interrupted = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({
            "workspaceId": &ws_id,
            "agentId": &agent_id,
            "content": "urgent",
            "priority": "interrupt",
        }),
    )
    .await;
    assert_eq!(interrupted["success"], true);

    // Outbound-prompt contract: poll the fixture's prompt log until the
    // interrupt turn's prompt lands (the first stream:end belongs to the
    // CANCELLED turn, before the combined prompt is even sent). The combined
    // prompt carries BOTH messages with the preempted "first" BEFORE the
    // interrupt "urgent".
    let mut combined_text = None;
    for _ in 0..50 {
        if let Ok(log) = std::fs::read_to_string(&prompt_log) {
            if let Some(text) = log
                .lines()
                .filter_map(|l| serde_json::from_str::<Value>(l).ok())
                .filter_map(|p| p["text"].as_str().map(str::to_string))
                .find(|t| t.contains("urgent"))
            {
                combined_text = Some(text);
                break;
            }
        }
        sleep(Duration::from_millis(200)).await;
    }
    let text = combined_text.expect("interrupt turn's prompt reached the mock");
    let first_pos = text
        .find("first")
        .unwrap_or_else(|| panic!("combined prompt carries the preempted message: {text}"));
    let urgent_pos = text
        .find("urgent")
        .unwrap_or_else(|| panic!("combined prompt carries the interrupt message: {text}"));
    assert!(
        first_pos < urgent_pos,
        "preempted message precedes the interrupt message: {text}"
    );

    // Combined delivery: the queue stays EMPTY — the preempted message rides
    // the interrupt turn's prompt, it is never re-queued behind it.
    let queue = wss_rpc(
        &mut rpc,
        13,
        "agent.getQueue",
        json!({ "agentId": &agent_id }),
    )
    .await;
    let queued = queue["queue"].as_array().expect("queue array");
    assert!(
        queued.is_empty(),
        "combined delivery leaves the queue empty (no requeue): {queue}"
    );

    // Durable interruption marker: the zero-output interrupt-send persists an
    // EMPTY interrupted assistant row, stamped with the machine-readable
    // reason + user sender attribution — but it never counts as turn progress
    // (the combined delivery above still fired). Transcript keeps BOTH user
    // rows intact, with the marker BETWEEN them (interrupted turn first).
    let conv = wss_rpc(
        &mut rpc,
        14,
        "agent.getConversation",
        json!({ "agentId": &agent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let markers: Vec<&Value> = messages
        .iter()
        .filter(|m| m["role"] == "assistant" && m["metadata"]["interrupted"] == true)
        .collect();
    assert_eq!(
        markers.len(),
        1,
        "zero-output interrupt-send persists exactly one interrupted marker row: {conv}"
    );
    let marker = markers[0];
    assert_eq!(
        marker["metadata"]["interruptReason"], "preempted_by_message",
        "marker row carries the machine-readable reason: {marker}"
    );
    assert_eq!(
        marker["metadata"]["interruptedBy"],
        json!({ "kind": "user" }),
        "FE-originated interrupt send stamps user attribution: {marker}"
    );
    let user_texts: Vec<&str> = messages
        .iter()
        .filter(|m| m["role"] == "user")
        .filter_map(|m| m["contentBlocks"][0]["text"].as_str())
        .collect();
    assert_eq!(
        user_texts,
        vec!["first", "urgent"],
        "both user rows persist in original order: {conv}"
    );
    // Ordering: the marker row lands between the preempted user row and the
    // interrupt message's user row.
    let marker_idx = messages
        .iter()
        .position(|m| m["id"] == marker["id"])
        .unwrap();
    let urgent_idx = messages
        .iter()
        .position(|m| m["role"] == "user" && m["contentBlocks"][0]["text"] == "urgent")
        .unwrap();
    assert!(
        marker_idx < urgent_idx,
        "interrupted marker row precedes the interrupt message row: {conv}"
    );
}

/// STAB-114 regression: When an interrupt lands AFTER streaming started, the
/// message is NOT re-queued (turn has progressed past zero output).
#[tokio::test]
async fn stab_114_interrupt_after_streaming_no_requeue_over_wss() {
    let Some(script) = gate("STAB-114 after-streaming no requeue E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let (ws_id, _note_id) = seed_workspace_and_note(&data_dir).await;
    // blockUntilCancel streams a chunk then parks
    let behavior = json!({ "blockUntilCancel": true, "response": "resumed" }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*", "chat:stream:delta"], "workspaceId": &ws_id }),
    )
    .await;
    assert!(sub_resp["subscriptionId"].is_string());

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": &ws_id, "name": "STAB114", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_string();

    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": &ws_id, "agentId": &agent_id, "content": "first" }),
    )
    .await;
    assert_eq!(sent["success"], true);

    // Wait for chunk to be streamed
    let mut saw_chunk = false;
    for _ in 0..30 {
        if let Some(frame) = wss_event_opt(&mut sub, 3).await {
            if frame["params"]["event"]["type"] == "chat:stream:delta"
                && frame["params"]["event"]["data"]["content"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("streaming-before-cancel")
            {
                saw_chunk = true;
                break;
            }
        }
    }
    assert!(saw_chunk, "agent streamed a chunk before parking");

    // Interrupt after streaming started
    let interrupted = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({
            "workspaceId": &ws_id,
            "agentId": &agent_id,
            "content": "urgent",
            "priority": "interrupt",
        }),
    )
    .await;
    assert_eq!(interrupted["success"], true);

    // Check queue: should be empty (message should NOT be re-queued)
    let queue = wss_rpc(
        &mut rpc,
        13,
        "agent.getQueue",
        json!({ "agentId": &agent_id }),
    )
    .await;
    let messages = queue["queue"].as_array().expect("queue array");
    assert!(
        messages.is_empty(),
        "STAB-114: interrupt after streaming should NOT re-queue"
    );
}

/// Pre-first-token stop: a plain `agent.stop` landing after the turn started
/// but BEFORE any assistant content streamed persists an EMPTY interrupted
/// assistant row (every interruption records the marker row), and the
/// terminal `agent:stream:end` carries `stopReason: "interrupted"` plus the
/// synthetic row's `messageId` so clients can render the Stopped indicator
/// live.
#[tokio::test]
async fn agent_stop_before_first_token_persists_empty_interrupted_row_over_wss() {
    let Some(script) = gate("pre-first-token agent.stop E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let (ws_id, _note_id) = seed_workspace_and_note(&data_dir).await;
    // parkBeforeFirstChunk parks immediately without streaming any chunks.
    let behavior = json!({ "parkBeforeFirstChunk": true, "response": "resumed" }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": &ws_id }),
    )
    .await;
    assert!(sub_resp["subscriptionId"].is_string());

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": &ws_id, "name": "PreToken", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_string();

    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": &ws_id, "agentId": &agent_id, "content": "first" }),
    )
    .await;
    assert_eq!(sent["success"], true);

    // Deterministic wait for phase="prompt": the live-turn slot opens (message
    // id minted) immediately BEFORE this status emit, so once observed the
    // turn has started — but nothing has streamed (the mock parks).
    let mut saw_prompt_phase = false;
    for _ in 0..50 {
        if let Some(frame) = wss_event_opt(&mut sub, 3).await {
            if frame["params"]["event"]["type"] == "agent:stream:status"
                && frame["params"]["event"]["data"]["phase"] == "prompt"
            {
                saw_prompt_phase = true;
                break;
            }
        }
    }
    assert!(saw_prompt_phase, "turn started (phase=prompt) before stop");

    // Plain stop before the first token.
    let stopped = wss_rpc(&mut rpc, 12, "agent.stop", json!({ "agentId": &agent_id })).await;
    assert_eq!(stopped["success"], true, "stop ok: {stopped}");

    // Terminal stream:end carries stopReason + the synthetic row's messageId.
    let mut interrupted_message_id = None;
    for _ in 0..50 {
        let frame = wss_event(&mut sub, 30).await;
        if frame["params"]["event"]["type"] == "agent:stream:end" {
            let data = &frame["params"]["event"]["data"];
            assert_eq!(
                data["stopReason"], "interrupted",
                "pre-first-token stop stream:end carries stopReason: {data}"
            );
            assert_eq!(
                data["interruptReason"], "user_stop",
                "pre-first-token stop stream:end carries interruptReason: {data}"
            );
            interrupted_message_id = Some(
                data["messageId"]
                    .as_str()
                    .unwrap_or_else(|| panic!("stream:end carries messageId: {data}"))
                    .to_string(),
            );
            break;
        }
    }
    let interrupted_message_id =
        interrupted_message_id.expect("terminal agent:stream:end emitted on stop");

    // The transcript durably records the stop: an EMPTY assistant row under
    // the event's messageId, tagged metadata.interrupted.
    let conv = wss_rpc(
        &mut rpc,
        13,
        "agent.getConversation",
        json!({ "agentId": &agent_id }),
    )
    .await;
    let row = conv["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .find(|m| m["id"] == interrupted_message_id.as_str())
        .unwrap_or_else(|| panic!("empty synthetic interrupted row persisted: {conv}"));
    assert_eq!(
        row["role"], "assistant",
        "synthetic row is assistant: {row}"
    );
    assert_eq!(
        row["contentBlocks"],
        json!([]),
        "synthetic row has empty contentBlocks: {row}"
    );
    assert_eq!(
        row["metadata"]["interrupted"], true,
        "synthetic row tagged metadata.interrupted: {row}"
    );
    assert_eq!(
        row["metadata"]["interruptReason"], "user_stop",
        "synthetic row carries the machine-readable reason: {row}"
    );
}

/// intent-hq/monorepo#1757 regression: a plain `agent.stop` landing on a
/// zero-output turn must NOT lose the stopped turn's user message. The
/// provider drops the cancelled prompt on `session/cancel`, so the daemon
/// arms a prompt-only redelivery payload and the next plain follow-up send
/// delivers the stopped message's text AND image attachment ahead of its own
/// content in the SAME `session/prompt` — while the transcript keeps exactly
/// the two original user rows (no duplicate).
///
/// Uses `parkBeforeFirstChunk` (zero output) + a deterministic wait for
/// phase="prompt" before the stop; the follow-up outbound prompt is asserted
/// via the fixture's `MOCK_AGENT_PROMPT_LOG` seam (`text` + `blockTypes`).
#[tokio::test]
async fn agent_stop_zero_output_redelivers_message_and_image_on_follow_up_over_wss() {
    let Some(script) = gate("zero-output stop redelivery E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let (ws_id, _note_id) = seed_workspace_and_note(&data_dir).await;
    let prompt_log = data_dir.join("prompts.jsonl");
    let prompt_log_str = prompt_log.to_string_lossy().into_owned();
    // parkBeforeFirstChunk parks the FIRST turn with zero assistant output;
    // the follow-up turn (promptCount 2) responds normally.
    let behavior = json!({ "parkBeforeFirstChunk": true, "response": "resumed" }).to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_PROMPT_LOG", &prompt_log_str),
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
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": &ws_id }),
    )
    .await;
    assert!(sub_resp["subscriptionId"].is_string());

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": &ws_id, "name": "StopRedeliver", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_string();

    // First message carries text + an image attachment (the monorepo#1757
    // repro shape: screenshot + prompt as the workspace's first message).
    let image_data =
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==";
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({
            "workspaceId": &ws_id,
            "agentId": &agent_id,
            "content": "first with screenshot",
            "imageBlocks": [
                { "type": "image", "data": image_data, "mimeType": "image/png" }
            ],
        }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Deterministic wait for phase="prompt": the ACP session is established
    // and the turn is cancellable, but nothing has streamed (the mock parks
    // before its first chunk — zero output).
    let mut saw_prompt_phase = false;
    for _ in 0..50 {
        if let Some(frame) = wss_event_opt(&mut sub, 3).await {
            if frame["params"]["event"]["type"] == "agent:stream:status"
                && frame["params"]["event"]["data"]["phase"] == "prompt"
            {
                saw_prompt_phase = true;
                break;
            }
        }
    }
    assert!(saw_prompt_phase, "turn started (phase=prompt) before stop");

    // Plain stop (keep-alive UserStop) with zero output.
    let stopped = wss_rpc(&mut rpc, 12, "agent.stop", json!({ "agentId": &agent_id })).await;
    assert_eq!(stopped["success"], true, "stop ok: {stopped}");

    // Wait for the stopped turn's terminal stream:end before following up.
    let mut saw_stream_end = false;
    for _ in 0..50 {
        let frame = wss_event(&mut sub, 30).await;
        if frame["params"]["event"]["type"] == "agent:stream:end" {
            assert_eq!(
                frame["params"]["event"]["data"]["stopReason"], "interrupted",
                "stop stream:end carries stopReason: {frame}"
            );
            saw_stream_end = true;
            break;
        }
    }
    assert!(saw_stream_end, "terminal agent:stream:end emitted on stop");

    // Plain follow-up send (NOT interrupt priority — the monorepo#1757 path).
    let follow_up = wss_rpc(
        &mut rpc,
        13,
        "agent.sendMessage",
        json!({
            "workspaceId": &ws_id,
            "agentId": &agent_id,
            "content": "sorry just file an issue",
        }),
    )
    .await;
    assert_eq!(follow_up["success"], true, "follow-up ok: {follow_up}");

    // Outbound-prompt contract: poll the fixture's prompt log until the
    // follow-up turn's prompt lands. It must carry the stopped message's
    // text BEFORE the follow-up text, plus the stopped message's image
    // block (redelivered — the provider dropped the cancelled prompt).
    let mut follow_up_prompt = None;
    for _ in 0..50 {
        if let Ok(log) = std::fs::read_to_string(&prompt_log) {
            if let Some(p) = log
                .lines()
                .filter_map(|l| serde_json::from_str::<Value>(l).ok())
                .find(|p| {
                    p["text"]
                        .as_str()
                        .unwrap_or_default()
                        .contains("sorry just file an issue")
                })
            {
                follow_up_prompt = Some(p);
                break;
            }
        }
        sleep(Duration::from_millis(200)).await;
    }
    let prompt = follow_up_prompt.expect("follow-up turn's prompt reached the mock");
    let text = prompt["text"].as_str().unwrap_or_default();
    let stopped_pos = text.find("first with screenshot").unwrap_or_else(|| {
        panic!("follow-up prompt redelivers the stopped message's text: {text}")
    });
    let follow_pos = text
        .find("sorry just file an issue")
        .unwrap_or_else(|| panic!("follow-up prompt carries its own content: {text}"));
    assert!(
        stopped_pos < follow_pos,
        "stopped message precedes the follow-up content: {text}"
    );
    let block_types: Vec<&str> = prompt["blockTypes"]
        .as_array()
        .expect("prompt log carries blockTypes")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    assert!(
        block_types.contains(&"image"),
        "follow-up prompt redelivers the stopped message's image block: {block_types:?}"
    );

    // Transcript integrity: exactly the two original user rows, in order,
    // with the first still carrying its image block — redelivery is
    // wire-only, nothing is re-persisted.
    let conv = wss_rpc(
        &mut rpc,
        14,
        "agent.getConversation",
        json!({ "agentId": &agent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let user_rows: Vec<&Value> = messages.iter().filter(|m| m["role"] == "user").collect();
    assert_eq!(
        user_rows.len(),
        2,
        "exactly the two original user rows persist (no duplicate): {conv}"
    );
    assert_eq!(
        user_rows[0]["contentBlocks"][0]["text"], "first with screenshot",
        "stopped message row intact: {conv}"
    );
    assert!(
        user_rows[0]["contentBlocks"]
            .as_array()
            .expect("contentBlocks")
            .iter()
            .any(|b| b["type"] == "image"),
        "stopped message row keeps its image block: {conv}"
    );
    assert_eq!(
        user_rows[1]["contentBlocks"][0]["text"], "sorry just file an issue",
        "follow-up row intact: {conv}"
    );
}

/// STAB-124 regression: an interrupt landing mid-tool-call must NOT persist an
/// anonymous `tool_use` block (`name: ""`). The mock parks after emitting a
/// `tool_call` (`in_progress`); on `session/cancel` it echoes a title-less
/// `tool_call_update` (failed, abort-error output) — the stale echo that,
/// pre-fix, the interrupt turn's fresh transcript fabricated into an anonymous
/// `tool_use` + errored `tool_result` pair that broke FE conversation loading.
/// After the interrupt turn completes, every persisted `tool_use` block must
/// carry a non-empty name.
#[tokio::test]
async fn stab_124_interrupt_mid_tool_call_never_persists_anonymous_tool_use() {
    let Some(script) = gate("STAB-124 anonymous tool_use E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    // parkMidToolCall: emit tool_call (in_progress) then park until cancel.
    let behavior = json!({ "parkMidToolCall": true, "response": "resumed" }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*", "chat:stream:delta"], "workspaceId": &ws_id }),
    )
    .await;
    assert!(sub_resp["subscriptionId"].is_string());

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": &ws_id, "name": "STAB124", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_string();

    // First message — the mock emits a tool_call (in_progress) then parks.
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": &ws_id, "agentId": &agent_id, "content": "first" }),
    )
    .await;
    assert_eq!(sent["success"], true);

    // Deterministic wait: the tool_call event proves the turn is mid-tool-call
    // (and the ACP session is established → the turn is cancellable).
    let mut saw_tool_call = false;
    for _ in 0..50 {
        if let Some(frame) = wss_event_opt(&mut sub, 3).await {
            if frame["params"]["event"]["type"] == "agent:tool:call" {
                saw_tool_call = true;
                break;
            }
        }
    }
    assert!(
        saw_tool_call,
        "STAB-124: must be mid-tool-call before interrupt"
    );

    // Interrupt mid-tool-call.
    let interrupted = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({
            "workspaceId": &ws_id,
            "agentId": &agent_id,
            "content": "urgent",
            "priority": "interrupt",
        }),
    )
    .await;
    assert_eq!(interrupted["success"], true);

    // Wait for the interrupt turn to complete: its chunk ("resumed") then a
    // terminal stream:end.
    let mut saw_resumed_chunk = false;
    let mut saw_end_after_chunk = false;
    for _ in 0..80 {
        let Some(frame) = wss_event_opt(&mut sub, 30).await else {
            break;
        };
        match frame["params"]["event"]["type"].as_str() {
            Some("chat:stream:delta") => {
                if frame["params"]["event"]["data"]["content"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("resumed")
                {
                    saw_resumed_chunk = true;
                }
            }
            Some("agent:stream:end") if saw_resumed_chunk => {
                saw_end_after_chunk = true;
                break;
            }
            _ => {}
        }
    }
    assert!(
        saw_resumed_chunk,
        "interrupt turn streamed on the same child"
    );
    assert!(saw_end_after_chunk, "interrupt turn completed");

    // THE regression assertion: no persisted tool_use block has an empty name.
    let conv = wss_rpc(
        &mut rpc,
        13,
        "agent.getConversation",
        json!({ "workspaceId": &ws_id, "agentId": &agent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    for m in messages {
        let Some(blocks) = m["contentBlocks"].as_array() else {
            continue;
        };
        for block in blocks {
            if block["type"] == "tool_use" {
                let name = block["name"].as_str().unwrap_or_default();
                assert!(
                    !name.trim().is_empty(),
                    "STAB-124: anonymous tool_use block persisted/served: {block}"
                );
            }
        }
    }
}

/// STAB-133 regression: `agent.sendMessage` with `imageBlocks` / `fileBlocks`
/// on the runtime manager path must persist the attachments into the user's
/// transcript row (after the text block) so `agent.getConversation` — the
/// conversation view's read — returns them. Pre-fix, only the text block was
/// persisted and reloading the conversation dropped the attachments.
#[tokio::test]
async fn stab_133_send_message_persists_attachment_blocks_in_transcript() {
    let Some(script) = gate("STAB-133 attachment persistence E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let behavior = json!({ "response": "seen" }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": &ws_id }),
    )
    .await;
    assert!(sub_resp["subscriptionId"].is_string());

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": &ws_id, "name": "STAB133", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_string();

    let image_data = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==";
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({
            "workspaceId": &ws_id,
            "agentId": &agent_id,
            "content": "look at these",
            "imageBlocks": [
                { "type": "image", "data": image_data, "mimeType": "image/png" }
            ],
            "fileBlocks": [
                { "type": "file", "data": "ZmlsZWRhdGE=", "mimeType": "text/plain", "fileName": "notes.txt" }
            ],
        }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Wait for the turn to complete so the transcript is stable.
    let mut saw_end = false;
    for _ in 0..80 {
        let Some(frame) = wss_event_opt(&mut sub, 30).await else {
            break;
        };
        if frame["params"]["event"]["type"] == "agent:stream:end" {
            saw_end = true;
            break;
        }
    }
    assert!(saw_end, "turn completed");

    // THE regression assertion: the persisted user row carries the image and
    // file blocks after the text block.
    let conv = wss_rpc(
        &mut rpc,
        12,
        "agent.getConversation",
        json!({ "workspaceId": &ws_id, "agentId": &agent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let user_row = messages
        .iter()
        .find(|m| m["role"] == "user")
        .expect("user transcript row");
    let blocks = user_row["contentBlocks"]
        .as_array()
        .expect("contentBlocks array");
    assert_eq!(
        blocks[0]["type"], "text",
        "text block first: {:?}",
        user_row["contentBlocks"]
    );
    assert_eq!(blocks[0]["text"], "look at these");
    let image = blocks
        .iter()
        .find(|b| b["type"] == "image")
        .expect("image block persisted on the user row");
    assert_eq!(image["data"], image_data);
    assert_eq!(image["mimeType"], "image/png");
    let file = blocks
        .iter()
        .find(|b| b["type"] == "file")
        .expect("file block persisted on the user row");
    assert_eq!(file["data"], "ZmlsZWRhdGE=");
    assert_eq!(file["fileName"], "notes.txt");
    assert_eq!(file["mimeType"], "text/plain");
}

/// Sender attribution for agent-to-agent sends (PROTOCOL §5.5): when agent A
/// messages agent B through the `ws.agent.send` host binding, the delivered
/// user row on B's transcript must carry
/// `metadata == { type: "agent_message", fromAgentId, fromAgentName }` so
/// clients can render who sent it. A human `agent.sendMessage` (FE/RPC front
/// door, no caller agent) must stay untagged.
#[tokio::test]
async fn agent_to_agent_send_tags_sender_metadata_over_wss() {
    let Some(script) = gate("WSS agent-to-agent sender metadata E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    // Rule-matched behavior: the SENDER's kickoff prompt drives a real MCP
    // `workspace_api` call that finds the target by name and sends to it; the
    // TARGET's delivered message (and the human follow-up) fall through to
    // the plain default response.
    let send_code = "const agents = await ws.agent.list(true); \
                     const target = agents.find(a => a.name === 'TargetB'); \
                     return await ws.agent.send(target.id, 'cross-agent hello');";
    let behavior = json!({
        "rules": [
            {
                "ifPromptContains": "do the send",
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": send_code, "summary": "cross-agent send e2e" }
                },
                "response": "send dispatched"
            }
        ],
        "response": "plain reply"
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:stream:end"], "workspaceId": &ws_id }),
    )
    .await;
    assert!(sub_resp["subscriptionId"].is_string());

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let target = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": &ws_id, "name": "TargetB", "model": "mock:default" }),
    )
    .await;
    let target_id = target["agent"]["id"].as_str().unwrap().to_string();
    let sender = wss_rpc(
        &mut rpc,
        11,
        "agent.create",
        json!({ "workspaceId": &ws_id, "name": "SenderA", "model": "mock:default" }),
    )
    .await;
    let sender_id = sender["agent"]["id"].as_str().unwrap().to_string();

    // Kick off the sender's turn; its workspace_api call fans the message out
    // to the target, which then runs its own turn on the delivered message.
    let sent = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": &ws_id, "agentId": &sender_id, "content": "do the send" }),
    )
    .await;
    assert_eq!(sent["success"], true, "kickoff sendMessage ok: {sent}");

    // Wait for BOTH turns to complete: the sender's and the target's.
    let mut sender_done = false;
    let mut target_done = false;
    for _ in 0..120 {
        let Some(frame) = wss_event_opt(&mut sub, 30).await else {
            break;
        };
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:stream:end" {
            let ev_agent = ev["data"]["agentId"].as_str().unwrap_or_default();
            if ev_agent == sender_id {
                sender_done = true;
            } else if ev_agent == target_id {
                target_done = true;
            }
        }
        if sender_done && target_done {
            break;
        }
    }
    assert!(sender_done, "sender turn completed");
    assert!(target_done, "target turn completed");

    // THE assertion: the target's user row for the cross-agent message
    // carries the `agent_message` sender-attribution metadata.
    let conv = wss_rpc(
        &mut rpc,
        13,
        "agent.getConversation",
        json!({ "workspaceId": &ws_id, "agentId": &target_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let tagged = messages
        .iter()
        .find(|m| {
            m["role"] == "user"
                && m["contentBlocks"][0]["text"]
                    .as_str()
                    .is_some_and(|t| t.starts_with("cross-agent hello"))
        })
        .expect("cross-agent user row present");
    assert_eq!(
        tagged["metadata"],
        json!({
            "type": "agent_message",
            "fromAgentId": sender_id,
            "fromAgentName": "SenderA",
        }),
        "agent-originated send must carry sender attribution: {tagged}"
    );

    // Control: a human send (FE/RPC front door) stays untagged.
    let human = wss_rpc(
        &mut rpc,
        14,
        "agent.sendMessage",
        json!({ "workspaceId": &ws_id, "agentId": &target_id, "content": "human follow-up" }),
    )
    .await;
    assert_eq!(human["success"], true, "human sendMessage ok: {human}");
    let mut human_done = false;
    for _ in 0..80 {
        let Some(frame) = wss_event_opt(&mut sub, 30).await else {
            break;
        };
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:stream:end"
            && ev["data"]["agentId"].as_str() == Some(target_id.as_str())
        {
            human_done = true;
            break;
        }
    }
    assert!(human_done, "human follow-up turn completed");

    let conv2 = wss_rpc(
        &mut rpc,
        15,
        "agent.getConversation",
        json!({ "workspaceId": &ws_id, "agentId": &target_id }),
    )
    .await;
    let human_row = conv2["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .find(|m| {
            m["role"] == "user"
                && m["contentBlocks"][0]["text"]
                    .as_str()
                    .is_some_and(|t| t.starts_with("human follow-up"))
        })
        .expect("human user row present")
        .clone();
    assert_ne!(
        human_row["metadata"]["type"],
        json!("agent_message"),
        "human send must NOT carry agent_message metadata: {human_row}"
    );
}

/// Sender attribution for the remaining agent-originated send paths
/// (PROTOCOL §5.5): `ws.agent.sendToTask` must tag the assignee's delivered
/// row with the `agent_message` attribution, and the `ws.agent.create`
/// kickoff message must carry the same auto-tag — an explicit
/// `messageMetadata` keeps its own fields but the attribution fields are
/// daemon-stamped (never caller-controlled).
/// Drives all three through the full daemon stack: a real mock-ACP sender
/// turn invokes the MCP `workspace_api` bindings, and the assertions read
/// the persisted transcripts back over WSS `agent.getConversation`.
#[tokio::test]
async fn send_to_task_and_create_kickoff_tag_sender_metadata_over_wss() {
    let Some(script) = gate("WSS sendToTask/create sender metadata E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let (ws_id, note_id) = seed_workspace_and_note(&data_dir).await;
    // The SENDER's rule-matched turn fires one workspace_api call covering
    // all three paths: sendToTask to the task assignee, an auto-tagged
    // create kickoff, and a create kickoff with explicit messageMetadata.
    let ops_code = format!(
        "const st = await ws.agent.sendToTask({note}, 'task hello'); \
         const auto = await ws.agent.create('ChildAuto', 'kickoff hello', {{ model: 'mock:default' }}); \
         const explicit = await ws.agent.create('ChildExplicit', 'kickoff explicit', \
             {{ model: 'mock:default', messageMetadata: {{ type: 'custom_tag', note: 'explicit wins' }} }}); \
         return {{ st, auto, explicit }};",
        note = json!(note_id),
    );
    let behavior = json!({
        "rules": [
            {
                "ifPromptContains": "do the sends",
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": ops_code, "summary": "sendToTask + create kickoff attribution e2e" }
                },
                "response": "sends dispatched"
            }
        ],
        "response": "plain reply"
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:stream:end"], "workspaceId": &ws_id }),
    )
    .await;
    assert!(sub_resp["subscriptionId"].is_string());

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let target = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": &ws_id, "name": "TaskTarget", "model": "mock:default" }),
    )
    .await;
    let target_id = target["agent"]["id"].as_str().unwrap().to_string();
    let sender = wss_rpc(
        &mut rpc,
        11,
        "agent.create",
        json!({ "workspaceId": &ws_id, "name": "SenderA", "model": "mock:default" }),
    )
    .await;
    let sender_id = sender["agent"]["id"].as_str().unwrap().to_string();

    // Make the seeded note a task and assign the target so sendToTask
    // resolves an assignee.
    let marked = wss_rpc(
        &mut rpc,
        12,
        "task.markAsTask",
        json!({ "workspaceId": &ws_id, "noteId": note_id, "status": "in_progress" }),
    )
    .await;
    assert_eq!(marked["ok"], true, "markAsTask ok: {marked}");
    let assigned = wss_rpc(
        &mut rpc,
        13,
        "task.assignAgent",
        json!({ "workspaceId": &ws_id, "noteId": note_id, "agentId": target_id }),
    )
    .await;
    assert_eq!(assigned["ok"], true, "assignAgent ok: {assigned}");

    // Kick off the sender's turn; its workspace_api call fans out to the
    // task assignee and both created children, each running its own turn.
    let sent = wss_rpc(
        &mut rpc,
        14,
        "agent.sendMessage",
        json!({ "workspaceId": &ws_id, "agentId": &sender_id, "content": "do the sends" }),
    )
    .await;
    assert_eq!(sent["success"], true, "kickoff sendMessage ok: {sent}");

    // Wait until the SPECIFIC expected agents finished a turn, accumulating
    // every `agent:stream:end` agentId in one set so nothing is lost across
    // the two phases (unrelated agents' events cannot exit the wait early).
    // Phase 1: the ids known upfront — sender and task assignee.
    let mut done: Vec<String> = Vec::new();
    for _ in 0..200 {
        if done.contains(&sender_id) && done.contains(&target_id) {
            break;
        }
        let Some(frame) = wss_event_opt(&mut sub, 30).await else {
            break;
        };
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:stream:end" {
            let id = ev["data"]["agentId"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            if !id.is_empty() && !done.contains(&id) {
                done.push(id);
            }
        }
    }
    assert!(done.contains(&sender_id), "sender turn completed: {done:?}");
    assert!(
        done.contains(&target_id),
        "task assignee turn completed: {done:?}"
    );

    // The sender's turn is over, so both `ws.agent.create` calls have
    // returned — resolve the created children by name.
    let list = wss_rpc(&mut rpc, 15, "agent.list", json!({ "workspaceId": &ws_id })).await;
    let agents = list["agents"].as_array().expect("agents array");
    let by_name = |name: &str| -> String {
        agents
            .iter()
            .find(|a| a["name"] == name)
            .and_then(|a| a["id"].as_str())
            .unwrap_or_else(|| panic!("agent {name} listed: {list}"))
            .to_string()
    };
    let auto_child = by_name("ChildAuto");
    let explicit_child = by_name("ChildExplicit");

    // Phase 2: keep draining until BOTH created children finished their
    // kickoff turns (their earlier stream:ends are already in `done`).
    for _ in 0..200 {
        if done.contains(&auto_child) && done.contains(&explicit_child) {
            break;
        }
        let Some(frame) = wss_event_opt(&mut sub, 30).await else {
            break;
        };
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:stream:end" {
            let id = ev["data"]["agentId"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            if !id.is_empty() && !done.contains(&id) {
                done.push(id);
            }
        }
    }
    assert!(
        done.contains(&auto_child),
        "auto-tag child turn completed: {done:?}"
    );
    assert!(
        done.contains(&explicit_child),
        "explicit-metadata child turn completed: {done:?}"
    );

    let expected_tag = json!({
        "type": "agent_message",
        "fromAgentId": sender_id,
        "fromAgentName": "SenderA",
    });
    let user_row = |conv: &Value, text: &str| -> Value {
        conv["messages"]
            .as_array()
            .expect("messages array")
            .iter()
            .find(|m| {
                m["role"] == "user"
                    && m["contentBlocks"][0]["text"]
                        .as_str()
                        .is_some_and(|t| t.starts_with(text))
            })
            .unwrap_or_else(|| panic!("user row `{text}` present: {conv}"))
            .clone()
    };

    // 1. sendToTask: the assignee's delivered row carries the auto-tag.
    let conv = wss_rpc(
        &mut rpc,
        16,
        "agent.getConversation",
        json!({ "workspaceId": &ws_id, "agentId": &target_id }),
    )
    .await;
    let row = user_row(&conv, "task hello");
    assert_eq!(
        row["metadata"], expected_tag,
        "sendToTask must carry sender attribution: {row}"
    );

    // 2. create kickoff (no explicit metadata): auto-tagged.
    let conv = wss_rpc(
        &mut rpc,
        17,
        "agent.getConversation",
        json!({ "workspaceId": &ws_id, "agentId": &auto_child }),
    )
    .await;
    let row = user_row(&conv, "kickoff hello");
    assert_eq!(
        row["metadata"], expected_tag,
        "create kickoff must carry sender attribution: {row}"
    );

    // 3. create kickoff with explicit messageMetadata: the caller's own
    // fields persist, with the attribution fields daemon-stamped (the
    // guard/ownership key is never caller-controlled).
    let conv = wss_rpc(
        &mut rpc,
        18,
        "agent.getConversation",
        json!({ "workspaceId": &ws_id, "agentId": &explicit_child }),
    )
    .await;
    let row = user_row(&conv, "kickoff explicit");
    assert_eq!(
        row["metadata"],
        json!({
            "type": "custom_tag",
            "note": "explicit wins",
            "fromAgentId": sender_id,
            "fromAgentName": "SenderA",
        }),
        "explicit messageMetadata must keep its fields with daemon-stamped attribution: {row}"
    );
}

/// SUB-1 child→parent watch suppression + §7.1 delta metadata over the real
/// WSS transport (regression for intentd#773).
///
/// A parent spawns a child through `ws.agent.create` (persisting the
/// `parent_agent_id` linkage), the child sends a coordination message back to
/// its parent through `ws.agent.send`, and:
/// - the child's persisted send tool result carries NO `subscriptionId` (the
///   SUB-1 sender auto-watch is suppressed for child→parent sends), while a
///   parentless bystander's identical send DOES get one — and only the
///   bystander is later woken by the parent's completion;
/// - the parent's `chat.subscribe` delta for the delivered row lifts the
///   persisted `agent_message` sender-attribution `metadata` onto the wire
///   entity (§7.1), while a human `agent.sendMessage` row carries no
///   attribution metadata (lean metadata-free shape, or at most the drain-time
///   `queueInfo` stamp if the send raced the parent's busy window).
#[tokio::test]
async fn child_to_parent_send_suppresses_watch_and_delta_carries_metadata_over_wss() {
    let Some(script) = gate("WSS child→parent watch suppression + delta metadata E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    // Rule 1 (parent kickoff): spawn the child through the real MCP
    // `workspace_api` binding so the session persists `parent_agent_id`.
    // Rule 2 (child kickoff + bystander kickoff): find the parent by name and
    // send to it — same code path for both callers; only the caller's parent
    // linkage differs. `emitToolBlocks` persists the tool results so the
    // suppression (no `subscriptionId`) is asserted from the transcript.
    let spawn_code = "return await ws.agent.create('ChildC', 'please MESSAGE_PARENT now', \
                      { model: 'mock:default' });";
    let send_code = "const agents = await ws.agent.list(true); \
                     const target = agents.find(a => a.name === 'Coordinator'); \
                     return await ws.agent.send(target.id, 'child says hi');";
    let behavior = json!({
        "rules": [
            {
                "ifPromptContains": "SPAWN_CHILD",
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": spawn_code, "summary": "spawn child e2e" }
                },
                "response": "child spawned",
                "emitToolBlocks": true
            },
            {
                "ifPromptContains": "MESSAGE_PARENT",
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": send_code, "summary": "send to parent e2e" }
                },
                "response": "sent upward",
                "emitToolBlocks": true
            }
        ],
        "response": "plain reply"
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:stream:end"], "workspaceId": &ws_id }),
    )
    .await;
    assert!(sub_resp["subscriptionId"].is_string());

    let mut rpc = connect_ws(port, cfg.clone()).await;
    // Pin `workspaceApi.toonOutput` off so the persisted `ws.agent.send` tool
    // result stays plain JSON for the `serde_json::from_str` extraction below
    // (TOON encoding is on by default).
    let updated = wss_rpc(
        &mut rpc,
        9,
        "settings.update",
        json!({ "changes": [ { "path": "workspaceApi.toonOutput", "value": false } ] }),
    )
    .await;
    assert!(
        updated["applied"].is_array(),
        "toonOutput pinned: {updated}"
    );
    let parent = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": &ws_id, "name": "Coordinator", "model": "mock:default" }),
    )
    .await;
    let parent_id = parent["agent"]["id"].as_str().unwrap().to_string();

    // CHAT conn on the PARENT — subscribed BEFORE any turn so the delivered
    // child→parent row's delta is observed live.
    let mut chat = connect_ws(port, cfg.clone()).await;
    let chat_resp = wss_rpc(
        &mut chat,
        20,
        "chat.subscribe",
        json!({ "agentId": parent_id }),
    )
    .await;
    assert!(
        chat_resp["subscriptionId"].is_string(),
        "chat subscribed: {chat_resp}"
    );
    let snap = wss_push(&mut chat, 15).await;
    assert_eq!(snap["params"]["kind"], "snapshot", "push: {snap}");

    // Kick off: the parent's turn spawns the child (MCP create → the session
    // persists `parent_agent_id`), whose kickoff turn sends back upward.
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": &ws_id, "agentId": &parent_id, "content": "please SPAWN_CHILD" }),
    )
    .await;
    assert_eq!(sent["success"], true, "kickoff sendMessage ok: {sent}");

    // Wait for the parent's spawn turn, resolve the child by name, then wait
    // for the child's kickoff turn (which performed the upward send).
    let mut done: Vec<String> = Vec::new();
    for _ in 0..200 {
        if done.contains(&parent_id) {
            break;
        }
        let Some(frame) = wss_event_opt(&mut sub, 30).await else {
            break;
        };
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:stream:end" {
            let id = ev["data"]["agentId"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            if !id.is_empty() && !done.contains(&id) {
                done.push(id);
            }
        }
    }
    assert!(done.contains(&parent_id), "parent turn completed: {done:?}");
    let list = wss_rpc(&mut rpc, 12, "agent.list", json!({ "workspaceId": &ws_id })).await;
    let child_id = list["agents"]
        .as_array()
        .expect("agents array")
        .iter()
        .find(|a| a["name"] == "ChildC")
        .and_then(|a| a["id"].as_str())
        .unwrap_or_else(|| panic!("child listed: {list}"))
        .to_string();
    for _ in 0..200 {
        if done.contains(&child_id) {
            break;
        }
        let Some(frame) = wss_event_opt(&mut sub, 30).await else {
            break;
        };
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:stream:end" {
            let id = ev["data"]["agentId"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            if !id.is_empty() && !done.contains(&id) {
                done.push(id);
            }
        }
    }
    assert!(done.contains(&child_id), "child turn completed: {done:?}");

    // Extract a caller's persisted `ws.agent.send` tool result (the JSON the
    // MCP binding returned) from its transcript.
    let send_tool_result = |conv: &Value, target: &str| -> Value {
        conv["messages"]
            .as_array()
            .expect("messages array")
            .iter()
            .filter_map(|m| m["contentBlocks"].as_array())
            .flatten()
            .filter(|b| b["type"] == "tool_result")
            .filter_map(|b| b["output"].as_array().and_then(|arr| arr.first()))
            .filter_map(|item| item["text"].as_str())
            .filter_map(|text| serde_json::from_str::<Value>(text).ok())
            .find(|v| v["agentId"] == json!(target))
            .unwrap_or_else(|| panic!("send tool result persisted: {conv}"))
    };

    // THE SUB-1 assertion: the child's send to its own parent registered NO
    // sender auto-watch — the tool result has no `subscriptionId`.
    let conv = wss_rpc(
        &mut rpc,
        13,
        "agent.getConversation",
        json!({ "workspaceId": &ws_id, "agentId": &child_id }),
    )
    .await;
    let result = send_tool_result(&conv, &parent_id);
    assert_eq!(result["ok"], json!(true), "child send ok: {result}");
    assert!(
        result.get("subscriptionId").is_none(),
        "child→parent send must NOT register a sender watch: {result}"
    );

    // THE §7.1 assertion: the delivered row's delta entity on the parent's
    // chat channel carries the persisted `agent_message` metadata.
    let tagged = timeout(Duration::from_secs(60), async {
        loop {
            let frame = wss_push(&mut chat, 60).await;
            if frame["params"]["kind"] != "delta" {
                continue;
            }
            let delta = frame["params"]["delta"].clone();
            if let Some(entity) = delta["added"]
                .as_array()
                .into_iter()
                .flatten()
                .chain(delta["updated"].as_array().into_iter().flatten())
                .find(|e| {
                    e["block"]["text"]
                        .as_str()
                        .is_some_and(|t| t.contains("child says hi"))
                })
            {
                return entity.clone();
            }
        }
    })
    .await
    .expect("child→parent delivered row reached the chat channel");
    assert_eq!(
        tagged["role"], "user",
        "delivered row is a user row: {tagged}"
    );
    assert_eq!(
        tagged["metadata"]["type"],
        json!("agent_message"),
        "delta entity lifts the persisted sender-attribution metadata: {tagged}"
    );
    assert_eq!(
        tagged["metadata"]["fromAgentId"],
        json!(child_id),
        "attribution names the child sender: {tagged}"
    );
    assert_eq!(
        tagged["metadata"]["fromAgentName"],
        json!("ChildC"),
        "attribution carries the sender name: {tagged}"
    );

    // Lean-shape control: a human send's delta entity carries NO
    // sender-attribution metadata. Wait for the parent's child-message turn
    // to finish on the chat channel first so the send is (usually) delivered
    // directly — but if it still races the busy window and queues, the row
    // legitimately gains ONLY the drain-time `queueInfo` stamp (PROTOCOL
    // §5.5), never `agent_message` attribution; the assertion below allows
    // exactly that.
    timeout(Duration::from_secs(60), async {
        loop {
            let frame = wss_push(&mut chat, 60).await;
            if frame["params"]["kind"] != "delta" {
                continue;
            }
            let delta = frame["params"]["delta"].clone();
            let done = delta["added"]
                .as_array()
                .into_iter()
                .flatten()
                .chain(delta["updated"].as_array().into_iter().flatten())
                .any(|e| {
                    e["role"] == "assistant"
                        && e["streamingComplete"] == json!(true)
                        && e["block"]["text"]
                            .as_str()
                            .is_some_and(|t| t.contains("plain reply"))
                });
            if done {
                return;
            }
        }
    })
    .await
    .expect("parent's child-message turn replied on the chat channel");
    let human = wss_rpc(
        &mut rpc,
        14,
        "agent.sendMessage",
        json!({ "workspaceId": &ws_id, "agentId": &parent_id, "content": "human hello" }),
    )
    .await;
    assert_eq!(human["success"], true, "human sendMessage ok: {human}");
    let lean = timeout(Duration::from_secs(60), async {
        loop {
            let frame = wss_push(&mut chat, 60).await;
            if frame["params"]["kind"] != "delta" {
                continue;
            }
            let delta = frame["params"]["delta"].clone();
            if let Some(entity) = delta["added"]
                .as_array()
                .into_iter()
                .flatten()
                .chain(delta["updated"].as_array().into_iter().flatten())
                .find(|e| {
                    e["block"]["text"]
                        .as_str()
                        .is_some_and(|t| t.contains("human hello"))
                })
            {
                return entity.clone();
            }
        }
    })
    .await
    .expect("human row reached the chat channel");
    match lean.get("metadata") {
        None => {} // direct delivery: the lean metadata-free entity shape
        Some(md) => {
            // Queued delivery race: only the drain-time queueInfo stamp is
            // allowed — human sends never gain A2A attribution metadata.
            let keys: Vec<&String> = md
                .as_object()
                .unwrap_or_else(|| panic!("metadata is an object: {lean}"))
                .keys()
                .collect();
            assert_eq!(
                keys,
                vec!["queueInfo"],
                "a human row carries at most the queueInfo stamp: {lean}"
            );
        }
    }

    // Contrast: a parentless BYSTANDER running the identical send DOES get
    // the SUB-1 sender watch — and is later woken by the parent's completion.
    let bystander = wss_rpc(
        &mut rpc,
        15,
        "agent.create",
        json!({ "workspaceId": &ws_id, "name": "Bystander", "model": "mock:default" }),
    )
    .await;
    let bystander_id = bystander["agent"]["id"].as_str().unwrap().to_string();
    let sent = wss_rpc(
        &mut rpc,
        16,
        "agent.sendMessage",
        json!({ "workspaceId": &ws_id, "agentId": &bystander_id, "content": "please MESSAGE_PARENT now" }),
    )
    .await;
    assert_eq!(sent["success"], true, "bystander kickoff ok: {sent}");
    for _ in 0..200 {
        if done.contains(&bystander_id) {
            break;
        }
        let Some(frame) = wss_event_opt(&mut sub, 30).await else {
            break;
        };
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:stream:end" {
            let id = ev["data"]["agentId"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            if !id.is_empty() && !done.contains(&id) {
                done.push(id);
            }
        }
    }
    assert!(
        done.contains(&bystander_id),
        "bystander turn completed: {done:?}"
    );
    let conv = wss_rpc(
        &mut rpc,
        17,
        "agent.getConversation",
        json!({ "workspaceId": &ws_id, "agentId": &bystander_id }),
    )
    .await;
    let result = send_tool_result(&conv, &parent_id);
    assert!(
        result["subscriptionId"].is_string(),
        "a non-child sender still gets the SUB-1 watch: {result}"
    );

    // The parent's post-send idle fires the bystander's watch — the
    // wake lands in the bystander transcript. The CHILD, whose watch was
    // suppressed, has no wake despite the parent idling multiple times since
    // its earlier send.
    let mut woken = false;
    for attempt in 0..120i64 {
        let conv = wss_rpc(
            &mut rpc,
            100 + attempt,
            "agent.getConversation",
            json!({ "workspaceId": &ws_id, "agentId": &bystander_id }),
        )
        .await;
        let text = serde_json::to_string(&conv["messages"]).unwrap_or_default();
        if text.contains("[WORKSPACE EVENTS]") {
            woken = true;
            break;
        }
        sleep(Duration::from_millis(250)).await;
    }
    assert!(woken, "bystander received the parent-completion wake");
    let conv = wss_rpc(
        &mut rpc,
        300,
        "agent.getConversation",
        json!({ "workspaceId": &ws_id, "agentId": &child_id }),
    )
    .await;
    let text = serde_json::to_string(&conv["messages"]).unwrap_or_default();
    assert!(
        !text.contains("[WORKSPACE EVENTS]"),
        "the child must NOT be woken by its own parent's completion: {text}"
    );
}

// ---------------------------------------------------------------------------
// agent.editAndRegenerate (PROTOCOL §5.5 extension)
// ---------------------------------------------------------------------------

/// `agent.editAndRegenerate` happy path over WSS: after two full turns, edit
/// the SECOND user message. The transcript truncates to just before it
/// (`agent:updated { truncatedCount: 2 }`), the ACP session is recreated, and
/// the regenerated turn's outbound prompt replays the kept prefix as
/// `<supervisor>` XML with the edited text — WITHOUT the truncated content
/// (asserted via the mock fixture's `MOCK_AGENT_PROMPT_LOG` seam).
#[tokio::test]
async fn edit_and_regenerate_truncates_and_replays_history_over_wss() {
    let Some(script) = gate("WSS editAndRegenerate happy-path E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let prompt_log = data_dir.join("prompts.jsonl");
    let prompt_log_str = prompt_log.to_string_lossy().into_owned();
    let behavior = json!({ "response": "the answer" }).to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_PROMPT_LOG", &prompt_log_str),
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
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Edit", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // Two full turns so there is a kept prefix AND a truncated tail.
    for (rpc_id, content) in [(11, "first question"), (12, "second question")] {
        let sent = wss_rpc(
            &mut rpc,
            rpc_id,
            "agent.sendMessage",
            json!({ "workspaceId": ws_id, "agentId": agent_id, "content": content }),
        )
        .await;
        assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
        let mut saw_end = false;
        for _ in 0..80 {
            let frame = wss_event(&mut sub, 30).await;
            if frame["params"]["event"]["type"] == "agent:stream:end" {
                saw_end = true;
                break;
            }
        }
        assert!(saw_end, "turn '{content}' completed");
    }

    let conv = wss_rpc(
        &mut rpc,
        13,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    assert_eq!(messages.len(), 4, "two full exchanges persisted: {conv}");
    assert_eq!(messages[2]["role"], "user");
    let edit_target = messages[2]["id"].as_str().expect("target id").to_string();

    let edited = wss_rpc(
        &mut rpc,
        14,
        "agent.editAndRegenerate",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "messageId": edit_target,
            "content": "edited question",
        }),
    )
    .await;
    assert_eq!(edited["success"], true, "editAndRegenerate ok: {edited}");
    assert_eq!(
        edited["truncatedCount"],
        json!(2),
        "edited user message + trailing assistant truncated: {edited}"
    );
    let regenerated_message_id = edited["messageId"]
        .as_str()
        .expect("regenerated messageId")
        .to_string();

    // The truncation emits `agent:updated { truncatedCount }`; the regenerated
    // user message emits `agent:message` (role=user, PROTOCOL §5.5 step 6 —
    // the FE folds the edited message back in on this event, no reload); the
    // regenerated turn then streams and ends.
    let mut saw_truncation_update = false;
    let mut saw_regenerated_user_event = false;
    let mut saw_end = false;
    for _ in 0..80 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        match event["type"].as_str() {
            Some("agent:updated") if event["data"]["truncatedCount"] == json!(2) => {
                saw_truncation_update = true;
            }
            Some("agent:message")
                if event["data"]["role"] == "user"
                    && event["data"]["messageId"].as_str()
                        == Some(regenerated_message_id.as_str()) =>
            {
                saw_regenerated_user_event = true;
            }
            Some("agent:stream:end") => {
                saw_end = true;
                break;
            }
            _ => {}
        }
    }
    assert!(
        saw_truncation_update,
        "agent:updated with truncatedCount emitted for the truncation"
    );
    assert!(
        saw_regenerated_user_event,
        "agent:message (role=user) emitted for the regenerated user message with the \
         result's messageId — the FE convergence contract for the edit flow"
    );
    assert!(saw_end, "regenerated turn completed");

    // Transcript: kept prefix + edited user + fresh assistant.
    let conv = wss_rpc(
        &mut rpc,
        15,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    assert_eq!(messages.len(), 4, "prefix + edited + regenerated: {conv}");
    assert_eq!(messages[0]["contentBlocks"][0]["text"], "first question");
    assert_eq!(messages[2]["role"], "user");
    assert_eq!(
        messages[2]["contentBlocks"][0]["text"], "edited question",
        "edited content persisted as the new user message"
    );
    assert_eq!(
        messages[2]["id"].as_str(),
        Some(regenerated_message_id.as_str()),
        "result messageId names the persisted regenerated user row (PROTOCOL §5.5)"
    );
    assert_eq!(messages[3]["role"], "assistant");

    // Outbound-prompt contract (fresh session + history replay): the
    // regenerated turn's prompt carries the kept prefix as `<supervisor>` XML
    // plus the edited text, and NOT the truncated second exchange.
    let log = std::fs::read_to_string(&prompt_log).expect("prompt log");
    let last_prompt: Value =
        serde_json::from_str(log.lines().last().expect("prompt lines")).expect("prompt log line");
    let text = last_prompt["text"].as_str().unwrap_or_default();
    assert!(
        text.contains("<supervisor>"),
        "regenerated prompt replays history as <supervisor> XML: {text}"
    );
    assert!(
        text.contains("first question"),
        "kept prefix replayed: {text}"
    );
    assert!(
        text.contains("edited question"),
        "edited content sent: {text}"
    );
    assert!(
        !text.contains("second question"),
        "truncated content must NOT reach the provider: {text}"
    );
}

/// `agent.editAndRegenerate` on a BUSY agent stops the in-flight turn first
/// (hard-stop semantics), then truncates and regenerates — no wedged
/// state. The first turn parks mid-flight (`parkIfPromptContains`); the edit
/// lands while it is in flight.
#[tokio::test]
async fn edit_and_regenerate_stops_in_flight_turn_over_wss() {
    let Some(script) = gate("WSS editAndRegenerate busy-agent E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let behavior = json!({
        "parkIfPromptContains": "PARK_ME",
        "response": "regenerated answer",
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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
    wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*", "chat:stream:delta"], "workspaceId": ws_id }),
    )
    .await;

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Busy", "model": "mock:default" }),
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
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "please PARK_ME now" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Wait until the turn is provably in flight.
    let mut busy = false;
    for i in 0..100 {
        let got = wss_rpc(
            &mut rpc,
            20 + i,
            "agent.get",
            json!({ "workspaceId": ws_id, "agentId": agent_id }),
        )
        .await;
        if got["agent"]["turnInFlight"] == true {
            busy = true;
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(busy, "first turn is in flight (parked by the mock)");

    let conv = wss_rpc(
        &mut rpc,
        200,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let target = conv["messages"]
        .as_array()
        .expect("messages")
        .iter()
        .find(|m| m["role"] == "user")
        .expect("parked user row")["id"]
        .as_str()
        .expect("id")
        .to_string();

    // Edit while mid-turn: the daemon must stop the parked turn first.
    let edited = wss_rpc(
        &mut rpc,
        201,
        "agent.editAndRegenerate",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "messageId": target,
            "content": "edited while busy",
        }),
    )
    .await;
    assert_eq!(edited["success"], true, "editAndRegenerate ok: {edited}");
    assert_eq!(edited["queued"], false, "delivered immediately: {edited}");

    // The regenerated turn streams the mock's response and terminates.
    let mut saw_regen_chunk = false;
    let mut saw_end = false;
    for _ in 0..80 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        match event["type"].as_str() {
            Some("chat:stream:delta") => {
                if event["data"]["content"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("regenerated answer")
                {
                    saw_regen_chunk = true;
                }
            }
            Some("agent:stream:end") if saw_regen_chunk => {
                saw_end = true;
                break;
            }
            _ => {}
        }
    }
    assert!(saw_regen_chunk, "regenerated turn streamed");
    assert!(saw_end, "regenerated turn completed");

    // The parked original message was truncated away: edited user + assistant.
    let conv = wss_rpc(
        &mut rpc,
        202,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    assert_eq!(messages.len(), 2, "old parked message truncated: {conv}");
    assert_eq!(messages[0]["role"], "user");
    assert_eq!(messages[0]["contentBlocks"][0]["text"], "edited while busy");
    assert_eq!(messages[1]["role"], "assistant");

    // And the agent is not wedged: liveness fields reset.
    let mut reset = false;
    for i in 0..40 {
        let got = wss_rpc(
            &mut rpc,
            300 + i,
            "agent.get",
            json!({ "workspaceId": ws_id, "agentId": agent_id }),
        )
        .await;
        if got["agent"]["turnInFlight"] == false {
            reset = true;
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(reset, "agent returned to idle after the regenerated turn");
}

/// Invalid and non-user `messageId` → `-32602` with NO transcript mutation
/// (PROTOCOL §5.5: validation precedes any state change).
#[tokio::test]
async fn edit_and_regenerate_rejects_bad_message_ids_over_wss() {
    let Some(script) = gate("WSS editAndRegenerate bad-id E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let behavior = json!({ "response": "fine" }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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
    wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Guard", "model": "mock:default" }),
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
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "hello" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    let mut saw_end = false;
    for _ in 0..80 {
        let frame = wss_event(&mut sub, 30).await;
        if frame["params"]["event"]["type"] == "agent:stream:end" {
            saw_end = true;
            break;
        }
    }
    assert!(saw_end, "turn completed");

    let conv = wss_rpc(
        &mut rpc,
        12,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let before = messages.len();
    assert!(before >= 2, "user + assistant persisted: {conv}");
    let assistant_id = messages
        .iter()
        .find(|m| m["role"] == "assistant")
        .expect("assistant row")["id"]
        .as_str()
        .expect("id")
        .to_string();

    // Unknown messageId → -32602.
    let resp = wss_rpc_envelope(
        &mut rpc,
        13,
        "agent.editAndRegenerate",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "messageId": "msg-does-not-exist",
            "content": "edited",
        }),
    )
    .await;
    assert_eq!(
        resp["error"]["code"],
        json!(-32602),
        "unknown messageId is -32602: {resp}"
    );

    // Non-user messageId → -32602.
    let resp = wss_rpc_envelope(
        &mut rpc,
        14,
        "agent.editAndRegenerate",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "messageId": assistant_id,
            "content": "edited",
        }),
    )
    .await;
    assert_eq!(
        resp["error"]["code"],
        json!(-32602),
        "non-user messageId is -32602: {resp}"
    );

    // No transcript mutation from either rejection.
    let conv = wss_rpc(
        &mut rpc,
        15,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        conv["messages"].as_array().expect("messages").len(),
        before,
        "transcript untouched by rejected edits: {conv}"
    );
}

/// Regression: a user interrupt (`agent.stop`) mid-stream persists the partial
/// assistant turn, so the chat channel's terminal reconcile KEEPS the streamed
/// blocks instead of removing them. Drives the real wire path: `chat.subscribe`
/// over WSS → mock streams a chunk then parks (`blockUntilCancel`) →
/// `agent.stop` → the terminal `subscription.push` delta carries the streamed
/// block with `streamingComplete: true` and an EMPTY `removedIds`, and a fresh
/// `agent.getConversation` holds the interrupted partial assistant row tagged
/// `metadata.interrupted = true` + `stopReason = "interrupted"`.
///
/// Before the fix, the abort dropped the live-turn slot unflushed: the
/// persisted transcript had no assistant row, so the reconcile emitted the
/// streamed block id in `removedIds` and the FE erased the partial output.
#[tokio::test]
async fn interrupt_mid_stream_keeps_partial_blocks_over_wss() {
    let Some(script) = gate("WSS interrupt partial-flush E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    // The mock's first turn streams one chunk ("streaming-before-cancel") then
    // parks until session/cancel — a deterministic mid-stream state.
    let behavior = json!({ "blockUntilCancel": true }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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

    // SUBSCRIBER conn — agent:* events, to observe the mid-stream chunk.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_resp["subscriptionId"].is_string());

    // RPC conn — create the agent.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Interruptee", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // CHAT conn — subscribe BEFORE the turn so every stream delta is observed.
    let mut chat = connect_ws(port, cfg.clone()).await;
    let chat_resp = wss_rpc(
        &mut chat,
        20,
        "chat.subscribe",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert!(
        chat_resp["subscriptionId"].is_string(),
        "chat subscribed: {chat_resp}"
    );
    let snap = wss_push(&mut chat, 15).await;
    assert_eq!(snap["params"]["kind"], "snapshot", "push: {snap}");

    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "start" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // The agent is observably mid-stream once the chunk event lands. The whole
    // wait is bounded by a single deadline (per-frame reads inside `wss_event`
    // would otherwise reset on heartbeat Pings and hang on a missing chunk).
    timeout(Duration::from_secs(30), async {
        loop {
            let frame = wss_event(&mut sub, 30).await;
            if frame["params"]["event"]["type"] == "agent:stream:activity" {
                return;
            }
        }
    })
    .await
    .expect("mock streamed its pre-park chunk");

    // The chat channel saw the streamed block too — capture its id so the
    // terminal assertions below key off the exact block that streamed live.
    // Single total deadline, same rationale as above.
    let streamed_block_id = timeout(Duration::from_secs(30), async {
        loop {
            let frame = wss_push(&mut chat, 30).await;
            assert_eq!(frame["params"]["kind"], "delta", "push: {frame}");
            let delta = &frame["params"]["delta"];
            if let Some(entity) = delta["added"]
                .as_array()
                .into_iter()
                .flatten()
                .chain(delta["updated"].as_array().into_iter().flatten())
                .find(|e| {
                    e["block"]["text"]
                        .as_str()
                        .is_some_and(|t| t.contains("streaming-before-cancel"))
                })
            {
                return entity["block"]["id"].as_str().map(String::from);
            }
        }
    })
    .await
    .expect("streamed text block reached chat channel in time")
    .expect("streamed text block carries an id");

    // User interrupt mid-stream.
    let stopped = wss_rpc(&mut rpc, 12, "agent.stop", json!({ "agentId": agent_id })).await;
    assert_eq!(stopped["success"], true, "stop ok: {stopped}");

    // Terminal reconcile: the streamed block survives (added or updated with
    // `streamingComplete: true`) and NOTHING is removed. Single total deadline.
    let terminal = timeout(Duration::from_secs(30), async {
        loop {
            let frame = wss_push(&mut chat, 30).await;
            assert_eq!(frame["params"]["kind"], "delta", "push: {frame}");
            let delta = frame["params"]["delta"].clone();
            let is_terminal = ["added", "updated"].iter().any(|key| {
                delta[*key]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .any(|e| e.get("streamingComplete") == Some(&Value::Bool(true)))
            });
            if is_terminal {
                return delta;
            }
        }
    })
    .await
    .expect("terminal (streamingComplete) delta arrived");
    assert_eq!(
        terminal["removedIds"],
        json!([]),
        "the interrupted partial's blocks are NOT removed: {terminal}"
    );
    assert!(
        ["added", "updated"].iter().any(|key| {
            terminal[*key]
                .as_array()
                .into_iter()
                .flatten()
                .any(|e| e["block"]["id"].as_str() == Some(streamed_block_id.as_str()))
        }),
        "the streamed block {streamed_block_id} is reconciled as kept: {terminal}"
    );

    // The transcript holds the interrupted partial assistant row.
    let conv = wss_rpc(
        &mut rpc,
        13,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let assistant = messages
        .iter()
        .find(|m| m["role"] == "assistant")
        .expect("interrupted partial assistant row persisted");
    assert_eq!(
        assistant["metadata"]["interrupted"],
        json!(true),
        "assistant row: {assistant}"
    );
    assert_eq!(
        assistant["metadata"]["stopReason"],
        json!("interrupted"),
        "assistant row: {assistant}"
    );
    assert!(
        serde_json::to_string(&assistant["contentBlocks"])
            .unwrap()
            .contains("streaming-before-cancel"),
        "the streamed-so-far text persisted: {assistant}"
    );
}

/// §7.1: a tool completing with a proposal-MIME resource item in its output
/// surfaces a STANDALONE `resource` block over the live `chat.subscribe`
/// channel — in addition to the `tool_result` that still carries the item in
/// its `output` array — and the terminal reconcile KEEPS that block (its id
/// matches the persisted transcript, so `removedIds` stays empty for it).
#[tokio::test]
async fn proposal_resource_standalone_block_over_chat_subscribe() {
    let Some(script) = gate("WSS proposal-resource chat.subscribe E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    // The mock echoes a canned tool_call → tool_call_update pair whose
    // rawOutput carries the proposal-MIME resource item (no MCP round-trip).
    let behavior = json!({
        "response": "proposal shown",
        "rawUpdates": [
            { "sessionUpdate": "tool_call", "toolCallId": "tc_prop",
              "title": "workspace_api", "kind": "other", "status": "in_progress",
              "rawInput": { "code": "ws.app.proposal.show(p)" } },
            { "sessionUpdate": "tool_call_update", "toolCallId": "tc_prop",
              "status": "completed",
              "rawOutput": [
                  { "type": "text", "text": "Proposal shown" },
                  { "type": "resource", "resource": {
                      "uri": "intent-proposal://settings-change/Update",
                      "name": "Update",
                      "mimeType": "application/vnd.intent.proposal+json",
                      "text": "{\"kind\":\"settings-change\"}" } }
              ] }
        ]
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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

    // RPC conn — create the agent.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Proposer", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // CHAT conn — subscribe BEFORE the turn so every stream delta is observed.
    let mut chat = connect_ws(port, cfg.clone()).await;
    let chat_resp = wss_rpc(
        &mut chat,
        20,
        "chat.subscribe",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert!(
        chat_resp["subscriptionId"].is_string(),
        "chat subscribed: {chat_resp}"
    );
    let snap = wss_push(&mut chat, 15).await;
    assert_eq!(snap["params"]["kind"], "snapshot", "push: {snap}");

    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "show proposal" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // The live channel delivers the standalone proposal block. Single total
    // deadline (per-frame reads would reset on heartbeat Pings).
    let proposal_block_id = timeout(Duration::from_secs(30), async {
        loop {
            let frame = wss_push(&mut chat, 30).await;
            assert_eq!(frame["params"]["kind"], "delta", "push: {frame}");
            let delta = &frame["params"]["delta"];
            if let Some(entity) = delta["added"]
                .as_array()
                .into_iter()
                .flatten()
                .chain(delta["updated"].as_array().into_iter().flatten())
                .find(|e| {
                    e["block"]["type"] == "resource"
                        && e["block"]["resource"]["mimeType"]
                            == "application/vnd.intent.proposal+json"
                })
            {
                assert_eq!(
                    entity["block"]["resource"]["text"],
                    json!("{\"kind\":\"settings-change\"}"),
                    "resource echoed verbatim: {entity}"
                );
                return entity["block"]["id"].as_str().map(String::from);
            }
        }
    })
    .await
    .expect("standalone proposal block reached chat channel in time")
    .expect("standalone proposal block carries an id");

    // Terminal reconcile: the proposal block's id matches the persisted
    // transcript, so it is NOT in `removedIds` (clean reconcile).
    let terminal = timeout(Duration::from_secs(30), async {
        loop {
            let frame = wss_push(&mut chat, 30).await;
            assert_eq!(frame["params"]["kind"], "delta", "push: {frame}");
            let delta = frame["params"]["delta"].clone();
            let is_terminal = ["added", "updated"].iter().any(|key| {
                delta[*key]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .any(|e| e.get("streamingComplete") == Some(&Value::Bool(true)))
            });
            if is_terminal {
                return delta;
            }
        }
    })
    .await
    .expect("terminal (streamingComplete) delta arrived");
    assert!(
        !terminal["removedIds"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|id| id.as_str() == Some(proposal_block_id.as_str())),
        "the proposal block {proposal_block_id} survives the reconcile: {terminal}"
    );

    // The persisted transcript holds the same standalone block under the SAME
    // id — the byte-for-byte agreement the live channel depends on.
    let conv = wss_rpc(
        &mut rpc,
        12,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let persisted_proposal = messages
        .iter()
        .filter_map(|m| m["contentBlocks"].as_array())
        .flatten()
        .find(|b| {
            b["type"] == "resource"
                && b["resource"]["mimeType"] == "application/vnd.intent.proposal+json"
        })
        .expect("standalone proposal block persisted");
    assert_eq!(
        persisted_proposal["id"].as_str(),
        Some(proposal_block_id.as_str()),
        "live and persisted block ids agree: {persisted_proposal}"
    );
    // The tool_result still carries the resource item in its output array.
    let tool_result = messages
        .iter()
        .filter_map(|m| m["contentBlocks"].as_array())
        .flatten()
        .find(|b| b["type"] == "tool_result")
        .expect("tool_result persisted");
    assert!(
        tool_result["output"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|item| item["resource"]["mimeType"] == "application/vnd.intent.proposal+json"),
        "tool_result.output keeps the resource item: {tool_result}"
    );
}

/// §7.1 fallback (intent-hq/monorepo#511 regression class): a provider that
/// collapses the MCP content items into `{ output: "<stringified {ok,
/// proposal}>" }` — dropping the resource item entirely, as auggie does —
/// still surfaces the standalone proposal `resource` block over the live
/// `chat.subscribe` channel, and the terminal reconcile keeps it (its id
/// matches the persisted transcript).
#[tokio::test]
async fn proposal_lifted_from_collapsed_output_over_chat_subscribe() {
    let Some(script) = gate("WSS collapsed-proposal chat.subscribe E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    // The canned tool_call_update carries the auggie-collapsed rawOutput: the
    // daemon's own {ok, proposal} text-item payload stringified under
    // `output`, with the resource item dropped.
    let proposal = json!({
        "kind": "settings-change",
        "preview": { "title": "Update" },
        "payload": { "key": "test.setting" },
    });
    let collapsed_text = serde_json::to_string_pretty(&json!({ "ok": true, "proposal": proposal }))
        .expect("serialize collapsed payload");
    let behavior = json!({
        "response": "proposal shown",
        "rawUpdates": [
            { "sessionUpdate": "tool_call", "toolCallId": "tc_prop",
              "title": "workspace_api", "kind": "other", "status": "in_progress",
              "rawInput": { "code": "ws.app.proposal.show(p)" } },
            { "sessionUpdate": "tool_call_update", "toolCallId": "tc_prop",
              "status": "completed",
              "rawOutput": { "output": collapsed_text } }
        ]
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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

    // RPC conn — create the agent.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Collapser", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // CHAT conn — subscribe BEFORE the turn so every stream delta is observed.
    let mut chat = connect_ws(port, cfg.clone()).await;
    let chat_resp = wss_rpc(
        &mut chat,
        20,
        "chat.subscribe",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert!(
        chat_resp["subscriptionId"].is_string(),
        "chat subscribed: {chat_resp}"
    );
    let snap = wss_push(&mut chat, 15).await;
    assert_eq!(snap["params"]["kind"], "snapshot", "push: {snap}");

    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "show proposal" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // The live channel delivers the REBUILT standalone proposal block even
    // though the rawOutput carried no resource item.
    let proposal_block_id = timeout(Duration::from_secs(30), async {
        loop {
            let frame = wss_push(&mut chat, 30).await;
            assert_eq!(frame["params"]["kind"], "delta", "push: {frame}");
            let delta = &frame["params"]["delta"];
            if let Some(entity) = delta["added"]
                .as_array()
                .into_iter()
                .flatten()
                .chain(delta["updated"].as_array().into_iter().flatten())
                .find(|e| {
                    e["block"]["type"] == "resource"
                        && e["block"]["resource"]["mimeType"]
                            == "application/vnd.intent.proposal+json"
                })
            {
                assert_eq!(
                    entity["block"]["resource"]["uri"],
                    json!("intent-proposal://settings-change/Update"),
                    "uri rebuilt from the collapsed payload: {entity}"
                );
                return entity["block"]["id"].as_str().map(String::from);
            }
        }
    })
    .await
    .expect("standalone proposal block reached chat channel in time")
    .expect("standalone proposal block carries an id");

    // Terminal reconcile keeps the rebuilt block (live and persisted agree).
    let terminal = timeout(Duration::from_secs(30), async {
        loop {
            let frame = wss_push(&mut chat, 30).await;
            assert_eq!(frame["params"]["kind"], "delta", "push: {frame}");
            let delta = frame["params"]["delta"].clone();
            let is_terminal = ["added", "updated"].iter().any(|key| {
                delta[*key]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .any(|e| e.get("streamingComplete") == Some(&Value::Bool(true)))
            });
            if is_terminal {
                return delta;
            }
        }
    })
    .await
    .expect("terminal (streamingComplete) delta arrived");
    assert!(
        !terminal["removedIds"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|id| id.as_str() == Some(proposal_block_id.as_str())),
        "the proposal block {proposal_block_id} survives the reconcile: {terminal}"
    );

    // The persisted transcript holds the same rebuilt block under the SAME id,
    // and the tool_result keeps the collapsed output object unchanged.
    let conv = wss_rpc(
        &mut rpc,
        12,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let persisted_proposal = messages
        .iter()
        .filter_map(|m| m["contentBlocks"].as_array())
        .flatten()
        .find(|b| {
            b["type"] == "resource"
                && b["resource"]["mimeType"] == "application/vnd.intent.proposal+json"
        })
        .expect("standalone proposal block persisted");
    assert_eq!(
        persisted_proposal["id"].as_str(),
        Some(proposal_block_id.as_str()),
        "live and persisted block ids agree: {persisted_proposal}"
    );
    let persisted_text = persisted_proposal["resource"]["text"]
        .as_str()
        .expect("proposal text");
    let parsed: Value = serde_json::from_str(persisted_text).expect("proposal text parses");
    assert_eq!(parsed, proposal, "proposal round-trips byte-for-byte");
    let tool_result = messages
        .iter()
        .filter_map(|m| m["contentBlocks"].as_array())
        .flatten()
        .find(|b| b["type"] == "tool_result")
        .expect("tool_result persisted");
    assert_eq!(
        tool_result["output"]["output"],
        json!(collapsed_text),
        "tool_result.output keeps the collapsed object unchanged: {tool_result}"
    );
}

/// Live token-usage capture over the real WSS transport (§5.23 / §6.5): the
/// mock agent reports an end-of-turn `usage` snapshot on its `PromptResponse`
/// (the ACP `unstable_end_turn_token_usage` extension); the daemon persists it
/// and emits `workspace:tokenUsage-changed` immediately (no periodic scan),
/// with `cachedReadTokens`/`cachedWriteTokens` mapped to
/// `cacheReadTokens`/`cacheCreationTokens`. A second turn's larger cumulative
/// snapshot REPLACES the first (never summed), and `workspace.getTokenUsage`
/// returns the same tally over the wire.
#[tokio::test]
async fn token_usage_captured_at_turn_end_over_wss() {
    let Some(script) = gate("WSS token-usage E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    // Turn 1 (top-level behavior): cumulative snapshot 70/50 (+30/+4 cached).
    // Turn 2 (rule, matched on the second prompt's marker): grows to 100/80
    // (+45/+6) — cumulative per session, so it must REPLACE turn 1.
    let behavior = json!({
        "response": "turn one",
        "usage": {
            "totalTokens": 154,
            "inputTokens": 70,
            "outputTokens": 50,
            "cachedReadTokens": 30,
            "cachedWriteTokens": 4,
        },
        "rules": [{
            "ifPromptContains": "SECOND_TURN",
            "response": "turn two",
            "usage": {
                "totalTokens": 231,
                "inputTokens": 100,
                "outputTokens": 80,
                "cachedReadTokens": 45,
                "cachedWriteTokens": 6,
            },
        }],
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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

    // SUBSCRIBER conn — subscribe BEFORE the turn so no event is missed.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:tokenUsage-changed"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Usage", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // Turn 1 — the tokenUsage-changed event carries the mapped snapshot
    // (§6.5 self-sufficient payload: { workspaceId, tokenUsage }).
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "first turn" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage 1 ok: {sent}");
    let ev1 = wss_event(&mut sub, 30).await;
    let ev1 = &ev1["params"]["event"];
    assert_eq!(ev1["type"], "workspace:tokenUsage-changed");
    assert_eq!(ev1["data"]["workspaceId"], json!(ws_id));
    let totals1 = &ev1["data"]["tokenUsage"]["totals"];
    assert_eq!(totals1["inputTokens"], 70, "event totals: {ev1}");
    assert_eq!(totals1["outputTokens"], 50);
    assert_eq!(
        totals1["cacheReadTokens"], 30,
        "cachedReadTokens maps to cacheReadTokens: {ev1}"
    );
    assert_eq!(
        totals1["cacheCreationTokens"], 4,
        "cachedWriteTokens maps to cacheCreationTokens: {ev1}"
    );

    // Turn 2 — the larger cumulative snapshot REPLACES turn 1 (100/80,
    // NOT 170/130).
    let sent2 = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "SECOND_TURN please" }),
    )
    .await;
    assert_eq!(sent2["success"], true, "sendMessage 2 ok: {sent2}");
    let ev2 = wss_event(&mut sub, 30).await;
    let totals2 = &ev2["params"]["event"]["data"]["tokenUsage"]["totals"];
    assert_eq!(
        totals2["inputTokens"], 100,
        "cumulative snapshot replaces, never sums: {ev2}"
    );
    assert_eq!(totals2["outputTokens"], 80);
    assert_eq!(totals2["cacheReadTokens"], 45);
    assert_eq!(totals2["cacheCreationTokens"], 6);

    // workspace.getTokenUsage over WSS returns the same durable tally, keyed
    // by agent and model (§5.23 response shape).
    let read = wss_rpc(
        &mut rpc,
        13,
        "workspace.getTokenUsage",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    let usage = &read["tokenUsage"];
    assert_eq!(usage["totals"]["inputTokens"], 100, "getTokenUsage: {read}");
    assert_eq!(usage["totals"]["outputTokens"], 80);
    assert_eq!(usage["byAgentId"][&agent_id]["inputTokens"], 100);
    assert!(
        usage["lastScanAt"].is_string(),
        "lastScanAt stamped by the live update: {read}"
    );
}

/// ACP `usage_update` cost capture over the real WSS transport (§5.23): the
/// mock agent streams a `usage_update` session notification carrying a
/// cumulative `cost` object, and the daemon folds it onto the workspace tally
/// so `workspace:tokenUsage-changed` and `workspace.getTokenUsage` both carry
/// `cost: { amount, currency }` on `totals`, `byAgentId`, and `byModel`.
/// Cost is cumulative per ACP session, so a second turn's report REPLACES the
/// first.
#[tokio::test]
async fn usage_update_cost_captured_over_wss() {
    let Some(script) = gate("WSS usage-cost E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let behavior = json!({
        "response": "turn one",
        "usage": { "totalTokens": 120, "inputTokens": 70, "outputTokens": 50 },
        "rawUpdates": [{
            "sessionUpdate": "usage_update",
            "used": 53_000,
            "size": 200_000,
            "cost": { "amount": 0.5, "currency": "USD" },
        }],
        "rules": [{
            "ifPromptContains": "SECOND_TURN",
            "response": "turn two",
            "usage": { "totalTokens": 180, "inputTokens": 100, "outputTokens": 80 },
            "rawUpdates": [{
                "sessionUpdate": "usage_update",
                "used": 61_000,
                "size": 200_000,
                "cost": { "amount": 1.25, "currency": "USD" },
            }],
        }],
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:tokenUsage-changed"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Cost", "model": "mock:default" }),
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
    assert_eq!(sent["success"], true, "sendMessage 1 ok: {sent}");
    let ev1 = wss_event(&mut sub, 30).await;
    let usage1 = &ev1["params"]["event"]["data"]["tokenUsage"];
    assert_eq!(usage1["totals"]["cost"]["amount"], 0.5, "event: {ev1}");
    assert_eq!(usage1["totals"]["cost"]["currency"], "USD");
    assert_eq!(usage1["byAgentId"][&agent_id]["cost"]["amount"], 0.5);
    assert_eq!(usage1["byModel"]["mock:default"]["cost"]["amount"], 0.5);

    // Turn 2 — cumulative per ACP session, so 1.25 REPLACES 0.5 (not 1.75).
    let sent2 = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "SECOND_TURN please" }),
    )
    .await;
    assert_eq!(sent2["success"], true, "sendMessage 2 ok: {sent2}");
    let ev2 = wss_event(&mut sub, 30).await;
    assert_eq!(
        ev2["params"]["event"]["data"]["tokenUsage"]["totals"]["cost"]["amount"], 1.25,
        "cumulative cost replaces, never sums: {ev2}"
    );

    let read = wss_rpc(
        &mut rpc,
        13,
        "workspace.getTokenUsage",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    let usage = &read["tokenUsage"];
    assert_eq!(usage["totals"]["cost"]["amount"], 1.25, "read: {read}");
    assert_eq!(usage["totals"]["cost"]["currency"], "USD");
    assert_eq!(usage["totals"]["inputTokens"], 100);
}

/// Title-preserving tool updates (the claude-code "Run" collapse): ACP
/// `tool_call_update`s carry **only changed fields** — a richer title/input
/// arrives on one update, later status-only updates carry no title at all.
/// The daemon must treat the transcript block as the authoritative merged
/// state: merge non-empty update fields into it, and backfill sparse event
/// fields from it before publishing `agent:tool:call`, so neither the live
/// `chat.subscribe` channel nor the persisted conversation ever regresses to
/// the sparse first-sight title.
///
/// The mock echoes the exact three-step sequence via `rawUpdates`:
/// `tool_call` (sparse title "Run") → `tool_call_update` (richer title +
/// rawInput, no status) → `tool_call_update` (status-only `completed` with
/// output — no title, no input). Asserts over the real wire:
///  1. the status-only completed `agent:tool:call` still carries the richest
///     title / a non-empty toolName (backfilled, not blanked);
///  2. the LAST live `chat.subscribe` `tool_use` block keeps `name` and
///     `input._acpTitle` equal to the richest title seen;
///  3. the persisted block (`agent.getConversation`) is byte-identical to the
///     live one (§7.1 parity), richer title included.
#[tokio::test]
async fn status_only_tool_update_preserves_richer_title_over_wss() {
    const SPARSE_TITLE: &str = "Run";
    const RICH_TITLE: &str = "Run: cargo test --workspace";
    let Some(script) = gate("WSS title-preserving tool-update E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let behavior = json!({
        "response": "title preserved",
        "rawUpdates": [
            { "sessionUpdate": "tool_call", "toolCallId": "tc_title",
              "title": SPARSE_TITLE, "kind": "execute", "status": "in_progress" },
            { "sessionUpdate": "tool_call_update", "toolCallId": "tc_title",
              "title": RICH_TITLE,
              "rawInput": { "command": "cargo test --workspace" } },
            { "sessionUpdate": "tool_call_update", "toolCallId": "tc_title",
              "status": "completed",
              "rawOutput": { "output": "ok: 42 tests passed" } }
        ]
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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

    // SUBSCRIBER conn — agent:* events, to capture every agent:tool:call.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // RPC conn — create the agent.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Titler", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // CHAT conn — subscribe BEFORE the turn so every stream delta is observed.
    let mut chat = connect_ws(port, cfg.clone()).await;
    let chat_resp = wss_rpc(
        &mut chat,
        20,
        "chat.subscribe",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert!(
        chat_resp["subscriptionId"].is_string(),
        "chat subscribed: {chat_resp}"
    );
    let snap = wss_push(&mut chat, 15).await;
    assert_eq!(snap["params"]["kind"], "snapshot", "push: {snap}");

    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "run the tests" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Collect the agent:tool:call events for the call until the terminal
    // stream:end. Single total deadline (per-frame reads would reset on
    // heartbeat Pings).
    let tool_events = timeout(Duration::from_secs(30), async {
        let mut events: Vec<Value> = Vec::new();
        loop {
            let frame = wss_event(&mut sub, 30).await;
            let ev = &frame["params"]["event"];
            match ev["type"].as_str() {
                Some("agent:tool:call")
                    if ev["data"]["toolCallId"].as_str() == Some("tc_title") =>
                {
                    events.push(ev["data"].clone());
                }
                Some("agent:stream:end") => return events,
                _ => {}
            }
        }
    })
    .await
    .expect("turn reached its terminal stream:end in time");
    assert_eq!(
        tool_events.len(),
        3,
        "one agent:tool:call per update: {tool_events:?}"
    );
    // First sight: the sparse title as sent.
    assert_eq!(
        tool_events[0]["title"], SPARSE_TITLE,
        "first sight carries the sparse title: {:?}",
        tool_events[0]
    );
    assert_eq!(tool_events[0]["status"], "started");
    // The richer update carries its title verbatim.
    assert_eq!(
        tool_events[1]["title"], RICH_TITLE,
        "richer update carries its title: {:?}",
        tool_events[1]
    );
    // THE REGRESSION: the status-only completed update carries no title on
    // the wire — the daemon must backfill the richest title/toolName from the
    // transcript block instead of publishing empty strings.
    assert_eq!(tool_events[2]["status"], "completed");
    assert_eq!(
        tool_events[2]["title"], RICH_TITLE,
        "status-only completed update keeps the richest title (backfilled): {:?}",
        tool_events[2]
    );
    assert!(
        tool_events[2]["toolName"]
            .as_str()
            .is_some_and(|n| !n.trim().is_empty()),
        "status-only completed update keeps a non-empty toolName: {:?}",
        tool_events[2]
    );

    // Drain the chat channel to the terminal reconcile, tracking the LAST
    // live `tool_use` block state — the block the FE would render after the
    // final delta. Before the fix, the status-only update shipped a rebuilt
    // block with `name: ""` and no `_acpTitle`, wiping the title live.
    let mut last_tool_block: Option<Value> = None;
    let terminal = timeout(Duration::from_secs(30), async {
        loop {
            let frame = wss_push(&mut chat, 30).await;
            assert_eq!(frame["params"]["kind"], "delta", "push: {frame}");
            let delta = frame["params"]["delta"].clone();
            let mut is_terminal = false;
            for key in ["added", "updated"] {
                for e in delta[key].as_array().into_iter().flatten() {
                    if e["block"]["type"] == "tool_use" && e["block"]["toolCallId"] == "tc_title" {
                        last_tool_block = Some(e["block"].clone());
                    }
                    // Terminal = the ASSISTANT turn's reconcile; user-row
                    // deltas also carry `streamingComplete: true` but arrive
                    // before the turn streams.
                    if e["role"] == "assistant"
                        && e.get("streamingComplete") == Some(&Value::Bool(true))
                    {
                        is_terminal = true;
                    }
                }
            }
            if is_terminal {
                return delta;
            }
        }
    })
    .await
    .expect("terminal (streamingComplete) delta arrived");
    let live_block = last_tool_block.expect("tool_use block reached the chat channel");
    assert!(
        !terminal["removedIds"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|id| id == &live_block["id"]),
        "the tool_use block survives the reconcile: {terminal}"
    );
    assert_eq!(
        live_block["input"]["_acpTitle"], RICH_TITLE,
        "final live tool_use block keeps the richest title as _acpTitle: {live_block}"
    );
    assert!(
        live_block["name"]
            .as_str()
            .is_some_and(|n| !n.trim().is_empty()),
        "final live tool_use block keeps a non-empty name: {live_block}"
    );
    assert_eq!(
        live_block["metadata"]["status"], "completed",
        "final live tool_use block reached completed: {live_block}"
    );

    // The persisted transcript holds the SAME merged block — richer title
    // included — byte-identical to the live one (§7.1 parity). Before the
    // fix, `record_tool` only patched `metadata.status` on known ids, so the
    // richer title never persisted and a reload regressed to bare "Run".
    let conv = wss_rpc(
        &mut rpc,
        12,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let persisted_block = messages
        .iter()
        .filter_map(|m| m["contentBlocks"].as_array())
        .flatten()
        .find(|b| b["type"] == "tool_use" && b["toolCallId"] == "tc_title")
        .expect("tool_use block persisted");
    assert_eq!(
        persisted_block["input"]["_acpTitle"], RICH_TITLE,
        "persisted tool_use block keeps the richest title: {persisted_block}"
    );
    assert_eq!(
        persisted_block, &live_block,
        "live and persisted tool_use blocks agree byte-for-byte (§7.1)"
    );
}

// ---------------------------------------------------------------------------
// chat.subscribe user-row deltas: persisted non-assistant rows (direct sends,
// queue drains) surface as live `subscription.push` deltas carrying the row's
// real role, so subscribed clients render new user messages with no refetch.
// ---------------------------------------------------------------------------

/// Scan chat-channel pushes for the next delta entity matching `pred`,
/// returning the (entity, delta) pair. Bounded by one shared deadline.
async fn await_chat_entity<S, F>(
    chat: &mut WebSocketStream<S>,
    secs: u64,
    mut pred: F,
) -> (Value, Value)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    F: FnMut(&Value) -> bool,
{
    timeout(Duration::from_secs(secs), async {
        loop {
            let frame = wss_push(chat, secs).await;
            assert_eq!(frame["params"]["kind"], "delta", "push: {frame}");
            let delta = frame["params"]["delta"].clone();
            if let Some(entity) = delta["added"]
                .as_array()
                .into_iter()
                .flatten()
                .chain(delta["updated"].as_array().into_iter().flatten())
                .find(|e| pred(e))
            {
                return (entity.clone(), delta);
            }
        }
    })
    .await
    .expect("matching chat delta entity arrived in time")
}

/// Queue-drain path: client A queues a message behind a busy agent; when the
/// first turn completes and the queue drains, client B's `chat.subscribe`
/// receives the dequeued user row as a delta (role `user`, terminal fields)
/// BEFORE the second turn's assistant chunks — no refetch needed.
#[tokio::test]
async fn queue_drain_user_row_delta_over_chat_subscribe() {
    let Some(script) = gate("WSS queue-drain user-row chat delta E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    // First turn is slow so the second send lands on a busy agent and queues.
    let behavior = json!({ "response": "mock reply", "firstTurnDelayMs": 2000 }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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

    // Conn A (RPC) — create the agent.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "QueueDrainDelta", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // Conn B (chat) — subscribe BEFORE any message so every delta is observed.
    let mut chat = connect_ws(port, cfg.clone()).await;
    let chat_resp = wss_rpc(
        &mut chat,
        20,
        "chat.subscribe",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert!(
        chat_resp["subscriptionId"].is_string(),
        "chat subscribed: {chat_resp}"
    );
    let snap = wss_push(&mut chat, 15).await;
    assert_eq!(snap["params"]["kind"], "snapshot", "push: {snap}");

    // Conn A — first message streams (idle agent), second queues (busy agent).
    let send1 = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "first message" }),
    )
    .await;
    assert_eq!(send1["queued"], false, "first send streams: {send1}");
    sleep(Duration::from_millis(200)).await;
    let send2 = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "queued message" }),
    )
    .await;
    assert_eq!(send2["queued"], true, "second send queues: {send2}");

    // Conn B — the drained user row arrives as a delta with role "user" and
    // the queued content, BEFORE the second turn's assistant chunks. The
    // daemon does not order the drain's user-row emit against the FIRST
    // turn's terminal frames (independent async paths, STAB-4 precedent), so
    // the ordering gate keys off assistant message IDENTITY: any assistant
    // entity with a second distinct messageId seen before the user row is a
    // second-turn chunk that jumped the queue.
    let mut assistant_message_ids: Vec<String> = Vec::new();
    let mut second_turn_chunk_before_user = false;
    let (user_entity, user_delta) = await_chat_entity(&mut chat, 30, |e| {
        let role = e["role"].as_str().unwrap_or("");
        let text = e["block"]["text"].as_str().unwrap_or("");
        if role == "assistant" {
            if let Some(mid) = e["messageId"].as_str() {
                if !assistant_message_ids.iter().any(|m| m == mid) {
                    assistant_message_ids.push(mid.to_string());
                }
                if assistant_message_ids.len() >= 2 {
                    second_turn_chunk_before_user = true;
                }
            }
        }
        role == "user" && text.starts_with("queued message")
    })
    .await;
    assert!(
        !second_turn_chunk_before_user,
        "the dequeued user row must arrive before the second turn's assistant \
         chunks (assistant messageIds seen first: {assistant_message_ids:?})"
    );
    assert_eq!(user_entity["agentId"], json!(agent_id));
    assert_eq!(user_entity["streamingComplete"], json!(true));
    assert!(
        user_entity["messageSeq"].is_u64(),
        "user-row entity carries the authoritative seq: {user_entity}"
    );
    assert!(
        user_entity["timestamp"].is_string(),
        "user-row entity carries the row timestamp: {user_entity}"
    );
    assert_eq!(
        user_delta["removedIds"],
        json!([]),
        "user-row delta removes nothing: {user_delta}"
    );
    let message_id = user_entity["messageId"].as_str().expect("messageId");
    assert_eq!(
        user_entity["block"]["id"],
        json!(format!("{message_id}:0")),
        "stable synthetic block id: {user_entity}"
    );

    // Byte-consistency with the persisted row: same id, role, seq, timestamp,
    // and text as agent.getConversation reports.
    let conv = wss_rpc(
        &mut rpc,
        13,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let row = conv["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .find(|m| m["id"].as_str() == Some(message_id))
        .expect("dequeued user row persisted")
        .clone();
    assert_eq!(row["role"], "user");
    assert_eq!(row["seq"], user_entity["messageSeq"]);
    assert_eq!(row["timestamp"], user_entity["timestamp"]);
    assert_eq!(
        row["contentBlocks"][0]["text"], user_entity["block"]["text"],
        "delta block text matches the persisted row"
    );
    // monorepo#1114: the snapshot path stamps the same synthetic id at serve
    // time, so `agent.getConversation` and the delta agree byte-for-byte on
    // block identity.
    assert_eq!(
        row["contentBlocks"][0]["id"], user_entity["block"]["id"],
        "serve-time stamped snapshot id matches the delta's block id"
    );
    // monorepo#1157 omission side: this send carried no userAppMessageId, so
    // the delta entity must omit `appMessageId` entirely (no null).
    assert!(
        user_entity.get("appMessageId").is_none(),
        "rows without a client id omit appMessageId: {user_entity}"
    );
}

/// Direct-send path: a plain `agent.sendMessage` from connection A (idle
/// agent) surfaces as a user-row delta on connection B's `chat.subscribe`
/// BEFORE any assistant chunk of the triggered turn. The send carries a
/// client-minted `userAppMessageId`, so the delta entity must lift it as
/// `appMessageId` (monorepo#1157) and the served conversation row must carry
/// the same serve-time stamped block id as the delta (monorepo#1114). A fresh
/// `chat.subscribe` afterwards must serve a seq-0 snapshot whose user row
/// carries the same `appMessageId` (snapshot/delta parity, the intentd#780
/// review note).
#[tokio::test]
async fn direct_send_user_row_delta_over_chat_subscribe() {
    let Some(script) = gate("WSS direct-send user-row chat delta E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let behavior = json!({ "response": "direct reply" }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "DirectSendDelta", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    let mut chat = connect_ws(port, cfg.clone()).await;
    let chat_resp = wss_rpc(
        &mut chat,
        20,
        "chat.subscribe",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert!(
        chat_resp["subscriptionId"].is_string(),
        "chat subscribed: {chat_resp}"
    );
    let snap = wss_push(&mut chat, 15).await;
    assert_eq!(snap["params"]["kind"], "snapshot", "push: {snap}");

    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "content": "hello from A",
            "userAppMessageId": "app-msg-delta-e2e",
        }),
    )
    .await;
    assert_eq!(sent["queued"], false, "idle agent streams: {sent}");
    let sent_row_id = sent["messageId"].as_str().expect("messageId").to_string();

    // The user row is persisted (and its agent:message published) BEFORE the
    // turn worker spawns, so it must be the first delta B sees — before any
    // assistant chunk.
    let mut saw_assistant_first = false;
    let (user_entity, _) = await_chat_entity(&mut chat, 30, |e| {
        let role = e["role"].as_str().unwrap_or("");
        if role == "assistant" {
            saw_assistant_first = true;
        }
        role == "user" && e["block"]["text"].as_str() == Some("hello from A")
    })
    .await;
    assert!(
        !saw_assistant_first,
        "the user row precedes every assistant chunk of the triggered turn"
    );
    assert_eq!(user_entity["agentId"], json!(agent_id));
    assert_eq!(
        user_entity["messageId"],
        json!(sent_row_id),
        "delta names the exact persisted row the RPC result returned"
    );
    assert_eq!(user_entity["role"], "user");
    assert_eq!(user_entity["streamingComplete"], json!(true));
    assert_eq!(
        user_entity["block"]["id"],
        json!(format!("{sent_row_id}:0")),
        "stable synthetic block id: {user_entity}"
    );
    // monorepo#1157: the delta entity lifts the client-minted id so the FE
    // can dedup its optimistic row on the delta path — no refetch needed.
    assert_eq!(
        user_entity["appMessageId"],
        json!("app-msg-delta-e2e"),
        "delta entity carries the send's appMessageId: {user_entity}"
    );

    // monorepo#1114: the served conversation row carries the same serve-time
    // stamped `{messageId}:{index}` block id the delta emitted, so snapshot
    // and delta agree byte-for-byte on block identity.
    let conv = wss_rpc(
        &mut rpc,
        12,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let row = conv["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .find(|m| m["id"].as_str() == Some(&sent_row_id))
        .expect("sent user row persisted")
        .clone();
    assert_eq!(
        row["contentBlocks"][0]["id"],
        json!(format!("{sent_row_id}:0")),
        "agent.getConversation serves the stamped synthetic block id: {row}"
    );
    assert_eq!(
        row["contentBlocks"][0]["id"], user_entity["block"]["id"],
        "snapshot block id matches the delta's block id"
    );
    assert_eq!(
        row["appMessageId"],
        json!("app-msg-delta-e2e"),
        "persisted row surfaces the appMessageId on reads: {row}"
    );

    // Re-subscribe on a fresh connection: the seq-0 snapshot reuses the
    // `agent.getConversation` read shape verbatim, so its user row must echo
    // the same lifted `appMessageId` the delta carried (snapshot/delta
    // parity, the intentd#780 review note).
    let mut chat2 = connect_ws(port, cfg.clone()).await;
    let resub = wss_rpc(
        &mut chat2,
        30,
        "chat.subscribe",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert!(
        resub["subscriptionId"].is_string(),
        "re-subscribed: {resub}"
    );
    let snap2 = wss_push(&mut chat2, 15).await;
    assert_eq!(snap2["params"]["kind"], "snapshot", "push: {snap2}");
    let snap_row = snap2["params"]["snapshot"]["messages"]
        .as_array()
        .expect("snapshot messages")
        .iter()
        .find(|m| m["id"].as_str() == Some(&sent_row_id))
        .unwrap_or_else(|| panic!("user row present in fresh snapshot: {snap2}"))
        .clone();
    assert_eq!(
        snap_row["appMessageId"],
        json!("app-msg-delta-e2e"),
        "fresh snapshot user row carries the send's appMessageId: {snap_row}"
    );
    assert_eq!(
        snap_row["contentBlocks"][0]["id"], user_entity["block"]["id"],
        "fresh snapshot block id matches the delta's block id"
    );
}

/// Tool-call activity pings over the real wire (monorepo#1414): a turn whose
/// FIRST stretch is tool calls with no assistant text still ticks
/// `agent:stream:activity`, and the ping carries `lastToolUse { name, status }`
/// for the call just recorded. Before the fix only the text-chunk arm emitted,
/// so a watched-agent row froze at the previous turn's text through the whole
/// tool stretch.
///
/// The mock echoes three `tool_call` updates via `rawUpdates` BEFORE its text
/// response, so the leading-edge ping of the turn necessarily comes from the
/// tool arm: it names the first tool (`bash`, derived from the ACP title) and
/// carries no `lastAgentResponse` (nothing had streamed yet).
#[tokio::test]
async fn tool_call_activity_pings_carry_last_tool_use_over_wss() {
    let Some(script) = gate("WSS tool-call activity E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let behavior = json!({
        "response": "tools done\n",
        "rawUpdates": [
            { "sessionUpdate": "tool_call", "toolCallId": "tca_1",
              "title": "bash: cargo test --workspace", "kind": "execute",
              "status": "in_progress" },
            { "sessionUpdate": "tool_call", "toolCallId": "tca_2",
              "title": "view: src/lib.rs", "kind": "read",
              "status": "in_progress" },
            { "sessionUpdate": "tool_call", "toolCallId": "tca_3",
              "title": "bash: cargo fmt", "kind": "execute",
              "status": "in_progress" }
        ]
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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

    // SUBSCRIBER conn — agent:* BEFORE the turn so no activity frame is missed.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Tooler", "model": "mock:default" }),
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
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "run the tools" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Collect every activity frame up to the terminal stream:end under one
    // total deadline (per-frame reads would reset on heartbeat Pings).
    let activities = timeout(Duration::from_secs(30), async {
        let mut frames: Vec<Value> = Vec::new();
        loop {
            let frame = wss_event(&mut sub, 30).await;
            let ev = &frame["params"]["event"];
            match ev["type"].as_str() {
                Some("agent:stream:activity") => frames.push(ev["data"].clone()),
                Some("agent:stream:end") => return frames,
                _ => {}
            }
        }
    })
    .await
    .expect("turn reached its terminal stream:end in time");

    assert!(
        !activities.is_empty(),
        "the tool-only stretch emits at least one activity over WSS"
    );
    // Sanity cap, not throttle verification: the mock emits exactly 3
    // `rawUpdates` plus one `agent_message_chunk` for the whole response, so 4
    // is the no-throttle maximum. It guards against a future fixture change
    // multiplying the emit count; the deterministic window-boundary coverage
    // lives in the `agent_session` unit tests.
    assert!(
        activities.len() <= 4,
        "at most one ping per emitter (3 tools + 1 chunk): {activities:?}"
    );
    // The leading-edge ping of the turn came from the tool arm: it names the
    // first call and has no preview text yet.
    let first = &activities[0];
    assert_eq!(
        first["agentId"].as_str(),
        Some(agent_id.as_str()),
        "activity carries the agent id: {first}"
    );
    assert_eq!(
        first["lastToolUse"],
        json!({ "name": "bash", "status": "started" }),
        "tool-arm activity carries the just-recorded call's derived name + status: {first}"
    );
    assert!(
        first.get("lastAgentResponse").is_none(),
        "no assistant text had streamed when the first tool call landed: {first}"
    );
    assert!(
        first.get("content").is_none(),
        "activity payload never carries transcript content: {first}"
    );
}

/// Streamed reasoning over the real WSS transport: two `agent_thought_chunk`
/// updates coalesce into ONE `thinking` block on the live `chat.subscribe`
/// channel, the assistant text that follows opens a separate `text` block,
/// and both persist in stream order under the same ids `agent.getConversation`
/// returns.
#[tokio::test]
async fn thinking_blocks_stream_and_persist_over_wss() {
    let Some(script) = gate("WSS thinking-block E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    // The mock echoes the canned thought chunks before its response text.
    let behavior = json!({
        "response": "The answer is 42.",
        "rawUpdates": [
            { "sessionUpdate": "agent_thought_chunk",
              "content": { "type": "text", "text": "Let me " } },
            { "sessionUpdate": "agent_thought_chunk",
              "content": { "type": "text", "text": "think." } }
        ]
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Thinker", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // CHAT conn — subscribe BEFORE the turn so every stream delta is observed.
    let mut chat = connect_ws(port, cfg.clone()).await;
    let chat_resp = wss_rpc(
        &mut chat,
        20,
        "chat.subscribe",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert!(
        chat_resp["subscriptionId"].is_string(),
        "chat subscribed: {chat_resp}"
    );
    let snap = wss_push(&mut chat, 15).await;
    assert_eq!(snap["params"]["kind"], "snapshot", "push: {snap}");

    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "think then answer" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Collect live blocks until the turn's terminal (streamingComplete) delta.
    // Single total deadline (per-frame reads would reset on heartbeat Pings).
    let live: Vec<Value> = timeout(Duration::from_secs(30), async {
        // Latest-wins per block id: the accumulating chunk deltas re-send the
        // whole block, so the last copy seen is the complete one.
        let mut blocks: Vec<Value> = Vec::new();
        loop {
            let frame = wss_push(&mut chat, 30).await;
            assert_eq!(frame["params"]["kind"], "delta", "push: {frame}");
            let delta = &frame["params"]["delta"];
            let mut terminal = false;
            for entity in delta["added"]
                .as_array()
                .into_iter()
                .flatten()
                .chain(delta["updated"].as_array().into_iter().flatten())
                // The echoed user message rides the same channel and is
                // complete on arrival; only the assistant turn ends the loop.
                .filter(|e| {
                    !e["messageId"]
                        .as_str()
                        .is_some_and(|id| id.starts_with("user-msg"))
                })
            {
                terminal |= entity.get("streamingComplete") == Some(&Value::Bool(true));
                let block = entity["block"].clone();
                match blocks.iter_mut().find(|b| b["id"] == block["id"]) {
                    Some(existing) => *existing = block,
                    None => blocks.push(block),
                }
            }
            if terminal {
                return blocks;
            }
        }
    })
    .await
    .expect("turn reached its terminal delta in time");

    let thinking = live
        .iter()
        .find(|b| b["type"] == "thinking")
        .unwrap_or_else(|| panic!("a thinking block reached the chat channel: {live:?}"));
    assert_eq!(
        thinking["text"],
        json!("Let me think."),
        "consecutive thought chunks coalesce into one block: {thinking}"
    );
    let thinking_id = thinking["id"].as_str().expect("thinking id").to_string();
    let text = live
        .iter()
        .find(|b| b["type"] == "text")
        .expect("the assistant text block reached the chat channel");
    assert_ne!(
        text["id"].as_str(),
        Some(thinking_id.as_str()),
        "assistant text opens a block of its own: {text}"
    );

    // The persisted transcript holds both blocks in stream order under the
    // SAME ids the live channel used.
    let conv = wss_rpc(
        &mut rpc,
        12,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let assistant = conv["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .find(|m| m["role"] == "assistant")
        .expect("assistant row persisted");
    let persisted = assistant["contentBlocks"]
        .as_array()
        .expect("contentBlocks array");
    let types: Vec<&str> = persisted
        .iter()
        .filter_map(|b| b["type"].as_str())
        .collect();
    assert_eq!(
        types,
        vec!["thinking", "text"],
        "reasoning persists before the answer: {assistant}"
    );
    assert_eq!(persisted[0]["text"], json!("Let me think."));
    assert_eq!(
        persisted[0]["id"].as_str(),
        Some(thinking_id.as_str()),
        "live and persisted thinking-block ids agree: {assistant}"
    );
    assert!(
        persisted[1]["text"]
            .as_str()
            .is_some_and(|t| t.contains("The answer is 42.")),
        "the answer persists as assistant text: {assistant}"
    );
    // Reasoning never leaks into the agent-list preview.
    let listed = wss_rpc(
        &mut rpc,
        13,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let preview = listed["agent"]["lastAgentResponse"].as_str().unwrap_or("");
    assert!(
        !preview.contains("Let me think."),
        "thought text stays out of lastAgentResponse: {listed}"
    );
}

/// TTL idle-sweep eviction is observable on the real wire and a follow-up
/// send auto-restores (monorepo#3040): drive one mock turn to idle, let the
/// sub-second `INTENTD_IDLE_REAP_MS` sweep reap the child, and assert the
/// `agent:process:evicted` notification lands on a live `events.subscribe`
/// channel with the §6.7 self-sufficient payload — `agentId`, `used`, `cap`,
/// and the additive reason `"idle-ttl"`. Then send a second message to the
/// reaped agent and assert the RPC succeeds and the turn streams to a normal
/// `agent:stream:end` (lazy respawn), with both user rows persisted — the
/// send was restored, not silently dropped.
#[tokio::test]
async fn ttl_reap_evicted_event_and_send_restores_over_wss() {
    let Some(script) = gate("WSS TTL-reap eviction E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let behavior = json!({ "response": "hello from mock" }).to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        // Sub-second TTL + sweep (test-only seam; §13.1) so the eviction is
        // observable without a ≥30s wait.
        ("INTENTD_IDLE_REAP_MS", "800"),
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

    // SUBSCRIBER conn — subscribe BEFORE any turn so the eviction notification
    // cannot be missed.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Reap", "model": "mock:default" }),
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
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "turn one" }),
    )
    .await;
    assert_eq!(sent["success"], true, "first sendMessage ok: {sent}");

    // First turn completes (stream:end), the child goes idle, and the TTL
    // sweep evicts it: the wire carries `agent:process:evicted` with the
    // additive `"idle-ttl"` reason and the §6.7 self-sufficient payload.
    let mut ends = 0u32;
    let mut evicted_frame: Option<Value> = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while evicted_frame.is_none() {
        let frame = wss_event_opt_until(&mut sub, deadline)
            .await
            .expect("agent:process:evicted reached the WSS subscriber");
        let ev = &frame["params"]["event"];
        if ev["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        match ev["type"].as_str() {
            Some("agent:failed") => panic!("no agent:failed during reap: {ev}"),
            Some("agent:stream:end") => ends += 1,
            Some("agent:process:evicted") => evicted_frame = Some(frame.clone()),
            _ => {}
        }
    }
    assert_eq!(ends, 1, "the first turn completed before the eviction");
    let evicted = &evicted_frame.expect("evicted frame")["params"]["event"];
    let data = &evicted["data"];
    assert_eq!(
        data["reason"], "idle-ttl",
        "TTL sweep evictions carry the idle-ttl reason: {evicted}"
    );
    assert_eq!(data["agentId"].as_str(), Some(agent_id.as_str()));
    assert!(data["used"].is_u64(), "used is numeric: {evicted}");
    assert!(data["cap"].is_u64(), "cap is numeric: {evicted}");

    // A send to the reaped agent restores instead of silently dropping: the
    // RPC succeeds and the turn streams to a normal terminal stream:end on a
    // lazily respawned child.
    let sent2 = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "turn two" }),
    )
    .await;
    assert_eq!(sent2["success"], true, "post-reap sendMessage ok: {sent2}");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let frame = wss_event_opt_until(&mut sub, deadline)
            .await
            .expect("post-reap turn reached stream:end on the WSS subscriber");
        let ev = &frame["params"]["event"];
        if ev["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        match ev["type"].as_str() {
            Some("agent:failed") => panic!("post-reap send must not fail: {ev}"),
            Some("agent:stream:end") => break,
            _ => {}
        }
    }

    // Both user rows persisted — nothing was silently dropped.
    let conv = wss_rpc(
        &mut rpc,
        13,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let user_texts: Vec<String> = messages
        .iter()
        .filter(|m| m["role"] == "user")
        .map(|m| serde_json::to_string(&m["contentBlocks"]).unwrap_or_default())
        .collect();
    assert!(
        user_texts.iter().any(|t| t.contains("turn one"))
            && user_texts.iter().any(|t| t.contains("turn two")),
        "both user rows persisted across the reap: {conv}"
    );
    let assistant_rows = messages.iter().filter(|m| m["role"] == "assistant").count();
    assert_eq!(
        assistant_rows, 2,
        "both turns produced assistant replies: {conv}"
    );
}

/// intent-hq/monorepo#3039 over the real WSS wire: `agent.stop` against a
/// WEDGED transport must still surface the client-visible terminal state.
/// The mock streams one chunk, then STOPS draining its stdin and floods
/// unawaited `fs/read_text_file` requests; the daemon's serve loop answers
/// every one into the unread pipe, so the writer task blocks mid-write and
/// the bounded writer channel saturates — the incident's exact wedge (a
/// multi-MB tool result stalling the child). Before the fix the stop's
/// `session/cancel` awaited channel capacity forever: the RPC never
/// completed, no terminal event followed, and the FE spun on "Thinking"
/// until the idle sweep reaped the agent silently. Asserts over the wire:
/// - `agent.stop` returns `{ success: true }` within a bounded window;
/// - the terminal `agent:stream:end` (`stopReason: "interrupted"`) and
///   `agent:idle` both reach the `events.event` subscriber — never an
///   `agent:failed`;
/// - the daemon log carries the wedged-cancel WARN, proving the timeout arm
///   (not a plain wire error) produced the teardown;
/// - `agent.getSession` settles to `status: "idle"`.
#[tokio::test]
async fn agent_stop_on_wedged_transport_emits_terminal_events_over_wss() {
    let Some(script) = gate("WSS wedged-transport stop E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    // 5000 unawaited reads ≈ 5000 small error frames — comfortably more than
    // the OS pipe + the child's paused stream buffer + the 256-slot writer
    // channel can absorb, so the serve loop is provably parked on a full
    // channel when the stop's cancel tries to enqueue.
    let behavior = json!({ "wedgeTransport": { "requestCount": 5000 } }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("port fits u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

    // SUBSCRIBER conn — subscribe BEFORE the turn so no terminal event is missed.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Wedged", "model": "mock:default" }),
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
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "ingest something huge" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // The turn is observably live once the pre-wedge chunk lands; the mock
    // then wedges the transport (paused stdin + request flood). Give the
    // flood a bounded moment to saturate the daemon's writer channel: the
    // serve loop must already be parked on a full channel when the stop's
    // cancel tries to enqueue, or the cancel would land normally and the
    // test would pass vacuously (the daemon-log WARN assert below keeps
    // this honest either way).
    timeout(Duration::from_secs(30), async {
        loop {
            let frame = wss_event(&mut sub, 30).await;
            if frame["params"]["event"]["type"] == "agent:stream:activity"
                && frame["params"]["event"]["data"]["agentId"].as_str() == Some(&agent_id)
            {
                return;
            }
        }
    })
    .await
    .expect("mock streamed its pre-wedge chunk");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Stop the wedged turn. Before the fix this RPC hung forever (the cancel
    // notify parked on the saturated channel ahead of the terminal emits).
    let stopped = wss_rpc(&mut rpc, 12, "agent.stop", json!({ "agentId": agent_id })).await;
    assert_eq!(stopped["success"], true, "stop ok: {stopped}");

    // The terminal events reach the wire: stream:end (interrupted) + idle,
    // never a failed.
    let mut end_frame: Option<Value> = None;
    let mut saw_idle = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !(end_frame.is_some() && saw_idle) {
        let frame = wss_event_opt_until(&mut sub, deadline)
            .await
            .expect("terminal stream:end + idle reached the WSS subscriber");
        let ev = &frame["params"]["event"];
        if ev["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        match ev["type"].as_str() {
            Some("agent:failed") => panic!("stop must not fail the agent: {ev}"),
            Some("agent:stream:end") => end_frame = Some(frame.clone()),
            Some("agent:idle") => saw_idle = true,
            _ => {}
        }
    }
    let end = &end_frame.expect("stream:end frame")["params"]["event"];
    assert_eq!(
        end["data"]["stopReason"], "interrupted",
        "terminal stream:end carries the interrupt stopReason: {end}"
    );

    // The wedged-cancel WARN proves the bounded-timeout arm ran — the cancel
    // was UNDELIVERABLE (parked on the full channel), not merely errored.
    let log_path = data_dir.join("daemon.log");
    let mut warned = false;
    for _ in 0..200 {
        if tokio::fs::read_to_string(&log_path)
            .await
            .unwrap_or_default()
            .contains("session/cancel undeliverable (transport wedged)")
        {
            warned = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        warned,
        "daemon log carries the wedged-cancel WARN (the timeout arm ran)"
    );

    // The session settles idle — nothing left for the idle sweep to reap
    // silently. Poll: the idle event precedes the status persist (monorepo#1164).
    let mut last = Value::Null;
    let mut settled = false;
    for i in 0..100 {
        last = wss_rpc(
            &mut rpc,
            100 + i,
            "agent.getSession",
            json!({ "workspaceId": ws_id, "agentId": agent_id }),
        )
        .await;
        if last["session"]["status"] == "idle" {
            settled = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(settled, "agent session settled to idle; last: {last}");
}
