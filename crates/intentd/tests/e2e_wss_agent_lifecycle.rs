//! WSS end-to-end agent lifecycle (WSS-1): the UDS analogue in
//! `uds_agent_runtime.rs` ported to the WebSocket transport.
//!
//! Boots a real `intentd serve --listen both` against the mock ACP provider and
//! drives the full agent lifecycle over a pinned TLS WebSocket — one persistent
//! SUBSCRIBER connection (events.event notifications) and one RPC connection
//! (request/response). Mirrors the lifecycle assertions of the UDS suite and
//! the §5.14 `locality == "remote"` guarantee from `wss_integration.rs`.
//!
//! Gated on `node` + the mock script; skips cleanly otherwise.

#![cfg(unix)]

use std::net::{Ipv4Addr, TcpListener as StdTcpListener};
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
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::{sleep, timeout};
use tokio_rustls::TlsConnector;
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

fn free_port() -> u16 {
    StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
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
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .arg("--listen")
        .arg(listen)
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd.spawn().expect("spawn intentd serve")
}

async fn await_uds(socket: &Path) -> bool {
    timeout(Duration::from_secs(10), async {
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

/// One UDS JSON-RPC round-trip (used only to discover bound port + fingerprint).
async fn uds_rpc(socket: &Path, id: i64, method: &str, params: Value) -> Value {
    let stream = UnixStream::connect(socket).await.expect("connect uds");
    let (read_half, mut write_half) = stream.into_split();
    let mut line = serde_json::to_string(
        &json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }),
    )
    .unwrap();
    line.push('\n');
    write_half.write_all(line.as_bytes()).await.unwrap();
    write_half.flush().await.unwrap();
    let mut reader = BufReader::new(read_half);
    let mut buf = String::new();
    timeout(Duration::from_secs(5), reader.read_line(&mut buf))
        .await
        .expect("uds rpc timed out")
        .expect("read uds response");
    serde_json::from_str(buf.trim_end()).expect("invalid JSON frame")
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
    let tcp = TcpStream::connect((Ipv4Addr::LOCALHOST, port))
        .await
        .expect("tcp connect");
    let name = ServerName::try_from("localhost").unwrap();
    TlsConnector::from(cfg)
        .connect(name, tcp)
        .await
        .expect("tls connect")
}

/// Open an authenticated WSS connection (token in the query string).
async fn connect_ws(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let tls = tls_connect(port, cfg).await;
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    let (ws, _resp) = tokio_tungstenite::client_async(url, tls)
        .await
        .expect("ws handshake");
    ws
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
    loop {
        let remaining = match deadline.checked_duration_since(tokio::time::Instant::now()) {
            Some(d) if !d.is_zero() => d,
            _ => return None,
        };
        let next = match timeout(remaining, ws.next()).await {
            Ok(next) => next,
            Err(_) => return None,
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
            Some(Ok(_)) => continue,
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
            Some(Ok(_)) => continue,
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
    let behavior = json!({
        "toolCall": {
            "name": "workspace_api",
            "arguments": { "code": js, "summary": "WSS E2E ws.note.add" },
        },
        "response": "added via mcp over wss",
    })
    .to_string();
    let port_s = free_port().to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", &port_s),
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
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
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
    // real WSS transport AND arrive before the first `agent:stream:chunk`.
    let mut chunks = 0u32;
    let mut ends = 0u32;
    let mut saw_note_updated = false;
    let mut first_chunk_at: Option<usize> = None;
    let mut status_frames: Vec<(usize, Value)> = Vec::new();
    for i in 0..80 {
        let frame = wss_event(&mut sub, 30).await;
        match frame["params"]["event"]["type"].as_str() {
            Some("agent:stream:chunk") => {
                chunks += 1;
                if first_chunk_at.is_none() {
                    first_chunk_at = Some(i);
                }
            }
            Some("agent:stream:end") => {
                ends += 1;
                break;
            }
            Some("agent:stream:status") => {
                status_frames.push((i, frame["params"]["event"].clone()));
            }
            Some("note:updated") => saw_note_updated = true,
            _ => {}
        }
    }
    assert!(chunks >= 1, "at least one agent:stream:chunk over WSS");
    assert_eq!(ends, 1, "exactly one terminal agent:stream:end over WSS");
    assert!(
        saw_note_updated,
        "tool's note:updated domain event delivered over WSS"
    );

    // STAT-1 — pre-first-token status hints must reach the FE over the real
    // WSS transport and MUST all arrive before the first `agent:stream:chunk`
    // (that's the whole point: they populate the spinner *before* streaming
    // starts, then the chunk-reducer clears them). The daemon emits at four
    // real turn-startup transitions for a brand-new agent's first turn:
    // `launch` (spawn), `init` (handshake), `session-create` (session/new),
    // `prompt` (session/prompt). At least the last one — `prompt` — MUST land
    // before the first chunk on the same subscription.
    let first_chunk_at =
        first_chunk_at.expect("at least one agent:stream:chunk observed to anchor ordering");
    assert!(
        !status_frames.is_empty(),
        "expected >=1 agent:stream:status frame over WSS before the first chunk"
    );
    for (ord, ev) in &status_frames {
        assert!(
            *ord < first_chunk_at,
            "agent:stream:status at ordinal {ord} arrived AT/AFTER first agent:stream:chunk \
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

/// Session-status lifecycle persistence (P0 — chat-spinner clear). A normal
/// `agent.sendMessage` turn must drive the persisted `agent_session.status`
/// through `Idle → active → idle` and emit the matching
/// `agent:status-changed` self-sufficient events (PROTOCOL §6.5/§6.7), so a
/// hydrated/reloaded chat reflects the post-turn idle state. A freshly
/// created session persists the legacy `AgentStatus::Idle` (wire form
/// `"Idle"`) for reference parity with `agent-factory.ts:435`; end-of-turn
/// then rewrites to `AgentStatus::RuntimeIdle` (wire form `"idle"`).
/// Co-emitted with `agent:idle` at turn end.
#[tokio::test]
async fn agent_session_status_persists_idle_active_idle_over_wss() {
    let Some(script) = gate("WSS status-lifecycle E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let behavior = json!({ "response": "status lifecycle ok" }).to_string();
    let port_s = free_port().to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", &port_s),
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
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
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
    let mut saw_idle = false;
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
                saw_idle = true;
            }
            _ => {}
        }
        if saw_idle && transitions.contains(&("idle".to_string(), false)) {
            break;
        }
    }
    assert!(
        saw_idle,
        "terminal agent:idle emitted at turn end (transitions so far: {transitions:?})"
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
    let port_s = free_port().to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", &port_s),
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
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
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
        if frame["params"]["event"]["type"] == "agent:stream:chunk"
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

    // Stop the agent mid-turn — interrupt (not kill); terminal stream:end fires.
    let stopped = wss_rpc(&mut rpc, 12, "agent.stop", json!({ "agentId": agent_id })).await;
    assert_eq!(stopped["success"], true, "stop ok: {stopped}");
    let mut saw_stop_end = false;
    for _ in 0..50 {
        let frame = wss_event(&mut sub, 30).await;
        if frame["params"]["event"]["type"] == "agent:stream:end" {
            assert_eq!(
                frame["params"]["event"]["data"]["agentId"]
                    .as_str()
                    .unwrap_or_default(),
                agent_id,
                "terminal stream:end carries the agent id"
            );
            saw_stop_end = true;
            break;
        }
    }
    assert!(saw_stop_end, "terminal agent:stream:end emitted on stop");

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
            Some("agent:stream:chunk") => {
                if frame["params"]["event"]["data"]["content"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("turn=2")
                {
                    saw_resume_chunk = true;
                }
            }
            Some("agent:stream:end") => {
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
    let port_s = free_port().to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", &port_s),
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
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
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
        if frame["params"]["event"]["type"] == "agent:stream:chunk"
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
    let mut saw_preempt_end = false;
    let mut saw_interrupt_chunk = false;
    let mut saw_interrupt_end = false;
    for _ in 0..80 {
        let frame = wss_event(&mut sub, 30).await;
        match frame["params"]["event"]["type"].as_str() {
            Some("agent:stream:end") if !saw_preempt_end => {
                assert_eq!(
                    frame["params"]["event"]["data"]["agentId"]
                        .as_str()
                        .unwrap_or_default(),
                    agent_id,
                    "terminal stream:end carries the agent id"
                );
                saw_preempt_end = true;
            }
            Some("agent:stream:chunk") => {
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
            Some("agent:stream:chunk") => {
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
    let port_s = free_port().to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", &port_s),
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
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
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
        if frame["params"]["event"]["type"] == "agent:stream:chunk"
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
            Some("agent:stream:chunk") => {
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
    let port_s = free_port().to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", &port_s),
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
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
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
        if frame["params"]["event"]["type"] == "agent:stream:chunk"
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
            Some("agent:stream:chunk") => {
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
            Some("agent:stream:chunk") => {
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
    let port_s = free_port().to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", &port_s),
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
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
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
        if frame["params"]["event"]["type"] == "agent:stream:chunk"
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

/// AUDIT-P2-1b: when an agent has a real pending completion watch (its
/// MCP-delegated child is still running), the daemon emits the specific
/// `waitingForAgentIds: [childId]` alongside `isWaitingForOtherAgents: true`
/// over the WSS wire (PROTOCOL §5.5/§7.1) — proving the id list reflects the
/// genuine parent→child watch registered by the MCP `delegate_task` tool.
/// Drives the full MCP loop (mock ACP fires `delegate_task`)
/// and parks the child so the watch persists for observation.
#[tokio::test]
async fn agent_waiting_for_agent_ids_reflects_pending_watch_over_wss() {
    let Some(script) = gate("WSS waitingForAgentIds E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    // Parent fires `delegate_task` with instructions carrying a marker; the
    // delegated child sees the marker in its first prompt and parks. The
    // parent then returns end_turn and goes idle — the watch persists because
    // the child never completes.
    const CHILD_MARK: &str = "AUDIT_P2_1B_PARK_CHILD";
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
    let port_s = free_port().to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", &port_s),
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
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
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
    let mut parent_idle = false;
    for _ in 0..120 {
        let frame = wss_event(&mut sub, 60).await;
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:idle" && ev["data"]["agentId"] == json!(parent_id) {
            parent_idle = true;
            break;
        }
    }
    assert!(parent_idle, "parent went idle after firing delegate tool");

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
/// the generic `Agent xxxxxx` fallback), ≥1 `agent:stream:chunk` + exactly one
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
    let port_s = free_port().to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", &port_s),
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
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
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
            Some("agent:stream:chunk") => {
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

/// WAKE-1: `after_all` delegation fan-in over WSS, end to end. A parent fires
/// TWO MCP `delegate_task` calls with `waitMode: "after_all"`; each child
/// reports via `report_to_parent` (suppressed — no immediate parent message)
/// and completes. Asserts (PROTOCOL §5.5/§6.5):
/// - the parent transcript carries EXACTLY ONE `[WORKSPACE EVENTS]` wake with
///   BOTH child reports aggregated, and zero individual report deliveries;
/// - the wake runs a REAL parent turn — `agent:stream:chunk` + one
///   `agent:stream:end` + a trailing `agent:idle`, all keyed by the parent;
/// - `isWaitingForOtherAgents` is true (with both child ids) while waiting and
///   false after delivery, with `agent:subscriptions-changed` watch-change
///   events observed on the wire for both transitions.
#[tokio::test]
async fn after_all_group_delivers_single_aggregated_wake_over_wss() {
    let Some(script) = gate("WSS after_all aggregated wake E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    const CHILD_A: &str = "WAKE1_CHILD_ALPHA";
    const CHILD_B: &str = "WAKE1_CHILD_BETA";
    const REPORT_A: &str = "REPORT_ALPHA finished the alpha task";
    const REPORT_B: &str = "REPORT_BETA finished the beta task";
    const PARENT_GO: &str = "WAKE1_PARENT_GO";
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
    let port_s = free_port().to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", &port_s),
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
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
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
            Some("agent:stream:chunk") => wake_chunks += 1,
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
/// single parent wake is driven by the child's terminal `agent:idle`,
/// carrying the persisted `completionReport` via `format_completion_wake`'s
/// `Report:` branch (which wins over `lastResponseSummary`). Exercised on the
/// real WSS wire (not just the `intent-services` unit tests) per the repo's
/// e2e requirement.
///
/// A parent's opening turn delegates one child (immediate, ungrouped —
/// `waitMode: "immediate"`), the child calls `ws.agent.reportToParent` and
/// then finishes, and the parent's wake turn acknowledges. Asserts:
/// - the parent transcript carries EXACTLY ONE `[WORKSPACE EVENTS]` wake
///   message (proving `reportToParent` emitted zero additional wakes);
/// - that wake carries the `Report: <report>` framing and does NOT fall
///   through to the `Summary:` branch (report-preferred formatting);
/// - the wake turn runs on the parent AFTER the child's `agent:idle` — no
///   parent `agent:stream:*` fires between the child's first stream chunk
///   and the child's terminal `agent:idle`.
#[tokio::test]
async fn report_to_parent_metadata_only_then_idle_delivers_single_wake_over_wss() {
    let Some(script) = gate("WSS reportToParent SUB-2 E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    const CHILD_TAG: &str = "SUB2_WSS_CHILD";
    const REPORT: &str = "SUB2_WSS_REPORT shipped the thing";
    const PARENT_GO: &str = "SUB2_WSS_PARENT_GO";
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
    let port_s = free_port().to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", &port_s),
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
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
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

    // Track the observable event ordering:
    // - parent goes idle after the delegating turn;
    // - child streams (chunk/end) → child agent:idle (report already persisted);
    // - THEN the parent's wake turn runs (chunk/end) → parent idles again.
    // If `reportToParent` had emitted an immediate wake, the parent's second
    // `stream:chunk` would fire BEFORE the child's `agent:idle` here.
    let mut child_id: Option<String> = None;
    let mut parent_idle_after_delegate = false;
    let mut child_first_chunk_seen = false;
    let mut child_idle = false;
    let mut parent_wake_chunk_before_child_idle = false;
    let mut parent_wake_ends = 0u32;
    let mut parent_idle_after_wake = false;
    for _ in 0..400 {
        let frame = wss_event(&mut sub, 60).await;
        let ev = &frame["params"]["event"];
        let ev_agent = ev["data"]["agentId"].as_str().unwrap_or_default();
        let ev_type = ev["type"].as_str().unwrap_or_default();
        // Learn the child id from the first non-parent stream chunk (the
        // child's own turn keys every stream event by its agent id).
        if child_id.is_none()
            && ev_type == "agent:stream:chunk"
            && !ev_agent.is_empty()
            && ev_agent != parent_id
        {
            child_id = Some(ev_agent.to_string());
        }
        if ev_agent == parent_id && ev_type == "agent:idle" && !parent_idle_after_delegate {
            parent_idle_after_delegate = true;
            continue;
        }
        if let Some(cid) = child_id.as_deref() {
            if ev_agent == cid && ev_type == "agent:stream:chunk" {
                child_first_chunk_seen = true;
            }
            if ev_agent == cid && ev_type == "agent:idle" {
                child_idle = true;
            }
        }
        // Between the child's first chunk and the child's idle, the parent
        // MUST NOT stream a wake turn — that would prove `reportToParent`
        // delivered an immediate wake.
        if ev_agent == parent_id
            && ev_type == "agent:stream:chunk"
            && child_first_chunk_seen
            && !child_idle
        {
            parent_wake_chunk_before_child_idle = true;
        }
        if ev_agent == parent_id && ev_type == "agent:stream:end" && child_idle {
            parent_wake_ends += 1;
        }
        if ev_agent == parent_id && ev_type == "agent:idle" && child_idle {
            parent_idle_after_wake = true;
        }
        if parent_idle_after_wake && parent_wake_ends >= 1 {
            break;
        }
    }
    assert!(
        parent_idle_after_delegate,
        "parent went idle after the delegating turn"
    );
    assert!(child_id.is_some(), "child agent id observed on the wire");
    assert!(child_idle, "child emitted agent:idle after reportToParent");
    assert!(
        !parent_wake_chunk_before_child_idle,
        "reportToParent MUST NOT emit an immediate parent wake — parent streamed before child idled"
    );
    assert_eq!(
        parent_wake_ends, 1,
        "exactly one wake-turn stream:end on the parent (single wake driven by child idle)"
    );
    assert!(
        parent_idle_after_wake,
        "parent idled again after the wake turn"
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

/// Pre-seed the daemon's SQLite store with a workspace + target note for the
/// MCP tool call (the daemon opens the same data dir on launch).
async fn seed_workspace_and_note(data_dir: &Path) -> (String, String) {
    use intent_core::{NoteCreate, WorkspaceApi, WorkspaceId};
    use intent_services::Services;
    use intent_store::Store;
    let db_path = data_dir.join("intentd.db");
    let store = Store::open(&db_path).await.expect("open store");
    let services = Services::new(store.clone()).with_workspaces_root(
        std::env::temp_dir().join(format!("itd-hermetic-ws-{}", uuid::Uuid::new_v4())),
    );
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
        .expect("create note");
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
    ws.send(Message::Text(frame.to_string()))
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
            Some(Ok(_)) => continue,
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
        let next = match timeout(dur, ws.next()).await {
            Ok(v) => v,
            Err(_) => return None,
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
            Some(Ok(_)) => continue,
            None | Some(Err(_)) => return None,
        }
    }
}

/// Boot a hermetic `intentd serve --listen both` (no mock-agent env), seed a
/// workspace + note, and return `(daemon, ws_id, note_id, port, fingerprint)`.
/// Used by the no-node read-arm sweep below.
async fn boot_daemon_with_seeded_note() -> (Daemon, String, String, u16, String) {
    let data_dir = temp_data_dir();
    let (ws_id, note_id) = seed_workspace_and_note(&data_dir).await;
    let port_s = free_port().to_string();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", &port_s)];
    let child = spawn_serve(&data_dir, "both", &env);
    let daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
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
            "name": "echo-wss",
            "command": "echo wss",
            "mode": "command",
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
    let terms = term_list.as_array().expect("terminals array");
    assert!(
        terms.iter().any(|t| t["id"] == json!(terminal_id)),
        "created terminal listed: {term_list}"
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
        let next = match timeout(remaining, sub.next()).await {
            Ok(next) => next,
            Err(_) => break,
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
            Some(Ok(_)) => continue,
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
    let port_s = free_port().to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", &port_s),
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
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
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
            Some("agent:stream:chunk") => agent_chunks += 1,
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
    let port_s = free_port().to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", &port_s),
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
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
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
        if frame["params"]["event"]["type"] == "agent:stream:chunk" {
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
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => total.extend_from_slice(&buf[..n]),
            Ok(Err(_)) => break,
            Err(_) => break,
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
    let port_s = free_port().to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", &port_s),
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
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
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
                    .map(|q| !q.is_empty())
                    .unwrap_or(false)
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
    let port_s = free_port().to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", &port_s),
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
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
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
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
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

    // Send first message — agent will be busy for 2000ms (firstTurnDelayMs).
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

    // Collect events and look for agent:message events for both user messages.
    let mut saw_dequeued_user_message = false;
    let mut saw_queue_drain = false;
    let mut stream_end_count = 0;

    for i in 0..120 {
        let frame = wss_event(&mut sub, 30).await;
        let evt = &frame["params"]["event"];
        match evt["type"].as_str() {
            Some("agent:queue:updated") => {
                // After the first turn completes, the queue drains to empty
                if stream_end_count >= 1
                    && evt["data"]["queue"]
                        .as_array()
                        .map(|q| q.is_empty())
                        .unwrap_or(false)
                {
                    saw_queue_drain = true;
                }
            }
            Some("agent:message") => {
                assert_eq!(evt["data"]["agentId"].as_str(), Some(agent_id.as_str()));
                let role = evt["data"]["role"].as_str();
                // The dequeued message event should arrive after the queue drain
                if role == Some("user") && saw_queue_drain {
                    saw_dequeued_user_message = true;
                }
            }
            Some("agent:stream:end") => {
                stream_end_count += 1;
                // After two turns complete and we've seen the dequeued message event, we're done
                if stream_end_count >= 2 && saw_dequeued_user_message {
                    break;
                }
                // Give up after seeing both stream ends + some extra iterations
                if stream_end_count >= 2 && i >= 10 {
                    break;
                }
            }
            _ => {}
        }
    }

    assert!(
        saw_queue_drain,
        "queue should have drained after first turn"
    );
    assert!(
        saw_dequeued_user_message,
        "agent:message event for dequeued user message — STAB-4 fix"
    );
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
    let port_s = free_port().to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", &port_s),
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
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
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
    let port_s = free_port().to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", &port_s),
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
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
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

    // RPC conn — one workspace.create carrying the full initialAgent payload.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let agent_id = format!("agent-{}", Uuid::new_v4());
    let created = wss_rpc(
        &mut rpc,
        10,
        "workspace.create",
        json!({
            "title": "Orchestrated WS",
            "branch": "feat/initial-agent-e2e",
            "idempotencyKey": "wss-create-idem-1",
            "initialAgent": {
                "agentId": agent_id,
                "prompt": "build the initial feature",
                "name": "Initial agent",
                "model": "mock:default",
                "specialist": "implementor",
            },
        }),
    )
    .await;
    let ws_id = created["workspace"]["id"].as_str().expect("workspace id");
    assert_eq!(
        created["initialAgent"]["id"], agent_id,
        "result carries the created agent: {created}"
    );
    assert_eq!(created["initialAgent"]["name"], "Initial agent");

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
            Some("agent:stream:chunk") => {
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

/// Regression for the composite `(id, workspace_id)` note PK (migration 0030
/// + `feat(services): workspace-scope note lookups + seed spec per workspace`):
/// two `workspace.create` calls each seed their own `spec` note. Over the
/// real WSS transport the client can call `note.get {noteId: "spec"}` against
/// either workspace and receive a distinct row scoped to that workspace, with
/// no cross-workspace bleed of body, title, or `workspaceId`.
#[tokio::test]
async fn workspace_create_seeds_per_workspace_spec_over_wss() {
    let data_dir = temp_data_dir();
    let port_s = free_port().to_string();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", &port_s)];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
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
            .map(|s| s.success())
            .unwrap_or(false)
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
    let port_s = free_port().to_string();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", &port_s)];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
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
    let port_s = free_port().to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", &port_s),
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
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
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
    let port_s = free_port().to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", &port_s),
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
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
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

/// SP-1 (Suggested Next Steps): the `--rules` file assembled by
/// `agent_manager::create_agent` for a top-level (non-sub-agent) interactive
/// agent MUST contain the `## Suggested Next Steps` heading — the directive
/// that tells the model to emit a `<!-- suggested-prompts ... -->` block at
/// the end of user-facing responses. The daemon writes the temp file into
/// `std::env::temp_dir()` and keeps it alive for the lifetime of the agent
/// handle, so we redirect the daemon's `TMPDIR` to a test-controlled
/// directory and scan it after the first turn kicks off spawning.
#[tokio::test]
async fn assembled_rules_file_contains_suggested_next_steps_over_wss() {
    let Some(script) = gate("WSS SP-1 rules-file E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    // Dedicated TMPDIR so the daemon's `std::env::temp_dir()` writes the
    // `intentd-rules-*.md` and `intentd-mcp-*.json` files where this test
    // can inspect them.
    let tmp_dir = data_dir.join("tmp");
    std::fs::create_dir_all(&tmp_dir).expect("mkdir tmp dir");
    let tmp_dir_s = tmp_dir.to_string_lossy().into_owned();

    // Any behavior works — we don't care what the mock does after spawn,
    // only that the daemon actually reached the rules-file assembly path.
    let behavior = json!({ "response": "ok" }).to_string();
    let port_s = free_port().to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", &port_s),
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
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
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

    // Poll the redirected TMPDIR for the `intentd-rules-*.md` the daemon
    // writes during spawn. Bounded wait so a hung spawn fails loudly.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut rules_body: Option<String> = None;
    while std::time::Instant::now() < deadline {
        let entries = std::fs::read_dir(&tmp_dir).expect("read TMPDIR");
        let mut hit: Option<PathBuf> = None;
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_s = name.to_string_lossy();
            if name_s.starts_with("intentd-rules-") && name_s.ends_with(".md") {
                hit = Some(entry.path());
                break;
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
        "expected `intentd-rules-*.md` to be written under the redirected TMPDIR \
         during agent spawn",
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
    let port_s = free_port().to_string();
    let env: [(&str, &str); 3] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", &port_s),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
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
    let agent_id = format!("agent-{}", Uuid::new_v4());
    let created = wss_rpc(
        &mut rpc,
        10,
        "workspace.create",
        json!({
            "title": "No-prompt WS",
            "branch": "feat/initial-agent-no-prompt-e2e",
            "initialAgent": {
                "agentId": agent_id,
                "name": "Coordinator",
                "model": "mock:default",
                "specialist": "implementor",
            },
        }),
    )
    .await;
    let ws_id = created["workspace"]["id"].as_str().expect("workspace id");
    assert_eq!(
        created["initialAgent"]["id"], agent_id,
        "no-prompt create still returns the agent: {created}"
    );
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
