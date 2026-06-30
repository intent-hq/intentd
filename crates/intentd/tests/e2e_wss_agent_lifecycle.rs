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
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .arg("--listen")
        .arg(listen)
        .env("INTENTD_DATA_DIR", data_dir)
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
    let behavior = json!({
        "toolCall": {
            "name": "add_to_note_workspace-mcp",
            "arguments": { "noteId": note_id, "content": MARKER },
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
    // note:updated (from the MCP tool's domain event).
    let mut chunks = 0u32;
    let mut ends = 0u32;
    let mut saw_note_updated = false;
    for _ in 0..80 {
        let frame = wss_event(&mut sub, 30).await;
        match frame["params"]["event"]["type"].as_str() {
            Some("agent:stream:chunk") => chunks += 1,
            Some("agent:stream:end") => {
                ends += 1;
                break;
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
/// through `pending → active → idle` and emit the matching
/// `agent:status-changed` self-sufficient events (PROTOCOL §6.5/§6.7), so a
/// hydrated/reloaded chat reflects the post-turn idle state rather than the
/// stored `pending` placeholder. Co-emitted with `agent:idle` at turn end.
#[tokio::test]
async fn agent_session_status_persists_pending_active_idle_over_wss() {
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

    // A brand-new agent persists with the default `pending` status; the runtime
    // must transition it as the turn runs.
    let pre = wss_rpc(
        &mut rpc,
        11,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        pre["agent"]["status"], "pending",
        "fresh agent persisted with status=pending: {pre}"
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
    // parked turn's only block is text — and parents no other agents).
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

    // agent.get: the idle agent reports every flag false.
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

    // agent.list carries the same per-agent flags.
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

    // Release the parked worker so the daemon tears down cleanly.
    let stopped = wss_rpc(&mut rpc, 17, "agent.stop", json!({ "agentId": busy_id })).await;
    assert_eq!(stopped["success"], true, "stop ok: {stopped}");
}

/// Agent delegation over WSS: `agent.delegate` with `agentInstructions` must
/// create the child AND start its turn, with every `agent:stream:*` event keyed
/// by the CHILD `agentId` (PROTOCOL §5.5/§6.5). Drives the RPC front door
/// (caller-less) so the child is the only agent that ever runs — proving the
/// streamed output belongs to the child's session, not a parent's. Asserts: a
/// non-empty `agentId` in the result, ≥1 `agent:stream:chunk` + exactly one
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
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let delegated = wss_rpc(
        &mut rpc,
        10,
        "agent.delegate",
        json!({
            "workspaceId": ws_id,
            "agentInstructions": "do the delegated work",
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

/// Pre-seed the daemon's SQLite store with a workspace + target note for the
/// MCP tool call (the daemon opens the same data dir on launch).
async fn seed_workspace_and_note(data_dir: &Path) -> (String, String) {
    use intent_core::{NoteCreate, WorkspaceApi, WorkspaceId};
    use intent_services::Services;
    use intent_store::Store;
    let db_path = data_dir.join("intentd.db");
    let store = Store::open(&db_path).await.expect("open store");
    let services = Services::new(store.clone());
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
        json!({ "scriptId": script_id }),
    )
    .await;
    assert!(status.is_object(), "script.status object: {status}");
    let removed = wss_rpc(
        &mut rpc,
        13,
        "script.remove",
        json!({ "scriptId": script_id }),
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
    let behavior = json!({
        "toolCall": {
            "name": "add_to_note_workspace-mcp",
            "arguments": { "noteId": note_id, "content": "filter-branch-marker" },
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
