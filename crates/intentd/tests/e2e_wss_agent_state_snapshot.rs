//! WSS e2e for the per-turn agent state snapshot (`ws.agent.snapshot`).
//!
//! Drives an agent on the mock ACP provider over the real WSS transport and
//! asserts — via the mock fixture's `MOCK_AGENT_PROMPT_LOG` seam — the exact
//! prompt text the provider received per turn:
//!
//! * Turn 1 (idle agent, trivial snapshot): NO
//!   `current ws.agent.snapshot() => {...}` line — a `time`-only snapshot
//!   never injects. The mock's tool call registers an event subscription,
//!   making the NEXT snapshot non-trivial.
//! * Turn 2: the prompt STARTS with the snapshot line (outermost recurring
//!   decoration), whose single-line JSON parses and reports
//!   `eventSubscriptions: 1`.
//! * Toggle (session-bound): flip `agentFeatures.stateSnapshot` off via
//!   `settings.update` — turn 3 on the SAME session STILL injects the line
//!   (the toggle was captured ON in the session's harness feature snapshot
//!   at creation), while an agent created AFTER the flip captures it OFF and
//!   never injects, even once its own snapshot turns non-trivial.
//! * The `ws.agent.snapshot()` MCP tool is never gated: called through the
//!   agent's own bridge AFTER the flip it still returns
//!   `{ time, eventSubscriptions: 1 }` with zero-count fields omitted.
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
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

/// Fixed 64-hex token, adopted by the daemon via the `INTENTD_AUTH_TOKEN` seam.
const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

type Ws = WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;

/// The injected snapshot line's prefix (PROTOCOL §7.1).
const SNAPSHOT_PREFIX: &str = "current ws.agent.snapshot() => ";

/// Turn-1 trigger marker: the mock's `rules` entry matches on this, so the
/// subscription-registering tool call fires ONLY on the first user turn.
const SUBSCRIBE_MARKER: &str = "SUBSCRIBE_NOW_E2E";
const CHILD_COUNTER_MARKER: &str = "KEEP_CHILD_ACTIVE_FOR_SNAPSHOT_E2E";

/// Live `intentd serve` process; killed and its data dir removed on drop.
struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let log_path = self.data_dir.join("daemon.log");
        if let Ok(log) = std::fs::read_to_string(&log_path) {
            if !log.is_empty() {
                eprintln!("=== DAEMON LOG ===\n{log}\n=== END LOG ===");
            }
        }
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-snap-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    common::enable_ws_api(data_dir);
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
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

async fn wss_connect(port: u16, cfg: Arc<ClientConfig>) -> Ws {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

async fn wss_rpc(ws: &mut Ws, id: i64, method: &str, params: Value) -> Value {
    let req = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(req.to_string().into()))
        .await
        .expect("send");
    loop {
        let msg = timeout(Duration::from_secs(10), ws.next())
            .await
            .expect("ws rpc timeout")
            .expect("ws closed")
            .expect("ws error");
        if let Message::Text(text) = msg {
            let v: Value = serde_json::from_str(&text).expect("invalid json");
            if v["id"] == id {
                return v;
            }
        }
    }
}

async fn wss_event(ws: &mut Ws, secs: u64) -> Value {
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
            Some(Ok(Message::Close(_))) => panic!("websocket closed while waiting for event"),
            Some(Err(e)) => panic!("websocket error: {e}"),
            None => panic!("websocket stream ended"),
            _ => {}
        }
    }
}

/// Wait for `agent:stream:end` for a specific agent id.
async fn await_stream_end(sub: &mut Ws, agent_id: &str) {
    for _ in 0..120 {
        let frame = wss_event(sub, 30).await;
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:stream:end" && ev["data"]["agentId"] == agent_id {
            return;
        }
    }
    panic!("agent:stream:end not observed for agent {agent_id}");
}

/// Mock-agent gate (parity with the WSS agent-lifecycle suite): resolve the
/// fixture path (env override or the in-tree default) and skip cleanly when
/// `node` or the script is unavailable.
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

/// The mock fixture's per-turn prompt log: `{ turn, text }` JSON lines.
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

/// List the generated `intentd-mcp-*.json` config files under
/// `<data_dir>/agent-configs`, sorted for deterministic diffing.
#[allow(clippy::case_sensitive_file_extension_comparisons)] // extensions generated by our own code with fixed case
fn mcp_config_files(data_dir: &Path) -> Vec<PathBuf> {
    let dir = data_dir.join("agent-configs");
    let mut out: Vec<PathBuf> = std::fs::read_dir(&dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.starts_with("intentd-mcp-") && n.ends_with(".json"))
                })
                .collect()
        })
        .unwrap_or_default();
    out.sort();
    out
}

/// Extract the bridge `--connect <addr>` from a generated MCP config file.
fn bridge_addr_from_config(path: &Path) -> String {
    let cfg: Value = serde_json::from_str(&std::fs::read_to_string(path).expect("read mcp config"))
        .expect("parse mcp config");
    let args = cfg["mcpServers"]["workspace-mcp"]["args"]
        .as_array()
        .expect("workspace-mcp args");
    let idx = args
        .iter()
        .position(|a| a == "--connect")
        .expect("--connect flag in bridge args");
    args[idx + 1].as_str().expect("bridge addr").to_string()
}

/// Per-agent MCP bridge client (newline-delimited JSON-RPC over loopback),
/// exactly like a spawned provider child would speak.
struct BridgeClient {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    next_id: i64,
}

impl BridgeClient {
    async fn connect(addr: &str) -> Self {
        let stream = timeout(Duration::from_secs(10), TcpStream::connect(addr))
            .await
            .expect("bridge connect timeout")
            .expect("bridge connect");
        let (r, w) = stream.into_split();
        let mut c = BridgeClient {
            reader: BufReader::new(r),
            writer: w,
            next_id: 1,
        };
        let init = c.request("initialize", json!({})).await;
        assert!(
            init["result"]["serverInfo"].is_object(),
            "initialize: {init}"
        );
        c
    }

    async fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        let line = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        self.writer
            .write_all(format!("{line}\n").as_bytes())
            .await
            .expect("bridge write");
        let mut buf = String::new();
        loop {
            buf.clear();
            let n = timeout(Duration::from_secs(30), self.reader.read_line(&mut buf))
                .await
                .expect("bridge read timeout")
                .expect("bridge read");
            assert!(n > 0, "bridge closed while waiting for response");
            let v: Value = match serde_json::from_str(buf.trim()) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if v["id"] == id {
                return v;
            }
        }
    }

    /// `tools/call workspace_api` with agent JS; returns `(is_error, text)`.
    async fn call_js(&mut self, code: &str) -> (bool, String) {
        let resp = self
            .request(
                "tools/call",
                json!({
                    "name": "workspace_api",
                    "arguments": { "code": code, "summary": "e2e snapshot probe" },
                }),
            )
            .await;
        assert!(
            resp.get("error").is_none(),
            "tools/call transport error: {resp}"
        );
        let result = &resp["result"];
        let is_error = result["isError"].as_bool().unwrap_or(false);
        let text = result["content"][0]["text"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        (is_error, text)
    }
}

/// Split a logged prompt into (snapshot JSON, rest) when the snapshot line
/// leads it, or `None` when no snapshot line was injected.
fn split_snapshot(text: &str) -> Option<(Value, &str)> {
    let payload = text.strip_prefix(SNAPSHOT_PREFIX)?;
    let (line, rest) = payload.split_once("\n\n").unwrap_or((payload, ""));
    let v: Value = serde_json::from_str(line).expect("snapshot line must be valid JSON");
    Some((v, rest))
}

#[allow(clippy::similar_names)] // deliberate parallel naming across the scenario's instances
#[tokio::test]
async fn state_snapshot_injection_toggle_and_tool_over_wss() {
    let Some(script) = gate("agent state snapshot E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let socket = data_dir.join("intentd.sock");
    let prompt_log = data_dir.join("prompt-log.jsonl");
    let prompt_log_str = prompt_log.to_string_lossy().into_owned();
    // Turn 1 (rule-gated on the marker): register an event subscription via
    // the agent's own bridge, making the NEXT turn's snapshot non-trivial
    // (eventSubscriptions: 1). Later turns fall through to the plain response.
    let behavior = json!({
        "rules": [{
            "ifPromptContains": SUBSCRIBE_MARKER,
            "toolCalls": [{
                "name": "workspace_api",
                "arguments": {
                    "code": "return await ws.event.subscribe(['note:*'])",
                    "summary": "register event subscription",
                },
            }],
            "response": "subscribed",
        }, {
            "ifPromptContains": CHILD_COUNTER_MARKER,
            "delayMs": 5000,
            "response": "child done",
        }],
        "response": "done",
    })
    .to_string();

    let mut _daemon = Daemon {
        child: spawn_serve(
            &data_dir,
            &[
                ("INTENTD_AUTH_TOKEN", TOKEN),
                ("INTENTD_TCP_PORT", "0"),
                ("MOCK_AGENT_SCRIPT_PATH", &script),
                ("MOCK_AGENT_BEHAVIOR", &behavior),
                ("MOCK_AGENT_PROMPT_LOG", &prompt_log_str),
            ],
        ),
        data_dir: data_dir.clone(),
    };
    assert!(await_uds(&socket).await, "daemon did not start");

    let status = common::await_wss_status(&socket).await;
    let fp = status["result"]["fingerprint"].as_str().expect("fp");
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let cfg = client_config(fp);

    // Subscriber conn — before any agent activity.
    let mut sub = wss_connect(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"] }),
    )
    .await;
    assert!(
        sub_resp["result"]["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = wss_connect(port, cfg.clone()).await;

    // Plain-JSON tool bodies so the bridge probe below can parse the
    // `ws.agent.snapshot()` result as JSON (read live per invocation).
    let toon_off = wss_rpc(
        &mut rpc,
        5,
        "settings.update",
        json!({ "changes": [{ "path": "workspaceApi.toonOutput", "value": false }] }),
    )
    .await;
    assert_eq!(
        toon_off["result"]["applied"][0]["value"],
        json!(false),
        "toonOutput off: {toon_off}"
    );

    let created = wss_rpc(
        &mut rpc,
        10,
        "workspace.create",
        json!({
            "title": "state snapshot WS",
            "branch": "feat/state-snapshot-e2e",
            "idempotencyKey": "state-snapshot-e2e-1",
            "initialAgent": {
                "prompt": format!("first turn {SUBSCRIBE_MARKER}"),
                "name": "Snapshot Agent",
                "model": "default", "provider": "mock",
            },
        }),
    )
    .await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let agent = created["result"]["initialAgent"]["id"]
        .as_str()
        .expect("initial agent id")
        .to_string();
    await_stream_end(&mut sub, &agent).await;

    // Turn 2: the subscription registered on turn 1 makes this snapshot
    // non-trivial, so the line must lead the prompt.
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent, "content": "second turn" }),
    )
    .await;
    assert_eq!(sent["result"]["success"], true, "sendMessage ok: {sent}");
    await_stream_end(&mut sub, &agent).await;

    // The first agent's bridge config, captured while it is the only one so
    // the un-gated-tool probe below targets the right bridge.
    let first_configs = mcp_config_files(&data_dir);
    assert_eq!(
        first_configs.len(),
        1,
        "one agent → one mcp config: {first_configs:?}"
    );

    // Session-bound toggle: flip stateSnapshot off. The EXISTING session
    // captured the toggle ON in its harness feature snapshot at creation, so
    // turn 3 on the SAME session must still inject the line.
    let upd = wss_rpc(
        &mut rpc,
        12,
        "settings.update",
        json!({ "changes": [{ "path": "agentFeatures.stateSnapshot", "value": false }] }),
    )
    .await;
    assert_eq!(
        upd["result"]["applied"][0]["value"],
        json!(false),
        "toggle applied: {upd}"
    );
    let sent3 = wss_rpc(
        &mut rpc,
        13,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent, "content": "third turn" }),
    )
    .await;
    assert_eq!(sent3["result"]["success"], true, "sendMessage ok: {sent3}");
    await_stream_end(&mut sub, &agent).await;

    // An agent created AFTER the flip captures stateSnapshot OFF: its turn 1
    // registers a subscription (same marker rule), and its turn 2 — snapshot
    // now non-trivial — must still carry no line.
    let created2 = wss_rpc(
        &mut rpc,
        14,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Gated Agent", "model": "default", "provider": "mock" }),
    )
    .await;
    let agent2 = created2["result"]["agent"]["id"]
        .as_str()
        .expect("second agent id")
        .to_string();
    let sent4 = wss_rpc(
        &mut rpc,
        15,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent2,
            "content": format!("gated first turn {SUBSCRIBE_MARKER}"),
        }),
    )
    .await;
    assert_eq!(sent4["result"]["success"], true, "sendMessage ok: {sent4}");
    await_stream_end(&mut sub, &agent2).await;
    let sent5 = wss_rpc(
        &mut rpc,
        16,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent2, "content": "gated second turn" }),
    )
    .await;
    assert_eq!(sent5["result"]["success"], true, "sendMessage ok: {sent5}");
    await_stream_end(&mut sub, &agent2).await;

    // ---- Prompt-log assertions ----
    // Both mock children append to the same log; the first agent's three
    // turns complete before the second agent is created, so the order is
    // deterministic: agent-1 turns 1-3, then agent-2 turns 1-2.
    let log = read_prompt_log(&prompt_log);
    assert!(
        log.len() >= 5,
        "expected 5 logged prompts, got {}: {log:?}",
        log.len()
    );

    // Turn 1: fresh idle agent → trivial snapshot → NO line.
    let (_, first) = &log[0];
    assert!(
        !first.contains(SNAPSHOT_PREFIX),
        "trivial snapshot must not inject on turn 1: {first:?}"
    );

    // Turn 2: non-trivial (1 event subscription) → line leads the prompt.
    let (_, second) = &log[1];
    let (snap, rest) = split_snapshot(second)
        .unwrap_or_else(|| panic!("turn 2 must start with the snapshot line: {second:?}"));
    assert_eq!(
        snap["eventSubscriptions"],
        json!(1),
        "subscription count: {snap}"
    );
    assert!(snap["time"].is_string(), "time always present: {snap}");
    assert!(
        snap.get("hooks").is_none() && snap.get("queuedMessages").is_none(),
        "zero-count fields omitted: {snap}"
    );
    // The send may drain via the queue, which appends the dequeue-wait
    // system note after the user content — strip it before the tail check.
    let rest_tail = rest
        .split("\n\n[SYSTEM NOTE] This message was queued at")
        .next()
        .unwrap();
    assert!(
        rest_tail.ends_with("second turn"),
        "user content after the snapshot line: {second:?}"
    );

    // Turn 3 (toggle flipped off, SAME session): the line STILL leads the
    // prompt — the session's captured snapshot, not the live setting, gates
    // the injection.
    let (_, third) = &log[2];
    let (snap3, rest3) = split_snapshot(third).unwrap_or_else(|| {
        panic!("existing session must keep injecting after the flip: {third:?}")
    });
    assert_eq!(
        snap3["eventSubscriptions"],
        json!(1),
        "subscription count on turn 3: {snap3}"
    );
    let rest3_tail = rest3
        .split("\n\n[SYSTEM NOTE] This message was queued at")
        .next()
        .unwrap();
    assert!(
        rest3_tail.ends_with("third turn"),
        "user content after the snapshot line: {third:?}"
    );

    // Agent-2 turn 1: trivial snapshot, no line (and captured OFF anyway).
    let (_, fourth) = &log[3];
    assert!(
        !fourth.contains(SNAPSHOT_PREFIX),
        "no line on the gated agent's first turn: {fourth:?}"
    );
    assert!(
        fourth.contains("gated first turn"),
        "log[3] is the gated agent's first turn: {fourth:?}"
    );

    // Agent-2 turn 2: snapshot now non-trivial (1 event subscription), but
    // the session captured stateSnapshot OFF at creation → still no line.
    let (_, fifth) = &log[4];
    assert!(
        !fifth.contains(SNAPSHOT_PREFIX),
        "captured-off session must never inject: {fifth:?}"
    );
    assert!(
        fifth.contains("gated second turn"),
        "log[4] is the gated agent's second turn: {fifth:?}"
    );

    // ---- ws.agent.snapshot() tool stays un-gated after the flip ----
    // Probe the FIRST agent's bridge (captured before agent 2 existed)...
    let addr = bridge_addr_from_config(&first_configs[0]);
    let mut bridge = BridgeClient::connect(&addr).await;
    let (err, text) = bridge.call_js("return await ws.agent.snapshot()").await;
    assert!(!err, "ws.agent.snapshot must stay callable: {text}");
    let v: Value = serde_json::from_str(&text).expect("snapshot tool returns JSON");
    assert!(v["time"].is_string(), "time present: {v}");
    assert_eq!(
        v["eventSubscriptions"],
        json!(1),
        "tool reports the live subscription: {v}"
    );
    assert!(
        v.get("hooks").is_none(),
        "zero-count fields omitted from the tool result: {v}"
    );

    // ...and the SECOND (captured-off) agent's bridge: the tool is never
    // gated by the toggle, only the prompt injection is.
    let configs = mcp_config_files(&data_dir);
    assert_eq!(
        configs.len(),
        2,
        "two agents → two mcp configs: {configs:?}"
    );
    let second_config = configs
        .iter()
        .find(|p| !first_configs.contains(p))
        .expect("second agent's mcp config");
    let mut bridge2 = BridgeClient::connect(&bridge_addr_from_config(second_config)).await;
    let (err2, text2) = bridge2.call_js("return await ws.agent.snapshot()").await;
    assert!(
        !err2,
        "ws.agent.snapshot must stay callable on a captured-off session: {text2}"
    );
    let v2: Value = serde_json::from_str(&text2).expect("snapshot tool returns JSON");
    assert_eq!(
        v2["eventSubscriptions"],
        json!(1),
        "gated session's tool still reports its subscription: {v2}"
    );

    // Create a real delegated child through the MCP bridge, keep its provider
    // turn in flight, then verify all three child counters over WSS. The legacy
    // field remains the unsettled alias.
    let create_code = format!(
        "return await ws.agent.create('Snapshot Child', '{CHILD_COUNTER_MARKER}', {{ model: 'default', provider: 'mock' }})"
    );
    let (create_err, create_text) = bridge.call_js(&create_code).await;
    assert!(!create_err, "child creation must succeed: {create_text}");
    let (child_err, child_text) = bridge.call_js("return await ws.agent.snapshot()").await;
    assert!(!child_err, "child snapshot must succeed: {child_text}");
    let child_snapshot: Value =
        serde_json::from_str(&child_text).expect("child snapshot returns JSON");
    assert_eq!(
        child_snapshot["activeSubAgents"],
        json!(1),
        "{child_snapshot}"
    );
    assert_eq!(
        child_snapshot["unsettledSubAgents"],
        json!(1),
        "{child_snapshot}"
    );
    assert_eq!(
        child_snapshot["runningSubAgents"],
        json!(1),
        "legacy running count preserves in-flight status semantics: {child_snapshot}"
    );
}

/// monorepo#3384: `runningSubAgents` through the production WSS/MCP path —
/// the parent's `ws.agent.snapshot()` counts a delegated child only while it
/// is genuinely in flight, and once the child's turn ends (status settles to
/// idle) the field disappears. Pre-fix the count was backed by an
/// unsettled-children blocklist, so the settled-idle poll below never
/// converged: an idle child was reported as running forever.
#[tokio::test]
async fn snapshot_running_sub_agents_excludes_idle_children_over_wss() {
    const SPAWN_MARKER: &str = "SNAP3384_SPAWN_GO";
    const CHILD_MARKER: &str = "SNAP3384_CHILD_GO";
    let Some(script) = gate("runningSubAgents idle-child E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let socket = data_dir.join("intentd.sock");

    // The child's rule holds its turn open for a few seconds so the
    // parent-bridge poll below observes the in-flight child deterministically.
    let spawn_js = format!(
        "const r = await ws.agent.create('SnapChild', '{CHILD_MARKER} do the work', \
         {{ model: 'default', provider: 'mock' }}); return 'spawned=' + r.ok;"
    );
    let behavior = json!({
        "rules": [
            { "ifPromptContains": "[WORKSPACE EVENTS]", "response": "wake acknowledged" },
            {
                "ifPromptContains": SPAWN_MARKER,
                "toolCalls": [{
                    "name": "workspace_api",
                    "arguments": { "code": spawn_js, "summary": "spawn the child" },
                }],
                "response": "child spawned",
            },
            { "ifPromptContains": CHILD_MARKER, "delayMs": 3000, "response": "child done" },
        ],
        "response": "done",
    })
    .to_string();

    let mut _daemon = Daemon {
        child: spawn_serve(
            &data_dir,
            &[
                ("INTENTD_AUTH_TOKEN", TOKEN),
                ("INTENTD_TCP_PORT", "0"),
                ("MOCK_AGENT_SCRIPT_PATH", &script),
                ("MOCK_AGENT_BEHAVIOR", &behavior),
            ],
        ),
        data_dir: data_dir.clone(),
    };
    assert!(await_uds(&socket).await, "daemon did not start");

    let status = common::await_wss_status(&socket).await;
    let fp = status["result"]["fingerprint"].as_str().expect("fp");
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let cfg = client_config(fp);

    let mut sub = wss_connect(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"] }),
    )
    .await;
    assert!(
        sub_resp["result"]["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = wss_connect(port, cfg.clone()).await;

    // Plain-JSON tool bodies so the snapshot polls below parse as JSON.
    let toon_off = wss_rpc(
        &mut rpc,
        5,
        "settings.update",
        json!({ "changes": [{ "path": "workspaceApi.toonOutput", "value": false }] }),
    )
    .await;
    assert_eq!(
        toon_off["result"]["applied"][0]["value"],
        json!(false),
        "toonOutput off: {toon_off}"
    );

    let created = wss_rpc(
        &mut rpc,
        10,
        "workspace.create",
        json!({
            "title": "runningSubAgents WS",
            "branch": "feat/running-sub-agents-e2e",
            "idempotencyKey": "running-sub-agents-e2e-1",
            "initialAgent": {
                "prompt": "plain first turn",
                "name": "SnapParent",
                "model": "default", "provider": "mock",
            },
        }),
    )
    .await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let parent = created["result"]["initialAgent"]["id"]
        .as_str()
        .expect("initial agent id")
        .to_string();
    await_stream_end(&mut sub, &parent).await;

    // The parent's bridge config, captured while it is the only agent.
    let parent_configs = mcp_config_files(&data_dir);
    assert_eq!(
        parent_configs.len(),
        1,
        "one agent → one mcp config: {parent_configs:?}"
    );
    let mut bridge = BridgeClient::connect(&bridge_addr_from_config(&parent_configs[0])).await;

    // Baseline: no children → the field is absent (omitted when zero).
    let (err, text) = bridge.call_js("return await ws.agent.snapshot()").await;
    assert!(!err, "snapshot baseline: {text}");
    let v: Value = serde_json::from_str(&text).expect("snapshot tool returns JSON");
    assert!(v.get("runningSubAgents").is_none(), "no children yet: {v}");

    // Spawn the child; its held turn keeps it in flight for the poll below.
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": parent,
            "content": format!("go {SPAWN_MARKER}"),
        }),
    )
    .await;
    assert_eq!(sent["result"]["success"], true, "sendMessage ok: {sent}");

    // In flight: the parent's snapshot must count exactly the running child.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let (err, text) = bridge.call_js("return await ws.agent.snapshot()").await;
        assert!(!err, "snapshot poll (in flight): {text}");
        let v: Value = serde_json::from_str(&text).expect("snapshot tool returns JSON");
        if v["runningSubAgents"] == json!(1) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "child never observed in flight: {v}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Settled: once the child's turn ends it persists idle and must drop out
    // of the count. Pre-fix this poll never converged — the idle child is
    // non-terminal, so the unsettled blocklist kept reporting it as running.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let (err, text) = bridge.call_js("return await ws.agent.snapshot()").await;
        assert!(!err, "snapshot poll (settled): {text}");
        let v: Value = serde_json::from_str(&text).expect("snapshot tool returns JSON");
        if v.get("runningSubAgents").is_none() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "idle child still counted as running: {v}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The snapshot's `prs` field over the production WSS/MCP path: tracked open
/// PRs (persisted on the workspace row via `workspace.update`) surface in
/// `ws.agent.snapshot()` grouped by state with merged/closed excluded, open
/// tracked PRs alone force the per-turn injection line, and once no open PR
/// remains the field is omitted and the snapshot goes back to trivial (no
/// injection line).
#[tokio::test]
async fn snapshot_prs_groups_tracked_open_prs_over_wss() {
    let Some(script) = gate("snapshot prs E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let socket = data_dir.join("intentd.sock");
    let prompt_log = data_dir.join("prompt-log.jsonl");
    let prompt_log_str = prompt_log.to_string_lossy().into_owned();
    let behavior = json!({ "response": "done" }).to_string();

    let mut _daemon = Daemon {
        child: spawn_serve(
            &data_dir,
            &[
                ("INTENTD_AUTH_TOKEN", TOKEN),
                ("INTENTD_TCP_PORT", "0"),
                ("MOCK_AGENT_SCRIPT_PATH", &script),
                ("MOCK_AGENT_BEHAVIOR", &behavior),
                ("MOCK_AGENT_PROMPT_LOG", &prompt_log_str),
            ],
        ),
        data_dir: data_dir.clone(),
    };
    assert!(await_uds(&socket).await, "daemon did not start");

    let status = common::await_wss_status(&socket).await;
    let fp = status["result"]["fingerprint"].as_str().expect("fp");
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let cfg = client_config(fp);

    let mut sub = wss_connect(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"] }),
    )
    .await;
    assert!(
        sub_resp["result"]["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = wss_connect(port, cfg.clone()).await;

    // Plain-JSON tool bodies so the bridge probes parse as JSON.
    let toon_off = wss_rpc(
        &mut rpc,
        5,
        "settings.update",
        json!({ "changes": [{ "path": "workspaceApi.toonOutput", "value": false }] }),
    )
    .await;
    assert_eq!(
        toon_off["result"]["applied"][0]["value"],
        json!(false),
        "toonOutput off: {toon_off}"
    );

    let created = wss_rpc(
        &mut rpc,
        10,
        "workspace.create",
        json!({
            "title": "snapshot prs WS",
            "branch": "feat/snapshot-prs-e2e",
            "idempotencyKey": "snapshot-prs-e2e-1",
            "initialAgent": {
                "prompt": "plain first turn",
                "name": "PrsAgent",
                "model": "default", "provider": "mock",
            },
        }),
    )
    .await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let agent = created["result"]["initialAgent"]["id"]
        .as_str()
        .expect("initial agent id")
        .to_string();
    await_stream_end(&mut sub, &agent).await;

    let configs = mcp_config_files(&data_dir);
    assert_eq!(configs.len(), 1, "one agent → one mcp config: {configs:?}");
    let mut bridge = BridgeClient::connect(&bridge_addr_from_config(&configs[0])).await;

    // Baseline: no tracked PRs → the field is absent (omitted when empty).
    let (err, text) = bridge.call_js("return await ws.agent.snapshot()").await;
    assert!(!err, "snapshot baseline: {text}");
    let v: Value = serde_json::from_str(&text).expect("snapshot tool returns JSON");
    assert!(v.get("prs").is_none(), "no tracked PRs yet: {v}");

    // Seed the workspace pool over the wire: repo identity + a tracked-PR
    // list covering every group plus a merged PR that must never appear.
    let ts = "2026-01-01T00:00:00Z";
    let pr = |number: u64, status: &str, extra: Value| {
        let mut base = json!({
            "id": format!("PR_{number}"),
            "number": number,
            "url": format!("https://github.com/o/r/pull/{number}"),
            "title": format!("pr {number}"),
            "status": status,
            "createdAt": ts,
            "updatedAt": ts,
        });
        base.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        base
    };
    let updated = wss_rpc(
        &mut rpc,
        11,
        "workspace.update",
        json!({
            "workspaceId": ws_id,
            "repositoryOwner": "o",
            "repositoryName": "r",
            "pullRequests": [
                pr(1, "Open", json!({ "isDraft": true })),
                pr(2, "Open", json!({ "mergeableState": "dirty" })),
                pr(3, "Open", json!({ "mergeable": true, "mergeableState": "clean" })),
                pr(4, "Open", json!({})),
                pr(5, "Merged", json!({})),
            ],
        }),
    )
    .await;
    assert_eq!(
        updated["result"]["workspace"]["pullRequests"]
            .as_array()
            .map(Vec::len),
        Some(5),
        "tracked PRs persisted: {updated}"
    );

    // The tool groups the open PRs by state; the merged PR is excluded.
    let expected_prs = json!({
        "draft": ["o/r#1"],
        "blocked": ["o/r#2"],
        "mergeable": ["o/r#3"],
        "unknown": ["o/r#4"],
    });
    let (err, text) = bridge.call_js("return await ws.agent.snapshot()").await;
    assert!(!err, "snapshot with tracked PRs: {text}");
    let v: Value = serde_json::from_str(&text).expect("snapshot tool returns JSON");
    assert_eq!(v["prs"], expected_prs, "grouped open PRs: {v}");

    // Open tracked PRs alone force the injection line on the next turn.
    let sent = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent, "content": "second turn" }),
    )
    .await;
    assert_eq!(sent["result"]["success"], true, "sendMessage ok: {sent}");
    await_stream_end(&mut sub, &agent).await;

    // No open PR left (merged/closed only): the field drops out and the
    // snapshot goes back to trivial, so turn 3 must carry no line.
    let cleared = wss_rpc(
        &mut rpc,
        13,
        "workspace.update",
        json!({
            "workspaceId": ws_id,
            "pullRequests": [pr(1, "Merged", json!({})), pr(2, "Closed", json!({}))],
        }),
    )
    .await;
    assert_eq!(
        cleared["result"]["workspace"]["pullRequests"]
            .as_array()
            .map(Vec::len),
        Some(2),
        "tracked PRs replaced: {cleared}"
    );
    let (err, text) = bridge.call_js("return await ws.agent.snapshot()").await;
    assert!(!err, "snapshot after clear: {text}");
    let v: Value = serde_json::from_str(&text).expect("snapshot tool returns JSON");
    assert!(
        v.get("prs").is_none(),
        "merged/closed-only pools omit prs: {v}"
    );

    let sent3 = wss_rpc(
        &mut rpc,
        14,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent, "content": "third turn" }),
    )
    .await;
    assert_eq!(sent3["result"]["success"], true, "sendMessage ok: {sent3}");
    await_stream_end(&mut sub, &agent).await;

    // ---- Prompt-log assertions ----
    let log = read_prompt_log(&prompt_log);
    assert!(
        log.len() >= 3,
        "expected 3 logged prompts, got {}: {log:?}",
        log.len()
    );

    // Turn 1: no tracked PRs → trivial snapshot → NO line.
    let (_, first) = &log[0];
    assert!(
        !first.contains(SNAPSHOT_PREFIX),
        "trivial snapshot must not inject on turn 1: {first:?}"
    );

    // Turn 2: open tracked PRs alone make the snapshot non-trivial → the
    // line leads the prompt and carries the grouped labels.
    let (_, second) = &log[1];
    let (snap, _) = split_snapshot(second)
        .unwrap_or_else(|| panic!("turn 2 must start with the snapshot line: {second:?}"));
    assert_eq!(snap["prs"], expected_prs, "prs rides the line: {snap}");
    assert!(snap["time"].is_string(), "time always present: {snap}");

    // Turn 3: merged/closed only → trivial again → NO line.
    let (_, third) = &log[2];
    assert!(
        !third.contains(SNAPSHOT_PREFIX),
        "no open tracked PR must not inject on turn 3: {third:?}"
    );
}
