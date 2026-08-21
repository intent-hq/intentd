// WSS e2e coverage for `[agentFeatures]` gating (new-sessions-only).
//
// Drives the full toggle flow over the real WSS transport:
//   1. `settings.get` / `settings.update` / `settings.reset` round-trip for all
//      eleven `agentFeatures.*` paths (defaults on, except the opt-in
//      `taskGraph`).
//   2. Full session (defaults on): assembled system prompt CONTAINS the gated
//      sections, the per-agent MCP bridge advertises the full `workspace_api`
//      surface, and the gated `host({...})` methods dispatch successfully.
//   3. Flip `backgroundHooks` / `hostExec` / `terminalAccess` /
//      `richChatBlocks` / `attentionRequests` / `prMonitor` off via
//      `settings.update`, create a NEW session: prompt sections absent, tool
//      description pruned, dispatch denied with the explicit "disabled in
//      settings (<path> = false)" error (`hook.schedule` and the `pr.monitor*`
//      trio included, while method-level gating leaves `pr.snapshot` intact).
//   4. New-sessions-only: the EXISTING session's bridge — captured at agent
//      creation — still advertises the full surface and still dispatches the
//      gated methods after the flip.

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

const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

type Ws = WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;

/// The eleven `agentFeatures.*` settings paths with their defaults — all on
/// except the opt-in `taskGraph` (intent-hq/monorepo#2445).
const FEATURE_PATHS: [(&str, bool); 11] = [
    ("agentFeatures.backgroundHooks", true),
    ("agentFeatures.hostExec", true),
    ("agentFeatures.scripts", true),
    ("agentFeatures.terminalAccess", true),
    ("agentFeatures.browserAutomation", true),
    ("agentFeatures.richChatBlocks", true),
    ("agentFeatures.structuredQuestions", true),
    ("agentFeatures.attentionRequests", true),
    ("agentFeatures.stateSnapshot", true),
    ("agentFeatures.prMonitor", true),
    ("agentFeatures.taskGraph", false),
];

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
    let dir = PathBuf::from("/tmp").join(format!("itd-afg-{}", &id[..8]));
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

fn gate(name: &str) -> Option<String> {
    let key = "MOCK_AGENT_SCRIPT_PATH";
    match std::env::var(key) {
        Ok(path) if !path.is_empty() => Some(path),
        _ => {
            eprintln!("skipping {name}: {key} not set");
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Per-agent MCP bridge client: parse the generated `intentd-mcp-*.json` and
// speak newline-delimited JSON-RPC to the loopback bridge directly, exactly
// like a spawned provider child would.
// ---------------------------------------------------------------------------

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

    /// `tools/list` → the `workspace_api` tool description.
    async fn workspace_api_description(&mut self) -> String {
        let resp = self.request("tools/list", json!({})).await;
        let tools = resp["result"]["tools"].as_array().expect("tools array");
        tools
            .iter()
            .find(|t| t["name"] == "workspace_api")
            .expect("workspace_api tool listed")["description"]
            .as_str()
            .expect("description string")
            .to_string()
    }

    /// `tools/call workspace_api` with agent JS; returns `(is_error, text)`.
    async fn call_js(&mut self, code: &str) -> (bool, String) {
        let resp = self
            .request(
                "tools/call",
                json!({
                    "name": "workspace_api",
                    "arguments": { "code": code, "summary": "e2e feature-gate probe" },
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

// ---------------------------------------------------------------------------
// Test 1: settings round-trip for all nine agentFeatures.* paths over WSS.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn agent_features_settings_round_trip() {
    let data_dir = temp_data_dir();
    let socket = data_dir.join("intentd.sock");

    let mut _daemon = Daemon {
        child: spawn_serve(
            &data_dir,
            &[("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")],
        ),
        data_dir: data_dir.clone(),
    };
    assert!(await_uds(&socket).await, "daemon did not start");

    let status = common::await_wss_status(&socket).await;
    let fp = status["result"]["fingerprint"].as_str().expect("fp");
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let mut ws = wss_connect(port, client_config(fp)).await;

    let mut id = 100;
    for (path, default) in FEATURE_PATHS {
        // Default value.
        let got = wss_rpc(&mut ws, id, "settings.get", json!({ "path": path })).await;
        assert_eq!(
            got["result"]["value"],
            json!(default),
            "{path} should default to {default}: {got}"
        );
        id += 1;

        // Flip.
        let upd = wss_rpc(
            &mut ws,
            id,
            "settings.update",
            json!({ "changes": [{ "path": path, "value": !default }] }),
        )
        .await;
        assert_eq!(upd["result"]["applied"][0]["path"], path, "applied: {upd}");
        assert_eq!(upd["result"]["applied"][0]["value"], json!(!default));
        id += 1;

        // Persisted.
        let got2 = wss_rpc(&mut ws, id, "settings.get", json!({ "path": path })).await;
        assert_eq!(
            got2["result"]["value"],
            json!(!default),
            "{path} should now be {}",
            !default
        );
        id += 1;

        // Reset restores the default.
        let reset = wss_rpc(&mut ws, id, "settings.reset", json!({ "path": path })).await;
        assert_eq!(
            reset["result"]["value"],
            json!(default),
            "{path} reset should restore default {default}"
        );
        id += 1;
    }
}

// ---------------------------------------------------------------------------
// Test 2: full toggle flow — prompt pruning, tool-description pruning,
// dispatch denial, and new-sessions-only semantics.
// ---------------------------------------------------------------------------

#[allow(clippy::similar_names)] // deliberate parallel naming across the scenario's instances
#[tokio::test]
async fn agent_features_gate_new_sessions_only() {
    let Some(script) = gate("agentFeatures gating E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let socket = data_dir.join("intentd.sock");
    let behavior = json!({ "response": "done" }).to_string();

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

    // Plain-JSON tool bodies so the JS probes below can assert on exact
    // pretty-printed fragments (`workspaceApi.toonOutput` is read live per
    // invocation, so this applies to both sessions).
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

    // ===== Session A: all defaults on =====
    let created = wss_rpc(
        &mut rpc,
        10,
        "workspace.create",
        json!({
            "title": "agentFeatures full WS",
            "branch": "feat/agent-features-full-e2e",
            "idempotencyKey": "agent-features-e2e-1",
            "initialAgent": {
                "prompt": "say done",
                "name": "Full Surface Agent",
                "model": "mock:default",
            },
        }),
    )
    .await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let agent_a = created["result"]["initialAgent"]["id"]
        .as_str()
        .expect("initial agent id")
        .to_string();
    await_stream_end(&mut sub, &agent_a).await;

    // A's system prompt carries the gated sections (defaults on).
    let session_a = wss_rpc(
        &mut rpc,
        20,
        "agent.getSession",
        json!({ "agentId": agent_a }),
    )
    .await;
    let prompt_a = session_a["result"]["session"]["systemPrompt"]
        .as_str()
        .expect("systemPrompt populated for A");
    assert!(
        prompt_a.contains("## Waiting on External Conditions"),
        "full prompt must contain the backgroundHooks section"
    );
    assert!(
        prompt_a.contains("## Rich Chat Rendering"),
        "full prompt must contain the richChatBlocks section"
    );
    assert!(
        prompt_a.contains("## Raising Attention"),
        "full prompt must contain the attentionRequests section"
    );
    // `taskGraph` is the opt-in exception: with defaults, the task-relations
    // teaching is absent from the prompt (intent-hq/monorepo#2445).
    assert!(
        !prompt_a.contains("### Task relations during delegation"),
        "default prompt must not teach task relations (taskGraph opt-in)"
    );
    assert!(
        prompt_a.contains("## Delegating Tasks"),
        "single-task delegation guidance must survive taskGraph off"
    );

    // A's bridge (from the generated per-agent MCP config) advertises the
    // full workspace_api surface and dispatches gated methods.
    let configs_a = mcp_config_files(&data_dir);
    assert_eq!(
        configs_a.len(),
        1,
        "one agent → one mcp config: {configs_a:?}"
    );
    let addr_a = bridge_addr_from_config(&configs_a[0]);
    let mut bridge_a = BridgeClient::connect(&addr_a).await;

    let desc_a = bridge_a.workspace_api_description().await;
    for marker in [
        "ws.host.exec(",
        "ws.hook.schedule(",
        "ws.terminal.list()",
        "ws.terminal.readOutput(",
        "ws.browser.exec(",
        "ws.script.run(",
        "ws.agent.reportBlocker(",
        "ws.agent.requestDiscussion(",
        "ws.pr.monitor(",
        "ws.pr.unmonitor(",
        "ws.pr.monitors()",
    ] {
        assert!(desc_a.contains(marker), "full description missing {marker}");
    }

    let (err, text) = bridge_a.call_js("return await ws.hook.list()").await;
    assert!(!err, "ws.hook.list on full session must succeed: {text}");
    let (err, text) = bridge_a.call_js("return await ws.terminal.list()").await;
    assert!(
        !err,
        "ws.terminal.list on full session must succeed: {text}"
    );
    let (err, text) = bridge_a
        .call_js("return await ws.host.exec({ command: '/bin/echo', args: ['ok'] })")
        .await;
    assert!(!err, "ws.host.exec on full session must succeed: {text}");
    // The attention-request bindings are installed on the full session
    // (typeof probe — invoking them would raise a real attention request).
    let (err, text) = bridge_a
        .call_js(
            "return { rb: typeof ws.agent.reportBlocker, rd: typeof ws.agent.requestDiscussion }",
        )
        .await;
    assert!(!err, "typeof probe on full session must succeed: {text}");
    assert!(
        text.contains("\"rb\": \"function\"") && text.contains("\"rd\": \"function\""),
        "attention-request bindings must be installed on the full session: {text}"
    );
    // The PR-monitor bindings are installed on the full session (typeof probe
    // — registering would hit a real forge).
    let (err, text) = bridge_a
        .call_js(
            "return { m: typeof ws.pr.monitor, u: typeof ws.pr.unmonitor, l: typeof ws.pr.monitors, s: typeof ws.pr.snapshot }",
        )
        .await;
    assert!(!err, "ws.pr typeof probe must succeed: {text}");
    assert!(
        text.contains("\"m\": \"function\"")
            && text.contains("\"u\": \"function\"")
            && text.contains("\"l\": \"function\""),
        "pr-monitor bindings must be installed on the full session: {text}"
    );

    // ===== Flip the toggles off =====
    let flip = wss_rpc(
        &mut rpc,
        30,
        "settings.update",
        json!({ "changes": [
            { "path": "agentFeatures.backgroundHooks", "value": false },
            { "path": "agentFeatures.hostExec", "value": false },
            { "path": "agentFeatures.terminalAccess", "value": false },
            { "path": "agentFeatures.richChatBlocks", "value": false },
            { "path": "agentFeatures.attentionRequests", "value": false },
            { "path": "agentFeatures.prMonitor", "value": false },
        ] }),
    )
    .await;
    assert_eq!(
        flip["result"]["applied"].as_array().map(Vec::len),
        Some(6),
        "all six toggles applied: {flip}"
    );

    // ===== Session B: created AFTER the flip =====
    let created_b = wss_rpc(
        &mut rpc,
        40,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Gated Agent", "model": "mock:default" }),
    )
    .await;
    let agent_b = created_b["result"]["agent"]["id"]
        .as_str()
        .expect("agent B id")
        .to_string();
    let sent = wss_rpc(
        &mut rpc,
        41,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_b, "content": "say done" }),
    )
    .await;
    assert_eq!(sent["result"]["success"], true, "sendMessage ok: {sent}");
    await_stream_end(&mut sub, &agent_b).await;

    // B's system prompt lacks the gated sections; ungated sections remain.
    let session_b = wss_rpc(
        &mut rpc,
        50,
        "agent.getSession",
        json!({ "agentId": agent_b }),
    )
    .await;
    let prompt_b = session_b["result"]["session"]["systemPrompt"]
        .as_str()
        .expect("systemPrompt populated for B");
    assert!(
        !prompt_b.contains("## Waiting on External Conditions"),
        "gated prompt must NOT contain the backgroundHooks section"
    );
    assert!(
        !prompt_b.contains("## Rich Chat Rendering"),
        "gated prompt must NOT contain the richChatBlocks section"
    );
    assert!(
        !prompt_b.contains("## Raising Attention"),
        "gated prompt must NOT contain the attentionRequests section"
    );
    assert!(
        prompt_b.contains("## Response Organization"),
        "ungated common sections must survive pruning"
    );

    // B's bridge: description pruned of every gated namespace.
    let configs_b = mcp_config_files(&data_dir);
    assert_eq!(
        configs_b.len(),
        2,
        "two agents → two mcp configs: {configs_b:?}"
    );
    let config_b = configs_b
        .iter()
        .find(|p| !configs_a.contains(p))
        .expect("new mcp config for agent B");
    let addr_b = bridge_addr_from_config(config_b);
    let mut bridge_b = BridgeClient::connect(&addr_b).await;

    let desc_b = bridge_b.workspace_api_description().await;
    for gated in [
        "ws.host.",
        "ws.hook.",
        "ws.terminal.",
        "ws.agent.reportBlocker",
        "ws.agent.requestDiscussion",
        "pr.monitor",
    ] {
        assert!(
            !desc_b.contains(gated),
            "gated description must not mention {gated}"
        );
    }
    for kept in [
        "ws.browser.exec(",
        "ws.script.run(",
        "ws.note.read(",
        "ws.agent.reportToParent(",
        // `prMonitor` is method-level: `ws.pr.snapshot` survives it.
        "ws.pr.snapshot(",
    ] {
        assert!(desc_b.contains(kept), "gated description missing {kept}");
    }

    // B's JS prelude: the gated `ws.*` namespaces are not even defined, and
    // the method-level attentionRequests gate leaves the rest of `ws.agent`
    // installed while dropping the two attention-request bindings.
    let (err, text) = bridge_b
        .call_js("return { hook: typeof ws.hook, host: typeof ws.host, terminal: typeof ws.terminal, browser: typeof ws.browser, rb: typeof ws.agent.reportBlocker, rd: typeof ws.agent.requestDiscussion, rtp: typeof ws.agent.reportToParent }")
        .await;
    assert!(!err, "typeof probe must succeed: {text}");
    assert!(
        text.contains("\"hook\": \"undefined\"")
            && text.contains("\"host\": \"undefined\"")
            && text.contains("\"terminal\": \"undefined\""),
        "gated prelude namespaces must be undefined on B: {text}"
    );
    assert!(
        text.contains("\"rb\": \"undefined\"") && text.contains("\"rd\": \"undefined\""),
        "gated attention-request bindings must be undefined on B: {text}"
    );
    assert!(
        text.contains("\"browser\": \"object\"") && text.contains("\"rtp\": \"function\""),
        "ungated prelude surface must survive on B: {text}"
    );
    // `prMonitor` is method-level: `ws.pr` stays installed with only
    // `snapshot` on it.
    let (err, text) = bridge_b
        .call_js(
            "return { m: typeof ws.pr.monitor, u: typeof ws.pr.unmonitor, l: typeof ws.pr.monitors, s: typeof ws.pr.snapshot }",
        )
        .await;
    assert!(!err, "ws.pr typeof probe must succeed on B: {text}");
    assert!(
        text.contains("\"m\": \"undefined\"")
            && text.contains("\"u\": \"undefined\"")
            && text.contains("\"l\": \"undefined\""),
        "gated pr-monitor bindings must be undefined on B: {text}"
    );
    assert!(
        text.contains("\"s\": \"function\""),
        "ws.pr.snapshot must survive the prMonitor gate on B: {text}"
    );

    // B's dispatch (defense in depth): raw `host({...})` frames that bypass
    // the pruned prelude are denied with the explicit settings error.
    let (err, text) = bridge_b
        .call_js("return await host({ method: 'hook.schedule', args: { name: 'x', code: 'return {}', delayMs: 10000 } })")
        .await;
    assert!(err, "hook.schedule must be denied: {text}");
    assert!(
        text.contains(
            "host: method `hook.schedule` is disabled in settings (agentFeatures.backgroundHooks = false)"
        ),
        "hook.schedule denial must name the toggle: {text}"
    );
    let (err, text) = bridge_b
        .call_js("return await host({ method: 'host.exec', args: { command: '/bin/echo', args: ['ok'] } })")
        .await;
    assert!(err, "host.exec must be denied: {text}");
    assert!(
        text.contains(
            "host: method `host.exec` is disabled in settings (agentFeatures.hostExec = false)"
        ),
        "host.exec denial must name the toggle: {text}"
    );
    let (err, text) = bridge_b
        .call_js("return await host({ method: 'terminal.list' })")
        .await;
    assert!(err, "terminal.list must be denied: {text}");
    assert!(
        text.contains(
            "host: method `terminal.list` is disabled in settings (agentFeatures.terminalAccess = false)"
        ),
        "terminal.list denial must name the toggle: {text}"
    );
    let (err, text) = bridge_b
        .call_js("return await host({ method: 'agent.reportBlocker', args: { reason: 'r' } })")
        .await;
    assert!(err, "agent.reportBlocker must be denied: {text}");
    assert!(
        text.contains(
            "host: method `agent.reportBlocker` is disabled in settings (agentFeatures.attentionRequests = false)"
        ),
        "agent.reportBlocker denial must name the toggle: {text}"
    );
    let (err, text) = bridge_b
        .call_js("return await host({ method: 'agent.requestDiscussion', args: { reason: 'r' } })")
        .await;
    assert!(err, "agent.requestDiscussion must be denied: {text}");
    assert!(
        text.contains(
            "host: method `agent.requestDiscussion` is disabled in settings (agentFeatures.attentionRequests = false)"
        ),
        "agent.requestDiscussion denial must name the toggle: {text}"
    );
    for method in ["pr.monitor", "pr.unmonitor", "pr.monitors"] {
        let (err, text) = bridge_b
            .call_js(&format!(
                "return await host({{ method: '{method}', args: {{ prNumber: 1 }} }})"
            ))
            .await;
        assert!(err, "{method} must be denied: {text}");
        assert!(
            text.contains(&format!(
                "host: method `{method}` is disabled in settings (agentFeatures.prMonitor = false)"
            )),
            "{method} denial must name the toggle: {text}"
        );
    }
    // Defense in depth cuts only the monitor methods: `pr.snapshot` still
    // dispatches (it fails on the missing forge, not on the gate).
    let (_err, text) = bridge_b
        .call_js("return await host({ method: 'pr.snapshot', args: { prNumber: 1 } })")
        .await;
    assert!(
        !text.contains("disabled in settings"),
        "pr.snapshot must not be gated by prMonitor: {text}"
    );

    // Control: an ungated namespace still dispatches on B.
    let (err, text) = bridge_b.call_js("return await ws.script.list()").await;
    assert!(!err, "ws.script.list must stay available on B: {text}");
    // Control: sibling ws.agent.* methods still dispatch on B.
    let (err, text) = bridge_b.call_js("return await ws.agent.list()").await;
    assert!(!err, "ws.agent.list must stay available on B: {text}");

    // ===== New-sessions-only: session A is unaffected by the flip =====
    let mut bridge_a2 = BridgeClient::connect(&addr_a).await;
    let desc_a2 = bridge_a2.workspace_api_description().await;
    assert_eq!(
        desc_a, desc_a2,
        "A's tool description must be unchanged after the flip"
    );
    let (err, text) = bridge_a2.call_js("return await ws.terminal.list()").await;
    assert!(
        !err,
        "ws.terminal.list on the pre-flip session must still succeed: {text}"
    );
    let (err, text) = bridge_a2
        .call_js("return await ws.host.exec({ command: '/bin/echo', args: ['still-ok'] })")
        .await;
    assert!(
        !err,
        "ws.host.exec on the pre-flip session must still succeed: {text}"
    );
    // A's persisted prompt is untouched by the settings change.
    let session_a2 = wss_rpc(
        &mut rpc,
        60,
        "agent.getSession",
        json!({ "agentId": agent_a }),
    )
    .await;
    let prompt_a2 = session_a2["result"]["session"]["systemPrompt"]
        .as_str()
        .expect("systemPrompt still populated for A");
    assert!(
        prompt_a2.contains("## Waiting on External Conditions")
            && prompt_a2.contains("## Rich Chat Rendering")
            && prompt_a2.contains("## Raising Attention"),
        "A's persisted prompt must keep the gated sections after the flip"
    );
}

// ---------------------------------------------------------------------------
// Test 3: specialist `modelOptions` (PROTOCOL §5.11) surface in the per-agent
// bridge's `workspace_api` description — delegate docs list the compound ids
// and hints of every visible specialist that carries options, while a bridge
// created with no such specialist keeps the default description free of the
// injected block.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn specialist_model_options_surface_in_bridge_description() {
    let Some(script) = gate("specialist modelOptions description E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let socket = data_dir.join("intentd.sock");
    let behavior = json!({ "response": "done" }).to_string();

    // Hermetic user tier: HOME=data_dir so the daemon reads
    // $HOME/.intent/specialists/. One specialist carries options (with and
    // without a hint), one does not.
    let specialists_dir = data_dir.join(".intent").join("specialists");
    std::fs::create_dir_all(&specialists_dir).expect("mkdir specialists dir");
    std::fs::write(
        specialists_dir.join("chooser.md"),
        "---\nname: \"Chooser\"\ndescription: \"Has options\"\nmodelOptions: [{\"model\":\"opencode:kimi-k3\",\"hint\":\"cheap\"},{\"model\":\"auggie:opus\"}]\n---\n\nChooser body.",
    )
    .expect("write chooser specialist");
    std::fs::write(
        specialists_dir.join("plain.md"),
        "---\nname: \"Plain\"\ndescription: \"No options\"\n---\n\nPlain body.",
    )
    .expect("write plain specialist");

    let home = data_dir.to_str().expect("data_dir to str").to_string();
    let mut _daemon = Daemon {
        child: spawn_serve(
            &data_dir,
            &[
                ("INTENTD_AUTH_TOKEN", TOKEN),
                ("INTENTD_TCP_PORT", "0"),
                ("MOCK_AGENT_SCRIPT_PATH", &script),
                ("MOCK_AGENT_BEHAVIOR", &behavior),
                ("HOME", &home),
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
    let created = wss_rpc(
        &mut rpc,
        10,
        "workspace.create",
        json!({
            "title": "modelOptions description WS",
            "branch": "feat/model-options-desc-e2e",
            "idempotencyKey": "model-options-desc-e2e-1",
            "initialAgent": {
                "prompt": "say done",
                "name": "Options Agent",
                "model": "mock:default",
            },
        }),
    )
    .await;
    let agent = created["result"]["initialAgent"]["id"]
        .as_str()
        .expect("initial agent id")
        .to_string();
    await_stream_end(&mut sub, &agent).await;

    let configs = mcp_config_files(&data_dir);
    assert_eq!(configs.len(), 1, "one agent → one mcp config: {configs:?}");
    let addr = bridge_addr_from_config(&configs[0]);
    let mut bridge = BridgeClient::connect(&addr).await;

    let desc = bridge.workspace_api_description().await;
    assert!(
        desc.contains("Specialist model options"),
        "delegate docs must carry the options header:\n{desc}"
    );
    assert!(
        desc.contains(
            "chooser: default: provider default, `opencode:kimi-k3` (cheap), `auggie:opus`"
        ),
        "options line must name the resolved default then compound ids + hints in order: {desc}"
    );
    assert!(
        !desc.contains("plain:"),
        "specialists without options must not be listed"
    );
    // The block reads as part of the delegate entry: between the
    // `ws.agent.delegate` doc line and the next method line.
    let delegate_idx = desc.find("ws.agent.delegate(").expect("delegate line");
    let block_idx = desc.find("Specialist model options").expect("block");
    let send_idx = desc[delegate_idx..]
        .find("ws.agent.send(")
        .map(|i| i + delegate_idx)
        .expect("send line after delegate");
    assert!(
        delegate_idx < block_idx && block_idx < send_idx,
        "options block must sit inside the delegate docs"
    );
}
