//! WSS e2e for agent secret requests (PROTOCOL §5.56).
//!
//! Drives a top-level mock agent with `agentFeatures.secretRequests` on:
//!
//! * Turn 1: the mock calls `ws.secret.request`; the persisted final
//!   assistant message ends with one
//!   `application/vnd.intent.secret-request+json` card carrying the request
//!   id, agent, name and reason — never a value.
//! * `secret.provide` stores the value in daemon memory, delivers the
//!   provided notice (name only) as the agent's next turn, and is single-use.
//! * The agent's own bridge runs `ws.host.exec` with `secretEnv` and sees the
//!   value replaced with `[secret:NAME]`; a second top-level agent in the same
//!   workspace gets `secret-unavailable`. The wire `host.exec` and
//!   `ws.script.create` reject `secretEnv`.
//! * `secret.dismiss` delivers the declined notice; `ws.secret.forget` drops
//!   the value.
//! * The workspace shows `needs_attention` while the request is open, the
//!   hello advertises `secretRequests: 1`.
//! * Finally every file under the daemon's data dir (store, WAL, logs, the
//!   mock's prompt log) is scanned: the value must appear nowhere.
//!
//! Gated on `node` + the mock script; skips cleanly otherwise.

#![cfg(unix)]

use crate::common;

use intentd_test_support::GuardedChild;
use std::path::{Path, PathBuf};
use std::process::Stdio;
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

/// Fixed 64-hex token, adopted by the daemon via the `INTENTD_AUTH_TOKEN` seam.
const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

/// MIME type of a secret-request card (PROTOCOL §5.56).
const SECRET_MIME: &str = "application/vnd.intent.secret-request+json";

/// Turn-1 trigger: only this prompt makes the mock request a secret.
const ASK_MARKER: &str = "ASK_SECRET_NOW_E2E";

/// The secret value. Distinctive so the final data-dir scan cannot match
/// anything by accident.
const SECRET_VALUE: &str = "sv-7Qm2-e2e-Zx9Lk4-secret";

/// Live `intentd serve` process; killed and its data dir removed on drop.
struct Daemon {
    child: GuardedChild,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill_tree();
        let log_path = self.data_dir.join("daemon.log");
        if let Ok(log) = std::fs::read_to_string(&log_path) {
            eprintln!("=== DAEMON LOG ===\n{log}\n=== END LOG ===");
        }
    }
}

fn temp_data_dir() -> tempfile::TempDir {
    common::test_tempdir_in("/tmp", "itd-secret-")
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

async fn await_uds(socket: &Path) -> bool {
    timeout(common::daemon_startup_timeout(), async {
        loop {
            if UnixStream::connect(socket).await.is_ok() {
                return;
            }
            // timing-guard: poll interval while the daemon creates its socket; the outer timeout bounds the wait
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

/// Mock-agent gate (parity with the other WSS suites).
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

/// Drain subscriber events until an `agent:stream:end` for `agent_id` arrives;
/// returns the event's `data` payload so callers can assert its shape.
async fn await_stream_end<S>(sub: &mut WebSocketStream<S>, agent_id: &str) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    for _ in 0..120 {
        let frame = wss_event(sub, 30).await;
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:stream:end" && ev["data"]["agentId"].as_str() == Some(agent_id) {
            return ev["data"].clone();
        }
    }
    panic!("no agent:stream:end for {agent_id}");
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

/// Pre-seed the daemon's `SQLite` store with a regular (NON-chief) workspace —
/// the daemon opens the same data dir on launch.
async fn seed_workspace_only(data_dir: &Path, repository_path: Option<&Path>) -> String {
    use intent_core::{now_iso, Workspace, WorkspaceId};
    use intent_store::Store;
    let db_path = data_dir.join("intentd.db");
    let store = Store::open(&db_path).await.expect("open store");
    let ws = WorkspaceId::new();
    let ts = now_iso();
    store
        .insert_workspace(&Workspace {
            id: ws.clone(),
            title: "SECRET-E2E".to_string(),
            branch: "main".to_string(),
            created_at: ts.clone(),
            updated_at: ts,
            repository_path: repository_path.map(|path| path.to_string_lossy().into_owned()),
            repository_owner: repository_path.map(|_| "intent-hq".to_string()),
            repository_name: repository_path.map(|_| "intentd".to_string()),
            ..Default::default()
        })
        .await
        .expect("insert ws");
    ws.0
}

/// Send one JSON-RPC frame and return the whole response frame (result or
/// error) whose id matches.
async fn wss_rpc_raw<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
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

/// Poll `workspace.get` until its derived `displayStatus` equals `want`.
async fn poll_display_status<S>(rpc: &mut WebSocketStream<S>, ws_id: &str, want: &str)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut last = String::new();
    for attempt in 0..120 {
        let got = wss_rpc(
            rpc,
            900 + attempt,
            "workspace.get",
            json!({ "workspaceId": ws_id }),
        )
        .await;
        last = got["workspace"]["displayStatus"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        if last == want {
            return;
        }
        // timing-guard: poll interval for a derived status; the loop bound caps the wait
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("displayStatus never reached {want}; last {last}");
}

/// Write `[agentFeatures] secretRequests = true` before the daemon starts.
fn enable_secret_requests(data_dir: &Path) {
    std::fs::write(
        data_dir.join("config.toml"),
        "[agentFeatures]\nsecretRequests = true\n",
    )
    .expect("write config.toml");
}

/// Every file under `dir`, recursively.
fn all_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&next) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            match entry.file_type() {
                Ok(t) if t.is_dir() => stack.push(path),
                Ok(t) if t.is_file() => out.push(path),
                _ => {}
            }
        }
    }
    out
}

/// The decoded payload of the final assistant message's trailing secret card.
fn trailing_secret_card(conv: &Value) -> Value {
    let messages = conv["messages"].as_array().expect("messages array");
    let assistant = messages
        .iter()
        .rfind(|m| m["role"] == "assistant")
        .expect("final assistant message persisted");
    let blocks = assistant["contentBlocks"]
        .as_array()
        .expect("contentBlocks array");
    let last = blocks.last().expect("at least one block");
    assert_eq!(last["type"], "resource", "trailing block shape: {last}");
    assert_eq!(last["resource"]["mimeType"], SECRET_MIME, "MIME: {last}");
    let payload: Value = serde_json::from_str(
        last["resource"]["text"]
            .as_str()
            .expect("resource text string"),
    )
    .expect("resource text is JSON");
    assert_eq!(
        last["resource"]["uri"],
        json!(format!(
            "intent-secret-request://{}",
            payload["requestId"].as_str().expect("requestId")
        )),
        "URI names the request: {last}"
    );
    assert_eq!(last["resource"]["name"], payload["name"], "name: {last}");
    payload
}

/// Secret request round trip over the real WSS transport and MCP bridge.
#[tokio::test]
async fn secret_request_round_trip_over_wss() {
    let Some(script) = gate("WSS secret request E2E") else {
        return;
    };

    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    enable_secret_requests(&data_dir);
    let ws_id = seed_workspace_only(&data_dir, None).await;
    let prompt_log = data_dir.join("prompt-log.jsonl");
    let prompt_log_str = prompt_log.to_string_lossy().into_owned();
    let ask = "return await ws.secret.request({ name: 'E2E_KEY', reason: '  Needed to run the e2e check.  ' });";
    let behavior = json!({
        "rules": [{
            "ifPromptContains": ASK_MARKER,
            "toolCalls": [{
                "name": "workspace_api",
                "arguments": { "code": ask, "summary": "ask for a key" },
            }],
            "response": "I asked you for the key.",
        }],
        "response": "ok",
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_PROMPT_LOG", &prompt_log_str),
    ];
    let child = spawn_serve(&data_dir, &env);
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
    let hello = wss_rpc(&mut rpc, 9, "client.hello", json!({ "name": "secret-e2e" })).await;
    assert_eq!(
        hello["server"]["capabilities"]["secretRequests"],
        json!(1),
        "secretRequests advertised: {hello}"
    );
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Asker", "model": "default", "provider": "mock" }),
    )
    .await;
    let agent_a = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // ---- Turn 1: the mock requests a secret ----
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_a, "content": format!("set up the key {ASK_MARKER}") }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    await_stream_end(&mut sub, &agent_a).await;

    let conv = wss_rpc(
        &mut rpc,
        12,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_a }),
    )
    .await;
    let card = trailing_secret_card(&conv);
    let request_id = card["requestId"].as_str().expect("requestId").to_string();
    assert!(request_id.starts_with("sec-"), "request id: {request_id}");
    assert_eq!(
        card,
        json!({
            "requestId": request_id,
            "agentId": agent_a,
            "name": "E2E_KEY",
            "reason": "Needed to run the e2e check.",
        }),
        "card payload carries no value"
    );

    // An idle agent waiting on its secret request needs the user.
    poll_display_status(&mut rpc, &ws_id, "needs_attention").await;

    let listed = wss_rpc(
        &mut rpc,
        13,
        "secret.list",
        json!({ "workspaceId": ws_id, "agentId": agent_a }),
    )
    .await;
    assert_eq!(
        listed,
        json!({
            "secrets": [],
            "pendingRequests": [{ "requestId": request_id, "name": "E2E_KEY" }],
            "answeredRequests": [],
        })
    );

    // The wire host.exec has no agent identity: secretEnv is rejected.
    let wire_exec = wss_rpc_raw(
        &mut rpc,
        14,
        "host.exec",
        json!({ "workspaceId": ws_id, "command": "true", "secretEnv": ["E2E_KEY"] }),
    )
    .await;
    assert_eq!(
        wire_exec["error"]["code"],
        json!(-32602),
        "wire host.exec: {wire_exec}"
    );

    // ---- Provide: single-use, then the provided notice reaches the agent ----
    let provided = wss_rpc(
        &mut rpc,
        15,
        "secret.provide",
        json!({ "workspaceId": ws_id, "requestId": request_id, "value": SECRET_VALUE }),
    )
    .await;
    assert_eq!(
        provided,
        json!({ "ok": true, "requestId": request_id, "name": "E2E_KEY" })
    );
    await_stream_end(&mut sub, &agent_a).await;
    // Answered: nothing left that needs the user.
    poll_display_status(&mut rpc, &ws_id, "idle").await;
    let again = wss_rpc_raw(
        &mut rpc,
        16,
        "secret.provide",
        json!({ "workspaceId": ws_id, "requestId": request_id, "value": SECRET_VALUE }),
    )
    .await;
    assert_eq!(
        again["error"]["code"],
        json!(-32602),
        "second provide: {again}"
    );
    assert_eq!(
        again["error"]["data"]["code"],
        json!("secret-request-unknown")
    );
    assert!(
        !again.to_string().contains(SECRET_VALUE),
        "error echoes no value"
    );

    let listed = wss_rpc(
        &mut rpc,
        17,
        "secret.list",
        json!({ "workspaceId": ws_id, "agentId": agent_a }),
    )
    .await;
    assert_eq!(
        listed,
        json!({
            "secrets": [{ "name": "E2E_KEY" }],
            "pendingRequests": [],
            "answeredRequests": [{ "requestId": request_id, "name": "E2E_KEY", "outcome": "provided" }],
        })
    );
    let prompts = read_prompt_log(&prompt_log);
    assert!(
        prompts
            .iter()
            .any(|(_, text)| text.contains("The user provided secret E2E_KEY")),
        "provided notice reached the agent: {prompts:?}"
    );

    // ---- The owning agent uses it through ws.host.exec ----
    let configs_a = mcp_config_files(&data_dir);
    assert_eq!(
        configs_a.len(),
        1,
        "one agent → one mcp config: {configs_a:?}"
    );
    let mut bridge_a = BridgeClient::connect(&bridge_addr_from_config(&configs_a[0])).await;
    let desc = bridge_a.workspace_api_description().await;
    assert!(desc.contains("ws.secret.request("), "feature advertised");
    let (err, text) = bridge_a
        .call_js(
            "return await ws.host.exec({ command: 'sh', args: ['-c', 'printf \"v=%s\" \"$E2E_KEY\"; printf \"e=%s\" \"$E2E_KEY\" >&2'], secretEnv: ['E2E_KEY'] });",
        )
        .await;
    assert!(!err, "host.exec with secretEnv: {text}");
    assert!(
        text.contains("v=[secret:E2E_KEY]"),
        "stdout redacted: {text}"
    );
    assert!(
        text.contains("e=[secret:E2E_KEY]"),
        "stderr redacted: {text}"
    );
    assert!(!text.contains(SECRET_VALUE), "value never returned: {text}");
    let (err, text) = bridge_a
        .call_js(&format!(
            "return await ws.host.exec({{ command: 'sh', args: ['-c', 'test ${{#RENAMED}} -eq {}'], secretEnv: {{ RENAMED: 'E2E_KEY' }} }});",
            SECRET_VALUE.len()
        ))
        .await;
    assert!(!err, "mapped secretEnv: {text}");
    assert!(
        text.contains("exitCode: 0") || text.contains("\"exitCode\": 0"),
        "mapped variable carries the value: {text}"
    );
    let (err, text) = bridge_a.call_js("return await ws.secret.list();").await;
    assert!(!err, "ws.secret.list: {text}");
    assert!(
        text.contains("E2E_KEY") && !text.contains("answeredRequests"),
        "{text}"
    );
    let (err, text) = bridge_a
        .call_js(
            "return await ws.script.create('s', 'true', 'command', { secretEnv: ['E2E_KEY'] });",
        )
        .await;
    assert!(err, "script secretEnv must be refused: {text}");
    assert!(
        text.contains("secrets work only with ws.host.exec"),
        "{text}"
    );

    // ---- Another top-level agent in the same workspace cannot use it ----
    let created_b = wss_rpc(
        &mut rpc,
        20,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Other", "model": "default", "provider": "mock" }),
    )
    .await;
    let agent_b = created_b["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let sent_b = wss_rpc(
        &mut rpc,
        21,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_b, "content": "say ok" }),
    )
    .await;
    assert_eq!(sent_b["success"], true, "sendMessage ok: {sent_b}");
    await_stream_end(&mut sub, &agent_b).await;
    let config_b = mcp_config_files(&data_dir)
        .into_iter()
        .find(|p| !configs_a.contains(p))
        .expect("mcp config for the second agent");
    let mut bridge_b = BridgeClient::connect(&bridge_addr_from_config(&config_b)).await;
    let (err, text) = bridge_b
        .call_js("return await ws.host.exec({ command: 'true', secretEnv: ['E2E_KEY'] });")
        .await;
    assert!(err, "another agent must not use the secret: {text}");
    assert!(text.contains("secret-unavailable"), "{text}");
    let (err, text) = bridge_b.call_js("return await ws.secret.list();").await;
    assert!(!err, "{text}");
    assert!(
        !text.contains("E2E_KEY"),
        "another agent lists nothing: {text}"
    );
    let listed_b = wss_rpc(
        &mut rpc,
        22,
        "secret.list",
        json!({ "workspaceId": ws_id, "agentId": agent_b }),
    )
    .await;
    assert_eq!(listed_b["secrets"], json!([]));

    // ---- Decline path ----
    let (err, text) = bridge_a
        .call_js("return await ws.secret.request({ name: 'OTHER_KEY', reason: 'second key' });")
        .await;
    assert!(!err, "second request: {text}");
    let listed = wss_rpc(
        &mut rpc,
        30,
        "secret.list",
        json!({ "workspaceId": ws_id, "agentId": agent_a }),
    )
    .await;
    let other_request = listed["pendingRequests"][0]["requestId"]
        .as_str()
        .expect("pending OTHER_KEY request")
        .to_string();
    let dismissed = wss_rpc(
        &mut rpc,
        31,
        "secret.dismiss",
        json!({ "workspaceId": ws_id, "requestId": other_request }),
    )
    .await;
    assert_eq!(
        dismissed,
        json!({ "ok": true, "requestId": other_request, "name": "OTHER_KEY" })
    );
    await_stream_end(&mut sub, &agent_a).await;
    let prompts = read_prompt_log(&prompt_log);
    assert!(
        prompts
            .iter()
            .any(|(_, text)| text.contains("The user declined to provide secret OTHER_KEY")),
        "declined notice reached the agent: {prompts:?}"
    );

    // ---- Forget drops the value ----
    let (err, text) = bridge_a
        .call_js("return await ws.secret.forget('E2E_KEY');")
        .await;
    assert!(!err, "{text}");
    assert!(
        text.contains("forgotten: true") || text.contains("\"forgotten\": true"),
        "{text}"
    );
    let (err, text) = bridge_a
        .call_js("return await ws.host.exec({ command: 'true', secretEnv: ['E2E_KEY'] });")
        .await;
    assert!(err && text.contains("secret-unavailable"), "{text}");

    // ---- The value never reached disk, logs or the model ----
    drop(bridge_a);
    drop(bridge_b);
    drop(daemon);
    for path in all_files(&data_dir) {
        let bytes = std::fs::read(&path).unwrap_or_default();
        assert!(
            !bytes
                .windows(SECRET_VALUE.len())
                .any(|w| w == SECRET_VALUE.as_bytes()),
            "secret value leaked into {}",
            path.display()
        );
    }
}

fn mcp_config_files(data_dir: &Path) -> Vec<PathBuf> {
    let dir = data_dir.join("agent-configs");
    let mut out: Vec<PathBuf> = std::fs::read_dir(&dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| {
                    p.file_name().and_then(|n| n.to_str()).is_some_and(|n| {
                        n.starts_with("intentd-mcp-")
                            && Path::new(n)
                                .extension()
                                .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
                    })
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
    reader: tokio::io::BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
    next_id: i64,
}

impl BridgeClient {
    async fn connect(addr: &str) -> Self {
        use tokio::io::BufReader;
        let stream = timeout(
            intentd_test_support::test_timeout(Duration::from_secs(10)),
            TcpStream::connect(addr),
        )
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
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
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
            let n = timeout(
                intentd_test_support::test_timeout(Duration::from_secs(30)),
                self.reader.read_line(&mut buf),
            )
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
                    "arguments": { "code": code, "summary": "e2e secret request probe" },
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
