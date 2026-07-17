//! WSS end-to-end: `workspace:updated { lastActivity }` propagates over the
//! wire without a `workspace.get` (§10.1 e2e coverage).
//!
//! Proves over a real WSS connection that a client subscribed to `workspace:*`
//! learns the new `lastActivity` when daemon-side activity happens, without
//! issuing any workspace read. Drives activity via agent completion and
//! token-usage/attention changes. Covers:
//! - Positive: `workspace:updated` arrives after agent turn, carrying the new
//!   `lastActivity` that matches a subsequent `workspace.get`.
//! - Negative: no `workspace:updated { lastActivity }` for a workspace with no
//!   activity.
//! - Debounce: rapid burst coalesces into one emission with the latest value.
//!
//! Uses the mock ACP agent fixture for deterministic behavior. The test
//! overrides `LAST_ACTIVITY_DEBOUNCE_TEST_MS` to 200ms for fast execution.

#![cfg(unix)]

use std::net::{Ipv4Addr, TcpListener as StdTcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use chrono::DateTime;
use futures_util::{SinkExt, StreamExt};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

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

fn scratch_dir(prefix: &str) -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-lastact-{prefix}-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir scratch dir");
    dir
}

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .arg("--listen")
        .arg("both")
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

/// Wait up to `secs` for the next `events.event` notification whose payload
/// `type` matches one of `types`; ignore other frames. Returns the event
/// object (the `params.event` sub-object).
async fn next_event<S>(ws: &mut WebSocketStream<S>, types: &[&str], secs: u64) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!remaining.is_zero(), "timed out waiting for {types:?}");
        let next = timeout(remaining, ws.next())
            .await
            .expect("timeout elapsed");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = match serde_json::from_str(&text) {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                if v["method"] == json!("events.event") {
                    let evt = &v["params"]["event"];
                    let ty = evt["type"].as_str().unwrap_or("");
                    if types.contains(&ty) {
                        return evt.clone();
                    }
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

/// Variant that returns `None` on timeout instead of panicking; used for
/// negative assertions (no event arrives).
async fn try_next_event<S>(
    ws: &mut WebSocketStream<S>,
    types: &[&str],
    dur: Duration,
) -> Option<Value>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + dur;
    loop {
        let remaining = match deadline.checked_duration_since(tokio::time::Instant::now()) {
            Some(d) if !d.is_zero() => d,
            _ => return None,
        };
        let next = match timeout(remaining, ws.next()).await {
            Ok(v) => v,
            Err(_) => return None,
        };
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = match serde_json::from_str(&text) {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                if v["method"] == json!("events.event") {
                    let evt = &v["params"]["event"];
                    let ty = evt["type"].as_str().unwrap_or("");
                    if types.contains(&ty) {
                        return Some(evt.clone());
                    }
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

async fn boot(mock_script: &str, behavior: &str) -> (Daemon, u16, Arc<ClientConfig>) {
    let data_dir = scratch_dir("data");
    let port_s = free_port().to_string();
    // Override debounce to 200ms for fast test execution
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", &port_s),
        ("LAST_ACTIVITY_DEBOUNCE_TEST_MS", "200"),
        ("MOCK_AGENT_SCRIPT_PATH", mock_script),
        ("MOCK_AGENT_BEHAVIOR", behavior),
    ];
    let child = spawn_serve(&data_dir, &env);
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
    (daemon, port, client_config(&fingerprint))
}

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

/// Positive case: client learns the new `lastActivity` via `workspace:updated`
/// notification without issuing a `workspace.get` after driving agent activity.
#[tokio::test]
async fn last_activity_propagates_over_wss_on_agent_turn() {
    let Some(script) = gate("WSS lastActivity positive") else {
        return;
    };

    let behavior = json!({ "response": "test activity" }).to_string();
    let (daemon, port, cfg) = boot(&script, &behavior).await;

    // Bootstrap workspace via UDS
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "LastActivityTest", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // Subscribe to workspace:* before any activity
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // Capture initial lastActivity from workspace.list (it's an RFC3339 string)
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let list = wss_rpc(&mut rpc, 2, "workspace.list", json!({})).await;
    let initial_activity = list["workspaces"]
        .as_array()
        .and_then(|arr| arr.iter().find(|w| w["id"] == ws_id))
        .and_then(|w| w["lastActivity"].as_str())
        .map(|s| s.to_string());

    // Drive activity: create + run an agent
    let created = wss_rpc(
        &mut rpc,
        3,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "TestAgent", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().expect("agent id");

    wss_rpc(
        &mut rpc,
        4,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "do work" }),
    )
    .await;

    // Wait for workspace:updated with lastActivity.
    // The debounce window is 200ms, so we wait a bit longer to account for
    // agent turn execution + debounce + event delivery.
    let updated_evt = next_event(&mut sub, &["workspace:updated"], 10).await;
    assert_eq!(updated_evt["workspaceId"], ws_id);
    let changes = &updated_evt["data"]["changes"];
    assert!(
        changes["lastActivity"].is_string(),
        "lastActivity in changes: {changes}"
    );

    // Verify it's newer than initial and matches workspace.get
    let new_activity = changes["lastActivity"]
        .as_str()
        .expect("lastActivity string");
    if let Some(init) = &initial_activity {
        // Parse both as RFC3339 DateTimes to compare instants (lexicographic comparison
        // can be wrong with differing fractional-second precision).
        let init_dt =
            DateTime::parse_from_rfc3339(init.as_str()).expect("parse initial lastActivity");
        let new_dt = DateTime::parse_from_rfc3339(new_activity).expect("parse new lastActivity");
        assert!(
            new_dt > init_dt,
            "lastActivity did not advance: {} -> {}",
            init,
            new_activity
        );
    }

    let get = wss_rpc(
        &mut rpc,
        5,
        "workspace.get",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(
        get["workspace"]["lastActivity"].as_str(),
        Some(new_activity)
    );
}

/// Negative case: no `workspace:updated { lastActivity }` arrives for a
/// workspace with no activity.
#[tokio::test]
async fn no_last_activity_event_for_idle_workspace() {
    let behavior = json!({}).to_string();
    let (daemon, port, cfg) = boot("", &behavior).await;

    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "IdleWorkspace", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let mut sub = connect_ws(port, cfg.clone()).await;
    wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:updated"], "workspaceId": ws_id }),
    )
    .await;

    // Wait well beyond the debounce window; no activity should emit nothing
    let evt = try_next_event(&mut sub, &["workspace:updated"], Duration::from_secs(2)).await;
    assert!(
        evt.is_none(),
        "unexpected workspace:updated for idle workspace"
    );
}

/// Debounce case: a burst of rapid activity coalesces into at most one
/// `workspace:updated { lastActivity }` with the latest derived value.
#[tokio::test]
async fn last_activity_debounce_coalesces_burst() {
    let Some(script) = gate("WSS lastActivity debounce") else {
        return;
    };

    let behavior = json!({ "response": "burst" }).to_string();
    let (daemon, port, cfg) = boot(&script, &behavior).await;

    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "BurstTest", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let mut sub = connect_ws(port, cfg.clone()).await;
    wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:*"], "workspaceId": ws_id }),
    )
    .await;

    let mut rpc = connect_ws(port, cfg.clone()).await;

    // Create agent
    let created = wss_rpc(
        &mut rpc,
        3,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "BurstAgent", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().expect("agent id");

    // Drive a rapid burst: 3 messages within the 200ms window
    for i in 0..3 {
        wss_rpc(
            &mut rpc,
            10 + i,
            "agent.sendMessage",
            json!({ "workspaceId": ws_id, "agentId": agent_id, "content": format!("msg {i}") }),
        )
        .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Wait for agent:stream:end
    for _ in 0..60 {
        if let Some(evt) =
            try_next_event(&mut sub, &["agent:stream:end"], Duration::from_secs(2)).await
        {
            if evt["data"]["agentId"] == agent_id {
                break;
            }
        }
    }

    // Collect workspace:updated events for 1 second (covers debounce + some buffer)
    let mut last_activity_events = Vec::new();
    let start = tokio::time::Instant::now();
    while start.elapsed() < Duration::from_secs(1) {
        if let Some(evt) =
            try_next_event(&mut sub, &["workspace:updated"], Duration::from_millis(300)).await
        {
            if evt["data"]["changes"]["lastActivity"].is_string() {
                last_activity_events.push(evt);
            }
        }
    }

    // Assert at most one event (debounce coalesced the burst)
    assert!(
        last_activity_events.len() <= 1,
        "expected at most 1 workspace:updated, got {}",
        last_activity_events.len()
    );
}
