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
//! - Debounce: a rapid burst coalesces into at most one emission per debounce
//!   window it spans, the last one carrying the latest value.
//!
//! Uses the mock ACP agent fixture for deterministic behavior. The test
//! overrides `LAST_ACTIVITY_DEBOUNCE_TEST_MS` to [`DEBOUNCE_MS`] for fast
//! execution.

#![cfg(unix)]

mod common;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
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
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

struct Daemon {
    child: Child,
    _data_dir_guard: tempfile::TempDir,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn scratch_dir(prefix: &str) -> tempfile::TempDir {
    common::test_tempdir_in("/tmp", &format!("itd-wss-lastact-{prefix}-"))
}

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> Child {
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
    timeout(common::rpc_read_timeout(), reader.read_line(&mut buf))
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

async fn connect_ws(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

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
            Some(Ok(_)) => {}
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
        let Ok(next) = timeout(remaining, ws.next()).await else {
            return None;
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
            Some(Ok(_)) => {}
            None | Some(Err(_)) => return None,
        }
    }
}

/// `try_next_event` yields `None` both when the deadline elapses and when the
/// subscription socket closes or errors; name which one it was so a failure
/// under load is triaged from the panic message alone.
fn wait_failure_kind(deadline: tokio::time::Instant) -> &'static str {
    if tokio::time::Instant::now() >= deadline {
        "timed out"
    } else {
        "subscription socket closed before the deadline"
    }
}

/// Wait until `count` terminal `agent:stream:end` events for `agent_id` have
/// arrived on an `agent:*` subscription. One overall deadline bounds the whole
/// wait so a missing event fails fast instead of polling a fixed iteration
/// budget.
async fn await_stream_ends<S>(ws: &mut WebSocketStream<S>, agent_id: &str, count: usize)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(60));
    let mut seen = 0usize;
    while seen < count {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let evt = try_next_event(ws, &["agent:stream:end"], remaining)
            .await
            .unwrap_or_else(|| {
                panic!(
                    "{} waiting for {count} agent:stream:end events (saw {seen})",
                    wait_failure_kind(deadline)
                )
            });
        if evt["data"]["agentId"] == agent_id {
            seen += 1;
        }
    }
}

/// Wait for every submitted contribution to appear in a persisted user preview
/// and for all carrying turns to end. Human merging can put several contributions
/// in one row, while a batch flush can put several rows in one turn. Neither row
/// count nor stream-end count is therefore a submission count. The short fixture
/// messages fit the preview; verify the full durable text separately below.
async fn await_user_turns_ended<S>(
    ws: &mut WebSocketStream<S>,
    agent_id: &str,
    contributions: &[&str],
) -> HashSet<String>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(60));
    let mut seen = vec![0usize; contributions.len()];
    let mut seen_order = Vec::new();
    let mut open_turns: HashSet<String> = HashSet::new();
    let mut ended_turns: HashSet<String> = HashSet::new();
    while seen.contains(&0) || open_turns.iter().any(|t| !ended_turns.contains(t)) {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let evt = try_next_event(ws, &["agent:last-message", "agent:stream:end"], remaining)
            .await
            .unwrap_or_else(|| {
                let still_open = open_turns
                    .iter()
                    .filter(|t| !ended_turns.contains(*t))
                    .count();
                panic!(
                    "{} waiting for contributions {contributions:?} and their turns to end \
                     (seen {seen:?}, {still_open} turns still open)",
                    wait_failure_kind(deadline)
                )
            });
        if evt["data"]["agentId"] != agent_id {
            continue;
        }
        let turn_id = evt["data"]["turnId"].as_str().map(str::to_string);
        match evt["type"].as_str() {
            Some("agent:last-message") if evt["data"]["role"] == json!("user") => {
                let text = evt["data"]["lastUserMessage"]
                    .as_str()
                    .expect("user preview");
                let mut positions = Vec::new();
                for (i, contribution) in contributions.iter().enumerate() {
                    for (offset, _) in text.match_indices(contribution) {
                        seen[i] += 1;
                        assert_eq!(seen[i], 1, "duplicate contribution: {evt}");
                        positions.push((offset, i));
                    }
                }
                if !positions.is_empty() {
                    positions.sort_unstable();
                    seen_order.extend(positions.into_iter().map(|(_, i)| i));
                    open_turns.insert(turn_id.expect("user preview carries turnId"));
                }
            }
            Some("agent:stream:end") => {
                if let Some(tid) = turn_id {
                    ended_turns.insert(tid);
                }
            }
            _ => {}
        }
    }
    assert_eq!(seen_order, (0..contributions.len()).collect::<Vec<_>>());
    open_turns
}

/// Debounce window the booted daemon runs with (`LAST_ACTIVITY_DEBOUNCE_TEST_MS`
/// override): short for fast execution, large enough that CI scheduler stalls
/// between activity touches don't routinely split a burst across windows.
const DEBOUNCE_MS: u64 = 500;

async fn boot(mock_script: &str, behavior: &str) -> (Daemon, u16, Arc<ClientConfig>) {
    let data_dir_guard = scratch_dir("data");
    let data_dir = data_dir_guard.path().to_path_buf();
    let debounce_ms = DEBOUNCE_MS.to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("LAST_ACTIVITY_DEBOUNCE_TEST_MS", debounce_ms.as_str()),
        ("MOCK_AGENT_SCRIPT_PATH", mock_script),
        ("MOCK_AGENT_BEHAVIOR", behavior),
    ];
    let child = spawn_serve(&data_dir, &env);
    let daemon = Daemon {
        child,
        _data_dir_guard: data_dir_guard,
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
        .map(std::string::ToString::to_string);

    // Second subscription on `agent:*`: turn completion is observed via
    // `agent:stream:end`, which a `workspace:*` subscription never receives.
    let mut agent_sub = connect_ws(port, cfg.clone()).await;
    wss_rpc(
        &mut agent_sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;

    // Drive activity: create + run an agent
    let created = wss_rpc(
        &mut rpc,
        3,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "TestAgent", "model": "default", "provider": "mock" }),
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

    // Quiesce the activity source before the paired reads below: wait for the
    // agent turn to finish so no in-flight turn keeps bumping lastActivity
    // between the event read and the workspace.get (monorepo#1004 — under
    // coverage instrumentation a late bump landed between the two reads and
    // broke their byte-equality).
    await_stream_ends(&mut agent_sub, agent_id, 1).await;

    // Wait for workspace:updated with lastActivity.
    // The debounce window is 500ms, so we wait a bit longer to account for
    // agent turn execution + debounce + event delivery.
    let updated_evt = next_event(&mut sub, &["workspace:updated"], 10).await;
    assert_eq!(updated_evt["workspaceId"], ws_id);
    let changes = &updated_evt["data"]["changes"];
    assert!(
        changes["lastActivity"].is_string(),
        "lastActivity in changes: {changes}"
    );
    let mut new_activity = changes["lastActivity"]
        .as_str()
        .expect("lastActivity string")
        .to_string();

    // The turn is complete, but its trailing activity touches may still be
    // debouncing. Drain further workspace:updated emissions until the
    // subscription has been quiet for well over one debounce window (same
    // pattern as the burst test), keeping the latest lastActivity. Once quiet
    // no bump is pending, so the workspace.get below must observe exactly
    // this value and the byte-equality assertion is deterministic (#1004).
    let drain_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while let Some(evt) = try_next_event(
        &mut sub,
        &["workspace:updated"],
        Duration::from_millis(1500),
    )
    .await
    {
        assert!(
            tokio::time::Instant::now() < drain_deadline,
            "workspace:updated drain never went quiet within 30s"
        );
        if let Some(latest) = evt["data"]["changes"]["lastActivity"].as_str() {
            new_activity = latest.to_string();
        }
    }

    // Verify it's newer than initial and matches workspace.get
    let new_activity = new_activity.as_str();
    if let Some(init) = &initial_activity {
        // Parse both as RFC3339 DateTimes to compare instants (lexicographic comparison
        // can be wrong with differing fractional-second precision).
        let init_dt =
            DateTime::parse_from_rfc3339(init.as_str()).expect("parse initial lastActivity");
        let new_dt = DateTime::parse_from_rfc3339(new_activity).expect("parse new lastActivity");
        assert!(
            new_dt > init_dt,
            "lastActivity did not advance: {init} -> {new_activity}"
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

/// Wait up to `secs` for the next `subscription.push` notification on `ws`;
/// ignore other frames. Returns the `params` sub-object
/// (`{ subscriptionId, kind, seq, snapshot|delta }`).
async fn next_subscription_push<S>(ws: &mut WebSocketStream<S>, secs: u64) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for subscription.push"
        );
        let next = timeout(remaining, ws.next())
            .await
            .expect("timeout elapsed");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = match serde_json::from_str(&text) {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                if v["method"] == json!("subscription.push") {
                    return v["params"].clone();
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

/// Persistence case (intent-hq/monorepo#1580): the debounced derivation writes
/// its result to the workspace row, so the `workspace.subscribe` seq-0 snapshot
/// — served by the lite list, which never derives `lastActivity` — carries the
/// same value the `workspace:updated` event announced. Before the fix the
/// snapshot served the stale stored column (the post-restart regression).
#[tokio::test]
async fn last_activity_persisted_for_workspace_subscribe_snapshot() {
    let Some(script) = gate("WSS lastActivity persistence") else {
        return;
    };

    let behavior = json!({ "response": "persisted activity" }).to_string();
    let (daemon, port, cfg) = boot(&script, &behavior).await;

    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "PersistTest", "branch": "main", "skipWorktree": true }),
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

    let mut agent_sub = connect_ws(port, cfg.clone()).await;
    wss_rpc(
        &mut agent_sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;

    // Drive activity: create + run an agent.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        3,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "PersistAgent", "model": "default", "provider": "mock" }),
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
    await_stream_ends(&mut agent_sub, agent_id, 1).await;

    // Take the latest announced lastActivity, draining until the subscription
    // has been quiet for well over one debounce window so no bump is pending
    // when the snapshot below is read (same pattern as the positive test).
    let evt = next_event(&mut sub, &["workspace:updated"], 10).await;
    let mut announced = evt["data"]["changes"]["lastActivity"]
        .as_str()
        .expect("lastActivity string")
        .to_string();
    let drain_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while let Some(evt) = try_next_event(
        &mut sub,
        &["workspace:updated"],
        Duration::from_millis(1500),
    )
    .await
    {
        assert!(
            tokio::time::Instant::now() < drain_deadline,
            "workspace:updated drain never went quiet within 30s"
        );
        if let Some(latest) = evt["data"]["changes"]["lastActivity"].as_str() {
            announced = latest.to_string();
        }
    }

    // A fresh `workspace.subscribe` seq-0 snapshot (lite list — no derivation)
    // must carry exactly that value, which is only possible if it was persisted.
    let mut snap_conn = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(&mut snap_conn, 1, "workspace.subscribe", json!({})).await;
    let sub_id = sub_res["subscriptionId"]
        .as_str()
        .expect("subscriptionId")
        .to_string();
    let push = next_subscription_push(&mut snap_conn, 10).await;
    assert_eq!(push["subscriptionId"], sub_id.as_str(), "push: {push}");
    assert_eq!(push["kind"], json!("snapshot"), "push: {push}");
    assert_eq!(push["seq"], json!(0), "push: {push}");
    let row = push["snapshot"]
        .as_array()
        .expect("snapshot array")
        .iter()
        .find(|e| e["id"] == json!(ws_id))
        .cloned()
        .expect("workspace in snapshot");
    assert_eq!(
        row["lastActivity"].as_str(),
        Some(announced.as_str()),
        "seq-0 snapshot must serve the persisted derived lastActivity: {row}"
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
/// `workspace:updated { lastActivity }` per debounce window the burst spans —
/// exactly one when the whole burst lands inside a single window — with the
/// last emission carrying the latest derived value.
///
/// The burst is three back-to-back agent turns whose wall-clock span the test
/// does not control: under host load (intent-hq/intent#4999) the turns can
/// straddle a window boundary, which legitimately yields two emissions. So
/// the assertion bounds the emission count by the windows the burst provably
/// spanned instead of assuming a single window; see [`burst_debounce_case`].
#[tokio::test]
async fn last_activity_debounce_coalesces_burst() {
    let Some(script) = gate("WSS lastActivity debounce") else {
        return;
    };
    burst_debounce_case(&script, json!({ "response": "burst" }), None).await;
}

/// Same burst, with the first burst turn held open by a file barrier until
/// the remaining two messages have provably queued behind it, so they drain
/// as ONE combined flush turn. Pins the turn-identity wait in
/// [`await_user_turns_ended`]: this is the interleaving a loaded host produces
/// nondeterministically (intent-hq/intent#4947), and a fixed count of three
/// `agent:stream:end` events times out here. A barrier rather than a timer:
/// the msg 0 turn cannot end before the test releases it, so the queued sends
/// and the two-turn folding are asserted, not hoped for.
///
/// The barrier deliberately spreads the burst over wall-clock time the test
/// does not bound (three RPC round trips plus the release), so this variant
/// asserts `lastActivity` convergence — the announced value advanced past the
/// pre-burst value and matches `workspace.get` — not the coalescing bound,
/// which only the plain burst above pins.
#[tokio::test]
async fn last_activity_debounce_coalesces_burst_with_queued_flush() {
    let Some(script) = gate("WSS lastActivity debounce (queued flush)") else {
        return;
    };
    let release_dir = scratch_dir("release");
    let release_file = release_dir.path().join("release-msg-0");
    burst_debounce_case(
        &script,
        json!({
            "response": "burst",
            "rules": [{ "ifPromptContains": "msg 0", "releaseFile": release_file }],
        }),
        Some(&release_file),
    )
    .await;
}

/// `release_file`: when set, the mock holds the msg 0 turn open until this
/// file exists; the burst then asserts msgs 1 and 2 queued behind it and folded
/// into exactly one combined turn, and asserts `lastActivity` convergence
/// instead of the coalescing bound.
async fn burst_debounce_case(script: &str, behavior: Value, release_file: Option<&Path>) {
    let behavior = behavior.to_string();
    let (daemon, port, cfg) = boot(script, &behavior).await;

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

    // Second subscription on `agent:*`: turn completion is observed via
    // `agent:stream:end`, which a `workspace:*` subscription never receives.
    let mut agent_sub = connect_ws(port, cfg.clone()).await;
    wss_rpc(
        &mut agent_sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;

    let mut rpc = connect_ws(port, cfg.clone()).await;

    // Create agent
    let created = wss_rpc(
        &mut rpc,
        3,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "BurstAgent", "model": "default", "provider": "mock" }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().expect("agent id");

    // Warm-up turn: absorb the one-off agent process spawn latency so a slow
    // spawn on a loaded host can't open a quiet gap that splits the measured
    // burst below into multiple debounce windows.
    wss_rpc(
        &mut rpc,
        4,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "warm-up" }),
    )
    .await;
    await_stream_ends(&mut agent_sub, agent_id, 1).await;

    // Drain the warm-up turn's own lastActivity emission(s): read until the
    // workspace:* subscription has been quiet for well over one debounce
    // window, so nothing from the warm-up leaks into the burst count. An
    // outer deadline hard-bounds the drain even if some event source kept
    // emitting less than one quiet window apart.
    let drain_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while try_next_event(
        &mut sub,
        &["workspace:updated"],
        Duration::from_millis(1500),
    )
    .await
    .is_some()
    {
        assert!(
            tokio::time::Instant::now() < drain_deadline,
            "warm-up drain never went quiet within 30s"
        );
    }

    // Pre-burst baseline the burst's announced lastActivity must advance past.
    let before = wss_rpc(
        &mut rpc,
        5,
        "workspace.get",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    let before_activity = before["workspace"]["lastActivity"]
        .as_str()
        .expect("pre-burst lastActivity")
        .to_string();

    // Drive a rapid burst: 3 messages sent 50ms apart, normally well inside one
    // debounce window. Every debounce schedule the burst triggers postdates
    // this instant, which anchors the windows-spanned bound below. Wall clock
    // on purpose: it is compared against the daemon's own event timestamps.
    let burst_started = chrono::Utc::now();
    let mut sends = Vec::new();
    for i in 0..3 {
        let sent = wss_rpc(
            &mut rpc,
            10 + i,
            "agent.sendMessage",
            json!({ "workspaceId": ws_id, "agentId": agent_id, "content": format!("msg {i}") }),
        )
        .await;
        assert_eq!(sent["success"], json!(true), "msg {i} send: {sent}");
        sends.push(sent);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Barrier variant: msg 0 is still held open, so msgs 1 and 2 must have
    // queued behind it. Only now let the msg 0 turn end.
    if let Some(release) = release_file {
        for (i, sent) in sends.iter().enumerate().skip(1) {
            assert_eq!(
                sent["queued"],
                json!(true),
                "msg {i} must queue behind the held msg 0 turn: {sent}"
            );
        }
        std::fs::write(release, b"go").expect("write release file");
    }

    // Wait (bounded) until every turn carrying a burst message has completed.
    // Neither three rows nor three stream:ends: pending same-human inputs
    // merge into one row, and queued rows can drain together in one turn.
    let burst_turns =
        await_user_turns_ended(&mut agent_sub, agent_id, &["msg 0", "msg 1", "msg 2"]).await;
    if release_file.is_some() {
        assert_eq!(
            burst_turns.len(),
            2,
            "held msg 0 turn + one combined flush turn for msgs 1 and 2: {burst_turns:?}"
        );
    }

    let store = intent_store::Store::open(&daemon.data_dir.join("intentd.db"))
        .await
        .unwrap();
    let session = store
        .get_agent_session(&intent_core::AgentId::from(agent_id))
        .await
        .unwrap();
    let text = session
        .messages
        .iter()
        .filter(|m| m.role == "user")
        .flat_map(|m| m.content.as_array().into_iter().flatten())
        .filter_map(|block| block["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n\n");
    for contribution in ["msg 0", "msg 1", "msg 2"] {
        assert_eq!(text.matches(contribution).count(), 1, "transcript: {text}");
    }
    assert!(
        text.find("msg 0") < text.find("msg 1") && text.find("msg 1") < text.find("msg 2"),
        "transcript: {text}"
    );
    store.close().await;

    // Collect workspace:updated events until the subscription has been quiet
    // for well over one debounce window (covers the trailing debounce fire).
    // Same outer deadline pattern as the warm-up drain above.
    let collect_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut last_activity_events = Vec::new();
    while let Some(evt) = try_next_event(
        &mut sub,
        &["workspace:updated"],
        Duration::from_millis(1500),
    )
    .await
    {
        assert!(
            tokio::time::Instant::now() < collect_deadline,
            "workspace:updated collection never went quiet within 30s"
        );
        if evt["data"]["changes"]["lastActivity"].is_string() {
            last_activity_events.push(evt);
        }
    }

    // Non-vacuous: the burst announced a lastActivity at all.
    assert!(
        !last_activity_events.is_empty(),
        "expected the burst to emit a workspace:updated {{ lastActivity }}"
    );

    if release_file.is_none() {
        // Plain rapid burst: bound the emission count by the debounce windows
        // the burst provably spanned. The debounce is trailing-edge: an
        // emission fires only after one full window of quiet following the
        // schedule that armed it, and a schedule that arms a further emission
        // must postdate the previous timer's expiry (an earlier one would have
        // cancelled that timer instead). So `n` emissions need at least
        // `n * DEBOUNCE_MS` between the first burst schedule — which postdates
        // `burst_started` — and the daemon timestamp of the last emission:
        // `n <= floor((last_emit - burst_started) / DEBOUNCE_MS)`. A burst that
        // lands inside one window therefore still gets exactly one emission,
        // while a burst that straddled a boundary under load (#4999) is
        // allowed its second — and a debounce that fires per touch, or on the
        // leading edge, still fails.
        let last_emit = last_activity_events
            .last()
            .and_then(|evt| evt["timestamp"].as_str())
            .map(|ts| DateTime::parse_from_rfc3339(ts).expect("parse emission timestamp"))
            .expect("last emission carries a timestamp");
        let spanned_ms =
            u64::try_from((last_emit.to_utc() - burst_started).num_milliseconds()).unwrap_or(0);
        let windows_spanned = spanned_ms / DEBOUNCE_MS;
        assert!(
            u64::try_from(last_activity_events.len()).expect("emission count fits in u64")
                <= windows_spanned,
            "expected at most {windows_spanned} workspace:updated (last emission {spanned_ms}ms \
             after the burst started, {DEBOUNCE_MS}ms debounce), got {}",
            last_activity_events.len()
        );
    }

    // Convergence (both variants): the latest announced value advanced past
    // the pre-burst baseline and is what workspace.get now serves.
    let announced = last_activity_events
        .last()
        .and_then(|evt| evt["data"]["changes"]["lastActivity"].as_str())
        .expect("lastActivity string");
    let before_dt =
        DateTime::parse_from_rfc3339(&before_activity).expect("parse pre-burst lastActivity");
    let announced_dt =
        DateTime::parse_from_rfc3339(announced).expect("parse announced lastActivity");
    assert!(
        announced_dt > before_dt,
        "lastActivity did not advance across the burst: {before_activity} -> {announced}"
    );
    let get = wss_rpc(
        &mut rpc,
        6,
        "workspace.get",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(
        get["workspace"]["lastActivity"].as_str(),
        Some(announced),
        "workspace.get must serve the last announced lastActivity"
    );
}
