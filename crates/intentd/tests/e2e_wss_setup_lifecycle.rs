//! WSS end-to-end setup lifecycle events (§6.5): a live `events.subscribe`
//! client sees `workspace:setup:started` then exactly one
//! `workspace:setup:completed` for a `workspace.create` with a setup script,
//! and NO `file:*` frame for the workspace before the completion — watcher
//! registration is deferred until the setup stage finishes, so setup-script
//! churn is dropped (never published, never buffered). After completion the
//! watcher is live: a control write surfaces as `file:*` normally while the
//! setup-window artifact stays silent forever. Drives a real `intentd serve`
//! over pinned-TLS WSS; mirrors the harness of
//! `e2e_wss_gitignore_suppression.rs`.
//!
//! Also covers the agent-facing setup state (`ws.workspace.details()
//! .setupStatus`, MCP-only): a mock ACP agent's turn started during the
//! setup window reads `pending` / `running`, a turn after
//! `workspace:setup:completed` reads `completed` with the exit code, and a
//! failing script reads `failed`.

#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use intentd_test_support::{Barrier, GuardedChild};
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

const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

fn scratch_dir(prefix: &str) -> tempfile::TempDir {
    common::test_tempdir_in("/tmp", &format!("itd-wss-setuplc-{prefix}-"))
}

/// Spawn `intentd serve` with a hermetic HOME so host git config (global
/// excludes) never leaks into the watcher under test. The daemon leads its
/// own process group so the guard's drop also reaps ACP children (the mock
/// agent's `node`).
fn spawn_serve(data_dir: &Path, home_dir: &Path, env: &[(&str, &str)]) -> GuardedChild {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    common::enable_ws_api(data_dir);
    let mut cmd = common::serve_command();
    cmd.env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("HOME", home_dir)
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    for (k, v) in env {
        cmd.env(k, v);
    }
    GuardedChild::spawn(&mut cmd).expect("spawn intentd serve")
}

/// Mock-agent gate (parity with the other WSS E2E suites): the fixture
/// script path when `node` and the fixture are available, else `None` with
/// a skip notice.
fn mock_agent_gate(test: &str) -> Option<String> {
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
    if !Path::new(&script).exists() {
        eprintln!("skipping {test}: mock script missing at {script}");
        return None;
    }
    Some(script)
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

/// Create a git repo with one commit so `workspace.create` can provision a
/// worktree from it.
fn create_test_repo() -> tempfile::TempDir {
    let repo_dir = scratch_dir("repo");
    let repo_path = repo_dir.path().to_path_buf();
    let run = |args: &[&str]| {
        let out = Command::new("git")
            .arg("-C")
            .arg(&repo_path)
            .args(args)
            .output()
            .expect("run git");
        assert!(
            out.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    };
    run(&["init", "-q", "--initial-branch=main"]);
    run(&["config", "user.email", "e2e@example.com"]);
    run(&["config", "user.name", "E2E"]);
    std::fs::write(repo_path.join("README.md"), "# Test\n").expect("write readme");
    run(&["add", "."]);
    run(&["commit", "-q", "-m", "initial commit"]);
    repo_dir
}

/// The next `events.event` frame's event object (answers pings, skips
/// non-event frames).
async fn next_event<S>(ws: &mut WebSocketStream<S>, wait: Duration) -> Option<Value>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return None;
        }
        match timeout(remaining, ws.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                let v: Value = match serde_json::from_str(&text) {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                if v["method"] == json!("events.event") {
                    return Some(v["params"]["event"].clone());
                }
            }
            Ok(Some(Ok(Message::Ping(p)))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Ok(Some(Ok(_))) => {}
            Ok(other) => panic!("subscription socket ended: {other:?}"),
            Err(_) => return None,
        }
    }
}

/// End-to-end: the setup lifecycle events surface over WSS in order and the
/// deferred watcher keeps the setup window silent — no `file:*` frame for the
/// workspace arrives before `workspace:setup:completed`, the setup-written
/// artifact never surfaces at all (dropped, not buffered), and a post-setup
/// control write emits `file:*` normally.
#[tokio::test]
async fn setup_lifecycle_events_and_file_suppression_over_wss() {
    let data_dir_guard = scratch_dir("data");
    let data_dir = data_dir_guard.path().to_path_buf();
    let home_dir = data_dir.join("home");
    std::fs::create_dir_all(&home_dir).expect("mkdir hermetic home");
    let repo_dir = create_test_repo();
    let repo_path = repo_dir.path().to_path_buf();

    let _daemon = spawn_serve(&data_dir, &home_dir, &[]);
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

    // Subscribe BEFORE the create (globally: the workspace id does not exist
    // yet) so no lifecycle or file event can be missed.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:setup:*", "file:*"] }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // The setup script writes a non-gitignored artifact into the worktree and
    // lingers, widening the setup window: were the watcher live during setup
    // (regression), inotify + the watcher debounce would emit the artifact's
    // `file:*` before `workspace:setup:completed`.
    let setup_script = r#"#!/bin/sh
echo artifact > "${WORKTREE_PATH}/setup-artifact.txt"
sleep 2
exit 0
"#;
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({
            "title": "Setup lifecycle",
            "repositoryPath": repo_path.to_string_lossy(),
            "setupScript": setup_script,
        }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let worktree = PathBuf::from(
        create["result"]["workspace"]["worktreePath"]
            .as_str()
            .expect("worktreePath"),
    );

    // Phase 1: drain frames until `workspace:setup:completed`. The setup
    // window must be silent on `file:*` for this workspace, and the lifecycle
    // events arrive in order with the §6.5 payload shapes.
    let mut seen_started = false;
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(30));
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "workspace:setup:completed never arrived"
        );
        let evt = next_event(&mut sub, remaining)
            .await
            .expect("subscription frame before completion");
        let ty = evt["type"].as_str().unwrap_or("");
        if ty.starts_with("file:") {
            assert_ne!(
                evt["workspaceId"],
                json!(ws_id),
                "file event for the workspace leaked during the setup window: {evt}"
            );
            continue;
        }
        match ty {
            "workspace:setup:started" => {
                assert_eq!(evt["workspaceId"], json!(ws_id));
                assert_eq!(evt["data"], json!({ "workspaceId": ws_id }));
                assert!(!seen_started, "started must fire exactly once");
                seen_started = true;
            }
            "workspace:setup:completed" => {
                assert!(seen_started, "completed must follow started");
                assert_eq!(evt["workspaceId"], json!(ws_id));
                assert_eq!(
                    evt["data"],
                    json!({ "workspaceId": ws_id, "ranScript": true, "exitCode": 0 })
                );
                break;
            }
            other => panic!("unexpected event type {other}: {evt}"),
        }
    }

    // Phase 2: the watcher is registered on completion. Re-write the control
    // until its `file:*` frame arrives (watch establishment can lag, #1621);
    // the setup artifact must never surface — dropped, not buffered.
    let control = "post-setup.txt";
    assert!(
        worktree.join("setup-artifact.txt").exists(),
        "setup script should have written the artifact"
    );
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(30));
    let mut attempt: u64 = 0;
    let mut next_write = tokio::time::Instant::now();
    loop {
        if tokio::time::Instant::now() >= next_write {
            attempt += 1;
            std::fs::write(worktree.join(control), format!("attempt-{attempt}"))
                .expect("write control");
            next_write = tokio::time::Instant::now() + Duration::from_secs(1);
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!remaining.is_zero(), "control file event never arrived");
        let wait = remaining.min(next_write.saturating_duration_since(tokio::time::Instant::now()));
        let Some(evt) = next_event(&mut sub, wait.max(Duration::from_millis(10))).await else {
            continue;
        };
        let ty = evt["type"].as_str().unwrap_or("");
        if !ty.starts_with("file:") || evt["workspaceId"] != json!(ws_id) {
            continue;
        }
        let rel = evt["data"]["relativePath"].as_str().unwrap_or_default();
        assert_ne!(
            rel, "setup-artifact.txt",
            "setup-window artifact surfaced after completion (buffered, not dropped): {evt}"
        );
        if rel == control {
            break;
        }
    }
}

/// Drain subscription frames until `pred` matches one (returned) or the
/// deadline passes (panics naming `what`).
async fn await_event<S>(
    ws: &mut WebSocketStream<S>,
    what: &str,
    mut pred: impl FnMut(&Value) -> bool,
) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(60));
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!remaining.is_zero(), "{what} never arrived");
        let Some(evt) = next_event(ws, remaining).await else {
            continue;
        };
        if pred(&evt) {
            return evt;
        }
    }
}

/// The LAST JSON-parsable `tool_result` payload in an `agent.getConversation`
/// transcript: the mock provider's `emitToolBlocks` persists each MCP tool
/// call as a `tool_use` + `tool_result` pair whose `output[0].text` carries
/// the JSON the agent-side JS returned, so with one `workspace_api` call per
/// turn this is the most recent turn's `ws.workspace.details()` result.
fn last_tool_result(transcript: &Value) -> Value {
    transcript["messages"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|m| m["contentBlocks"].as_array())
        .flatten()
        .filter(|b| b["type"] == json!("tool_result"))
        .filter_map(|b| b["output"].as_array().and_then(|arr| arr.first()))
        .filter_map(|item| item["text"].as_str())
        .filter_map(|text| serde_json::from_str::<Value>(text).ok())
        .next_back()
        .unwrap_or_else(|| panic!("no tool_result persisted in transcript: {transcript}"))
}

/// Poll until the setup script has reached `barrier` (proving its spawn
/// succeeded and it is parked there) or `budget` elapses.
async fn wait_for_entered(barrier: &Barrier, budget: Duration) {
    let deadline = tokio::time::Instant::now() + budget;
    while !barrier.entered() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "setup script never reached {}",
            barrier.path().display()
        );
        // timing-guard: poll interval for the barrier's arrival file
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Drive one turn of `agent_id` (a `workspace_api` call returning
/// `ws.workspace.details()`), wait for its terminal `agent:stream:end`, and
/// return the persisted `details()` result.
async fn details_via_agent_turn<S>(
    rpc: &mut WebSocketStream<S>,
    sub: &mut WebSocketStream<S>,
    id_base: i64,
    ws_id: &str,
    agent_id: &str,
    content: &str,
) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let sent = wss_rpc(
        rpc,
        id_base,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": content }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    await_event(sub, &format!("agent:stream:end for {agent_id}"), |evt| {
        evt["type"] == json!("agent:stream:end") && evt["data"]["agentId"] == json!(agent_id)
    })
    .await;
    let conv = wss_rpc(
        rpc,
        id_base + 1,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    last_tool_result(&conv)
}

/// End-to-end, agent-facing: `ws.workspace.details().setupStatus` tracks the
/// §6.5 setup lifecycle from inside a mock ACP agent's turns over the MCP
/// bridge. The initial agent's first turn starts before the setup script is
/// spawned and the script is parked on a barrier, so that turn reads
/// `pending` or `running` (never a terminal state); after the barrier is
/// released and `workspace:setup:completed` fires, a second turn reads
/// `completed` with the exit code and both timestamps. A second workspace
/// whose script exits `3` reads `failed` with `exitCode: 3`.
#[tokio::test]
async fn setup_status_visible_to_agents_over_mcp() {
    let Some(script) = mock_agent_gate("setup status over MCP") else {
        return;
    };

    let data_dir_guard = scratch_dir("data");
    let data_dir = data_dir_guard.path().to_path_buf();
    let home_dir = data_dir.join("home");
    std::fs::create_dir_all(&home_dir).expect("mkdir hermetic home");
    let repo_dir = create_test_repo();
    let repo_path = repo_dir.path().to_path_buf();

    // Daemon-level mock behavior: every turn makes one `workspace_api` call
    // returning `ws.workspace.details()` and persists it as tool blocks.
    let behavior = json!({
        "toolCall": {
            "name": "workspace_api",
            "arguments": {
                "code": "return await ws.workspace.details();",
                "summary": "read workspace details (setup status e2e)",
            },
        },
        "response": "details inspected",
        "emitToolBlocks": true,
    })
    .to_string();
    let env: [(&str, &str); 2] = [
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
    ];
    let _daemon = spawn_serve(&data_dir, &home_dir, &env);
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

    // Subscribe BEFORE the create (the workspace ids do not exist yet).
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:setup:*", "agent:*"] }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");
    let mut rpc = connect_ws(port, cfg.clone()).await;

    // Plain-JSON tool bodies (`workspaceApi.toonOutput` is read live per
    // invocation) so the persisted tool_result parses back into a Value.
    let toon_off = wss_rpc(
        &mut rpc,
        2,
        "settings.update",
        json!({ "changes": [{ "path": "workspaceApi.toonOutput", "value": false }] }),
    )
    .await;
    assert_eq!(
        toon_off["applied"][0]["value"],
        json!(false),
        "toonOutput off: {toon_off}"
    );

    // Workspace 1: the setup script parks on the barrier so the setup window
    // stays open for as long as the first turn's assertions take.
    let barrier = Barrier::new(&data_dir, "setup");
    let setup_script = format!(
        "#!/bin/sh\n{}\n{}\nexit 0\n",
        barrier.sh_arrive(),
        barrier.sh_wait()
    );
    let created = wss_rpc(
        &mut rpc,
        10,
        "workspace.create",
        json!({
            "title": "Setup status (slow)",
            "repositoryPath": repo_path.to_string_lossy(),
            "setupScript": setup_script,
            "initialAgent": {
                "prompt": "inspect the workspace",
                "name": "Setup inspector",
                "model": "default", "provider": "mock",
            },
        }),
    )
    .await;
    let ws_id = created["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let agent_id = created["initialAgent"]["id"]
        .as_str()
        .expect("initial agent id")
        .to_string();

    // First turn: started by the create before the script is spawned, and
    // the script cannot finish while the barrier holds — so the details read
    // inside the turn is `pending` or `running`, never terminal.
    await_event(
        &mut sub,
        &format!("agent:stream:end for {agent_id}"),
        |evt| evt["type"] == json!("agent:stream:end") && evt["data"]["agentId"] == json!(agent_id),
    )
    .await;
    let conv = wss_rpc(
        &mut rpc,
        11,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let during = last_tool_result(&conv);
    assert_eq!(
        during["id"],
        json!(ws_id),
        "details for the agent's workspace: {during}"
    );
    let during_status = &during["setupStatus"];
    let during_state = during_status["state"].as_str().unwrap_or_default();
    assert!(
        matches!(during_state, "pending" | "running"),
        "setup in flight during the first turn, got {during_status}"
    );
    assert!(
        during_status.get("exitCode").is_none() && during_status.get("finishedAt").is_none(),
        "no terminal details while in flight: {during_status}"
    );
    if during_state == "running" {
        assert!(
            during_status["startedAt"].is_string() && during_status["terminalId"].is_string(),
            "running carries startedAt + terminalId: {during_status}"
        );
    }

    // Once the script has reached the barrier the spawn succeeded, so a
    // details read now must be `running` with its terminal attached — the
    // `running → completed` half of the sequence, held stable by the barrier.
    wait_for_entered(&barrier, Duration::from_secs(20)).await;
    let running = details_via_agent_turn(
        &mut rpc,
        &mut sub,
        15,
        &ws_id,
        &agent_id,
        "inspect the workspace while setup runs",
    )
    .await;
    let running_status = &running["setupStatus"];
    assert_eq!(
        running_status["state"],
        json!("running"),
        "{running_status}"
    );
    assert!(
        running_status["startedAt"].is_string() && running_status["terminalId"].is_string(),
        "running carries startedAt + terminalId: {running_status}"
    );
    assert!(
        running_status.get("exitCode").is_none() && running_status.get("finishedAt").is_none(),
        "no terminal details while running: {running_status}"
    );

    // Release the script; the lifecycle completes with exit 0.
    barrier.release();
    let completed = await_event(&mut sub, "workspace:setup:completed (slow)", |evt| {
        evt["type"] == json!("workspace:setup:completed") && evt["workspaceId"] == json!(ws_id)
    })
    .await;
    assert_eq!(
        completed["data"],
        json!({ "workspaceId": ws_id, "ranScript": true, "exitCode": 0 })
    );
    assert!(barrier.entered(), "the setup script ran to the barrier");

    // Second turn: reads the terminal `completed` snapshot.
    let after = details_via_agent_turn(
        &mut rpc,
        &mut sub,
        20,
        &ws_id,
        &agent_id,
        "inspect the workspace again",
    )
    .await;
    let after_status = &after["setupStatus"];
    assert_eq!(after_status["state"], json!("completed"), "{after_status}");
    assert_eq!(after_status["exitCode"], json!(0), "{after_status}");
    assert!(
        after_status["terminalId"].is_string()
            && after_status["startedAt"].is_string()
            && after_status["finishedAt"].is_string(),
        "completed carries terminalId + both timestamps: {after_status}"
    );

    // Workspace 2: a failing script → `failed` with the exit code, read by
    // an agent created after the lifecycle settled.
    let created2 = wss_rpc(
        &mut rpc,
        30,
        "workspace.create",
        json!({
            "title": "Setup status (failing)",
            "repositoryPath": repo_path.to_string_lossy(),
            "setupScript": "#!/bin/sh\nexit 3\n",
        }),
    )
    .await;
    let failing_ws_id = created2["workspace"]["id"]
        .as_str()
        .expect("workspace 2 id")
        .to_string();
    let completed2 = await_event(&mut sub, "workspace:setup:completed (failing)", |evt| {
        evt["type"] == json!("workspace:setup:completed")
            && evt["workspaceId"] == json!(failing_ws_id)
    })
    .await;
    assert_eq!(
        completed2["data"],
        json!({ "workspaceId": failing_ws_id, "ranScript": true, "exitCode": 3 })
    );
    let created_agent = wss_rpc(
        &mut rpc,
        31,
        "agent.create",
        json!({ "workspaceId": failing_ws_id, "name": "Failure inspector", "model": "default", "provider": "mock" }),
    )
    .await;
    let inspector_id = created_agent["agent"]["id"]
        .as_str()
        .expect("agent 2 id")
        .to_string();
    let failed = details_via_agent_turn(
        &mut rpc,
        &mut sub,
        40,
        &failing_ws_id,
        &inspector_id,
        "inspect the failed workspace",
    )
    .await;
    assert_eq!(failed["id"], json!(failing_ws_id), "{failed}");
    let failed_status = &failed["setupStatus"];
    assert_eq!(failed_status["state"], json!("failed"), "{failed_status}");
    assert_eq!(failed_status["exitCode"], json!(3), "{failed_status}");
    assert!(
        failed_status["startedAt"].is_string() && failed_status["finishedAt"].is_string(),
        "failed carries both timestamps: {failed_status}"
    );
}
