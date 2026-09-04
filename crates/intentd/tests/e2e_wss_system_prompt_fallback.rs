//! WSS e2e for the `FirstTurnPrepend` system-prompt fallback (§18.1).
//!
//! The `mock` provider is registered with
//! `InjectionMechanism::FirstTurnPrepend` (like cortex): it has no native
//! system-prompt mechanism, so the daemon must deliver the assembled prompt by
//! prepending it as a `<system>` block to the FIRST prompt of each fresh ACP
//! session. This suite drives a specialist agent over the real WSS transport
//! and asserts — via the mock fixture's `MOCK_AGENT_PROMPT_LOG` seam — the
//! exact prompt text the provider received on each turn:
//!
//! * Turn 1 starts with the `<system>`-wrapped assembled prompt (including the
//!   `<specialist_role>` section) BEFORE the role reminder and user content.
//! * Turn 2 (same session) does NOT repeat the block.
//!
//! `SessionMeta` note: the `_meta` mechanism (claude-code) is keyed off the
//! provider ID in `build_session_meta`, and the mock provider cannot be
//! spawned under that ID (spawn resolution and binary lookup are
//! provider-ID-keyed). The `_meta` payload shapes are covered by the unit
//! suites in `intent-services/src/agent_session/tests_meta.rs` and
//! `intent-acp/src/tests.rs` instead.
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
    let dir = PathBuf::from("/tmp").join(format!("itd-spf-{}", &id[..8]));
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

/// Pre-seed the daemon's `SQLite` store with a workspace (the daemon opens the
/// same data dir on launch).
async fn seed_workspace_only(data_dir: &Path) -> String {
    use intent_core::{
        now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceId, WorkspaceStatus,
    };
    use intent_store::Store;
    let db_path = data_dir.join("intentd.db");
    let store = Store::open(&db_path).await.expect("open store");
    let ws = WorkspaceId::new();
    let ts = now_iso();
    store
        .insert_workspace(&Workspace {
            id: ws.clone(),
            title: "SPF-E2E".to_string(),
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
            setup_result: None,
            is_remote: false,
            default_model: None,
            pr_number: None,
            pr_url: None,
            pr_status: None,
            active_pull_request: None,
            pull_requests: None,
            context_links: None,
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
        })
        .await
        .expect("insert ws");
    ws.0
}

/// `FirstTurnPrepend` over the real WSS transport: a specialist agent on the
/// `mock` provider (registered `InjectionMechanism::FirstTurnPrepend`) must
/// receive the assembled system prompt — `<system>`-wrapped, including the
/// `<specialist_role>` section — prepended to the FIRST prompt of its fresh
/// ACP session, ordered before the per-turn role reminder and the user
/// content; the SECOND turn on the same session must NOT repeat it.
#[tokio::test]
async fn first_turn_prepend_delivers_system_prompt_over_wss() {
    let Some(script) = gate("WSS FirstTurnPrepend E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    // Hermetic specialist tier: a bundled dir with one specialist whose id is
    // unique to this test (so a developer's user/project-tier `implementor.md`
    // can never shadow it) and whose behaviorPrompt is a unique marker, so the
    // assembled prompt provably contains the file-resolved <specialist_role>
    // section.
    let specialists_dir = data_dir.join("specialists");
    std::fs::create_dir_all(&specialists_dir).expect("mkdir specialists");
    std::fs::write(
        specialists_dir.join("spf-e2e-tester.md"),
        "---\nname: \"SpfTester\"\ndescription: \"d\"\nroleReminder: \"Stay in scope.\"\n---\n\nSPF_E2E_BEHAVIOR_MARKER: implement exactly what the task says.",
    )
    .expect("write specialist");
    let prompt_log = data_dir.join("prompt-log.jsonl");
    let prompt_log_str = prompt_log.to_string_lossy().into_owned();
    let behavior = json!({ "response": "ok" }).to_string();
    let env: [(&str, &str); 6] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_PROMPT_LOG", &prompt_log_str),
        (
            "INTENTD_BUNDLED_SPECIALISTS_DIR",
            specialists_dir.to_str().unwrap(),
        ),
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

    // SUBSCRIBER conn — events.subscribe BEFORE the turns so we miss nothing.
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

    // RPC conn — create a specialist agent on the mock provider and run two turns.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({
            "workspaceId": ws_id,
            "name": "SPF",
            "model": "default", "provider": "mock",
            "specialistId": "spf-e2e-tester",
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
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "first user turn" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    await_stream_end(&mut sub, &agent_id).await;

    let sent2 = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "second user turn" }),
    )
    .await;
    assert_eq!(sent2["success"], true, "second sendMessage ok: {sent2}");
    await_stream_end(&mut sub, &agent_id).await;

    // The mock child logged the exact prompt text it received per turn.
    let log = read_prompt_log(&prompt_log);
    assert!(
        log.len() >= 2,
        "expected 2 logged prompts, got {}: {log:?}",
        log.len()
    );
    let (first_turn, first_text) = &log[0];
    assert_eq!(*first_turn, 1, "first logged prompt is the child's turn 1");
    assert!(
        first_text.starts_with("<system>\n"),
        "turn 1 must START with the <system>-wrapped assembled prompt: {first_text:?}"
    );
    assert!(
        first_text.contains("<specialist_role>") && first_text.contains("SPF_E2E_BEHAVIOR_MARKER"),
        "assembled prompt must include the file-resolved <specialist_role> section: {first_text:?}"
    );
    let sys_end = first_text
        .find("\n</system>")
        .expect("closing </system> tag on turn 1");
    let after_system = &first_text[sys_end..];
    assert!(
        after_system.contains("[Role Reminder:"),
        "role reminder must follow the <system> block: {first_text:?}"
    );
    assert!(
        first_text
            .find("[Role Reminder:")
            .expect("role reminder present")
            > sys_end,
        "the <system> block must be OUTERMOST (before the role reminder)"
    );
    assert!(
        first_text.ends_with("first user turn"),
        "user content last on turn 1: {first_text:?}"
    );

    let (second_turn, second_text) = &log[1];
    assert_eq!(*second_turn, 2, "same child served turn 2 (no respawn)");
    assert!(
        !second_text.contains("<system>\n") && !second_text.contains("<specialist_role>"),
        "turn 2 on the SAME session must NOT repeat the system prompt: {second_text:?}"
    );
    assert!(
        second_text.contains("[Role Reminder:"),
        "per-turn role reminder still fires on turn 2: {second_text:?}"
    );
    // The send may drain via the queue, which appends the dequeue-wait
    // system note after the user content — strip it before the tail check.
    let second_tail = second_text
        .split("\n\n[SYSTEM NOTE] This message was queued at")
        .next()
        .unwrap();
    assert!(
        second_tail.ends_with("second user turn"),
        "user content last on turn 2: {second_text:?}"
    );
}

/// Specialist prompt freeze over the real WSS transport: `agent.create`
/// snapshots the resolved specialist injection into the session, so a
/// user-tier specialist file edited AFTER creation but BEFORE the first spawn
/// must not change the agent — the assembled prompt the provider receives
/// carries the ORIGINAL body, name, and role reminder, not the edited ones.
#[tokio::test]
async fn specialist_prompt_frozen_across_file_edit_over_wss() {
    let Some(script) = gate("WSS specialist freeze E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    // Hermetic USER tier: HOME=data_dir so the daemon reads
    // $HOME/.intent/specialists/ — the tier whose edits the freeze guards.
    let specialists_dir = data_dir.join(".intent").join("specialists");
    std::fs::create_dir_all(&specialists_dir).expect("mkdir specialists");
    let specialist_path = specialists_dir.join("freeze-e2e-tester.md");
    std::fs::write(
        &specialist_path,
        "---\nname: \"FrozenTester\"\ndescription: \"d\"\nroleReminder: \"Original reminder.\"\n---\n\nFREEZE_E2E_ORIGINAL_MARKER: original body.",
    )
    .expect("write specialist");
    let prompt_log = data_dir.join("prompt-log.jsonl");
    let prompt_log_str = prompt_log.to_string_lossy().into_owned();
    let behavior = json!({ "response": "ok" }).to_string();
    let home = data_dir.to_string_lossy().into_owned();
    let env: [(&str, &str); 6] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_PROMPT_LOG", &prompt_log_str),
        ("HOME", &home),
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
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // Create the specialist agent over WSS — this is where the snapshot is
    // persisted.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({
            "workspaceId": ws_id,
            "name": "Freeze",
            "model": "default", "provider": "mock",
            "specialistId": "freeze-e2e-tester",
        }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // Edit the specialist file AFTER creation, BEFORE the first spawn: new
    // name, reminder, and body.
    std::fs::write(
        &specialist_path,
        "---\nname: \"EditedTester\"\ndescription: \"d\"\nroleReminder: \"Edited reminder.\"\n---\n\nFREEZE_E2E_EDITED_MARKER: edited body.",
    )
    .expect("edit specialist");

    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "first user turn" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    await_stream_end(&mut sub, &agent_id).await;

    // The mock child logged the exact prompt text it received: the assembled
    // spawn prompt must carry the ORIGINAL frozen triple, not the edit.
    let log = read_prompt_log(&prompt_log);
    assert!(!log.is_empty(), "expected a logged prompt: {log:?}");
    let (first_turn, first_text) = &log[0];
    assert_eq!(*first_turn, 1, "first logged prompt is the child's turn 1");
    assert!(
        first_text.contains("FREEZE_E2E_ORIGINAL_MARKER"),
        "frozen original body must survive the file edit: {first_text:?}"
    );
    assert!(
        !first_text.contains("FREEZE_E2E_EDITED_MARKER"),
        "edited body must NOT reach the spawned prompt: {first_text:?}"
    );
    assert!(
        first_text.contains("[Role Reminder: You are a FrozenTester. Original reminder.]"),
        "frozen name + reminder must survive the file edit: {first_text:?}"
    );
}
