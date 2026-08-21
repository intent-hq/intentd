//! WSS end-to-end for LLM-generated auto-commit messages (LNI-1 §9.1).
//!
//! Boots a real `intentd serve` (WSS listener enabled via config) with a fake auggie binary that
//! emits a deterministic `{"subject": ..., "body": ...}` JSON reply. Drives an
//! agent turn that writes a file over WSS, waits for `agent:idle`, then asserts
//! via `git.commits` that the resulting auto-commit message is the generated
//! one with `Agent-Id:`/`Linked-Note-Id:` trailers intact, and via the raw
//! `git log` message that the body (escaped `\n` in the JSON reply) composes
//! as subject + blank line + body with the trailers appended after the body.
//! Also asserts the fallback: with auggie absent/failing, the commit still
//! lands with the deterministic subject (taskTitle → agentName → "Agent
//! changes").
//!
//! Gated on `node` + the mock ACP agent script; skips cleanly otherwise.

#![cfg(unix)]

mod common;

use std::os::unix::fs::PermissionsExt;
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

const TOKEN: &str = "1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a";

struct Daemon {
    child: Child,
    data_dir: PathBuf,
    auggie_dir: Option<PathBuf>,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.data_dir);
        if let Some(ref dir) = self.auggie_dir {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-aclm-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

/// Create a fake auggie binary that emits the given JSON reply verbatim.
/// `printf '%s'` keeps the reply byte-literal, so `\n` escapes inside JSON
/// strings survive to the parser instead of becoming real newlines.
fn fake_auggie(reply_json: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("intentd-e2e-auggie-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let bin = dir.join("auggie");
    let script = format!("#!/bin/sh\ncat > /dev/null\nprintf '%s' '{reply_json}'\n");
    std::fs::write(&bin, script).unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    bin
}

fn run_git(args: &[&str], cwd: &Path) -> String {
    let out = Command::new("git")
        .args(args)
        .env("GIT_AUTHOR_NAME", "e2e")
        .env("GIT_AUTHOR_EMAIL", "e2e@example.com")
        .env("GIT_COMMITTER_NAME", "e2e")
        .env("GIT_COMMITTER_EMAIL", "e2e@example.com")
        .current_dir(cwd)
        .stderr(Stdio::null())
        .output()
        .expect("run git");
    assert!(out.status.success(), "git {args:?} failed");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Initialize a git repo with a seed commit.
fn init_git_repo(dir: &Path) {
    run_git(&["init", "-q", "-b", "main"], dir);
    run_git(&["config", "user.name", "Test"], dir);
    run_git(&["config", "user.email", "test@example.com"], dir);
    run_git(&["config", "commit.gpgsign", "false"], dir);
    std::fs::write(dir.join("seed.txt"), "seed\n").unwrap();
    run_git(&["add", "seed.txt"], dir);
    run_git(&["commit", "-q", "-m", "Initial commit"], dir);
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
        .env("RUST_LOG", "intentd=debug,intent_services=debug")
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

/// Seed a workspace with a git repo worktree directly in the data dir store
/// (bypassing the workspace-create dance) so the daemon sees it on boot.
/// Optionally sets context.auggiePath if an `auggie_bin` is provided.
async fn seed_workspace_with_repo(data_dir: &Path, auggie_bin: Option<&Path>) -> (String, PathBuf) {
    use intent_core::{
        now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceId, WorkspaceStatus,
    };
    use intent_store::Store;
    let store = Store::open(&data_dir.join("intentd.db"))
        .await
        .expect("open store");
    let ws_id = WorkspaceId::new();
    let repo_dir = data_dir.join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    init_git_repo(&repo_dir);
    let ts = now_iso();
    let ws = Workspace {
        id: ws_id.clone(),
        title: "E2E".to_string(),
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
        worktree_path: Some(repo_dir.to_string_lossy().to_string()),
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
    };
    store.insert_workspace(&ws).await.expect("insert workspace");
    // Seed context.auggiePath via config.toml (TOML-backed setting) so the
    // daemon's settings registry picks it up on boot. providers.active must
    // be set too: unset provider settings resolve the completeOnce gate
    // CLOSED, which would skip LLM generation entirely.
    if let Some(bin) = auggie_bin {
        let toml = format!(
            "[context]\nauggiePath = {:?}\n\n[providers]\nactive = \"auggie\"\n",
            bin.to_string_lossy()
        );
        std::fs::write(data_dir.join("config.toml"), toml).expect("write config.toml");
    }
    drop(store);
    (ws_id.0, repo_dir)
}

#[tokio::test]
async fn auto_commit_uses_generated_message_over_wss() {
    let Some(script) = gate("WSS auto-commit LLM E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    // Subject + multi-line body reply: newlines inside the JSON string arrive
    // as `\n` escapes, per the commit-message.md contract.
    let auggie_bin = fake_auggie(
        r#"{"subject": "feat: add new feature via LLM", "body": "Adds the feature via the LLM path.\n\n- covers body composition"}"#,
    );
    let auggie_dir = auggie_bin.parent().unwrap().to_path_buf();
    let (ws_id, repo_dir) = seed_workspace_with_repo(&data_dir, Some(&auggie_bin)).await;

    // Mock agent behavior: write a file via the ACP fs/write_text_file client
    // service (the real attribution pipeline) then return (triggers agent:idle).
    let behavior = json!({
        "clientCalls": [
            {
                "method": "fs/write_text_file",
                "params": {
                    "sessionId": "mock-session-1",
                    "path": "change.txt",
                    "content": "agent wrote this\n",
                },
            },
        ],
        "response": "done",
    })
    .to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("RUST_LOG", "debug"),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
        auggie_dir: Some(auggie_dir),
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

    // Subscribe to agent:idle events.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:idle"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // RPC conn: create a task note first, then use agent.wakeOrCreate to link the agent properly.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let task_note = wss_rpc(
        &mut rpc,
        10,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "E2E Test Task", "content": "# E2E Test Task\n\nTest auto-commit.", "tags": [] }),
    )
    .await;
    let task_note_id = task_note["note"]["id"]
        .as_str()
        .expect("task note id")
        .to_string();

    // Mark the note as a task
    wss_rpc(
        &mut rpc,
        11,
        "task.markAsTask",
        json!({ "workspaceId": ws_id, "noteId": task_note_id, "status": "not_started" }),
    )
    .await;

    // Use agent.wakeOrCreate which properly links the agent to the task note
    let wake_result = wss_rpc(
        &mut rpc,
        12,
        "agent.wakeOrCreate",
        json!({
            "workspaceId": ws_id,
            "taskNoteId": task_note_id,
            "contextMessage": "do the task",
            "model": "mock:default",
            "create": {
                "name": "WSS Builder"
            }
        }),
    )
    .await;
    let agent_id = wake_result["agentId"]
        .as_str()
        .expect("agent id")
        .to_string();

    // Wait for agent:idle event (bounded wait).
    let idle_frame = wss_event(&mut sub, 60).await;
    eprintln!(
        "idle_frame: {}",
        serde_json::to_string_pretty(&idle_frame).unwrap()
    );
    assert_eq!(idle_frame["params"]["event"]["type"], "agent:idle");

    // Verify the agent session has the task_note_id set
    let agent_list = wss_rpc(&mut rpc, 14, "agent.list", json!({ "workspaceId": ws_id })).await;
    eprintln!(
        "agent.list: {}",
        serde_json::to_string_pretty(&agent_list).unwrap()
    );

    // Give auto-commit time to process the idle event and commit
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Check git status after auto-commit
    let status = wss_rpc(&mut rpc, 16, "git.status", json!({ "workspaceId": ws_id })).await;
    eprintln!(
        "git.status after idle: {}",
        serde_json::to_string_pretty(&status).unwrap()
    );

    // Assert the commit via git.commits RPC.
    let commits = wss_rpc(
        &mut rpc,
        17,
        "git.commits",
        json!({ "workspaceId": ws_id, "limit": 2 }),
    )
    .await;
    eprintln!(
        "commits response: {}",
        serde_json::to_string_pretty(&commits).unwrap()
    );

    // Print daemon logs on failure for debugging
    if commits["items"].as_array().map_or(0, std::vec::Vec::len) < 2 {
        eprintln!("\n=== DAEMON LOGS (last 200 lines) ===");
        if let Ok(logs) = std::fs::read_to_string(data_dir.join("daemon.log")) {
            let lines: Vec<&str> = logs.lines().collect();
            let start = lines.len().saturating_sub(200);
            for line in &lines[start..] {
                eprintln!("{line}");
            }
        }
        eprintln!("===================\n");
    }

    let items = commits["items"].as_array().expect("items array");
    assert!(
        items.len() >= 2,
        "expected at least 2 commits (seed + auto-commit), got {}",
        items.len()
    );
    let head = &items[0];
    let message = head["message"].as_str().expect("commit message");
    assert!(
        message.starts_with("feat: add new feature via LLM"),
        "generated message in commit: {message}"
    );
    // git.commits parses trailers into top-level fields.
    assert_eq!(
        head["agentId"].as_str(),
        Some(agent_id.as_str()),
        "Agent-Id trailer parsed: {head}"
    );
    assert_eq!(
        head["linkedNoteId"].as_str(),
        Some(&task_note_id[..]),
        "Linked-Note-Id trailer parsed: {head}"
    );

    // `git.commits` carries only the summary line; assert the full raw
    // message to prove the body survives the service-to-commit path: subject
    // + blank line + body, with attribution trailers appended after the body.
    let full_message = run_git(&["log", "-1", "--format=%B"], &repo_dir);
    let expected = format!(
        "feat: add new feature via LLM\n\nAdds the feature via the LLM path.\n\n- covers body composition\n\nAgent-Id: {agent_id}\nLinked-Note-Id: {task_note_id}"
    );
    assert_eq!(
        full_message, expected,
        "subject + body + trailers compose in order: {full_message}"
    );
}

#[tokio::test]
async fn auto_commit_falls_back_when_auggie_missing() {
    let Some(script) = gate("WSS auto-commit fallback E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let (ws_id, _repo_dir) = seed_workspace_with_repo(&data_dir, None).await;

    // Mock agent behavior: write a file via the ACP fs/write_text_file client
    // service (the real attribution pipeline) then return.
    let behavior = json!({
        "clientCalls": [
            {
                "method": "fs/write_text_file",
                "params": {
                    "sessionId": "mock-session-1",
                    "path": "fallback.txt",
                    "content": "fallback test\n",
                },
            },
        ],
        "response": "done",
    })
    .to_string();
    // NO INTENTD_AUGGIE_BIN set — auggie will not be found.
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
        auggie_dir: None,
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

    // RPC conn for settings + agent ops.
    let mut rpc = connect_ws(port, cfg.clone()).await;

    // Set context.auggiePath to a nonexistent path so auggie resolution fails deterministically.
    wss_rpc(
        &mut rpc,
        1,
        "settings.update",
        json!({ "changes": [{ "path": "context.auggiePath", "value": "/nonexistent/auggie/for/fallback/test" }] }),
    )
    .await;

    // Subscribe to agent:idle events.
    let mut sub = connect_ws(port, cfg.clone()).await;
    wss_rpc(
        &mut sub,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["agent:idle"], "workspaceId": ws_id }),
    )
    .await;

    // Create task note and agent linked to the task.
    let task = wss_rpc(
        &mut rpc,
        10,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Fallback Task", "content": "", "tags": [] }),
    )
    .await;
    let task_id = task["note"]["id"].as_str().expect("task note id");
    wss_rpc(
        &mut rpc,
        11,
        "task.markAsTask",
        json!({ "workspaceId": ws_id, "noteId": task_id, "status": "in_progress" }),
    )
    .await;

    // Wake or create an agent for the task.
    let woke = wss_rpc(
        &mut rpc,
        12,
        "agent.wakeOrCreate",
        json!({ "workspaceId": ws_id, "taskNoteId": task_id, "contextMessage": "write a file", "model": "mock:default" }),
    )
    .await;
    let agent_id = woke["agentId"].as_str().expect("agent id").to_string();

    // Wait for agent:idle event.
    wss_event(&mut sub, 60).await;

    // Give auto-commit time to process the idle event and commit
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Assert the commit used the fallback subject (task title).
    let commits = wss_rpc(
        &mut rpc,
        13,
        "git.commits",
        json!({ "workspaceId": ws_id, "limit": 2 }),
    )
    .await;
    let head = &commits["items"][0];
    let message = head["message"].as_str().expect("commit message");
    assert!(
        message.starts_with("Fallback Task"),
        "fallback subject in commit: {message}"
    );
    // Trailers still present (parsed into top-level fields).
    assert_eq!(
        head["agentId"].as_str(),
        Some(agent_id.as_str()),
        "Agent-Id trailer in fallback: {head}"
    );
    assert_eq!(
        head["linkedNoteId"].as_str(),
        Some(task_id),
        "Linked-Note-Id trailer in fallback: {head}"
    );
}
