//! WSS end-to-end for the suspected-truncation auto-redrive
//! (intent-hq/monorepo#2863).
//!
//! A parent delegates a task-linked child whose mock provider resolves a
//! clean `end_turn` after a sustained silent tail with ZERO streamed output —
//! the incident signature of a silently-truncated turn. The daemon must NOT
//! deliver an idle/completion wake to the parent for that turn; instead it
//! injects a system-origin auto-redrive nudge (a new user-role row tagged
//! `{"type": "auto_redrive"}`) that resumes the child on the same session.
//! Once the redriven turn makes real progress, the terminal `agent:idle`
//! fires as usual and the parent's wake arrives clean. A second scenario
//! exhausts the consecutive-redrive cap and asserts the fall-through: the
//! child idles with the #2669 advisory fields and the parent's wake carries
//! the #1016 stall annotation.

#![cfg(unix)]

mod common;

use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

struct Daemon {
    child: std::process::Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use nix::sys::signal::{self, Signal};
            use nix::unistd::Pid;
            let pid = Pid::from_raw(self.child.id().cast_signed());
            let _ = signal::killpg(pid, Signal::SIGKILL);
        }
        let _ = self.child.wait();
        if std::thread::panicking() {
            eprintln!("\n=== DAEMON CLEANUP (test panicked) ===");
            eprintln!("Data dir: {}", self.data_dir.display());
            let log_path = self.data_dir.join("daemon.log");
            if let Ok(log) = std::fs::read_to_string(&log_path) {
                let lines: Vec<_> = log.lines().rev().take(60).collect();
                eprintln!("Last 60 lines of daemon.log:");
                for line in lines.iter().rev() {
                    eprintln!("  {line}");
                }
            }
        }
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> std::process::Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    common::enable_ws_api(data_dir);
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::from(log));
    for (k, v) in env {
        cmd.env(k, v);
    }
    #[cfg(unix)]
    cmd.process_group(0);
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
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
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
                if v["id"] == json!(id) && v.get("result").is_some() {
                    return v["result"].clone();
                } else if v["id"] == json!(id) {
                    panic!("rpc errored: {v}");
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

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-redrive-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir");
    dir
}

fn gate(test: &str) -> Option<String> {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if !std::path::Path::new(&script).exists() {
        eprintln!("Skip {test}: mock ACP not found at {script}");
        return None;
    }
    Some(script)
}

const TASK_TITLE: &str = "Redrive e2e task";
const TASK_NOTE_ID: &str = "redrive-task-note";

/// Seed a workspace AND an `in_progress` task note into the daemon's `SQLite`
/// before it boots, so the mock behavior JSON can reference the note id.
async fn seed_workspace_and_task_note(data_dir: &Path) -> String {
    use intent_core::{
        now_iso, ContentType, Note, NoteId, NoteMetadata, NoteVisibility, TaskMetadata, TaskStatus,
        Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceId, WorkspaceStatus,
    };
    use intent_store::Store;
    let db_path = data_dir.join("intentd.db");
    let store = Store::open(&db_path).await.expect("open store");
    let ws = WorkspaceId::new();
    let ts = now_iso();
    store
        .insert_workspace(&Workspace {
            id: ws.clone(),
            title: "REDRIVE-E2E".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            status_image_asset_id: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: ts.clone(),
            updated_at: ts.clone(),
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
            cow_supported: None,
            display_status: None,
            waiting: false,
            checkout_mode: None,
            disk_usage: None,
            pending_delete_at: None,
        })
        .await
        .expect("insert ws");
    store
        .insert_note(&Note {
            id: NoteId::from(TASK_NOTE_ID),
            workspace_id: ws.clone(),
            title: TASK_TITLE.to_string(),
            content: "do the redrive e2e work".to_string(),
            content_type: ContentType::Markdown,
            tags: vec![],
            is_pinned: false,
            is_archived: false,
            is_default: false,
            parent_id: None,
            visibility: NoteVisibility::Workspace,
            metadata: NoteMetadata {
                task: Some(TaskMetadata {
                    status: TaskStatus::InProgress,
                    ..Default::default()
                }),
            },
            created_at: ts.clone(),
            rev: 0,
            updated_at: ts,
        })
        .await
        .expect("insert task note");
    ws.0
}

const CHILD_MARK: &str = "REDRIVE_E2E_CHILD_TURN";
const PARENT_GO: &str = "REDRIVE_E2E_PARENT_GO";
/// Stable substring of the harness's auto-redrive nudge text, used both as
/// the mock's prompt-match marker for the redriven turn and as the transcript
/// assertion anchor.
const NUDGE_MARK: &str = "Automatic redrive (monorepo#2863)";

fn delegate_js() -> String {
    format!(
        "return await ws.agent.delegate({{ taskNoteId: {}, agentInstructions: {}, model: 'mock:default' }});",
        json!(TASK_NOTE_ID),
        json!(CHILD_MARK),
    )
}

/// intent-hq/monorepo#2863 over the wire — the recovery path: a delegated
/// task-linked child whose turn resolves a clean `end_turn` after a sustained
/// zero-output silent tail gets an auto-redrive nudge instead of a terminal
/// `agent:idle`; the redriven turn (which reports and streams output) then
/// idles normally, so the parent receives exactly ONE completion wake — the
/// clean post-recovery one — and the child transcript carries the
/// system-origin nudge row tagged `{"type": "auto_redrive"}`.
#[tokio::test]
async fn truncated_turn_redriven_and_no_premature_wake_over_wss() {
    let Some(script) = gate("WSS truncation auto-redrive E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_and_task_note(&data_dir).await;

    let report_js = "return await ws.agent.reportToParent('recovered after redrive');";
    let behavior = json!({
        "rules": [
            {
                // The redriven (nudge) turn: report + respond normally.
                "ifPromptContains": NUDGE_MARK,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": report_js, "summary": "report recovery" },
                },
                "response": "recovered after redrive",
            },
            {
                // The child's first turn: ZERO session/update traffic, then a
                // sustained silent tail, then a clean end_turn — the exact
                // monorepo#2863 truncation shape.
                "ifPromptContains": CHILD_MARK,
                "omitResponse": true,
                "silentTailBeforeResultMs": 3000,
            },
            {
                "ifPromptContains": "[WORKSPACE EVENTS]",
                "response": "parent acknowledged the wake",
            },
            {
                "ifPromptContains": PARENT_GO,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": delegate_js(), "summary": "delegate redrive child" },
                },
                "response": "parent delegated",
            },
        ],
    })
    .to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("INTENTD_SILENT_TAIL_SUSPECT_MS", "2000"),
    ];
    let child = spawn_serve(&data_dir, &env);
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

    // SUBSCRIBER conn — subscribe BEFORE the turn so no events are missed.
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

    // Drive to completion: the parent idles after the delegating turn and
    // again after the wake turn. Meanwhile count every OTHER agent's (the
    // child's) `agent:idle` — the truncated first turn must NOT produce one,
    // so exactly ONE child idle (the clean post-redrive one) is expected.
    // The break requires BOTH two parent idles and a child idle: a stray
    // parent wake (e.g. an unrelated `[WORKSPACE EVENTS]` batch) can produce
    // the second parent idle before the child's clean idle arrives, and
    // breaking on the parent count alone would miss the child idle.
    let mut parent_idles = 0u32;
    let mut child_idles: Vec<Value> = Vec::new();
    for _ in 0..400 {
        let frame = wss_event(&mut sub, 90).await;
        let ev = &frame["params"]["event"];
        if ev["type"] == json!("agent:idle") {
            if ev["data"]["agentId"] == json!(parent_id) {
                parent_idles += 1;
            } else {
                child_idles.push(ev.clone());
            }
            if parent_idles >= 2 && !child_idles.is_empty() {
                break;
            }
        }
    }
    assert!(
        parent_idles >= 2,
        "parent idled after the delegating turn AND the wake turn"
    );
    assert_eq!(
        child_idles.len(),
        1,
        "exactly one child idle — the truncated turn's was suppressed: {child_idles:?}"
    );
    let idle = &child_idles[0];
    assert!(
        idle["data"].get("suspectedTruncated").is_none(),
        "the post-redrive idle is clean (no advisory fields): {idle}"
    );
    assert_eq!(
        idle["data"]["completionReport"],
        json!("recovered after redrive"),
        "the redriven turn's report rides the (single) idle: {idle}"
    );
    let child_id = idle["data"]["agentId"].as_str().expect("child id");

    // Child transcript: the auto-redrive nudge is a persisted user-role row
    // tagged `{"type": "auto_redrive"}` referencing monorepo#2863.
    let conv = wss_rpc(
        &mut rpc,
        20,
        "agent.getConversation",
        json!({ "agentId": child_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let nudge = messages
        .iter()
        .find(|m| {
            m["role"] == json!("user")
                && serde_json::to_string(&m["contentBlocks"])
                    .unwrap_or_default()
                    .contains(NUDGE_MARK)
        })
        .expect("auto-redrive nudge row present in child transcript");
    assert_eq!(
        nudge["metadata"]["type"],
        json!("auto_redrive"),
        "nudge row is machine-attributable: {nudge}"
    );

    // Parent transcript: exactly one wake, and it is the CLEAN one (no
    // #1016 stall annotation — the child reported before idling).
    let conv = wss_rpc(
        &mut rpc,
        21,
        "agent.getConversation",
        json!({ "agentId": &parent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let wakes: Vec<_> = messages
        .iter()
        .filter(|m| {
            serde_json::to_string(&m["contentBlocks"])
                .unwrap_or_default()
                .contains("[WORKSPACE EVENTS]")
        })
        .collect();
    assert_eq!(wakes.len(), 1, "exactly one parent wake: {wakes:?}");
    let wake_text = serde_json::to_string(&wakes[0]["contentBlocks"]).expect("wake text");
    assert!(
        !wake_text.contains("may have stalled"),
        "the single wake is the clean post-recovery one: {wake_text}"
    );
    assert!(
        wake_text.contains("recovered after redrive"),
        "the wake carries the child's report: {wake_text}"
    );
}

/// intent-hq/monorepo#2863 over the wire — the bounded fall-through: a child
/// whose EVERY turn (initial + each nudge) truncates exhausts the consecutive
/// auto-redrive cap; the final truncated turn falls through to today's
/// behavior — a terminal `agent:idle` carrying the #2669 advisory fields —
/// and the parent's wake carries the #1016 stall annotation so coordinators
/// see the genuinely dead session without reading logs. The transcript shows
/// exactly `MAX_CONSECUTIVE_TRUNCATION_REDRIVES` (3) nudge rows.
#[tokio::test]
async fn redrive_cap_exhaustion_falls_through_to_annotated_idle_over_wss() {
    let Some(script) = gate("WSS truncation redrive-cap E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_and_task_note(&data_dir).await;

    let behavior = json!({
        "rules": [
            {
                // Every nudge turn truncates again: zero output + silent tail.
                "ifPromptContains": NUDGE_MARK,
                "omitResponse": true,
                "silentTailBeforeResultMs": 3000,
            },
            {
                "ifPromptContains": CHILD_MARK,
                "omitResponse": true,
                "silentTailBeforeResultMs": 3000,
            },
            {
                "ifPromptContains": "[WORKSPACE EVENTS]",
                "response": "parent acknowledged the wake",
            },
            {
                "ifPromptContains": PARENT_GO,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": delegate_js(), "summary": "delegate redrive child" },
                },
                "response": "parent delegated",
            },
        ],
    })
    .to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("INTENTD_SILENT_TAIL_SUSPECT_MS", "2000"),
    ];
    let child = spawn_serve(&data_dir, &env);
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

    // 4 truncated child turns (initial + 3 nudges) run back-to-back before
    // the fall-through idle fires; the parent then runs its wake turn.
    let mut parent_idles = 0u32;
    let mut child_idles: Vec<Value> = Vec::new();
    for _ in 0..400 {
        let frame = wss_event(&mut sub, 120).await;
        let ev = &frame["params"]["event"];
        if ev["type"] == json!("agent:idle") {
            if ev["data"]["agentId"] == json!(parent_id) {
                parent_idles += 1;
                if parent_idles >= 2 {
                    break;
                }
            } else {
                child_idles.push(ev.clone());
            }
        }
    }
    assert!(
        parent_idles >= 2,
        "parent idled after the delegating turn AND the wake turn"
    );
    assert_eq!(
        child_idles.len(),
        1,
        "exactly one child idle — the fall-through one: {child_idles:?}"
    );
    let idle = &child_idles[0];
    assert_eq!(
        idle["data"]["suspectedTruncated"],
        json!(true),
        "the fall-through idle carries the #2669 advisory: {idle}"
    );
    assert!(
        idle["data"]["silentTailMs"].as_u64().expect("silentTailMs") >= 2000,
        "advisory tail past the lowered threshold: {idle}"
    );
    let child_id = idle["data"]["agentId"]
        .as_str()
        .expect("child id")
        .to_string();

    // Exactly MAX_CONSECUTIVE_TRUNCATION_REDRIVES nudge rows were injected.
    let conv = wss_rpc(
        &mut rpc,
        20,
        "agent.getConversation",
        json!({ "agentId": &child_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let nudges = messages
        .iter()
        .filter(|m| {
            m["role"] == json!("user")
                && serde_json::to_string(&m["contentBlocks"])
                    .unwrap_or_default()
                    .contains(NUDGE_MARK)
        })
        .count();
    assert_eq!(nudges, 3, "the redrive cap bounds the nudges at 3");

    // Parent wake: the #1016 stall annotation surfaces the dead session.
    let conv = wss_rpc(
        &mut rpc,
        21,
        "agent.getConversation",
        json!({ "agentId": &parent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let wake = messages
        .iter()
        .find(|m| {
            serde_json::to_string(&m["contentBlocks"])
                .unwrap_or_default()
                .contains("[WORKSPACE EVENTS]")
        })
        .expect("wake message present in parent transcript");
    let wake_text = serde_json::to_string(&wake["contentBlocks"]).expect("wake text");
    assert!(
        wake_text.contains("may have stalled rather than finished (monorepo#1016)"),
        "wake carries the stall annotation: {wake_text}"
    );
    assert!(
        wake_text.contains(TASK_TITLE),
        "annotation names the task: {wake_text}"
    );
    let metadata = &wake["metadata"];
    assert_eq!(
        metadata["stallSuspected"],
        json!(true),
        "stallSuspected lifted to metadata: {metadata}"
    );
}
