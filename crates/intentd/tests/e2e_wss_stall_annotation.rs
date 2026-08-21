//! WSS end-to-end for the suspected-stall wake annotation (monorepo#1016).
//!
//! A parent delegates a task-linked child (`ws.agent.delegate({ taskNoteId })`)
//! whose mock provider goes idle WITHOUT calling `ws.agent.reportToParent` and
//! without completing the task note. The parent's completion wake must carry
//! the suspected-stall annotation (task title + still-incomplete status +
//! wakeOrCreate hint) and its `event_notification` message metadata must
//! carry `stallSuspected: true` + `taskStatus` — all observed over the real
//! WSS transport (agent.getConversation after the wake turn settles).

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
                let lines: Vec<_> = log.lines().rev().take(40).collect();
                eprintln!("Last 40 lines of daemon.log:");
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
    let dir = PathBuf::from("/tmp").join(format!("itd-stall-{}", &id[..8]));
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

const TASK_TITLE: &str = "Stall e2e task";
const TASK_NOTE_ID: &str = "stall-task-note";

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
            title: "STALL-E2E".to_string(),
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
            content: "do the stall e2e work".to_string(),
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

/// monorepo#1016 over the wire: a task-linked delegated child that idles with
/// NO completion report while its task note is still `in_progress` produces a
/// parent wake whose text carries the suspected-stall annotation and whose
/// message metadata carries `stallSuspected: true` + `taskStatus`.
#[tokio::test]
async fn stall_annotated_wake_reaches_parent_over_wss() {
    let Some(script) = gate("WSS stall-annotation E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_and_task_note(&data_dir).await;

    const CHILD_MARK: &str = "STALL_E2E_CHILD_TURN";
    const PARENT_GO: &str = "STALL_E2E_PARENT_GO";
    // The child idles WITHOUT reportToParent and WITHOUT completing the task
    // note — the exact monorepo#1016 stall shape.
    let delegate_js = format!(
        "return await ws.agent.delegate({{ taskNoteId: {}, agentInstructions: {}, model: 'mock:default' }});",
        json!(TASK_NOTE_ID),
        json!(CHILD_MARK),
    );
    let behavior = json!({
        "rules": [
            {
                "ifPromptContains": CHILD_MARK,
                "response": "child ran out of context and just stopped",
            },
            {
                "ifPromptContains": "[WORKSPACE EVENTS]",
                "response": "parent acknowledged the wake",
            },
            {
                "ifPromptContains": PARENT_GO,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": delegate_js, "summary": "delegate stall child" },
                },
                "response": "parent delegated",
            },
        ],
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
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

    // The child completes (idle, no report) → the watch fires the annotated
    // wake → the parent runs its wake turn and idles again. Wait until the
    // parent has idled at least twice (delegating turn + wake turn).
    let mut parent_idles = 0u32;
    for _ in 0..400 {
        let frame = wss_event(&mut sub, 90).await;
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:idle" && ev["data"]["agentId"] == json!(parent_id) {
            parent_idles += 1;
            if parent_idles >= 2 {
                break;
            }
        }
    }
    assert!(
        parent_idles >= 2,
        "parent idled after the delegating turn AND the wake turn"
    );

    // Read the parent's transcript over WSS and find the wake message.
    let conv = wss_rpc(
        &mut rpc,
        20,
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

    // Annotated text: stall marker + task title + still-incomplete status +
    // the wakeOrCreate hint.
    assert!(
        wake_text.contains("may have stalled rather than finished (monorepo#1016)"),
        "wake text annotated: {wake_text}"
    );
    assert!(
        wake_text.contains(TASK_TITLE),
        "annotation names the task: {wake_text}"
    );
    assert!(
        wake_text.contains("is still in_progress"),
        "annotation carries the wire status: {wake_text}"
    );
    assert!(
        wake_text.contains("ws.agent.wakeOrCreate"),
        "annotation suggests wakeOrCreate: {wake_text}"
    );

    // Machine-readable metadata on the wake message row.
    let metadata = &wake["metadata"];
    assert_eq!(
        metadata["type"],
        json!("event_notification"),
        "wake metadata: {metadata}"
    );
    assert_eq!(
        metadata["stallSuspected"],
        json!(true),
        "stallSuspected lifted to metadata: {metadata}"
    );
    assert_eq!(
        metadata["taskStatus"],
        json!("in_progress"),
        "taskStatus in metadata: {metadata}"
    );
    assert_eq!(
        metadata["events"][0]["data"]["stallSuspected"],
        json!(true),
        "per-event data annotated: {metadata}"
    );
}
