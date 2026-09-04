//! WSS end-to-end regression for intent-hq/monorepo#2539: a daemon restart
//! mid-turn must not lose the conversation tail on resume.
//!
//! A `session/load` resume replays the PROVIDER's session checkpoint, which
//! only contains completed turns — the interrupted `session/prompt` never
//! resolved provider-side, so the interrupting user message and the partial
//! assistant output vanished from the model-visible context even though the
//! daemon transcript (and the UI) kept them. The fix rebuilds that tail from
//! the transcript and delivers it prompt-only ahead of the continuation.
//!
//! Flow: daemon1 parks the first turn after streaming one chunk
//! (`blockUntilCancel`), graceful shutdown (system.shutdown) flushes the
//! partial assistant row + captures the `interrupted_agent` row; daemon2 (mock
//! advertises `loadSession` and accepts the resume) resolves the agent via
//! `agent.resolveInterrupted { resume }`. Asserts on the resumed child's
//! prompt log: the continuation prompt carries the interrupting user message
//! AND the streamed partial marker with the cut-off disclosure, the session
//! was established via `session/load` (proving the resume branch, not the
//! recreate/history-replay branch, delivered the tail), and the recap is
//! prompt-only (never persisted as a transcript row).
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
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

/// The interrupting user message from the live occurrence (#2539).
const LOST_USER_MSG: &str = "build a simple local webapp that surfaces the board";
/// The chunk `blockUntilCancel` streams before parking — the partial output.
const PARTIAL_MARKER: &str = "streaming-before-cancel";
/// Stable prefix of the continuation wording in
/// `Services::resume_interrupted_agent` — the delivered message embeds a
/// per-resume humanized outage duration after it, so asserts match on the
/// prefix plus [`CONTINUATION_SUFFIX`].
const CONTINUATION_PREFIX: &str = "You were interrupted for about ";
/// The fixed remainder after the duration clause.
const CONTINUATION_SUFFIX: &str = "due to a harness shutdown and restart. You can now continue \
     your work and pick up where you left off.";

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-tail-recap-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
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
    use tokio::io::{AsyncBufReadExt, BufReader};
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
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        use sha2::{Digest, Sha256};
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

async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    loop {
        let next = timeout(Duration::from_secs(20), ws.next())
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

/// Skip gate: the mock ACP script must exist (mirrors the sibling suites).
fn gate(test: &str) -> Option<String> {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if !Path::new(&script).exists() {
        eprintln!("Skip {test}: mock ACP not found at {script}");
        return None;
    }
    Some(script)
}

fn spawn_daemon(
    data_dir: &Path,
    script: &str,
    behavior: &Value,
    prompt_log: &Path,
    session_log: &Path,
    log_name: &str,
) -> Child {
    use std::os::unix::process::CommandExt;
    common::enable_ws_api(data_dir);
    // Pin resumeInterruptedOnStart=off so the captured row stays pending until
    // the explicit `agent.resolveInterrupted` call (the `auto` default resumes
    // on headless hosts, which would race the assertions below).
    common::disable_resume_on_start(data_dir);
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_LEGACY_IMPORT_ROOTS", "")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("INTENTD_TCP_PORT", "0")
        .env("MOCK_AGENT_SCRIPT_PATH", script)
        .env("MOCK_AGENT_BEHAVIOR", behavior.to_string())
        .env("MOCK_AGENT_PROMPT_LOG", prompt_log)
        .env("MOCK_AGENT_SESSION_LOG", session_log)
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            std::fs::File::create(data_dir.join(log_name)).unwrap(),
        ));
    cmd.process_group(0);
    cmd.spawn().expect("spawn intentd serve")
}

/// Poll a JSONL log file until `pred` matches a line; returns matching lines.
async fn await_log_lines<F>(path: &Path, what: &str, pred: F) -> Vec<Value>
where
    F: Fn(&Value) -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let lines: Vec<Value> = std::fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect();
        if lines.iter().any(&pred) {
            return lines;
        }
        assert!(
            tokio::time::Instant::now() <= deadline,
            "timed out waiting for {what}; log so far: {lines:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Poll the store until the agent's transcript satisfies `pred`.
async fn await_transcript<F>(data_dir: &Path, agent_id: &str, what: &str, pred: F)
where
    F: Fn(&[intent_core::AgentMessage]) -> bool,
{
    use intent_core::AgentId;
    let store = intent_store::Store::open(&data_dir.join("intentd.db"))
        .await
        .expect("open store");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let session = store
            .get_agent_session(&AgentId(agent_id.to_string()))
            .await
            .expect("get agent session");
        if pred(&session.messages) {
            return;
        }
        assert!(
            tokio::time::Instant::now() <= deadline,
            "timed out waiting for {what}; transcript: {:?}",
            session
                .messages
                .iter()
                .map(|m| (m.role.clone(), m.content.to_string()))
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn workspace_seed(id: &intent_core::WorkspaceId) -> intent_core::Workspace {
    use intent_core::{now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus};
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "TAIL-RECAP-E2E".to_string(),
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
    }
}

/// The #2539 regression: interrupt an agent mid-turn with a graceful daemon
/// shutdown, resume on a fresh daemon whose provider ACCEPTS `session/load`,
/// and assert the continuation prompt replays the interrupted turn's tail.
#[tokio::test]
async fn resume_via_session_load_replays_interrupted_tail() {
    let Some(script) = gate("resume_via_session_load_replays_interrupted_tail") else {
        return;
    };

    let data_dir = temp_data_dir();
    let socket = data_dir.join("intentd.sock");
    let prompt_log = data_dir.join("prompts.jsonl");
    let session_log = data_dir.join("sessions.jsonl");
    let ws_id = "ws-tail-recap";

    {
        use intent_core::WorkspaceId;
        use intent_store::Store;
        let store = Store::open(&data_dir.join("intentd.db"))
            .await
            .expect("open store");
        store
            .insert_workspace(&workspace_seed(&WorkspaceId(ws_id.to_string())))
            .await
            .expect("insert ws");
    }

    // Both daemons share one behavior: `blockUntilCancel` parks the FIRST
    // turn after streaming `streaming-before-cancel\n` (the partial output),
    // and `loadSession: true` advertises + accepts `session/load`, forcing
    // the resume down the load branch (no recreate, no history replay) —
    // exactly the branch that lost the tail in the live occurrence.
    let behavior = json!({ "blockUntilCancel": true, "loadSession": true });

    // ── Phase 1: daemon1 — park the turn mid-stream, shut down gracefully.
    let child1 = spawn_daemon(
        &data_dir,
        &script,
        &behavior,
        &prompt_log,
        &session_log,
        "daemon1.log",
    );
    let mut daemon1 = common::DaemonGuard::new(child1, data_dir.clone(), false);
    assert!(await_uds(&socket).await, "daemon1 did not start");

    let created = uds_rpc(
        &socket,
        1,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Tail Agent", "model": "default", "provider": "mock" }),
    )
    .await;
    let agent_id = created["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    let sent = uds_rpc(
        &socket,
        2,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": LOST_USER_MSG }),
    )
    .await;
    assert_eq!(
        sent["result"]["success"],
        json!(true),
        "sendMessage: {sent}"
    );

    // Wait until the mock streamed the partial chunk (it is in the live turn
    // buffer once the prompt log has turn 1 — written before parking).
    await_log_lines(&prompt_log, "turn-1 prompt", |l| l["turn"] == json!(1)).await;

    // Graceful shutdown: flushes the partial assistant row (tagged
    // `metadata.interrupted`) and captures the interrupted_agent row.
    let shutdown = uds_rpc(&socket, 3, "system.shutdown", json!({})).await;
    assert_eq!(shutdown["result"].get("ok"), Some(&json!(true)));
    let exited = timeout(Duration::from_secs(15), async {
        loop {
            if let Ok(Some(_)) = daemon1.child_mut().try_wait() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(exited.is_ok(), "daemon1 did not exit after system.shutdown");
    drop(daemon1);

    // The durable transcript must already hold the tail: the user message
    // and the interrupted partial assistant row (this is what the UI shows).
    await_transcript(&data_dir, &agent_id, "flushed partial row", |msgs| {
        let has_user = msgs
            .iter()
            .any(|m| m.role == "user" && m.content.to_string().contains(LOST_USER_MSG));
        let has_partial = msgs.iter().any(|m| {
            m.role == "assistant"
                && m.metadata
                    .as_ref()
                    .and_then(|meta| meta.get("interrupted"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                && m.content.to_string().contains(PARTIAL_MARKER)
        });
        has_user && has_partial
    })
    .await;

    // ── Phase 2: daemon2 — resume via RPC; the mock accepts session/load.
    let child2 = spawn_daemon(
        &data_dir,
        &script,
        &behavior,
        &prompt_log,
        &session_log,
        "daemon2.log",
    );
    let _daemon2 = common::DaemonGuard::new(child2, data_dir.clone(), true);
    assert!(await_uds(&socket).await, "daemon2 did not start");

    let status = common::await_wss_status(&socket).await;
    let fp = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let cfg = client_config(&fp);
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    let mut ws = common::wss_connect_with_retry(port, cfg, &url).await;

    let list = wss_rpc(&mut ws, 4, "agent.listInterrupted", json!({})).await;
    assert!(
        list["agents"]
            .as_array()
            .is_some_and(|a| a.iter().any(|x| x["agentId"] == json!(agent_id))),
        "agent should be pending interrupted: {list}"
    );

    let resolved = wss_rpc(
        &mut ws,
        5,
        "agent.resolveInterrupted",
        json!({ "resume": [agent_id.clone()] }),
    )
    .await;
    assert_eq!(
        resolved["resumed"].as_array().map(Vec::len),
        Some(1),
        "resume failed: {resolved}"
    );

    // ── Phase 3: assert on the resumed child's logs.
    // The continuation prompt (turn 1 of the NEW child process — a fresh pid)
    // must carry the recap: the interrupting user message, the partial-output
    // marker, the cut-off disclosure, and the approved continuation wording.
    let is_continuation =
        |t: &str| t.contains(CONTINUATION_PREFIX) && t.contains(CONTINUATION_SUFFIX);
    let prompts = await_log_lines(&prompt_log, "continuation prompt", |l| {
        l["text"].as_str().is_some_and(is_continuation)
    })
    .await;
    let continuation = prompts
        .iter()
        .find(|l| l["text"].as_str().is_some_and(is_continuation))
        .expect("continuation prompt line");
    let text = continuation["text"].as_str().unwrap();
    assert!(
        text.contains(LOST_USER_MSG),
        "continuation prompt must replay the interrupting user message; got: {text}"
    );
    assert!(
        text.contains(PARTIAL_MARKER),
        "continuation prompt must replay the partial assistant output; got: {text}"
    );
    assert!(
        text.contains("did NOT complete"),
        "continuation prompt must disclose the cut-off explicitly; got: {text}"
    );
    // The replayed segments are far under the per-segment cap and nothing was
    // elided, so the truncation hint (intent#3696) must NOT ride the recap —
    // it is reserved for recaps that actually cut something.
    assert!(
        !text.contains("cut by this recap") && !text.contains("truncated=\""),
        "an untruncated recap must not carry the truncation hint; got: {text}"
    );

    // The tail must have been delivered on the session/load branch — prove
    // the resumed session came from `session/load`, not a recreate whose
    // history replay would have carried the tail anyway.
    let sessions: Vec<Value> = std::fs::read_to_string(&session_log)
        .unwrap_or_default()
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect();
    // The prompt log has no pid field to correlate on; the LAST session
    // establishment before the continuation prompt must be a load.
    assert_eq!(
        sessions.last().map(|s| s["method"].clone()),
        Some(json!("session/load")),
        "resume must go through session/load (recreate would mask the bug): {sessions:?}"
    );

    // The recap is prompt-only: no transcript row may contain it (the UI
    // already shows the real exchange; a persisted recap would duplicate it).
    let store = intent_store::Store::open(&data_dir.join("intentd.db"))
        .await
        .expect("open store");
    let session = store
        .get_agent_session(&intent_core::AgentId(agent_id.clone()))
        .await
        .expect("get agent session");
    assert!(
        !session
            .messages
            .iter()
            .any(|m| m.content.to_string().contains("interrupted_user_message")),
        "the tail recap must be prompt-only, never persisted to the transcript"
    );
    // And exactly one user row carries the interrupting message (no dupes).
    let user_rows_with_msg = session
        .messages
        .iter()
        .filter(|m| m.role == "user" && m.content.to_string().contains(LOST_USER_MSG))
        .count();
    assert_eq!(
        user_rows_with_msg, 1,
        "the interrupting user message must appear exactly once in the transcript"
    );
}
