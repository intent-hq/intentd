//! WSS e2e regression for the Q&A **question hold** lifecycle (PROTOCOL §5.5).
//!
//! Drives the full hold lifecycle over the real WSS transport with two mock
//! ACP agents per scenario:
//!
//! * An asker agent emits a `ws.app.question.ask` question (trailing
//!   `application/vnd.intent.question+json` resource block) — the hold
//!   activates.
//! * A sibling agent fires AUTOMATIC deliveries at the asker through the
//!   `ws.agent.send` host binding (one `priority: "queue"` opt-out, one
//!   default-interrupt — the binding delivers with interrupt priority when
//!   `priority` is omitted): both must park in the queue
//!   (`heldForQuestions: true` on the send results, captured via a note),
//!   with the interrupt entry ordered FIRST (`interruptPriority: true`),
//!   and NO user row reaches the asker's transcript while the hold is
//!   active. `agent:queue:updated` snapshots carry the held entries in
//!   interrupt-first order.
//! * Release path 1 (user answer): `agent.sendMessage` (user origin) delivers
//!   IMMEDIATELY (`queued: false` — never held), the hold flips false at turn
//!   end and the queue drains interrupt-first.
//! * Release path 2 (`agent.dismissQuestions`): persists the dismissal marker
//!   (`agent:updated` with `dismissedQuestionsMessageId`, surfaced on the
//!   `agent.list` metadata projection), delivers the questions-dismissed
//!   system notice to the agent BEFORE any held entry (tagged
//!   `messageMetadata.type: "questions_dismissed"` so the FE can render it
//!   as a system chip), and drains the held queue behind it — the asker's
//!   transcript gains the notice, then the drained held message.
//! * Dismissal notice, idle agent + empty queue: `agent.dismissQuestions`
//!   starts the notice turn IMMEDIATELY (never queues) — the mock's ack rule
//!   matches on the notice wording, proving the turn prompt carried the
//!   dismissal text, and the persisted user row carries the
//!   `questions_dismissed` metadata.
//! * Dismissal notice, undelivered queue entry: dismissing an OLDER question
//!   while a NEWER one still holds automatic deliveries parks the notice —
//!   `agent.getQueue` surfaces the entry with its `questions_dismissed`
//!   metadata promoted to the queue head.
//!
//! Gated on `node` + the mock script; skips cleanly otherwise.

#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicI64, Ordering};
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

/// MIME type the FE renders as QuestionCards (PROTOCOL §7.x question resource).
const QUESTION_MIME: &str = "application/vnd.intent.question+json";

/// Turn-1 trigger for the asker: the mock's rule matches on this so the
/// question fires ONLY on the kickoff turn.
const ASK_MARKER: &str = "ASK_HOLD_QUESTION_NOW_E2E";

/// Trigger for the sibling sender's automatic `ws.agent.send` calls.
const SEND_MARKER: &str = "SEND_HELD_MESSAGES_NOW_E2E";

/// Trigger for the second sibling's automatic interrupt `ws.agent.send`
/// (scenario 1 uses one sender per held message — the single-pending-message
/// guard refuses a second parked send from the same caller).
const SEND_URGENT_MARKER: &str = "SEND_HELD_URGENT_NOW_E2E";

/// The two automatic messages parked by the hold in scenario 1.
const HELD_NORMAL: &str = "held normal message";
const HELD_URGENT: &str = "held urgent message";

/// The single automatic message parked by the hold in scenario 2.
const HELD_DISMISS: &str = "held until dismissal";

/// Trigger for the asker's SECOND question turn (queued-notice scenario:
/// two distinct pending question messages).
const ASK_AGAIN_MARKER: &str = "ASK_SECOND_QUESTION_NOW_E2E";

/// The flattened `Q:`/`A:` answer a user sends after filling the QuestionCard.
const ANSWER_TEXT: &str = "Q: Which environment should I deploy to?\nA: Staging";
/// An UNTAGGED user message sent while questions are pending: never held
/// (user origin), but carries no `question_answers` tag, so it must NOT
/// release the hold.
const PLAIN_USER_TEXT: &str = "unrelated aside while questions are pending";

/// Leading wording of the questions-dismissed system notice (single-question
/// dismissals — both scenarios below dismiss one-question messages).
const NOTICE_PREFIX: &str = "User dismissed your 1 question without answering.";

/// The mock's ack reply for the dismissal-notice turn: its rule matches on
/// the notice wording, so this text appearing in the transcript proves the
/// turn PROMPT carried the dismissal text.
const DISMISS_ACK: &str = "dismissal notice acknowledged";

/// Monotonic JSON-RPC id source shared by all helpers/tests in this file.
static NEXT_ID: AtomicI64 = AtomicI64::new(100);

fn next_id() -> i64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

/// Live `intentd serve` process; killed and its data dir removed on drop.
struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        // Only dump the (potentially large) daemon log on test failure — cuts
        // CI noise on the common green-run path.
        if std::thread::panicking() {
            let log_path = self.data_dir.join("daemon.log");
            if let Ok(log) = std::fs::read_to_string(&log_path) {
                eprintln!("=== DAEMON LOG ===\n{}\n=== END LOG ===", log);
            }
        }
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-qhold-{}", &id[..8]));
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
async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let id = next_id();
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    loop {
        let next = timeout(Duration::from_secs(30), ws.next())
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
            Some(Ok(_)) => continue,
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

/// Pre-seed the daemon's SQLite store with a regular (NON-chief) workspace.
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
            title: "QHOLD-E2E".to_string(),
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
            execution_environment: None,
            disk_usage: None,
            pending_delete_at: None,
        })
        .await
        .expect("insert ws");
    ws.0
}

/// The single question the asker emits — one `ws.app.question.ask` call.
fn hold_question() -> Value {
    json!({
        "header": "Deploy target",
        "question": "Which environment should I deploy to?",
        "options": [
            { "label": "Staging" },
            { "label": "Production" }
        ]
    })
}

/// Mock-behavior rule that fires the asker's question on the turn whose
/// prompt carries `marker`.
fn ask_rule_with(marker: &str) -> Value {
    let ask_code = format!(
        "return await ws.app.question.ask({});",
        json!(hold_question())
    );
    json!({
        "ifPromptContains": marker,
        "toolCall": {
            "name": "workspace_api",
            "arguments": { "code": ask_code, "summary": "ask hold question" }
        },
        "response": "I have a clarifying question before I proceed."
    })
}

/// Mock-behavior rule that fires the asker's question on the kickoff turn.
fn ask_rule() -> Value {
    ask_rule_with(ASK_MARKER)
}

/// Mock-behavior rule acknowledging the questions-dismissed notice: it
/// matches on the notice WORDING, so a [`DISMISS_ACK`] assistant reply in
/// the transcript proves the turn prompt carried the dismissal text.
fn dismiss_ack_rule() -> Value {
    json!({
        "ifPromptContains": NOTICE_PREFIX,
        "response": DISMISS_ACK
    })
}

/// Spawn the daemon + return `(daemon, ws_id, port, cfg)` for a behavior.
async fn boot(script: &str, behavior: &str) -> (Daemon, String, u16, Arc<ClientConfig>) {
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", script),
        ("MOCK_AGENT_BEHAVIOR", behavior),
    ];
    let child = spawn_serve(&data_dir, &env);
    let daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    (daemon, ws_id, port, client_config(&fingerprint))
}

/// Create a mock agent and return its id.
async fn create_agent<S>(rpc: &mut WebSocketStream<S>, ws_id: &str, name: &str) -> String
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let created = wss_rpc(
        rpc,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": name, "model": "mock:default" }),
    )
    .await;
    created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string()
}

/// Drive the asker's question turn and return the persisted question
/// message's id, asserting the hold prerequisite: the transcript's LAST
/// message is the assistant row whose trailing block is the question
/// resource.
async fn drive_question_turn<S, T>(
    rpc: &mut WebSocketStream<S>,
    sub: &mut WebSocketStream<T>,
    ws_id: &str,
    asker_id: &str,
) -> String
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let sent = wss_rpc(
        rpc,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": asker_id,
            "content": format!("please plan the deploy {ASK_MARKER}"),
        }),
    )
    .await;
    assert_eq!(sent["success"], true, "ask kickoff ok: {sent}");
    await_stream_end(sub, asker_id).await;

    let conv = wss_rpc(
        rpc,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": asker_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let last = messages.last().expect("non-empty transcript");
    assert_eq!(
        last["role"], "assistant",
        "hold prerequisite: last message is the assistant question: {last}"
    );
    let blocks = last["contentBlocks"].as_array().expect("contentBlocks");
    let trailing = blocks.last().expect("blocks non-empty");
    assert_eq!(trailing["type"], "resource", "trailing block: {trailing}");
    assert_eq!(
        trailing["resource"]["mimeType"], QUESTION_MIME,
        "trailing block is the question resource: {trailing}"
    );
    last["id"]
        .as_str()
        .expect("question message id")
        .to_string()
}

/// Read a sender-captured results note by title and return its parsed JSON.
async fn read_capture_note<S>(rpc: &mut WebSocketStream<S>, ws_id: &str, title: &str) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let listed = wss_rpc(rpc, "note.list", json!({ "workspaceId": ws_id })).await;
    let note_id = listed["notes"]
        .as_array()
        .expect("notes array")
        .iter()
        .find(|n| n["title"] == title)
        .expect("capture note created by the sender")["id"]
        .as_str()
        .expect("note id")
        .to_string();
    let got = wss_rpc(
        rpc,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    let content = got["note"]["content"].as_str().expect("note content");
    serde_json::from_str(content).expect("capture note JSON")
}

/// Poll `agent.getConversation` until `pred` passes (bounded at 90s — turn
/// latency varies with host load, so the bound is deliberately generous;
/// green runs return as soon as the predicate holds); returns the final
/// conversation value.
async fn await_conversation<S, F>(
    rpc: &mut WebSocketStream<S>,
    ws_id: &str,
    agent_id: &str,
    what: &str,
    pred: F,
) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    F: Fn(&[Value]) -> bool,
{
    for _ in 0..360 {
        let conv = wss_rpc(
            rpc,
            "agent.getConversation",
            json!({ "workspaceId": ws_id, "agentId": agent_id }),
        )
        .await;
        let messages = conv["messages"].as_array().expect("messages array");
        if pred(messages) {
            return conv;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("conversation predicate never satisfied: {what}");
}

/// Poll `agent.get` until the persisted status is `idle` (bounded).
/// `agent:stream:end` precedes the status rewrite to idle by a small window,
/// so tests that depend on the idle-agent immediate-delivery path must wait
/// out that window explicitly instead of racing the dismissal against it.
async fn await_agent_idle<S>(rpc: &mut WebSocketStream<S>, agent_id: &str)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    for _ in 0..240 {
        let got = wss_rpc(rpc, "agent.get", json!({ "agentId": agent_id })).await;
        if got["agent"]["status"] == "idle" {
            return;
        }
        tokio::time::sleep(Duration::from_millis(125)).await;
    }
    panic!("agent {agent_id} never reached idle status");
}

/// 0-based index of the user row whose first text block starts with `text`
/// (held messages drain from the queue, so their rows may carry the appended
/// dequeue-wait system note).
fn user_row_index(messages: &[Value], text: &str) -> Option<usize> {
    messages.iter().position(|m| {
        m["role"] == "user"
            && m["contentBlocks"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|b| {
                    b["type"] == "text" && b["text"].as_str().is_some_and(|t| t.starts_with(text))
                })
    })
}

/// 0-based index of the assistant row whose text contains `text`.
fn assistant_row_index(messages: &[Value], text: &str) -> Option<usize> {
    messages.iter().position(|m| {
        m["role"] == "assistant"
            && m["contentBlocks"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|b| {
                    b["type"] == "text" && b["text"].as_str().is_some_and(|t| t.contains(text))
                })
    })
}

/// 0-based index of the user row whose block metadata marks it as the
/// dismissal notice for `dismissed_mid` (both notices in the two-question
/// scenario share the same wording, so text alone cannot tell them apart).
fn notice_row_index(messages: &[Value], dismissed_mid: &str) -> Option<usize> {
    messages.iter().position(|m| {
        m["role"] == "user"
            && m["contentBlocks"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|b| {
                    b["messageMetadata"]["dismissedQuestionsMessageId"] == json!(dismissed_mid)
                })
    })
}

/// The `messageMetadata` payload the dismissal notice carries on the wire —
/// on the queued entry while undelivered and on the persisted user-row block
/// once delivered (PROTOCOL §5.5).
fn dismissal_metadata(dismissed_mid: &str) -> Value {
    json!({
        "type": "questions_dismissed",
        "source": "system",
        "dismissedQuestionsMessageId": dismissed_mid,
    })
}

/// Hold lifecycle, release by USER ANSWER (PROTOCOL §5.5):
///
/// 1. The asker's kickoff turn emits a `ws.app.question.ask` question — the
///    hold activates (last transcript message = assistant question row).
/// 2. Two siblings each fire one AUTOMATIC `ws.agent.send` at the asker
///    (one `priority: "queue"` opt-out, one default-interrupt — omitted
///    `priority` resolves to interrupt in the binding; one sender per
///    message, the single-pending-message guard refuses a second parked
///    send from the same caller): both park in the queue with
///    `heldForQuestions: true` on the send results, the interrupt entry
///    FIRST (`interruptPriority: true` — spec §Decisions: interrupts are
///    held too, no exceptions), and NO user row reaches the asker's
///    transcript. `agent:queue:updated` carries the held snapshot in
///    interrupt-first order.
/// 3. The user sends an UNTAGGED message: user origin is never held, but with
///    a parked backlog it converts to a user-origin enqueue + drain kick
///    (monorepo#1791 — FIFO restore): the flush delivers the held entries and
///    the user message in ONE combined turn, transcript order urgent
///    (interrupt-first) → normal → plain, queue empty after. It carries no
///    answer tag, so the persisted pending-questions marker SURVIVES the
///    combined turn (pendingness is not superseded by an unrelated user
///    message or by the agent's own later turn).
/// 4. The user then answers via `agent.sendMessage`, tagged with
///    `messageMetadata { type: "question_answers", answeredQuestionsMessageId }`
///    — the queue is already empty so the answer delivers directly, and the
///    tag clears the marker so the hold flips false.
#[tokio::test]
async fn question_hold_parks_automatic_sends_until_user_answer_over_wss() {
    let Some(script) = gate("WSS question-hold user-answer E2E") else {
        return;
    };
    // The normal entry uses the explicit `priority: "queue"` opt-out (also
    // covering the opt-out path e2e); the urgent entry relies on the
    // binding's interrupt-by-default resolution (omitted priority).
    let send_normal_code = format!(
        "const agents = await ws.agent.list(true); \
         const target = agents.find(a => a.name === 'AskerA'); \
         const first = await ws.agent.send(target.id, '{HELD_NORMAL}', 'queue'); \
         await ws.note.create('hold-results', JSON.stringify({{ first }})); \
         return 'sent';"
    );
    let send_urgent_code = format!(
        "const agents = await ws.agent.list(true); \
         const target = agents.find(a => a.name === 'AskerA'); \
         const second = await ws.agent.send(target.id, '{HELD_URGENT}'); \
         await ws.note.create('hold-results-urgent', JSON.stringify({{ second }})); \
         return 'sent';"
    );
    let behavior = json!({
        "rules": [
            ask_rule(),
            {
                "ifPromptContains": SEND_MARKER,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": send_normal_code, "summary": "held send e2e" }
                },
                "response": "send dispatched"
            },
            {
                "ifPromptContains": SEND_URGENT_MARKER,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": send_urgent_code, "summary": "held urgent send e2e" }
                },
                "response": "urgent send dispatched"
            }
        ],
        "response": "plain reply"
    })
    .to_string();
    let (_daemon, ws_id, port, cfg) = boot(&script, &behavior).await;

    // SUBSCRIBER conn — events.subscribe BEFORE the turns so we miss nothing.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_resp["subscriptionId"].is_string(), "sub: {sub_resp}");

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let asker_id = create_agent(&mut rpc, &ws_id, "AskerA").await;
    let sender_id = create_agent(&mut rpc, &ws_id, "SenderB").await;
    let urgent_sender_id = create_agent(&mut rpc, &ws_id, "SenderC").await;

    // ---- (1) The asker's question turn activates the hold ----
    let asked_mid = drive_question_turn(&mut rpc, &mut sub, &ws_id, &asker_id).await;

    // ---- (2) Automatic sends park in the queue (one sender each) ----
    let sent = wss_rpc(
        &mut rpc,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": sender_id,
            "content": format!("message the asker {SEND_MARKER}"),
        }),
    )
    .await;
    assert_eq!(sent["success"], true, "sender kickoff ok: {sent}");
    await_stream_end(&mut sub, &sender_id).await;

    let sent = wss_rpc(
        &mut rpc,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": urgent_sender_id,
            "content": format!("message the asker {SEND_URGENT_MARKER}"),
        }),
    )
    .await;
    assert_eq!(sent["success"], true, "urgent sender kickoff ok: {sent}");

    // The wire contract (§6.5): an `agent:queue:updated` snapshot for the
    // asker carries BOTH held entries, interrupt-first. (The interim 1-entry
    // snapshot from the first send may also arrive; wait for the 2-entry one.)
    let mut held_snapshot: Option<Vec<Value>> = None;
    let mut sender_done = false;
    for _ in 0..120 {
        let frame = wss_event(&mut sub, 30).await;
        let ev = &frame["params"]["event"];
        match ev["type"].as_str() {
            Some("agent:queue:updated")
                if ev["data"]["agentId"].as_str() == Some(asker_id.as_str()) =>
            {
                let queue = ev["data"]["queue"].as_array().expect("queue array");
                if queue.len() == 2 {
                    held_snapshot = Some(queue.clone());
                }
            }
            Some("agent:stream:end")
                if ev["data"]["agentId"].as_str() == Some(urgent_sender_id.as_str()) =>
            {
                sender_done = true;
            }
            _ => {}
        }
        if held_snapshot.is_some() && sender_done {
            break;
        }
    }
    assert!(sender_done, "urgent sender turn completed");
    let snapshot = held_snapshot.expect("agent:queue:updated carried the 2-entry held snapshot");
    assert_eq!(
        snapshot[0]["content"], HELD_URGENT,
        "interrupt entry ordered FIRST: {snapshot:?}"
    );
    assert_eq!(
        snapshot[0]["interruptPriority"], true,
        "interrupt marker on the wire: {snapshot:?}"
    );
    assert_eq!(
        snapshot[1]["content"], HELD_NORMAL,
        "normal entry behind the interrupt: {snapshot:?}"
    );
    assert!(
        snapshot[1].get("interruptPriority").is_none(),
        "normal entry carries no interrupt marker: {snapshot:?}"
    );

    // The senders saw the hold on the RPC results (captured via the notes).
    let normal_capture = read_capture_note(&mut rpc, &ws_id, "hold-results").await;
    let urgent_capture = read_capture_note(&mut rpc, &ws_id, "hold-results-urgent").await;
    for (key, r) in [
        ("first", &normal_capture["first"]),
        ("second", &urgent_capture["second"]),
    ] {
        assert_eq!(r["success"], true, "{key} send ok: {r}");
        assert_eq!(r["queued"], true, "{key} send parked: {r}");
        assert_eq!(
            r["heldForQuestions"], true,
            "{key} send held by the question hold: {r}"
        );
    }
    assert_eq!(
        urgent_capture["second"]["queuedMessage"]["interruptPriority"], true,
        "interrupt send's queue entry carries the marker: {urgent_capture}"
    );

    // `agent.getQueue` agrees with the event snapshot, and the held messages
    // never reached the transcript.
    let q = wss_rpc(&mut rpc, "agent.getQueue", json!({ "agentId": asker_id })).await;
    let queue = q["queue"].as_array().expect("queue array");
    assert_eq!(queue.len(), 2, "both held entries parked: {queue:?}");
    assert_eq!(queue[0]["content"], HELD_URGENT);
    assert_eq!(queue[1]["content"], HELD_NORMAL);
    let conv = wss_rpc(
        &mut rpc,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": asker_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    assert!(
        user_row_index(messages, HELD_NORMAL).is_none()
            && user_row_index(messages, HELD_URGENT).is_none(),
        "held messages must NOT reach the transcript while the hold is active: {messages:?}"
    );

    // ---- (3) A plain user message drains the parked backlog FIFO ----
    // User origin is never held — and with a parked backlog the send
    // converts to a user-origin enqueue + drain kick (monorepo#1791): the
    // batch flush delivers the held entries and the user message in ONE
    // combined turn (drain order: urgent interrupt-first → normal → plain),
    // so newer user messages can no longer bypass an older parked wake.
    // Carrying no answer tag, the persisted pending-questions marker
    // survives the combined turn.
    let plain = wss_rpc(
        &mut rpc,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": asker_id,
            "content": PLAIN_USER_TEXT,
        }),
    )
    .await;
    assert_eq!(plain["success"], true, "plain user send ok: {plain}");
    assert_eq!(
        plain["queued"], true,
        "user send with a parked backlog converts to enqueue + flush: {plain}"
    );
    assert!(
        plain.get("heldForQuestions").is_none(),
        "user send bypasses the hold gate: {plain}"
    );

    // The combined flush turn delivers all three user rows.
    let conv = await_conversation(&mut rpc, &ws_id, &asker_id, "combined flush drained", |m| {
        user_row_index(m, PLAIN_USER_TEXT).is_some()
            && user_row_index(m, HELD_NORMAL).is_some()
            && user_row_index(m, HELD_URGENT).is_some()
    })
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let plain_idx = user_row_index(messages, PLAIN_USER_TEXT).expect("plain row");
    let urgent_idx = user_row_index(messages, HELD_URGENT).expect("urgent row");
    let normal_idx = user_row_index(messages, HELD_NORMAL).expect("normal row");
    assert!(
        urgent_idx < normal_idx && normal_idx < plain_idx,
        "drain order urgent (interrupt-first) -> normal -> plain (FIFO, \
         monorepo#1791): urgent={urgent_idx} normal={normal_idx} plain={plain_idx}"
    );
    let q = wss_rpc(&mut rpc, "agent.getQueue", json!({ "agentId": asker_id })).await;
    assert!(
        q["queue"].as_array().expect("queue array").is_empty(),
        "parked entries rode the user-led flush turn: {q}"
    );

    // The hold survived the untagged combined turn: a LATER automatic send
    // still parks.
    let held_again = wss_rpc(
        &mut rpc,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": sender_id,
            "content": format!("message the asker {SEND_MARKER}"),
        }),
    )
    .await;
    assert_eq!(
        held_again["success"], true,
        "re-send kickoff ok: {held_again}"
    );
    await_stream_end(&mut sub, &sender_id).await;
    let q = wss_rpc(&mut rpc, "agent.getQueue", json!({ "agentId": asker_id })).await;
    assert_eq!(
        q["queue"].as_array().expect("queue array").len(),
        1,
        "hold survives an untagged user message — later automatic sends still park: {q}"
    );

    // ---- (4) The user answer releases the hold and drains the backlog ----
    // The wizard tags the answer with `messageMetadata { type:
    // "question_answers", answeredQuestionsMessageId }` — the structured tag
    // (never the text) is what resolves the persisted pending marker. With
    // the re-parked entry waiting, the answer also converts to enqueue +
    // flush, delivering both in one combined turn.
    let answered = wss_rpc(
        &mut rpc,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": asker_id,
            "content": ANSWER_TEXT,
            "messageMetadata": {
                "type": "question_answers",
                "answeredQuestionsMessageId": asked_mid,
            },
        }),
    )
    .await;
    assert_eq!(answered["success"], true, "answer ok: {answered}");
    assert!(
        answered.get("heldForQuestions").is_none(),
        "user answer is never held: {answered}"
    );

    // The answer lands, the marker clears, and the queue fully drains.
    let conv = await_conversation(
        &mut rpc,
        &ws_id,
        &asker_id,
        "answer + backlog drained",
        |m| user_row_index(m, ANSWER_TEXT).is_some(),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    assert!(
        user_row_index(messages, ANSWER_TEXT).is_some(),
        "answer row landed: {messages:?}"
    );

    // Queue fully drained.
    let q = wss_rpc(&mut rpc, "agent.getQueue", json!({ "agentId": asker_id })).await;
    assert!(
        q["queue"].as_array().expect("queue array").is_empty(),
        "queue drained after the hold released: {q}"
    );
}

/// Hold lifecycle, release by `agent.dismissQuestions` (PROTOCOL §5.5):
///
/// 1. The asker's kickoff turn emits the question — hold active.
/// 2. A sibling's automatic `ws.agent.send` parks in the queue
///    (`heldForQuestions: true`).
/// 3. `agent.dismissQuestions` persists the dismissal marker: the RPC result
///    echoes `dismissedQuestionsMessageId`, `agent:updated` carries it on the
///    wire, and the `agent.list` metadata projection surfaces it (survives as
///    session metadata).
/// 4. The dismissal delivers the questions-dismissed system notice FIRST
///    (user row tagged `messageMetadata.type: "questions_dismissed"`, wording
///    carries the question count), then kicks the drain: the asker's user
///    rows are exactly kickoff → notice → drained held message, and the
///    queue is empty.
#[tokio::test]
async fn question_hold_released_by_dismiss_questions_over_wss() {
    let Some(script) = gate("WSS question-hold dismissQuestions E2E") else {
        return;
    };
    let send_code = format!(
        "const agents = await ws.agent.list(true); \
         const target = agents.find(a => a.name === 'AskerA'); \
         const first = await ws.agent.send(target.id, '{HELD_DISMISS}'); \
         await ws.note.create('hold-results', JSON.stringify({{ first }})); \
         return 'sent';"
    );
    let behavior = json!({
        "rules": [
            ask_rule(),
            {
                "ifPromptContains": SEND_MARKER,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": send_code, "summary": "held send e2e" }
                },
                "response": "send dispatched"
            }
        ],
        "response": "plain reply"
    })
    .to_string();
    let (_daemon, ws_id, port, cfg) = boot(&script, &behavior).await;

    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_resp["subscriptionId"].is_string(), "sub: {sub_resp}");

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let asker_id = create_agent(&mut rpc, &ws_id, "AskerA").await;
    let sender_id = create_agent(&mut rpc, &ws_id, "SenderB").await;

    // ---- (1) Question turn: hold active; capture the question message id ----
    let question_mid = drive_question_turn(&mut rpc, &mut sub, &ws_id, &asker_id).await;

    // ---- (2) One automatic send parks in the queue ----
    let sent = wss_rpc(
        &mut rpc,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": sender_id,
            "content": format!("message the asker {SEND_MARKER}"),
        }),
    )
    .await;
    assert_eq!(sent["success"], true, "sender kickoff ok: {sent}");
    await_stream_end(&mut sub, &sender_id).await;

    let capture = read_capture_note(&mut rpc, &ws_id, "hold-results").await;
    assert_eq!(capture["first"]["queued"], true, "parked: {capture}");
    assert_eq!(
        capture["first"]["heldForQuestions"], true,
        "held by the question hold: {capture}"
    );
    let q = wss_rpc(&mut rpc, "agent.getQueue", json!({ "agentId": asker_id })).await;
    assert_eq!(
        q["queue"].as_array().expect("queue array").len(),
        1,
        "held entry parked: {q}"
    );

    // ---- (3) Dismissal persists the marker ----
    let dismissed = wss_rpc(
        &mut rpc,
        "agent.dismissQuestions",
        json!({ "workspaceId": ws_id, "agentId": asker_id, "messageId": question_mid }),
    )
    .await;
    assert_eq!(dismissed["success"], true, "dismiss ok: {dismissed}");
    assert_eq!(
        dismissed["dismissedQuestionsMessageId"], question_mid,
        "result echoes the marker: {dismissed}"
    );

    // `agent:updated` carries the marker on the wire.
    let mut saw_updated = false;
    for _ in 0..120 {
        let frame = wss_event(&mut sub, 30).await;
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:updated"
            && ev["data"]["agentId"].as_str() == Some(asker_id.as_str())
            && ev["data"]["dismissedQuestionsMessageId"] == json!(question_mid)
        {
            saw_updated = true;
            break;
        }
    }
    assert!(saw_updated, "agent:updated carried the dismissal marker");

    // The `agent.list` metadata projection surfaces the persisted marker.
    let list = wss_rpc(&mut rpc, "agent.list", json!({ "workspaceId": ws_id })).await;
    let listed = list["agents"]
        .as_array()
        .expect("agents array")
        .iter()
        .find(|a| a["id"] == json!(asker_id))
        .expect("asker listed")
        .clone();
    assert_eq!(
        listed["metadata"]["dismissedQuestionsMessageId"], question_mid,
        "dismissal marker persisted on the metadata projection: {listed}"
    );

    // ---- (4) The dismissal notified the agent, then drained the queue ----
    let conv = await_conversation(&mut rpc, &ws_id, &asker_id, "held message drained", |m| {
        user_row_index(m, HELD_DISMISS).is_some()
    })
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    // The dismissal system notice reaches the model BEFORE the held message:
    // user rows are exactly kickoff → notice → drained held message.
    let user_rows: Vec<&Value> = messages.iter().filter(|m| m["role"] == "user").collect();
    assert_eq!(
        user_rows.len(),
        3,
        "exactly kickoff + dismissal notice + drained held message: {user_rows:?}"
    );
    assert!(
        user_rows[0]["contentBlocks"][0]["text"]
            .as_str()
            .unwrap_or_default()
            .contains(SEND_MARKER)
            || user_rows[0]["contentBlocks"][0]["text"]
                .as_str()
                .unwrap_or_default()
                .contains(ASK_MARKER),
        "first user row is the kickoff: {user_rows:?}"
    );
    let notice_text = user_rows[1]["contentBlocks"][0]["text"]
        .as_str()
        .unwrap_or_default();
    assert!(
        notice_text.starts_with("User dismissed your 1 question without answering."),
        "second user row is the dismissal notice with the question count: {user_rows:?}"
    );
    assert_eq!(
        user_rows[1]["contentBlocks"][0]["messageMetadata"],
        json!({
            "type": "questions_dismissed",
            "source": "system",
            "dismissedQuestionsMessageId": question_mid,
        }),
        "notice block carries the questions_dismissed metadata: {user_rows:?}"
    );
    assert!(
        user_rows[2]["contentBlocks"][0]["text"]
            .as_str()
            .is_some_and(|t| t.starts_with(HELD_DISMISS)),
        "third user row is the drained held message: {user_rows:?}"
    );

    let q = wss_rpc(&mut rpc, "agent.getQueue", json!({ "agentId": asker_id })).await;
    assert!(
        q["queue"].as_array().expect("queue array").is_empty(),
        "queue drained after dismissal: {q}"
    );
}

/// Dismissal notice with an IDLE agent and an EMPTY queue (PROTOCOL §5.5):
///
/// 1. The asker's kickoff turn emits the question — hold active, queue empty.
/// 2. `agent.dismissQuestions` starts the notice turn IMMEDIATELY (never
///    queues): the mock's ack rule matches on the notice WORDING, so the
///    [`DISMISS_ACK`] assistant reply proves the turn prompt carried the
///    dismissal text.
/// 3. The persisted user row starts with the count-carrying notice wording
///    and its block carries the `questions_dismissed` metadata; the queue
///    stays empty throughout.
#[tokio::test]
async fn dismiss_questions_idle_empty_queue_starts_notice_turn_over_wss() {
    let Some(script) = gate("WSS dismissQuestions idle-agent E2E") else {
        return;
    };
    let behavior = json!({
        "rules": [ask_rule(), dismiss_ack_rule()],
        "response": "plain reply"
    })
    .to_string();
    let (_daemon, ws_id, port, cfg) = boot(&script, &behavior).await;

    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_resp["subscriptionId"].is_string(), "sub: {sub_resp}");

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let asker_id = create_agent(&mut rpc, &ws_id, "AskerA").await;

    // ---- (1) Question turn: hold active, queue EMPTY ----
    let question_mid = drive_question_turn(&mut rpc, &mut sub, &ws_id, &asker_id).await;
    // The immediate-delivery path requires a truly IDLE agent: stream:end
    // precedes the status rewrite, and a dismissal racing into that window
    // would take the queue path instead (flake observed at ~1 in 6 runs).
    await_agent_idle(&mut rpc, &asker_id).await;
    let q = wss_rpc(&mut rpc, "agent.getQueue", json!({ "agentId": asker_id })).await;
    assert!(
        q["queue"].as_array().expect("queue array").is_empty(),
        "precondition: queue empty before the dismissal: {q}"
    );

    // ---- (2) Dismissal: the notice turn starts immediately ----
    let dismissed = wss_rpc(
        &mut rpc,
        "agent.dismissQuestions",
        json!({ "workspaceId": ws_id, "agentId": asker_id, "messageId": question_mid }),
    )
    .await;
    assert_eq!(dismissed["success"], true, "dismiss ok: {dismissed}");
    await_stream_end(&mut sub, &asker_id).await;

    // ---- (3) Notice prompt + metadata persisted; mock acked the wording ----
    let conv = await_conversation(&mut rpc, &ws_id, &asker_id, "dismissal ack", |m| {
        assistant_row_index(m, DISMISS_ACK).is_some()
    })
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let notice_idx =
        notice_row_index(messages, &question_mid).expect("dismissal notice user row persisted");
    let notice_block = &messages[notice_idx]["contentBlocks"][0];
    assert!(
        notice_block["text"]
            .as_str()
            .is_some_and(|t| t.starts_with(NOTICE_PREFIX)),
        "notice row carries the dismissal text with the question count: {notice_block}"
    );
    assert_eq!(
        notice_block["messageMetadata"],
        dismissal_metadata(&question_mid),
        "notice block carries the questions_dismissed metadata: {notice_block}"
    );
    let ack_idx = assistant_row_index(messages, DISMISS_ACK).expect("ack row");
    assert!(
        notice_idx < ack_idx,
        "the ack turn FOLLOWS the notice row (prompt carried the dismissal \
         text): notice={notice_idx} ack={ack_idx}"
    );

    // Never queued: idle + empty queue delivers as an immediate turn.
    let q = wss_rpc(&mut rpc, "agent.getQueue", json!({ "agentId": asker_id })).await;
    assert!(
        q["queue"].as_array().expect("queue array").is_empty(),
        "notice never parked in the queue: {q}"
    );
}

/// Dismissal notice as an UNDELIVERED queue entry (PROTOCOL §5.5):
///
/// 1. The asker emits question A (kickoff turn), then question B (second
///    turn — a user-origin message is never held, so the turn runs while
///    A's hold is active).
/// 2. Dismissing A while B still holds automatic deliveries PARKS A's
///    notice: `agent.getQueue` surfaces the entry at the queue HEAD with its
///    `questions_dismissed` metadata — the DoD's undelivered-entry shape.
/// 3. Dismissing B releases the hold: B's notice delivers first (immediate),
///    A's parked notice drains behind it — transcript order B-notice →
///    A-notice, queue empty.
#[tokio::test]
async fn dismiss_questions_queued_notice_surfaces_metadata_over_wss() {
    let Some(script) = gate("WSS dismissQuestions queued-notice E2E") else {
        return;
    };
    let behavior = json!({
        "rules": [
            ask_rule(),
            ask_rule_with(ASK_AGAIN_MARKER),
            dismiss_ack_rule()
        ],
        "response": "plain reply"
    })
    .to_string();
    let (_daemon, ws_id, port, cfg) = boot(&script, &behavior).await;

    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_resp["subscriptionId"].is_string(), "sub: {sub_resp}");

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let asker_id = create_agent(&mut rpc, &ws_id, "AskerA").await;

    // ---- (1) Two question turns: A then B (user origin is never held) ----
    let mid_a = drive_question_turn(&mut rpc, &mut sub, &ws_id, &asker_id).await;
    let sent = wss_rpc(
        &mut rpc,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": asker_id,
            "content": format!("one more thing {ASK_AGAIN_MARKER}"),
        }),
    )
    .await;
    assert_eq!(sent["success"], true, "second ask kickoff ok: {sent}");
    // User origin is never held by A's question hold, but a brief post-turn
    // busy race can still park the send (`queued: true` WITHOUT
    // `heldForQuestions`) — the drain delivers it either way, so it must
    // never look hold-parked.
    assert!(
        sent["heldForQuestions"].as_bool() != Some(true),
        "user origin is never held by A's question hold: {sent}"
    );
    await_stream_end(&mut sub, &asker_id).await;
    // Question B is the LAST transcript row once its turn lands (polled: the
    // busy-race park above delivers via the drain a beat after stream end).
    let is_question_b = |messages: &[Value]| {
        messages.last().is_some_and(|last| {
            last["role"] == "assistant"
                && last["contentBlocks"]
                    .as_array()
                    .and_then(|blocks| blocks.last())
                    .is_some_and(|b| {
                        b["type"] == "resource" && b["resource"]["mimeType"] == QUESTION_MIME
                    })
                && last["id"].as_str() != Some(mid_a.as_str())
        })
    };
    let conv = await_conversation(&mut rpc, &ws_id, &asker_id, "question B row", |m| {
        is_question_b(m)
    })
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let last = messages.last().expect("non-empty transcript");
    let mid_b = last["id"].as_str().expect("question B id").to_string();

    // ---- (2) Dismiss A: B's hold parks the notice; getQueue surfaces it ----
    let dismissed = wss_rpc(
        &mut rpc,
        "agent.dismissQuestions",
        json!({ "workspaceId": ws_id, "agentId": asker_id, "messageId": mid_a }),
    )
    .await;
    assert_eq!(dismissed["success"], true, "dismiss A ok: {dismissed}");

    let mut queued_entry: Option<Value> = None;
    for _ in 0..120 {
        let q = wss_rpc(&mut rpc, "agent.getQueue", json!({ "agentId": asker_id })).await;
        let queue = q["queue"].as_array().expect("queue array");
        if !queue.is_empty() {
            queued_entry = Some(queue[0].clone());
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let entry = queued_entry.expect("A's notice parked in the queue by B's hold");
    assert!(
        entry["content"]
            .as_str()
            .is_some_and(|t| t.starts_with(NOTICE_PREFIX)),
        "queued entry is the dismissal notice: {entry}"
    );
    assert_eq!(
        entry["messageMetadata"],
        dismissal_metadata(&mid_a),
        "agent.getQueue surfaces the questions_dismissed metadata on the \
         undelivered entry: {entry}"
    );
    assert_eq!(
        entry["position"], 0,
        "the notice is promoted to the queue HEAD: {entry}"
    );

    // A's notice must NOT have reached the transcript while parked.
    let conv = wss_rpc(
        &mut rpc,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": asker_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    assert!(
        notice_row_index(messages, &mid_a).is_none(),
        "parked notice not yet delivered: {messages:?}"
    );

    // ---- (3) Dismiss B: its notice delivers first, A's drains behind ----
    let dismissed = wss_rpc(
        &mut rpc,
        "agent.dismissQuestions",
        json!({ "workspaceId": ws_id, "agentId": asker_id, "messageId": mid_b }),
    )
    .await;
    assert_eq!(dismissed["success"], true, "dismiss B ok: {dismissed}");

    let conv = await_conversation(&mut rpc, &ws_id, &asker_id, "both notices landed", |m| {
        notice_row_index(m, &mid_a).is_some() && notice_row_index(m, &mid_b).is_some()
    })
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let idx_b = notice_row_index(messages, &mid_b).expect("B notice row");
    let idx_a = notice_row_index(messages, &mid_a).expect("A notice row");
    assert!(
        idx_b < idx_a,
        "B's notice delivers FIRST, A's parked notice drains behind it: \
         b={idx_b} a={idx_a}"
    );

    let q = wss_rpc(&mut rpc, "agent.getQueue", json!({ "agentId": asker_id })).await;
    assert!(
        q["queue"].as_array().expect("queue array").is_empty(),
        "queue drained after both dismissals: {q}"
    );
}
