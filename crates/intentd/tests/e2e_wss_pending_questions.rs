//! WSS e2e regression for the Q&A **pending questions** lifecycle (PROTOCOL
//! §5.5) — the pending-question marker, its resolution paths, and the
//! contract that pending questions NEVER park automatic deliveries.
//!
//! Drives the lifecycle over the real WSS transport with mock ACP agents:
//!
//! * An asker agent emits a `ws.app.question.ask` question (trailing
//!   `application/vnd.intent.question+json` resource block) — the
//!   pending-questions marker is set.
//! * Sibling agents fire AUTOMATIC deliveries at the asker through the
//!   `ws.agent.send` host binding (one `priority: "queue"` opt-out, one
//!   default-interrupt — the binding delivers with interrupt priority when
//!   `priority` is omitted): both DELIVER immediately (`queued: false` +
//!   `turnId` + `delivery: "delivered"` on the send results, captured via a
//!   note; no `heldForQuestions` flag exists any more), the user rows reach
//!   the asker's transcript, and the queue stays empty. The persisted marker
//!   survives those turns.
//! * Resolution path 1 (user answer): `agent.sendMessage` tagged
//!   `messageMetadata.type: "question_answers"` clears the marker
//!   (`agent:updated` carries the empty clear marker) — delivered directly
//!   to an idle asker, or parked behind a BUSY automatic turn and drained
//!   at its end (no deadlock).
//! * Resolution path 2 (`agent.dismissQuestions`): persists the dismissal
//!   marker (`agent:updated` with `dismissedQuestionsMessageId`, surfaced on
//!   the `agent.list` metadata projection) and delivers the
//!   questions-dismissed system notice to the agent (tagged
//!   `messageMetadata.type: "questions_dismissed"` so the FE can render it
//!   as a system chip) — behind any automatic message that already reached
//!   the transcript.
//! * Dismissal notice, idle agent + empty queue: `agent.dismissQuestions`
//!   starts the notice turn IMMEDIATELY (never queues) — the mock's ack rule
//!   matches on the notice wording, proving the turn prompt carried the
//!   dismissal text, and the persisted user row carries the
//!   `questions_dismissed` metadata.
//! * Dismissal notice with a NEWER question still pending: dismissing an
//!   OLDER question delivers its notice immediately (pending questions park
//!   nothing — not even the notice) while the newer marker stays pending;
//!   dismissing the newer one delivers its notice behind it.
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

/// MIME type the FE renders as `QuestionCards` (PROTOCOL §7.x question resource).
const QUESTION_MIME: &str = "application/vnd.intent.question+json";

/// Turn-1 trigger for the asker: the mock's rule matches on this so the
/// question fires ONLY on the kickoff turn.
const ASK_MARKER: &str = "ASK_PENDING_QUESTION_NOW_E2E";

/// Trigger for the sibling sender's automatic `ws.agent.send` calls.
const SEND_MARKER: &str = "SEND_AUTO_MESSAGE_NOW_E2E";

/// Trigger for the second sibling's automatic interrupt `ws.agent.send`
/// (scenario 1 uses one sender per message so each capture note is
/// attributable to exactly one send).
const SEND_URGENT_MARKER: &str = "SEND_AUTO_URGENT_NOW_E2E";

/// The two automatic messages delivered under pending questions in scenario 1.
const AUTO_NORMAL: &str = "automatic normal message";
const AUTO_URGENT: &str = "automatic urgent message";

/// The single automatic message delivered before the dismissal in scenario 2.
const AUTO_BEFORE_DISMISS: &str = "delivered before dismissal";

/// The automatic message whose turn the mock deliberately drags out
/// (`delayMs`) so a user answer can be parked behind it.
const AUTO_SLOW: &str = "automatic slow message";
/// How long the mock sleeps on the [`AUTO_SLOW`] turn — long enough to
/// observe the asker `active` and park the answer, short enough for CI.
const SLOW_TURN_MS: u64 = 4_000;

/// Trigger for the asker's SECOND question turn (queued-notice scenario:
/// two distinct pending question messages).
const ASK_AGAIN_MARKER: &str = "ASK_SECOND_QUESTION_NOW_E2E";

/// The flattened `Q:`/`A:` answer a user sends after filling the `QuestionCard`.
const ANSWER_TEXT: &str = "Q: Which environment should I deploy to?\nA: Staging";
/// An UNTAGGED user message sent while questions are pending: it carries no
/// `question_answers` tag, so it must NOT clear the pending marker.
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
                eprintln!("=== DAEMON LOG ===\n{log}\n=== END LOG ===");
            }
        }
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-pendq-{}", &id[..8]));
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

/// Drain subscriber events until an `agent:updated` for `agent_id` carries
/// `pendingQuestionsMessageId == expected` (bounded generously: several
/// turns' worth of events may sit in the subscriber buffer by the time a
/// scenario reaches its resolution step).
async fn await_pending_marker_event<S>(sub: &mut WebSocketStream<S>, agent_id: &str, expected: &str)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    for _ in 0..600 {
        let frame = wss_event(sub, 30).await;
        let event = &frame["params"]["event"];
        if event["type"] == "agent:updated"
            && event["data"]["agentId"].as_str() == Some(agent_id)
            && event["data"]["pendingQuestionsMessageId"].as_str() == Some(expected)
        {
            return;
        }
    }
    panic!("no pendingQuestionsMessageId={expected:?} event for {agent_id}");
}

/// Pre-seed the daemon's `SQLite` store with a regular (NON-chief) workspace.
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
            title: "PENDQ-E2E".to_string(),
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
        })
        .await
        .expect("insert ws");
    ws.0
}

/// The single question the asker emits — one `ws.app.question.ask` call.
fn pending_question() -> Value {
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
        json!(pending_question())
    );
    json!({
        "ifPromptContains": marker,
        "toolCall": {
            "name": "workspace_api",
            "arguments": { "code": ask_code, "summary": "ask pending question" }
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
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
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
        json!({ "workspaceId": ws_id, "name": name, "model": "default", "provider": "mock" }),
    )
    .await;
    created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string()
}

/// Drive the asker's question turn and return the persisted question
/// message's id, asserting the pending prerequisite: the transcript's LAST
/// message is the assistant row whose trailing block is the question
/// resource, and the set marker is emitted + projected.
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
    let mut marker_event = None;
    let mut saw_stream_end = false;
    for _ in 0..120 {
        let frame = wss_event(sub, 30).await;
        let event = &frame["params"]["event"];
        if event["type"] == "agent:updated" && event["data"]["agentId"].as_str() == Some(asker_id) {
            if let Some(marker) = event["data"]["pendingQuestionsMessageId"].as_str() {
                marker_event = Some(marker.to_string());
            }
        }
        if event["type"] == "agent:stream:end"
            && event["data"]["agentId"].as_str() == Some(asker_id)
        {
            saw_stream_end = true;
            break;
        }
    }
    assert!(saw_stream_end, "no agent:stream:end for {asker_id}");

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
        "pending prerequisite: last message is the assistant question: {last}"
    );
    let blocks = last["contentBlocks"].as_array().expect("contentBlocks");
    let trailing = blocks.last().expect("blocks non-empty");
    assert_eq!(trailing["type"], "resource", "trailing block: {trailing}");
    assert_eq!(
        trailing["resource"]["mimeType"], QUESTION_MIME,
        "trailing block is the question resource: {trailing}"
    );
    let message_id = last["id"]
        .as_str()
        .expect("question message id")
        .to_string();
    assert_eq!(
        marker_event.as_deref(),
        Some(message_id.as_str()),
        "question completion emits the set marker"
    );
    let got = wss_rpc(rpc, "agent.get", json!({ "agentId": asker_id })).await;
    assert_eq!(
        got["agent"]["metadata"]["pendingQuestionsMessageId"], message_id,
        "AgentLite projects the additive set marker: {got}"
    );
    message_id
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

/// Poll `agent.get` until the persisted status equals `status` (bounded).
async fn await_agent_status<S>(rpc: &mut WebSocketStream<S>, agent_id: &str, status: &str)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    for _ in 0..240 {
        let got = wss_rpc(rpc, "agent.get", json!({ "agentId": agent_id })).await;
        if got["agent"]["status"] == status {
            return;
        }
        tokio::time::sleep(Duration::from_millis(125)).await;
    }
    panic!("agent {agent_id} never reached {status} status");
}

/// Poll `agent.get` until the persisted status is `idle` (bounded).
/// `agent:stream:end` precedes the status rewrite to idle by a small window,
/// so tests that depend on the idle-agent immediate-delivery path must wait
/// out that window explicitly instead of racing the next send against it.
async fn await_agent_idle<S>(rpc: &mut WebSocketStream<S>, agent_id: &str)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    await_agent_status(rpc, agent_id, "idle").await;
}

/// 0-based index of the user row whose first text block contains `text`
/// (A2A rows carry the prepended sender header, and rows that drained from
/// the queue may carry the appended dequeue-wait system note).
fn user_row_index(messages: &[Value], text: &str) -> Option<usize> {
    messages.iter().position(|m| {
        m["role"] == "user"
            && m["contentBlocks"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|b| {
                    b["type"] == "text" && b["text"].as_str().is_some_and(|t| t.contains(text))
                })
    })
}

/// Assert an automatic `ws.agent.send` result (captured via a note) DELIVERED
/// immediately: not queued, a turn was driven, the binding classified it
/// `delivered`, and no hold flag rides the result (`heldForQuestions` is gone
/// from the wire — pending questions never park automatic deliveries).
fn assert_delivered_now(result: &Value, ctx: &str) {
    assert_eq!(result["success"], true, "{ctx}: send ok: {result}");
    assert_ne!(
        result["queued"],
        json!(true),
        "{ctx}: pending questions must not park the send: {result}"
    );
    assert!(
        result["turnId"].is_string(),
        "{ctx}: the send drove a turn: {result}"
    );
    assert_eq!(
        result["delivery"], "delivered",
        "{ctx}: self-describing delivery outcome: {result}"
    );
    assert!(
        result.get("heldForQuestions").is_none(),
        "{ctx}: no hold flag on the wire: {result}"
    );
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

/// Pending questions never park automatic deliveries; the marker is cleared
/// by a USER ANSWER (PROTOCOL §5.5):
///
/// 1. The asker's kickoff turn emits a `ws.app.question.ask` question — the
///    pending-questions marker is set (last transcript message = assistant
///    question row).
/// 2. Two siblings each fire one AUTOMATIC `ws.agent.send` at the asker
///    (one `priority: "queue"` opt-out, one default-interrupt — omitted
///    `priority` resolves to interrupt in the binding): both DELIVER
///    immediately (`queued: false`, `turnId`, `delivery: "delivered"`, no
///    `heldForQuestions` on the wire), each user row reaches the asker's
///    transcript, and the queue stays empty throughout.
/// 3. The persisted pending-questions marker SURVIVES both automatic turns
///    and an UNTAGGED user message (pendingness is not superseded by
///    unrelated deliveries or by the agent's own later turns): `agent.get`
///    still projects the question message id.
/// 4. The user answers via `agent.sendMessage`, tagged with
///    `messageMetadata { type: "question_answers", answeredQuestionsMessageId }`
///    — the tag clears the marker (`agent:updated` carries the empty clear
///    marker, `agent.get` projects it) and the answer row lands.
#[tokio::test]
async fn pending_questions_do_not_park_automatic_sends_over_wss() {
    let Some(script) = gate("WSS pending-questions automatic-send E2E") else {
        return;
    };
    // The normal send uses the explicit `priority: "queue"` opt-out (also
    // covering the opt-out path e2e); the urgent send relies on the
    // binding's interrupt-by-default resolution (omitted priority).
    let send_normal_code = format!(
        "const agents = await ws.agent.list(true); \
         const target = agents.find(a => a.name === 'AskerA'); \
         const first = await ws.agent.send(target.id, '{AUTO_NORMAL}', 'queue'); \
         await ws.note.create('send-results', JSON.stringify({{ first }})); \
         return 'sent';"
    );
    let send_urgent_code = format!(
        "const agents = await ws.agent.list(true); \
         const target = agents.find(a => a.name === 'AskerA'); \
         const second = await ws.agent.send(target.id, '{AUTO_URGENT}'); \
         await ws.note.create('send-results-urgent', JSON.stringify({{ second }})); \
         return 'sent';"
    );
    let behavior = json!({
        "rules": [
            ask_rule(),
            {
                "ifPromptContains": SEND_MARKER,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": send_normal_code, "summary": "automatic send e2e" }
                },
                "response": "send dispatched"
            },
            {
                "ifPromptContains": SEND_URGENT_MARKER,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": send_urgent_code, "summary": "automatic urgent send e2e" }
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

    // ---- (1) The asker's question turn sets the pending marker ----
    let asked_mid = drive_question_turn(&mut rpc, &mut sub, &ws_id, &asker_id).await;
    await_agent_idle(&mut rpc, &asker_id).await;

    // ---- (2a) A queue-priority automatic send delivers immediately ----
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
    let normal_capture = read_capture_note(&mut rpc, &ws_id, "send-results").await;
    assert_delivered_now(&normal_capture["first"], "queue-priority automatic send");
    // The delivered row reaches the transcript (never parked).
    await_conversation(
        &mut rpc,
        &ws_id,
        &asker_id,
        "normal automatic row landed",
        |m| user_row_index(m, AUTO_NORMAL).is_some(),
    )
    .await;
    await_agent_idle(&mut rpc, &asker_id).await;
    let q = wss_rpc(&mut rpc, "agent.getQueue", json!({ "agentId": asker_id })).await;
    assert!(
        q["queue"].as_array().expect("queue array").is_empty(),
        "nothing parked by pending questions: {q}"
    );

    // ---- (2b) A default-interrupt automatic send delivers the same way ----
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
    await_stream_end(&mut sub, &urgent_sender_id).await;
    let urgent_capture = read_capture_note(&mut rpc, &ws_id, "send-results-urgent").await;
    assert_delivered_now(
        &urgent_capture["second"],
        "default-interrupt automatic send",
    );
    let conv = await_conversation(
        &mut rpc,
        &ws_id,
        &asker_id,
        "urgent automatic row landed",
        |m| user_row_index(m, AUTO_URGENT).is_some(),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let normal_idx = user_row_index(messages, AUTO_NORMAL).expect("normal row");
    let urgent_idx = user_row_index(messages, AUTO_URGENT).expect("urgent row");
    assert!(
        normal_idx < urgent_idx,
        "both automatic rows landed in send order: normal={normal_idx} urgent={urgent_idx}"
    );
    await_agent_idle(&mut rpc, &asker_id).await;
    let q = wss_rpc(&mut rpc, "agent.getQueue", json!({ "agentId": asker_id })).await;
    assert!(
        q["queue"].as_array().expect("queue array").is_empty(),
        "interrupt send was not parked either: {q}"
    );

    // ---- (3) The marker survives the automatic turns + a plain user message ----
    let got = wss_rpc(&mut rpc, "agent.get", json!({ "agentId": asker_id })).await;
    assert_eq!(
        got["agent"]["metadata"]["pendingQuestionsMessageId"], asked_mid,
        "pending marker survives automatic deliveries: {got}"
    );
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
    assert!(
        plain.get("heldForQuestions").is_none(),
        "no hold flag on the wire: {plain}"
    );
    await_conversation(&mut rpc, &ws_id, &asker_id, "plain row landed", |m| {
        user_row_index(m, PLAIN_USER_TEXT).is_some()
    })
    .await;
    await_agent_idle(&mut rpc, &asker_id).await;
    let got = wss_rpc(&mut rpc, "agent.get", json!({ "agentId": asker_id })).await;
    assert_eq!(
        got["agent"]["metadata"]["pendingQuestionsMessageId"], asked_mid,
        "an untagged user message does not clear the marker: {got}"
    );

    // ---- (4) The tagged user answer clears the marker ----
    // The wizard tags the answer with `messageMetadata { type:
    // "question_answers", answeredQuestionsMessageId }` — the structured tag
    // (never the text) is what resolves the persisted pending marker.
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
        "no hold flag on the wire: {answered}"
    );
    await_pending_marker_event(&mut sub, &asker_id, "").await;
    let got = wss_rpc(&mut rpc, "agent.get", json!({ "agentId": asker_id })).await;
    assert_eq!(
        got["agent"]["metadata"]["pendingQuestionsMessageId"], "",
        "AgentLite preserves the written-empty clear marker: {got}"
    );
    await_conversation(&mut rpc, &ws_id, &asker_id, "answer landed", |m| {
        user_row_index(m, ANSWER_TEXT).is_some()
    })
    .await;
    let q = wss_rpc(&mut rpc, "agent.getQueue", json!({ "agentId": asker_id })).await;
    assert!(
        q["queue"].as_array().expect("queue array").is_empty(),
        "queue empty after the answer: {q}"
    );
}

/// A user answer parked behind a BUSY automatic turn still drains and clears
/// the marker — no deadlock (PROTOCOL §5.5):
///
/// 1. The asker's kickoff turn emits the question — marker set.
/// 2. A sibling's automatic `ws.agent.send` delivers immediately and drives a
///    SLOW turn on the asker (the mock's rule sleeps before replying).
/// 3. While that automatic turn is in flight, the user answers via
///    `agent.sendMessage` tagged `question_answers`: the busy asker parks it
///    in the queue (`queued: true` — an ordinary busy park, not a hold).
/// 4. The end-of-turn drain delivers the parked answer: the marker clears
///    (`agent:updated` carries the empty clear marker), the answer row lands
///    behind the automatic row, and the queue is empty.
#[tokio::test]
async fn user_answer_parked_behind_busy_automatic_turn_clears_marker_over_wss() {
    let Some(script) = gate("WSS pending-questions busy-park answer E2E") else {
        return;
    };
    let send_code = format!(
        "const agents = await ws.agent.list(true); \
         const target = agents.find(a => a.name === 'AskerA'); \
         const first = await ws.agent.send(target.id, '{AUTO_SLOW}', 'queue'); \
         await ws.note.create('send-results', JSON.stringify({{ first }})); \
         return 'sent';"
    );
    let behavior = json!({
        "rules": [
            ask_rule(),
            {
                "ifPromptContains": SEND_MARKER,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": send_code, "summary": "automatic slow send e2e" }
                },
                "response": "send dispatched"
            },
            {
                "ifPromptContains": AUTO_SLOW,
                "delayMs": SLOW_TURN_MS,
                "response": "slow reply"
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

    // ---- (1) Question turn: marker set ----
    let asked_mid = drive_question_turn(&mut rpc, &mut sub, &ws_id, &asker_id).await;
    await_agent_idle(&mut rpc, &asker_id).await;

    // ---- (2) The automatic send drives a slow turn on the asker ----
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
    await_agent_status(&mut rpc, &asker_id, "active").await;

    // ---- (3) The tagged answer parks behind the busy turn ----
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
    assert_eq!(
        answered["queued"], true,
        "busy asker parks the answer (ordinary busy park): {answered}"
    );
    assert!(
        answered.get("heldForQuestions").is_none(),
        "no hold flag on the wire: {answered}"
    );
    // The marker is still set while the answer waits in the queue.
    let got = wss_rpc(&mut rpc, "agent.get", json!({ "agentId": asker_id })).await;
    assert_eq!(
        got["agent"]["metadata"]["pendingQuestionsMessageId"], asked_mid,
        "marker still set while the answer is parked: {got}"
    );

    // ---- (4) The drain delivers the answer and clears the marker ----
    await_pending_marker_event(&mut sub, &asker_id, "").await;
    let conv = await_conversation(&mut rpc, &ws_id, &asker_id, "answer drained", |m| {
        user_row_index(m, ANSWER_TEXT).is_some()
    })
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let auto_idx = user_row_index(messages, AUTO_SLOW).expect("automatic row");
    let answer_idx = user_row_index(messages, ANSWER_TEXT).expect("answer row");
    assert!(
        auto_idx < answer_idx,
        "the answer drained behind the automatic turn: auto={auto_idx} answer={answer_idx}"
    );
    let got = wss_rpc(&mut rpc, "agent.get", json!({ "agentId": asker_id })).await;
    assert_eq!(
        got["agent"]["metadata"]["pendingQuestionsMessageId"], "",
        "drained answer cleared the marker: {got}"
    );
    let capture = read_capture_note(&mut rpc, &ws_id, "send-results").await;
    assert_delivered_now(&capture["first"], "automatic send that drove the slow turn");
    await_agent_idle(&mut rpc, &asker_id).await;
    let q = wss_rpc(&mut rpc, "agent.getQueue", json!({ "agentId": asker_id })).await;
    assert!(
        q["queue"].as_array().expect("queue array").is_empty(),
        "queue empty after the drained answer: {q}"
    );
}

/// Marker resolved by `agent.dismissQuestions` after an automatic delivery
/// (PROTOCOL §5.5):
///
/// 1. The asker's kickoff turn emits the question — marker set.
/// 2. A sibling's automatic `ws.agent.send` DELIVERS immediately (pending
///    questions park nothing): the row lands and the queue stays empty.
/// 3. `agent.dismissQuestions` persists the dismissal marker: the RPC result
///    echoes `dismissedQuestionsMessageId`, `agent:updated` carries it on the
///    wire (alongside the pending marker), and the `agent.list` metadata
///    projection surfaces it (survives as session metadata).
/// 4. The dismissal delivers the questions-dismissed system notice (user row
///    tagged `messageMetadata.type: "questions_dismissed"`, wording carries
///    the question count): the asker's user rows are exactly kickoff →
///    delivered automatic message → notice, and the queue is empty.
#[tokio::test]
async fn dismiss_questions_after_delivered_automatic_send_over_wss() {
    let Some(script) = gate("WSS pending-questions dismissQuestions E2E") else {
        return;
    };
    let send_code = format!(
        "const agents = await ws.agent.list(true); \
         const target = agents.find(a => a.name === 'AskerA'); \
         const first = await ws.agent.send(target.id, '{AUTO_BEFORE_DISMISS}'); \
         await ws.note.create('send-results', JSON.stringify({{ first }})); \
         return 'sent';"
    );
    let behavior = json!({
        "rules": [
            ask_rule(),
            {
                "ifPromptContains": SEND_MARKER,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": send_code, "summary": "automatic send e2e" }
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

    // ---- (1) Question turn: marker set; capture the question message id ----
    let question_mid = drive_question_turn(&mut rpc, &mut sub, &ws_id, &asker_id).await;
    await_agent_idle(&mut rpc, &asker_id).await;

    // ---- (2) One automatic send delivers immediately ----
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

    let capture = read_capture_note(&mut rpc, &ws_id, "send-results").await;
    assert_delivered_now(&capture["first"], "automatic send under pending questions");
    await_conversation(&mut rpc, &ws_id, &asker_id, "automatic row landed", |m| {
        user_row_index(m, AUTO_BEFORE_DISMISS).is_some()
    })
    .await;
    await_agent_idle(&mut rpc, &asker_id).await;
    let q = wss_rpc(&mut rpc, "agent.getQueue", json!({ "agentId": asker_id })).await;
    assert!(
        q["queue"].as_array().expect("queue array").is_empty(),
        "nothing parked by pending questions: {q}"
    );
    let got = wss_rpc(&mut rpc, "agent.get", json!({ "agentId": asker_id })).await;
    assert_eq!(
        got["agent"]["metadata"]["pendingQuestionsMessageId"], question_mid,
        "pending marker survives the automatic delivery: {got}"
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

    // `agent:updated` carries the marker on the wire — and the session's
    // pending-questions marker alongside it, so the event is self-contained
    // (monorepo#3180): clients re-derive pendingness without an `agent.get`.
    let mut saw_updated = false;
    for _ in 0..600 {
        let frame = wss_event(&mut sub, 30).await;
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:updated"
            && ev["data"]["agentId"].as_str() == Some(asker_id.as_str())
            && ev["data"]["dismissedQuestionsMessageId"] == json!(question_mid)
        {
            assert_eq!(
                ev["data"]["pendingQuestionsMessageId"],
                json!(question_mid),
                "dismiss event carries the pending marker: {ev}"
            );
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

    // ---- (4) The dismissal notified the agent behind the delivered row ----
    let conv = await_conversation(
        &mut rpc,
        &ws_id,
        &asker_id,
        "dismissal notice landed",
        |m| notice_row_index(m, &question_mid).is_some(),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    // User rows are exactly kickoff → delivered automatic message → notice.
    let user_rows: Vec<&Value> = messages.iter().filter(|m| m["role"] == "user").collect();
    assert_eq!(
        user_rows.len(),
        3,
        "exactly kickoff + delivered automatic message + dismissal notice: {user_rows:?}"
    );
    assert!(
        user_rows[0]["contentBlocks"][0]["text"]
            .as_str()
            .unwrap_or_default()
            .contains(ASK_MARKER),
        "first user row is the kickoff: {user_rows:?}"
    );
    assert!(
        user_rows[1]["contentBlocks"][0]["text"]
            .as_str()
            .is_some_and(|t| t.contains(AUTO_BEFORE_DISMISS)),
        "second user row is the delivered automatic message: {user_rows:?}"
    );
    let notice_text = user_rows[2]["contentBlocks"][0]["text"]
        .as_str()
        .unwrap_or_default();
    assert!(
        notice_text.starts_with(NOTICE_PREFIX),
        "third user row is the dismissal notice with the question count: {user_rows:?}"
    );
    assert_eq!(
        user_rows[2]["contentBlocks"][0]["messageMetadata"],
        dismissal_metadata(&question_mid),
        "notice block carries the questions_dismissed metadata: {user_rows:?}"
    );

    let q = wss_rpc(&mut rpc, "agent.getQueue", json!({ "agentId": asker_id })).await;
    assert!(
        q["queue"].as_array().expect("queue array").is_empty(),
        "queue empty after dismissal: {q}"
    );
}

/// Dismissal notice with an IDLE agent and an EMPTY queue (PROTOCOL §5.5):
///
/// 1. The asker's kickoff turn emits the question — marker set, queue empty.
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

    // ---- (1) Question turn: marker set, queue EMPTY ----
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

/// Dismissal notice with a NEWER question still pending (PROTOCOL §5.5):
///
/// 1. The asker emits question A (kickoff turn), then question B (second
///    turn) — the single-slot pending marker advances to B.
/// 2. Dismissing A while B is still pending delivers A's notice IMMEDIATELY
///    (pending questions park nothing — not even the notice): the persisted
///    user row carries the `questions_dismissed` metadata naming A, the
///    queue stays empty, and B's marker stays pending (`agent.get` still
///    projects B; the dismiss event carries B as the pending marker).
/// 3. Dismissing B delivers its notice behind A's — transcript order
///    A-notice → B-notice, queue empty.
#[tokio::test]
async fn dismiss_older_question_delivers_notice_while_newer_pending_over_wss() {
    let Some(script) = gate("WSS dismissQuestions newer-pending E2E") else {
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

    // ---- (1) Two question turns: A then B ----
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
    assert!(
        sent.get("heldForQuestions").is_none(),
        "no hold flag on the wire: {sent}"
    );
    await_stream_end(&mut sub, &asker_id).await;
    // Question B is the LAST transcript row once its turn lands (polled: a
    // brief post-turn busy race can park the send, which the drain then
    // delivers a beat after stream end).
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
    await_agent_idle(&mut rpc, &asker_id).await;
    // The single-slot marker advanced to B (the set event itself was already
    // drained by `await_stream_end` above — it precedes stream end).
    let got = wss_rpc(&mut rpc, "agent.get", json!({ "agentId": asker_id })).await;
    assert_eq!(
        got["agent"]["metadata"]["pendingQuestionsMessageId"], mid_b,
        "marker advanced to question B: {got}"
    );

    // ---- (2) Dismiss A: its notice delivers immediately; B stays pending ----
    let dismissed = wss_rpc(
        &mut rpc,
        "agent.dismissQuestions",
        json!({ "workspaceId": ws_id, "agentId": asker_id, "messageId": mid_a }),
    )
    .await;
    assert_eq!(dismissed["success"], true, "dismiss A ok: {dismissed}");

    let mut saw_updated = false;
    for _ in 0..600 {
        let frame = wss_event(&mut sub, 30).await;
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:updated"
            && ev["data"]["agentId"].as_str() == Some(asker_id.as_str())
            && ev["data"]["dismissedQuestionsMessageId"] == json!(mid_a)
        {
            assert_eq!(
                ev["data"]["pendingQuestionsMessageId"],
                json!(mid_b),
                "dismissing A leaves B pending on the wire: {ev}"
            );
            saw_updated = true;
            break;
        }
    }
    assert!(saw_updated, "agent:updated carried A's dismissal marker");

    let conv = await_conversation(&mut rpc, &ws_id, &asker_id, "A notice landed", |m| {
        notice_row_index(m, &mid_a).is_some()
    })
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let notice_a = &messages[notice_row_index(messages, &mid_a).expect("A notice row")];
    let block = &notice_a["contentBlocks"][0];
    assert!(
        block["text"]
            .as_str()
            .is_some_and(|t| t.starts_with(NOTICE_PREFIX)),
        "A's notice row carries the dismissal text: {block}"
    );
    assert_eq!(
        block["messageMetadata"],
        dismissal_metadata(&mid_a),
        "A's notice block carries the questions_dismissed metadata: {block}"
    );
    let q = wss_rpc(&mut rpc, "agent.getQueue", json!({ "agentId": asker_id })).await;
    assert!(
        q["queue"].as_array().expect("queue array").is_empty(),
        "B's pending question parked nothing: {q}"
    );
    let got = wss_rpc(&mut rpc, "agent.get", json!({ "agentId": asker_id })).await;
    assert_eq!(
        got["agent"]["metadata"]["pendingQuestionsMessageId"], mid_b,
        "B stays pending after A's dismissal: {got}"
    );
    await_agent_idle(&mut rpc, &asker_id).await;

    // ---- (3) Dismiss B: its notice delivers behind A's ----
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
    let idx_a = notice_row_index(messages, &mid_a).expect("A notice row");
    let idx_b = notice_row_index(messages, &mid_b).expect("B notice row");
    assert!(
        idx_a < idx_b,
        "A's notice delivered first, B's behind it: a={idx_a} b={idx_b}"
    );

    let q = wss_rpc(&mut rpc, "agent.getQueue", json!({ "agentId": asker_id })).await;
    assert!(
        q["queue"].as_array().expect("queue array").is_empty(),
        "queue empty after both dismissals: {q}"
    );
}
