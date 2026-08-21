//! WSS e2e for the `ws.app.question.ask` Q&A round trip.
//!
//! Drives a NON-chief agent (regular seeded workspace — proving the binding
//! is chief-un-gated, unlike the rest of `ws.app.*`) on the mock ACP provider:
//!
//! * Turn 1: the mock calls `workspace_api` TWICE, each invoking
//!   `ws.app.question.ask` with ONE question. Both must land as trailing
//!   `application/vnd.intent.question+json` resource blocks on the persisted
//!   final assistant message (§7.1 `AtTurnEnd` drain), in call order, with
//!   the canonical payload (header/question/options/multiSelect) and the
//!   minted `attachmentId` echoed in the `intent-question://` URI.
//! * Turn 2: the client sends the flattened plain-text `Q:`/`A:` answers via
//!   `agent.sendMessage`; the daemon must deliver that text to the provider
//!   VERBATIM (asserted via the mock fixture's `MOCK_AGENT_PROMPT_LOG` seam)
//!   — there is no daemon-side answer intake or transformation.
//!
//! A second test proves the SUB-AGENT gate over the same real path: a
//! background agent's per-agent MCP bridge prunes `ws.app.question.*` from
//! its tool description and JS prelude and denies the raw dispatch frame
//! with the top-level-only redirect error, while a top-level agent's bridge
//! on the same daemon keeps the full surface.
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

/// MIME type the FE renders as `QuestionCards` (PROTOCOL §7.x question resource).
const QUESTION_MIME: &str = "application/vnd.intent.question+json";
const PROPOSAL_MIME: &str = "application/vnd.intent.proposal+json";

/// Turn-1 trigger marker: the mock's `rules` entry matches on this, so the
/// tool-calling behavior fires ONLY on the first user turn (the flattened
/// answers on turn 2 fall through to the plain top-level response).
const ASK_MARKER: &str = "ASK_QUESTIONS_NOW_E2E";

/// The flattened `Q:`/`A:` answers the FE would send after the user fills in
/// the `QuestionCards` — plain text, delivered to the provider verbatim.
const FLATTENED_ANSWERS: &str = "Q: Which authentication method should the new endpoint use?\n\
A: OAuth\n\
\n\
Q: Which database should the service target?\n\
A: (skipped)";

/// Live `intentd serve` process; killed and its data dir removed on drop.
struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let log_path = self.data_dir.join("daemon.log");
        if let Ok(log) = std::fs::read_to_string(&log_path) {
            eprintln!("=== DAEMON LOG ===\n{log}\n=== END LOG ===");
        }
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-qask-{}", &id[..8]));
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

/// Drain subscriber events until an `agent:stream:end` for `agent_id` arrives;
/// returns the event's `data` payload so callers can assert its shape.
async fn await_stream_end<S>(sub: &mut WebSocketStream<S>, agent_id: &str) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    for _ in 0..120 {
        let frame = wss_event(sub, 30).await;
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:stream:end" && ev["data"]["agentId"].as_str() == Some(agent_id) {
            return ev["data"].clone();
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

/// Pre-seed the daemon's `SQLite` store with a regular (NON-chief) workspace —
/// the daemon opens the same data dir on launch.
async fn seed_workspace_only(data_dir: &Path, repository_path: Option<&Path>) -> String {
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
            title: "QASK-E2E".to_string(),
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
            repository_path: repository_path.map(|path| path.to_string_lossy().into_owned()),
            repository_owner: repository_path.map(|_| "intent-hq".to_string()),
            repository_name: repository_path.map(|_| "intentd".to_string()),
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
    ws.0
}

/// The two questions the mock asks on turn 1 — one `ask` call each.
fn question_one() -> Value {
    json!({
        "header": "Auth method",
        "question": "Which authentication method should the new endpoint use?",
        "explanation": "The endpoint handles third-party callbacks.",
        "options": [
            { "label": "OAuth", "description": "Standard OAuth 2.0 flow" },
            { "label": "API key", "description": "Static key in header" }
        ]
    })
}

fn question_two() -> Value {
    json!({
        "header": "Database",
        "question": "Which database should the service target?",
        "options": [
            { "label": "Postgres" },
            { "label": "SQLite" },
            { "label": "MySQL" }
        ],
        "multiSelect": true
    })
}

/// Q&A round trip over the real WSS transport, on a NON-chief workspace agent
/// (the binding is deliberately un-gated — see `bindings/app/question.rs`):
///
/// 1. Turn 1 drives TWO `ws.app.question.ask` calls (one question per call)
///    through the `workspace_api` MCP tool; the persisted final assistant
///    message must END with two `application/vnd.intent.question+json`
///    resource blocks in call order, each carrying the canonical payload and
///    an `attachmentId` matching its `intent-question://` URI.
/// 2. Turn 2 sends the flattened plain-text `Q:`/`A:` pairs via
///    `agent.sendMessage`; the provider must receive that text VERBATIM (no
///    daemon-side transformation), asserted via `MOCK_AGENT_PROMPT_LOG`.
#[tokio::test]
async fn question_ask_round_trip_over_wss() {
    let Some(script) = gate("WSS question.ask E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir, None).await;
    let prompt_log = data_dir.join("prompt-log.jsonl");
    let prompt_log_str = prompt_log.to_string_lossy().into_owned();
    // Two workspace_api invocations, ONE ws.app.question.ask per call — the
    // model-facing contract is one question per ask() call. Rule-gated on the
    // turn-1 marker so the turn-2 answers fall through to the plain response.
    let ask_one = format!(
        "return await ws.app.question.ask({});",
        json!(question_one())
    );
    let ask_two = format!(
        "return await ws.app.question.ask({});",
        json!(question_two())
    );
    let behavior = json!({
        "rules": [{
            "ifPromptContains": ASK_MARKER,
            "toolCalls": [
                {
                    "name": "workspace_api",
                    "arguments": { "code": ask_one, "summary": "ask question 1" },
                },
                {
                    "name": "workspace_api",
                    "arguments": { "code": ask_two, "summary": "ask question 2" },
                },
            ],
            "response": "I have two clarifying questions before I proceed.",
        }],
        "response": "Answers received, proceeding.",
    })
    .to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_PROMPT_LOG", &prompt_log_str),
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

    // RPC conn — a plain agent on the seeded NON-chief workspace.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "QAsk", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // ---- Turn 1: the mock asks two questions via ws.app.question.ask ----
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "content": format!("please plan the endpoint {ASK_MARKER}"),
        }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    let end_data = await_stream_end(&mut sub, &agent_id).await;

    // The persisted final assistant message must END with the two question
    // resource blocks (AtTurnEnd drain order == ask() call order).
    let conv = wss_rpc(
        &mut rpc,
        12,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let assistant = messages
        .iter()
        .rfind(|m| m["role"] == "assistant")
        .expect("final assistant message persisted");
    let blocks = assistant["contentBlocks"]
        .as_array()
        .expect("contentBlocks array");
    assert!(
        blocks.len() >= 2,
        "assistant message has at least the two question blocks: {blocks:?}"
    );
    let trailing = &blocks[blocks.len() - 2..];
    for block in trailing {
        assert_eq!(block["type"], "resource", "trailing block shape: {block}");
        assert_eq!(
            block["resource"]["mimeType"], QUESTION_MIME,
            "trailing block MIME: {block}"
        );
    }
    // No question blocks anywhere BEFORE the trailing pair — AtTurnEnd means
    // trailing, and exactly two were registered.
    let question_blocks: Vec<&Value> = blocks
        .iter()
        .filter(|b| b["type"] == "resource" && b["resource"]["mimeType"] == QUESTION_MIME)
        .collect();
    assert_eq!(
        question_blocks.len(),
        2,
        "exactly two question resource blocks: {blocks:?}"
    );

    // Canonical payloads, in ask() call order, with the minted attachmentId
    // echoed in the intent-question:// URI and the header echoed as `name`.
    let expected = [
        ("Auth method", question_one()),
        ("Database", question_two()),
    ];
    for (block, (header, q)) in trailing.iter().zip(expected.iter()) {
        let resource = &block["resource"];
        assert_eq!(resource["name"].as_str(), Some(*header), "name: {block}");
        let payload: Value = serde_json::from_str(resource["text"].as_str().expect("text"))
            .expect("question payload json");
        assert_eq!(payload["header"], q["header"], "payload header: {payload}");
        assert_eq!(
            payload["question"], q["question"],
            "payload question: {payload}"
        );
        assert_eq!(
            payload["options"], q["options"],
            "payload options: {payload}"
        );
        assert_eq!(
            payload["multiSelect"],
            q.get("multiSelect").cloned().unwrap_or(json!(false)),
            "payload multiSelect: {payload}"
        );
        if let Some(explanation) = q.get("explanation") {
            assert_eq!(
                &payload["explanation"], explanation,
                "payload explanation: {payload}"
            );
        }
        let attachment_id = payload["attachmentId"].as_str().expect("attachmentId");
        assert_eq!(
            resource["uri"].as_str(),
            Some(format!("intent-question://{attachment_id}").as_str()),
            "URI reuses the minted attachment id: {block}"
        );
    }

    // Live delivery (monorepo#732 fix wave): the terminal `agent:stream:end`
    // frame itself must carry the drained trailing blocks as `trailingBlocks`
    // (byte-identical to the persisted blocks, registration order) plus the
    // turn's `messageId` — the FE finalizes the in-flight message from
    // accumulated chunks at stream-end, so blocks appended only after the
    // stream loop would otherwise never reach it live.
    assert_eq!(
        end_data["messageId"], assistant["id"],
        "stream:end carries the turn's messageId: {end_data}"
    );
    let live_trailing = end_data["trailingBlocks"]
        .as_array()
        .expect("stream:end carries trailingBlocks when AtTurnEnd blocks were drained");
    assert_eq!(
        live_trailing.as_slice(),
        trailing,
        "trailingBlocks are byte-identical to the persisted trailing blocks"
    );

    // ---- Turn 2: flattened Q:/A: answers delivered verbatim ----
    let sent2 = wss_rpc(
        &mut rpc,
        13,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": FLATTENED_ANSWERS }),
    )
    .await;
    assert_eq!(sent2["success"], true, "answers sendMessage ok: {sent2}");
    let end_data2 = await_stream_end(&mut sub, &agent_id).await;
    // No attachments were registered on turn 2 — `trailingBlocks` is omitted.
    assert!(
        end_data2.get("trailingBlocks").is_none(),
        "trailingBlocks omitted when no AtTurnEnd blocks were drained: {end_data2}"
    );

    // The mock child logged the exact prompt text it received per turn: the
    // flattened answers must arrive VERBATIM within turn 2 (any per-turn
    // preamble, e.g. a role reminder, precedes the user content; the send may
    // drain via the queue, appending the dequeue-wait system note after it).
    let log = read_prompt_log(&prompt_log);
    assert!(
        log.len() >= 2,
        "expected 2 logged prompts, got {}: {log:?}",
        log.len()
    );
    let (second_turn, second_text) = &log[log.len() - 1];
    assert_eq!(*second_turn, 2, "same child served turn 2 (no respawn)");
    assert!(
        second_text.contains(FLATTENED_ANSWERS),
        "flattened Q:/A: answers delivered verbatim on turn 2: {second_text:?}"
    );
    // Verbatim means UNTRANSFORMED: the multi-line Q:/A: text survives as one
    // contiguous byte sequence — including the blank separator line.
    assert!(
        second_text.contains("A: (skipped)"),
        "skipped-answer marker survives: {second_text:?}"
    );

    // The answers are also persisted as a plain user message (no daemon-side
    // structuring of the Q:/A: text).
    let conv2 = wss_rpc(
        &mut rpc,
        14,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let messages2 = conv2["messages"].as_array().expect("messages array");
    assert!(
        messages2.iter().any(|m| {
            m["role"] == "user"
                && m["contentBlocks"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .any(|b| {
                        b["type"] == "text"
                            && b["text"]
                                .as_str()
                                .is_some_and(|t| t.starts_with(FLATTENED_ANSWERS))
                    })
        }),
        "answers persisted as a plain user text message: {messages2:?}"
    );
}

#[tokio::test]
async fn workspace_sibling_proposal_round_trip_over_wss() {
    let Some(script) = gate("WSS workspace.proposeSibling E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let repo_path = data_dir.join("source-repository");
    assert!(Command::new("git")
        .args(["init", "-b", "main"])
        .arg(&repo_path)
        .status()
        .expect("run git init")
        .success());
    for (key, value) in [("user.name", "Test"), ("user.email", "test@example.com")] {
        assert!(Command::new("git")
            .arg("-C")
            .arg(&repo_path)
            .args(["config", key, value])
            .status()
            .expect("run git config")
            .success());
    }
    std::fs::write(repo_path.join("README.md"), "WSS proposal test\n").unwrap();
    assert!(Command::new("git")
        .arg("-C")
        .arg(&repo_path)
        .args(["add", "README.md"])
        .status()
        .expect("run git add")
        .success());
    assert!(Command::new("git")
        .arg("-C")
        .arg(&repo_path)
        .args(["commit", "-m", "initial"])
        .status()
        .expect("run git commit")
        .success());

    let ws_id = seed_workspace_only(&data_dir, Some(&repo_path)).await;
    let marker = "PROPOSE_SIBLING_NOW_E2E";
    let proposal_code = r#"return await ws.workspace.proposeSibling({
        title: "Focused follow-up",
        initialPrompt: "Implement only the focused follow-up and run its tests.",
        specialist: "implementor"
    });"#;
    let behavior = json!({
        "rules": [{
            "ifPromptContains": marker,
            "toolCall": {
                "name": "workspace_api",
                "arguments": {
                    "code": proposal_code,
                    "summary": "propose focused sibling workspace"
                }
            },
            "response": "I prepared the focused follow-up workspace proposal.",
            "emitToolBlocks": true
        }],
        "response": "plain response"
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
    let port = u16::try_from(status["result"]["port"].as_u64().expect("port")).unwrap();
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

    let mut sub = connect_ws(port, cfg.clone()).await;
    wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    let mut rpc = connect_ws(port, cfg).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "SiblingProposer", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_string();
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": marker }),
    )
    .await;
    assert_eq!(sent["success"], true);
    await_stream_end(&mut sub, &agent_id).await;

    let conversation = wss_rpc(
        &mut rpc,
        12,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let messages = conversation["messages"].as_array().unwrap();
    let block = messages
        .iter()
        .flat_map(|message| message["contentBlocks"].as_array().into_iter().flatten())
        .find(|block| block["type"] == "resource" && block["resource"]["mimeType"] == PROPOSAL_MIME)
        .unwrap_or_else(|| panic!("persisted proposal resource: {conversation:#}"));
    assert!(messages.iter().any(|message| {
        message["contentBlocks"].as_array().is_some_and(|blocks| {
            blocks.iter().any(|block| {
                block["type"] == "tool_result"
                    && block["output"].as_array().is_some_and(|output| {
                        output
                            .iter()
                            .any(|item| item["resource"]["mimeType"] == PROPOSAL_MIME)
                    })
            })
        })
    }));
    let proposal: Value =
        serde_json::from_str(block["resource"]["text"].as_str().unwrap()).unwrap();
    assert_eq!(proposal["kind"], "workspace-create");
    assert_eq!(proposal["preview"]["workspaceCreate"]["mode"], "sibling");
    assert_eq!(
        proposal["preview"]["workspaceCreate"]["title"],
        "Focused follow-up"
    );
    assert_eq!(proposal["preview"]["workspaceCreate"]["branch"], "main");
    assert_eq!(
        proposal["payload"]["params"]["repositoryPath"],
        repo_path.to_string_lossy().as_ref()
    );
    assert!(proposal["payload"]["params"]["idempotencyKey"]
        .as_str()
        .unwrap()
        .starts_with("sibling-workspace-"));
}

// ---------------------------------------------------------------------------
// Sub-agent gate: per-agent MCP bridge client (parse the generated
// `intentd-mcp-*.json` and speak newline-delimited JSON-RPC to the loopback
// bridge directly, exactly like a spawned provider child would).
// ---------------------------------------------------------------------------

/// List the generated `intentd-mcp-*.json` config files under
/// `<data_dir>/agent-configs`, sorted for deterministic diffing.
#[allow(clippy::case_sensitive_file_extension_comparisons)] // extensions generated by our own code with fixed case
fn mcp_config_files(data_dir: &Path) -> Vec<PathBuf> {
    let dir = data_dir.join("agent-configs");
    let mut out: Vec<PathBuf> = std::fs::read_dir(&dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.starts_with("intentd-mcp-") && n.ends_with(".json"))
                })
                .collect()
        })
        .unwrap_or_default();
    out.sort();
    out
}

/// Extract the bridge `--connect <addr>` from a generated MCP config file.
fn bridge_addr_from_config(path: &Path) -> String {
    let cfg: Value = serde_json::from_str(&std::fs::read_to_string(path).expect("read mcp config"))
        .expect("parse mcp config");
    let args = cfg["mcpServers"]["workspace-mcp"]["args"]
        .as_array()
        .expect("workspace-mcp args");
    let idx = args
        .iter()
        .position(|a| a == "--connect")
        .expect("--connect flag in bridge args");
    args[idx + 1].as_str().expect("bridge addr").to_string()
}

struct BridgeClient {
    reader: tokio::io::BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
    next_id: i64,
}

impl BridgeClient {
    async fn connect(addr: &str) -> Self {
        use tokio::io::BufReader;
        let stream = timeout(Duration::from_secs(10), TcpStream::connect(addr))
            .await
            .expect("bridge connect timeout")
            .expect("bridge connect");
        let (r, w) = stream.into_split();
        let mut c = BridgeClient {
            reader: BufReader::new(r),
            writer: w,
            next_id: 1,
        };
        let init = c.request("initialize", json!({})).await;
        assert!(
            init["result"]["serverInfo"].is_object(),
            "initialize: {init}"
        );
        c
    }

    async fn request(&mut self, method: &str, params: Value) -> Value {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
        let id = self.next_id;
        self.next_id += 1;
        let line = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        self.writer
            .write_all(format!("{line}\n").as_bytes())
            .await
            .expect("bridge write");
        let mut buf = String::new();
        loop {
            buf.clear();
            let n = timeout(Duration::from_secs(30), self.reader.read_line(&mut buf))
                .await
                .expect("bridge read timeout")
                .expect("bridge read");
            assert!(n > 0, "bridge closed while waiting for response");
            let v: Value = match serde_json::from_str(buf.trim()) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if v["id"] == id {
                return v;
            }
        }
    }

    /// `tools/list` → the `workspace_api` tool description.
    async fn workspace_api_description(&mut self) -> String {
        let resp = self.request("tools/list", json!({})).await;
        let tools = resp["result"]["tools"].as_array().expect("tools array");
        tools
            .iter()
            .find(|t| t["name"] == "workspace_api")
            .expect("workspace_api tool listed")["description"]
            .as_str()
            .expect("description string")
            .to_string()
    }

    /// `tools/call workspace_api` with agent JS; returns `(is_error, text)`.
    async fn call_js(&mut self, code: &str) -> (bool, String) {
        let resp = self
            .request(
                "tools/call",
                json!({
                    "name": "workspace_api",
                    "arguments": { "code": code, "summary": "e2e sub-agent gate probe" },
                }),
            )
            .await;
        assert!(
            resp.get("error").is_none(),
            "tools/call transport error: {resp}"
        );
        let result = &resp["result"];
        let is_error = result["isError"].as_bool().unwrap_or(false);
        let text = result["content"][0]["text"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        (is_error, text)
    }
}

/// Sub-agent question gate over the real WSS + bridge path: a background
/// agent's bridge (a sub-agent by the `parent_agent_id.is_some() ||
/// is_background` derivation) prunes `ws.app.question.*` from its tool
/// description and JS prelude and denies the raw `host({...})` frame with
/// the top-level-only redirect error; a top-level agent's bridge on the same
/// daemon keeps the full question surface (with `structuredQuestions` on).
#[tokio::test]
async fn sub_agent_question_ask_denied_over_wss() {
    let Some(script) = gate("WSS sub-agent question gate E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir, None).await;
    let behavior = json!({ "response": "done" }).to_string();
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

    // Subscriber conn — before any agent activity.
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

    // ---- Top-level agent: full question surface on its bridge ----
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "TopLevel", "model": "mock:default" }),
    )
    .await;
    let top_agent = created["agent"]["id"]
        .as_str()
        .expect("top-level agent id")
        .to_string();
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": top_agent, "content": "say done" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    await_stream_end(&mut sub, &top_agent).await;

    let configs_top = mcp_config_files(&data_dir);
    assert_eq!(
        configs_top.len(),
        1,
        "one agent → one mcp config: {configs_top:?}"
    );
    let mut bridge_top = BridgeClient::connect(&bridge_addr_from_config(&configs_top[0])).await;
    let desc_top = bridge_top.workspace_api_description().await;
    assert!(
        desc_top.contains("ws.app.question.ask("),
        "top-level description must advertise ws.app.question.ask"
    );
    let (err, text) = bridge_top
        .call_js("return typeof ws.app.question.ask;")
        .await;
    assert!(!err, "typeof probe on top-level bridge: {text}");
    assert!(
        text.contains("function"),
        "question installer present on top-level bridge: {text}"
    );

    // ---- Background agent (sub-agent): gated bridge ----
    let created_bg = wss_rpc(
        &mut rpc,
        20,
        "agent.create",
        json!({
            "workspaceId": ws_id,
            "name": "BgWorker",
            "model": "mock:default",
            "isBackground": true,
        }),
    )
    .await;
    let bg_agent = created_bg["agent"]["id"]
        .as_str()
        .expect("background agent id")
        .to_string();
    let sent_bg = wss_rpc(
        &mut rpc,
        21,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": bg_agent, "content": "say done" }),
    )
    .await;
    assert_eq!(sent_bg["success"], true, "bg sendMessage ok: {sent_bg}");
    await_stream_end(&mut sub, &bg_agent).await;

    let configs_all = mcp_config_files(&data_dir);
    assert_eq!(
        configs_all.len(),
        2,
        "two agents → two mcp configs: {configs_all:?}"
    );
    let config_bg = configs_all
        .iter()
        .find(|p| !configs_top.contains(p))
        .expect("new mcp config for the background agent");
    let mut bridge_bg = BridgeClient::connect(&bridge_addr_from_config(config_bg)).await;

    // Layer (a): description pruned of ws.app.question.*.
    let desc_bg = bridge_bg.workspace_api_description().await;
    assert!(
        !desc_bg.contains("ws.app.question."),
        "sub-agent description must not advertise ws.app.question.*"
    );
    assert!(
        desc_bg.contains("ws.agent.requestDiscussion("),
        "attention-request docs must survive the sub-agent gate"
    );

    // Layer (b): prelude omits the installer.
    let (err, text) = bridge_bg
        .call_js("return typeof (ws.app && ws.app.question);")
        .await;
    assert!(!err, "typeof probe on sub-agent bridge: {text}");
    assert!(
        text.contains("undefined"),
        "question installer must be absent on the sub-agent bridge: {text}"
    );

    // Layer (c): the raw dispatch frame is denied with the redirect error.
    let (err, text) = bridge_bg
        .call_js(
            "return await host({ method: 'app.question.ask', args: { question: { header: 'h', \
             question: 'q', options: [{label:'a'},{label:'b'}] } } });",
        )
        .await;
    assert!(err, "sub-agent ask must be denied: {text}");
    assert!(
        text.contains("only available to top-level agents")
            && text.contains("ws.agent.requestDiscussion")
            && text.contains("ws.agent.reportToParent"),
        "expected top-level-only redirect denial, got: {text}"
    );
    assert!(
        !text.contains("disabled in settings"),
        "sub-agent denial must not masquerade as a settings gate: {text}"
    );
}
