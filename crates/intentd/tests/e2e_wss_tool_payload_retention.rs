//! WSS end-to-end coverage for the tool-payload retention sweep
//! (`agents.toolPayloadRetentionDays`): once the sweep has compacted an
//! externalized tool body into its `*_replay` preview row, the daemon must
//!
//! 1. serve the slim `agent.getConversation` block UNCHANGED (same preview,
//!    same `outputTruncated` / `outputBytes` flags as an uncompacted block),
//! 2. answer `agent.getMessageBlock` on the pruned block with the stored
//!    preview + its flags PLUS the additive `inputPruned: true` /
//!    `outputPruned: true` instead of implying a full body, while an
//!    uncompacted block still hydrates to its full body with no flags, and
//! 3. replay the pruned block in the recovery `<supervisor>` history
//!    byte-identically to the uncompacted one — the 4000-char
//!    middle-truncated body with `truncated="true" original_chars="N"`, and
//! 4. keep 2. and 3. true after `agent.editAndRegenerate` of a LATER user
//!    message — the truncation must not drop the kept prefix's `*_replay`
//!    (or full) side rows, so the regenerated turn's replay renders the
//!    pruned block byte-identically to the pre-edit replay.
//!
//! The sweep itself ticks on a ≥5-minute cadence in the daemon, so the test
//! forces compaction through the same store method the loop calls
//! (`Store::compact_tool_payloads_before`) with a cutoff that catches only
//! the message whose `created_at` was back-dated — the fresh sibling must be
//! left untouched. The recreate path is driven exactly like
//! `e2e_wss_poisoned_session_recreate.rs`: turn 1 opens `session/new`, the
//! idle mock child is `SIGKILL`ed out-of-band, and turn 2's respawn (the mock
//! does not advertise `loadSession`) recreates the session and replays the
//! history; `MOCK_AGENT_PROMPT_LOG` records the exact prompt text.
//!
//! Gated on `node` + the mock script; skips cleanly otherwise.

#![cfg(unix)]

mod common;

use std::path::Path;
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

const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

struct Daemon {
    child: Child,
    data_dir: tempfile::TempDir,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let log_path = self.data_dir.path().join("daemon.log");
        if let Ok(log) = std::fs::read_to_string(&log_path) {
            eprintln!("=== DAEMON LOG ===\n{log}\n=== END LOG ===");
        }
    }
}

fn temp_data_dir() -> tempfile::TempDir {
    common::test_tempdir_in("/tmp", "itd-wss-retention-")
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

async fn seed_workspace_only(data_dir: &Path) -> String {
    use intent_core::WorkspaceId;
    use intent_store::Store;
    let db_path = data_dir.join("intentd.db");
    let store = Store::open(&db_path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace_seed(&ws))
        .await
        .expect("insert ws");
    ws.0
}

fn workspace_seed(id: &intent_core::WorkspaceId) -> intent_core::Workspace {
    use intent_core::{now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus};
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "WSS-RETENTION-E2E".to_string(),
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
        browser_client_id: None,
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
    }
}

/// Bounded poll: wait until the mock's `MOCK_AGENT_PID_FILE` holds at least
/// `n` pid lines (one appended per spawn) and return them all.
async fn await_pid_lines(path: &Path, n: usize) -> Vec<u32> {
    for _ in 0..400 {
        if let Ok(contents) = tokio::fs::read_to_string(path).await {
            let pids: Vec<u32> = contents
                .lines()
                .filter_map(|l| l.trim().parse().ok())
                .collect();
            if pids.len() >= n {
                return pids;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("pid file {} never reached {n} line(s)", path.display());
}

/// Bounded poll: wait until the daemon log contains `needle`.
async fn await_daemon_log_contains(data_dir: &Path, needle: &str) {
    let log_path = data_dir.join("daemon.log");
    for _ in 0..400 {
        if tokio::fs::read_to_string(&log_path)
            .await
            .unwrap_or_default()
            .contains(needle)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("daemon log never contained {needle:?}");
}

/// Bounded poll: wait until `agent.getSession` reports `status == "idle"`.
async fn await_session_idle<S>(
    ws: &mut WebSocketStream<S>,
    id_base: i64,
    ws_id: &str,
    agent_id: &str,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut last = Value::Null;
    for i in 0..100 {
        last = wss_rpc(
            ws,
            id_base + i,
            "agent.getSession",
            json!({ "workspaceId": ws_id, "agentId": agent_id }),
        )
        .await;
        if last["session"]["status"] == "idle" {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("agent session never settled to idle; last: {last}");
}

/// Drive one `agent.sendMessage` turn to its `agent:stream:end`, failing on
/// `agent:failed`.
async fn run_turn<S>(
    rpc: &mut WebSocketStream<S>,
    sub: &mut WebSocketStream<S>,
    id: i64,
    ws_id: &str,
    agent_id: &str,
    content: &str,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let sent = wss_rpc(
        rpc,
        id,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": content }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    for _ in 0..200 {
        let frame = wss_event(sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id) {
            continue;
        }
        match event["type"].as_str() {
            Some("agent:failed") => panic!("agent:failed during turn {content:?}: {frame}"),
            Some("agent:stream:end") => return,
            _ => {}
        }
    }
    panic!("turn {content:?} never reached agent:stream:end");
}

/// The prompt text of every prompt the mock child(ren) received, in order.
fn prompt_texts(path: &Path) -> Vec<String> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            let v: Value = serde_json::from_str(l).expect("prompt log line json");
            v["text"].as_str().expect("prompt text").to_string()
        })
        .collect()
}

/// The rendered `<tool_result tool_use_id="{tool_use_id}" …>…</tool_result>`
/// element of a replayed `<supervisor>` history.
fn tool_result_element(history: &str, tool_use_id: &str) -> String {
    let open = format!("<tool_result tool_use_id=\"{tool_use_id}\"");
    let start = history
        .find(&open)
        .unwrap_or_else(|| panic!("no tool_result for {tool_use_id} in replay: {history}"));
    let end = history[start..]
        .find("</tool_result>")
        .expect("tool_result closes")
        + start
        + "</tool_result>".len();
    history[start..end].to_string()
}

/// Same for `<tool_use name="bash" tool_use_id="{tool_use_id}" …>…</tool_use>`.
fn tool_use_element(history: &str, tool_use_id: &str) -> String {
    let open = format!("<tool_use name=\"bash\" tool_use_id=\"{tool_use_id}\"");
    let start = history
        .find(&open)
        .unwrap_or_else(|| panic!("no tool_use for {tool_use_id} in replay: {history}"));
    let end = history[start..]
        .find("</tool_use>")
        .expect("tool_use closes")
        + start
        + "</tool_use>".len();
    history[start..end].to_string()
}

/// Side-table row kinds for one message, sorted.
async fn payload_kinds(store: &intent_store::Store, message_id: &str) -> Vec<String> {
    let mut kinds: Vec<String> =
        sqlx::query_scalar("SELECT kind FROM agent_message_payload WHERE message_id = ?")
            .bind(message_id)
            .fetch_all(store.read_pool())
            .await
            .expect("payload kinds");
    kinds.sort();
    kinds
}

/// Persist one assistant message carrying an over-inline `tool_use` input and
/// `tool_result` output (both externalized to the side table on write) at
/// `created_at`, returning its row id. Both messages of the scenario share
/// the same bodies so their served / replayed shapes are directly comparable.
async fn append_heavy_tool_message(
    store: &intent_store::Store,
    agent: &intent_core::AgentId,
    tool_use_id: &str,
    big_in: &str,
    big_out: &str,
    created_at: &str,
) -> String {
    let content = json!([
        { "type": "tool_use", "id": tool_use_id, "name": "bash", "input": { "cmd": big_in } },
        { "type": "tool_result", "id": format!("{tool_use_id}:result"), "tool_use_id": tool_use_id,
          "output": big_out, "is_error": false },
    ]);
    store
        .append_agent_message(agent, "assistant", &content, created_at)
        .await
        .expect("append heavy tool message")
        .id
}

/// `agent.getMessageBlock` → the served `block`.
async fn get_block<S>(
    rpc: &mut WebSocketStream<S>,
    id: i64,
    agent_id: &str,
    message_id: &str,
    block_id: &str,
) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    wss_rpc(
        rpc,
        id,
        "agent.getMessageBlock",
        json!({ "agentId": agent_id, "messageId": message_id, "blockId": block_id }),
    )
    .await["block"]
        .clone()
}

/// Strip block ids so a pruned and an unpruned message's slim blocks compare.
fn without_ids(blocks: &[Value]) -> Vec<Value> {
    blocks
        .iter()
        .map(|b| {
            let mut b = b.clone();
            if let Some(obj) = b.as_object_mut() {
                obj.remove("id");
                obj.remove("tool_use_id");
            }
            b
        })
        .collect()
}

/// RETENTION-PRUNE: a compacted tool body serves the unchanged slim block on
/// `agent.getConversation`, the preview + `*Pruned: true` on
/// `agent.getMessageBlock`, and the identical 4000-char middle-truncated
/// element in the recovery replay as an uncompacted body; the fresh sibling
/// message is untouched by the sweep.
#[tokio::test]
async fn pruned_tool_payload_is_flagged_and_replays_identically_over_wss() {
    use intent_core::config::DEFAULT_HISTORY_REPLAY_TOOL_CONTENT_CHARS;
    use intent_core::AgentId;

    let Some(script) = gate("WSS tool-payload retention E2E") else {
        return;
    };
    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let ws_id = seed_workspace_only(&data_dir).await;
    let pid_file = data_dir.join("pids.txt");
    let pid_file_s = pid_file.to_string_lossy().into_owned();
    let prompt_log = data_dir.join("prompts.jsonl");
    let prompt_log_s = prompt_log.to_string_lossy().into_owned();
    // The mock does NOT advertise `loadSession`, so the post-kill respawn
    // takes the recreate + history-replay path.
    let behavior = json!({ "response": "ack" }).to_string();
    let env: [(&str, &str); 6] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_PID_FILE", &pid_file_s),
        ("MOCK_AGENT_PROMPT_LOG", &prompt_log_s),
    ];
    let child = spawn_serve(&data_dir, &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir_guard,
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
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "WSS-RETENTION", "model": "default", "provider": "mock" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let agent = AgentId(agent_id.clone());

    // Turn 1 opens the provider session (`session/new`) and settles idle.
    run_turn(&mut rpc, &mut sub, 11, &ws_id, &agent_id, "first turn").await;
    await_session_idle(&mut rpc, 100, &ws_id, &agent_id).await;

    // Two identical heavy tool messages: one back-dated 3 days (the sweep's
    // target), one fresh (must survive the sweep untouched). Bodies well past
    // the 4 KiB extraction threshold, the 2 KiB slim budget, and the 4000
    // char replay cap.
    let big_in = format!("IN-HEAD-{}-IN-TAIL", "i".repeat(12_000));
    let big_out = format!("OUT-HEAD-{}-OUT-TAIL", "o".repeat(20_000));
    let store = intent_store::Store::open(&data_dir.join("intentd.db"))
        .await
        .expect("open store");
    let old_id = append_heavy_tool_message(
        &store,
        &agent,
        "tc-old",
        &big_in,
        &big_out,
        &intent_core::iso_minutes_ago(3 * 24 * 60),
    )
    .await;
    let fresh_id = append_heavy_tool_message(
        &store,
        &agent,
        "tc-fresh",
        &big_in,
        &big_out,
        &intent_core::now_iso(),
    )
    .await;
    assert_eq!(
        payload_kinds(&store, &old_id).await,
        vec!["tool_result_output", "tool_use_input"],
        "both heavy bodies externalized as FULL rows on write"
    );

    // The sweep seam: the same store call the retention loop makes each tick
    // with `agents.toolPayloadRetentionDays = 1` (cutoff = now − 1 day) at
    // the default replay cap. Only the back-dated message is old enough.
    let replay_chars = DEFAULT_HISTORY_REPLAY_TOOL_CONTENT_CHARS as usize;
    let compacted = store
        .compact_tool_payloads_before(&intent_core::iso_minutes_ago(24 * 60), replay_chars)
        .await
        .expect("compact tool payloads");
    assert_eq!(
        compacted, 2,
        "exactly the old message's two bodies compacted"
    );
    assert_eq!(
        payload_kinds(&store, &old_id).await,
        vec!["tool_result_output_replay", "tool_use_input_replay"],
        "old message's full rows replaced by replay previews"
    );
    assert_eq!(
        payload_kinds(&store, &fresh_id).await,
        vec!["tool_result_output", "tool_use_input"],
        "fresh message untouched by the sweep"
    );

    // (1) The slim conversation read is unchanged for the pruned message:
    // block-for-block identical to the unpruned sibling (ids aside).
    let conv = wss_rpc(
        &mut rpc,
        20,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages");
    let slim_blocks = |id: &str| -> Vec<Value> {
        messages
            .iter()
            .find(|m| m["id"].as_str() == Some(id))
            .unwrap_or_else(|| panic!("message {id} served"))["contentBlocks"]
            .as_array()
            .expect("content blocks")
            .clone()
    };
    let old_slim = slim_blocks(&old_id);
    let fresh_slim = slim_blocks(&fresh_id);
    assert_eq!(old_slim[0]["inputTruncated"], json!(true));
    assert_eq!(old_slim[1]["outputTruncated"], json!(true));
    assert_eq!(old_slim[1]["outputBytes"], json!(big_out.len()));
    let old_preview = old_slim[1]["output"].as_str().expect("output preview");
    assert!(big_out.starts_with(old_preview) && old_preview.len() < big_out.len());
    assert!(
        old_slim[1].get("outputPruned").is_none() && old_slim[0].get("inputPruned").is_none(),
        "the slim projection never carries the pruned flag: {old_slim:?}"
    );
    assert_eq!(
        without_ids(&old_slim),
        without_ids(&fresh_slim),
        "pruned and unpruned messages serve identical slim blocks"
    );

    // (2) agent.getMessageBlock: the pruned blocks serve the stored preview
    // with the slim flags intact PLUS `*Pruned: true`; the unpruned sibling
    // hydrates the full body with no flags at all.
    let pruned_result = get_block(&mut rpc, 30, &agent_id, &old_id, "tc-old:result").await;
    assert_eq!(
        pruned_result["outputPruned"],
        json!(true),
        "{pruned_result}"
    );
    assert_eq!(pruned_result["outputTruncated"], json!(true));
    assert_eq!(pruned_result["outputBytes"], json!(big_out.len()));
    assert_eq!(pruned_result["output"].as_str(), Some(old_preview));
    assert_eq!(pruned_result["tool_use_id"], json!("tc-old"));
    assert!(pruned_result.get("inputPruned").is_none());
    let pruned_use = get_block(&mut rpc, 31, &agent_id, &old_id, "tc-old").await;
    assert_eq!(pruned_use["inputPruned"], json!(true), "{pruned_use}");
    assert_eq!(pruned_use["inputTruncated"], json!(true));
    assert_eq!(pruned_use["name"], json!("bash"));
    assert!(pruned_use.get("outputPruned").is_none());
    let full_result = get_block(&mut rpc, 32, &agent_id, &fresh_id, "tc-fresh:result").await;
    assert_eq!(full_result["output"].as_str(), Some(big_out.as_str()));
    for flag in ["outputPruned", "outputTruncated", "outputBytes"] {
        assert!(
            full_result.get(flag).is_none(),
            "hydrated block carries no {flag}: {full_result}"
        );
    }
    let full_use = get_block(&mut rpc, 33, &agent_id, &fresh_id, "tc-fresh").await;
    assert_eq!(full_use["input"]["cmd"].as_str(), Some(big_in.as_str()));
    assert!(full_use.get("inputPruned").is_none() && full_use.get("inputTruncated").is_none());

    // (3) Recovery replay: SIGKILL the idle mock child so turn 2 respawns a
    // fresh child that must recreate the session (no `loadSession`) and
    // replay the history as `<supervisor>` XML.
    let pids = await_pid_lines(&pid_file, 1).await;
    let killed = Command::new("kill")
        .args(["-9", &pids[0].to_string()])
        .status()
        .expect("run kill")
        .success();
    assert!(killed, "SIGKILL delivered to idle mock child {}", pids[0]);
    await_daemon_log_contains(
        &data_dir,
        "idle agent child exited unexpectedly; handle reaped",
    )
    .await;
    run_turn(
        &mut rpc,
        &mut sub,
        40,
        &ws_id,
        &agent_id,
        "second turn after prune",
    )
    .await;

    let prompts = prompt_texts(&prompt_log);
    assert_eq!(
        prompts.len(),
        2,
        "two prompts across both children: {prompts:?}"
    );
    assert!(
        !prompts[0].contains("<supervisor>"),
        "turn 1 on a fresh session replays nothing: {:?}",
        prompts[0]
    );
    let replay = &prompts[1];
    assert!(
        replay.contains("<supervisor>") && replay.contains("</supervisor>"),
        "recreated session's first prompt wraps history in <supervisor> XML: {replay:?}"
    );
    // The pruned and the unpruned message render the SAME element bytes
    // (ids aside): the 4000-char middle-truncated body carrying
    // `truncated="true" original_chars="N"`.
    let (expected_out, out_chars) =
        intent_core::replay_preview::truncate_marked(&big_out, replay_chars);
    let out_chars = out_chars.expect("output is over the replay cap");
    assert_eq!(out_chars, big_out.chars().count());
    let expected_in_str = intent_core::replay_preview::safe_stringify(&json!({ "cmd": big_in }));
    let (expected_in, in_chars) =
        intent_core::replay_preview::truncate_marked(&expected_in_str, replay_chars);
    let in_chars = in_chars.expect("input is over the replay cap");

    let old_result_el = tool_result_element(replay, "tc-old");
    let fresh_result_el = tool_result_element(replay, "tc-fresh");
    assert_eq!(
        old_result_el.replace("tc-old", "tc-fresh"),
        fresh_result_el,
        "pruned tool_result replays byte-identically to the unpruned one"
    );
    assert!(
        old_result_el.contains(&format!(
            "<tool_result tool_use_id=\"tc-old\" is_error=\"false\" truncated=\"true\" original_chars=\"{out_chars}\">"
        )),
        "pruned tool_result element head: {old_result_el}"
    );
    assert!(
        old_result_el.contains(&expected_out),
        "pruned tool_result carries the {replay_chars}-char middle-truncated body"
    );
    assert!(
        old_result_el.contains("OUT-HEAD-") && old_result_el.contains("-OUT-TAIL"),
        "middle truncation keeps head and tail"
    );
    assert!(
        old_result_el.chars().count() < replay_chars + 200,
        "replayed element is bounded near the cap, got {} chars",
        old_result_el.chars().count()
    );

    let old_use_el = tool_use_element(replay, "tc-old");
    let fresh_use_el = tool_use_element(replay, "tc-fresh");
    assert_eq!(
        old_use_el.replace("tc-old", "tc-fresh"),
        fresh_use_el,
        "pruned tool_use replays byte-identically to the unpruned one"
    );
    assert!(
        old_use_el.contains(&format!(
            "truncated=\"true\" original_chars=\"{in_chars}\">"
        )),
        "pruned tool_use element head: {old_use_el}"
    );
    assert!(
        old_use_el.contains(&expected_in.replace('"', "&quot;")),
        "pruned tool_use carries the {replay_chars}-char middle-truncated input"
    );

    // (4) agent.editAndRegenerate on the turn-2 user message: the truncation
    // keeps both heavy messages and the forced session recreate replays them
    // again on the regenerated turn. The pruned block must still serve its
    // preview + `outputPruned` and replay byte-identically to before — a
    // remint of the kept prefix would have swept its `*_replay` rows.
    let conv = wss_rpc(
        &mut rpc,
        50,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let turn2_user_id = conv["messages"]
        .as_array()
        .expect("messages")
        .iter()
        .find(|m| {
            m["role"] == "user"
                && m["contentBlocks"]
                    .as_array()
                    .is_some_and(|b| b.iter().any(|b| b["text"] == "second turn after prune"))
        })
        .and_then(|m| m["id"].as_str())
        .expect("turn-2 user message id")
        .to_string();
    let edited = wss_rpc(
        &mut rpc,
        51,
        "agent.editAndRegenerate",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "messageId": turn2_user_id,
            "content": "edited second turn",
        }),
    )
    .await;
    assert_eq!(edited["success"], true, "editAndRegenerate ok: {edited}");
    assert_eq!(
        edited["truncatedCount"],
        json!(2),
        "turn-2 user + assistant rows dropped: {edited}"
    );
    for _ in 0..200 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        match event["type"].as_str() {
            Some("agent:failed") => panic!("agent:failed during regenerated turn: {frame}"),
            Some("agent:stream:end") => break,
            _ => {}
        }
    }
    await_session_idle(&mut rpc, 300, &ws_id, &agent_id).await;
    let mut prompts = prompt_texts(&prompt_log);
    for _ in 0..100 {
        if prompts.len() >= 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        prompts = prompt_texts(&prompt_log);
    }
    assert_eq!(
        prompts.len(),
        3,
        "regenerated turn prompted a recreated session: {prompts:?}"
    );
    let regen = &prompts[2];
    assert!(
        regen.contains("<supervisor>") && !regen.contains("second turn after prune"),
        "regenerated turn replays only the kept prefix: {regen:?}"
    );
    assert_eq!(
        tool_result_element(regen, "tc-old"),
        old_result_el,
        "pruned tool_result replays byte-identically after the edit"
    );
    assert_eq!(
        tool_use_element(regen, "tc-old"),
        old_use_el,
        "pruned tool_use replays byte-identically after the edit"
    );
    assert_eq!(tool_result_element(regen, "tc-fresh"), fresh_result_el);
    assert_eq!(tool_use_element(regen, "tc-fresh"), fresh_use_el);

    assert_eq!(
        payload_kinds(&store, &old_id).await,
        vec!["tool_result_output_replay", "tool_use_input_replay"],
        "kept pruned message keeps its replay rows across the edit"
    );
    assert_eq!(
        payload_kinds(&store, &fresh_id).await,
        vec!["tool_result_output", "tool_use_input"],
        "kept full message keeps its full rows across the edit"
    );
    assert_eq!(
        get_block(&mut rpc, 60, &agent_id, &old_id, "tc-old:result").await,
        pruned_result,
        "pruned block serves identically after the edit"
    );
    assert_eq!(
        get_block(&mut rpc, 61, &agent_id, &old_id, "tc-old").await,
        pruned_use
    );
    assert_eq!(
        get_block(&mut rpc, 62, &agent_id, &fresh_id, "tc-fresh:result").await,
        full_result,
        "full block still hydrates after the edit"
    );
}
