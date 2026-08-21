//! WSS end-to-end for the live `chat.subscribe` delta stream's `tool_result`
//! block ids (monorepo#2029): a real turn whose tool completes AFTER other
//! blocks already took the index the old mapper predicted (`tool_use` + 1) must
//! deliver that tool's `tool_result` under the id the DURABLE transcript
//! assigned — not an invented one that clobbers a block the client already
//! holds.
//!
//! Boots a real `intentd serve` against the mock ACP provider and drives the
//! turn over a pinned-TLS WebSocket (HTTPS upgrade → JSON-RPC 2.0 → router →
//! services → store and back), so the whole production path is exercised:
//! ACP `session/update` → `route_notification` → `record_tool`'s real block
//! indices → the `agent:tool:call` event's `resultBlockId` → the chat delta
//! mapper → the wire. `uds_chat_subscription.rs` pins the same behaviour over
//! UDS with synthesized events; the unit coverage lives in
//! `intent-services/agent_session/tests.rs` and
//! `intent-transport/subscriptions/tests.rs`.
//!
//! Both regression shapes are covered — interleaved text between the call and
//! its completion, and parallel calls whose `tool_use` blocks both land before
//! either result — off ONE fixture: the same `MOCK_AGENT_BEHAVIOR` serves both,
//! with a prompt marker selecting the mock `rules` entry.
//!
//! Gated on `node` + the mock script; skips cleanly otherwise.

#![cfg(unix)]

mod common;

use std::collections::HashMap;
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
const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

/// Prompt markers selecting the mock's per-shape `rules` entry.
const INTERLEAVED: &str = "INTERLEAVED_TOOL_TURN";
const PARALLEL: &str = "PARALLEL_TOOL_TURN";

type TlsWs = WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;

/// Live `intentd serve` process; killed and its data dir removed on drop.
struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        // Kill the whole process group (daemon + Node.js ACP children) BEFORE
        // removing the data dir, so an orphaned child cannot re-create files
        // under it after cleanup. Spawned with process_group(0).
        {
            use nix::sys::signal::{self, Signal};
            use nix::unistd::Pid;
            let pid = Pid::from_raw(self.child.id().cast_signed());
            let _ = signal::killpg(pid, Signal::SIGKILL);
        }
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-chatids-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    let secrets_file = data_dir.join("secrets.json");
    common::enable_ws_api(data_dir);
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_SECRETS_FILE", &secrets_file)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    // Group leader so Daemon::drop can killpg the daemon + ACP children.
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd.spawn().expect("spawn intentd serve")
}

/// Wait for the daemon's UDS to accept connections, up to the shared
/// daemon-startup budget (see `common::daemon_startup_timeout`).
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
        .expect("protocol versions")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedVerifier {
            fingerprint: fingerprint.to_string(),
            provider,
        }))
        .with_no_client_auth();
    Arc::new(config)
}

/// Open an authenticated WSS connection (token in the query string).
async fn connect_ws(port: u16, cfg: Arc<ClientConfig>) -> TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// Send one JSON-RPC frame and return the result whose id matches; any
/// out-of-band notifications are ignored.
async fn wss_rpc(ws: &mut TlsWs, id: i64, method: &str, params: Value) -> Value {
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

/// Read one `subscription.push` notification, bounded by an absolute deadline
/// so heartbeat frames cannot extend the wait.
async fn wss_push_until(ws: &mut TlsWs, deadline: tokio::time::Instant) -> Value {
    loop {
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .filter(|d| !d.is_zero())
            .expect("subscription.push deadline elapsed");
        let next = timeout(remaining, ws.next())
            .await
            .expect("subscription.push timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == "subscription.push" {
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

/// Mock-agent gate (parity with the rest of the WSS e2e suite).
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

/// Pre-seed the workspace row the daemon will serve (no git worktree needed —
/// the turn is pure daemon + mock provider). The store handle is dropped before
/// the daemon opens the same data dir.
async fn seed_workspace(data_dir: &Path) -> String {
    use intent_core::{
        now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceId, WorkspaceStatus,
    };
    use intent_store::Store;
    let store = Store::open(&data_dir.join("intentd.db"))
        .await
        .expect("open store");
    let id = WorkspaceId::new();
    let ts = now_iso();
    let ws = Workspace {
        id: id.clone(),
        title: "WSS chat block ids".to_string(),
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
        disk_usage: None,
        pending_delete_at: None,
    };
    store.insert_workspace(&ws).await.expect("insert ws");
    id.0
}

fn text_update(text: &str) -> Value {
    json!({
        "sessionUpdate": "agent_message_chunk",
        "content": { "type": "text", "text": text },
    })
}

fn call_update(tool_call_id: &str) -> Value {
    json!({
        "sessionUpdate": "tool_call",
        "toolCallId": tool_call_id,
        "title": format!("bash: {tool_call_id}"),
        "name": "run_tests",
        "kind": "execute",
        "status": "in_progress",
        "rawInput": { "cmd": "cargo test" },
    })
}

fn done_update(tool_call_id: &str, output: &str) -> Value {
    json!({
        "sessionUpdate": "tool_call_update",
        "toolCallId": tool_call_id,
        "status": "completed",
        "rawOutput": { "summary": output },
    })
}

/// One `MOCK_AGENT_BEHAVIOR` driving both regression shapes, selected by a
/// marker in the prompt. Each `rawUpdates` sequence is echoed verbatim as ACP
/// `session/update` notifications before the rule's closing `response` chunk.
fn behavior() -> String {
    json!({
        "rules": [
            {
                "ifPromptContains": INTERLEAVED,
                "response": "Tests are green.",
                // text, call, MORE TEXT, completion: the interleaved text is
                // flushed into the index the old mapper predicted for the result.
                "rawUpdates": [
                    text_update("I'll run the tests. "),
                    call_update("call_a"),
                    text_update("<group:Setup>\nChecking output. "),
                    done_update("call_a", "12 passed"),
                ],
            },
            {
                "ifPromptContains": PARALLEL,
                "response": "Both are green.",
                // Both tool_use blocks land before either result, so the old
                // `tool_use + 1` prediction named t2's tool row for t1's result.
                "rawUpdates": [
                    text_update("Running both. "),
                    call_update("call_p1"),
                    call_update("call_p2"),
                    done_update("call_p1", "unit: 12 passed"),
                    done_update("call_p2", "e2e: 3 passed"),
                ],
            },
        ],
    })
    .to_string()
}

/// Apply one delta entity onto a reconstructed `messages[]`, exactly as the FE
/// reconciler does: find-or-create the message envelope, refresh authoritative
/// fields, then upsert the block BY ID (which is what makes a mispredicted id a
/// visible clobber rather than a harmless extra block).
fn apply_entity(messages: &mut Vec<Value>, entity: &Value) {
    let message_id = entity["messageId"].as_str().expect("messageId").to_string();
    let idx = messages
        .iter()
        .position(|m| m["id"].as_str() == Some(message_id.as_str()))
        .unwrap_or_else(|| {
            messages.push(json!({
                "id": message_id,
                "agentId": Value::Null,
                "seq": Value::Null,
                "role": Value::Null,
                "contentBlocks": [],
                "timestamp": Value::Null,
            }));
            messages.len() - 1
        });
    let msg = &mut messages[idx];
    for (from, to) in [
        ("agentId", "agentId"),
        ("role", "role"),
        ("messageSeq", "seq"),
        ("timestamp", "timestamp"),
    ] {
        if let Some(v) = entity.get(from) {
            msg[to] = v.clone();
        }
    }
    if entity.get("streamingComplete") == Some(&Value::Bool(true)) {
        if let Some(obj) = msg.as_object_mut() {
            obj.remove("isStreaming");
        }
    }
    let block = entity["block"].clone();
    let block_id = block["id"].as_str().expect("block id").to_string();
    let blocks = msg["contentBlocks"].as_array_mut().expect("contentBlocks");
    match blocks
        .iter()
        .position(|b| b["id"].as_str() == Some(block_id.as_str()))
    {
        Some(bi) => blocks[bi] = block,
        None => blocks.push(block),
    }
}

/// Reduce one `{ added, updated, removedIds }` delta onto `messages`.
fn apply_delta(messages: &mut Vec<Value>, delta: &Value) {
    for key in ["added", "updated"] {
        for entity in delta[key].as_array().into_iter().flatten() {
            apply_entity(messages, entity);
        }
    }
    for removed in delta["removedIds"].as_array().into_iter().flatten() {
        let Some(id) = removed.as_str() else { continue };
        for msg in messages.iter_mut() {
            if let Some(blocks) = msg["contentBlocks"].as_array_mut() {
                blocks.retain(|b| b["id"].as_str() != Some(id));
            }
        }
    }
}

/// Whether a delta is the turn's terminal (`stream:end`) reconcile frame. The
/// role guard matters: a persisted NON-assistant row (the `agent.sendMessage`
/// user row that opens the turn) is re-read and delivered as an authoritative
/// `streamingComplete` entity too, and must not be mistaken for the reconcile.
fn is_terminal_delta(delta: &Value) -> bool {
    ["added", "updated"].iter().any(|key| {
        delta[*key].as_array().into_iter().flatten().any(|e| {
            e.get("streamingComplete") == Some(&Value::Bool(true))
                && e.get("role") == Some(&json!("assistant"))
        })
    })
}

/// What one observed turn produced on the chat channel plus what it persisted.
struct Turn {
    /// Every entity carried by a delta BEFORE the terminal reconcile — the
    /// frames a live client renders and the only ones that can clobber.
    live: Vec<Value>,
    /// Snapshot + every delta, reduced the way the FE reconciler reduces them.
    reconstructed: Vec<Value>,
    /// A fresh `agent.getConversation` after the turn.
    durable: Vec<Value>,
}

impl Turn {
    /// The persisted assistant blocks of the turn (its durable layout).
    fn durable_blocks(&self) -> Vec<Value> {
        self.durable
            .iter()
            .filter(|m| m["role"] == json!("assistant"))
            .flat_map(|m| m["contentBlocks"].as_array().cloned().unwrap_or_default())
            .collect()
    }
}

/// Drive one shape's turn end-to-end over WSS: create the agent, subscribe to
/// its chat channel BEFORE prompting, send the marker prompt, then reduce every
/// delta up to and including the terminal reconcile.
async fn drive_turn(
    port: u16,
    cfg: Arc<ClientConfig>,
    ws_id: &str,
    name: &str,
    marker: &str,
) -> Turn {
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": name, "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // CHAT conn — subscribe before the turn so no delta is missed.
    let mut chat = connect_ws(port, cfg.clone()).await;
    let sub = wss_rpc(
        &mut chat,
        20,
        "chat.subscribe",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert!(
        sub["subscriptionId"].is_string(),
        "chat.subscribe result carries a subscriptionId: {sub}"
    );
    let snap = wss_push_until(
        &mut chat,
        tokio::time::Instant::now() + Duration::from_secs(30),
    )
    .await;
    assert_eq!(snap["params"]["kind"], "snapshot", "push: {snap}");
    assert_eq!(snap["params"]["seq"], 0, "push: {snap}");
    let mut reconstructed: Vec<Value> = snap["params"]["snapshot"]["messages"]
        .as_array()
        .cloned()
        .expect("snapshot messages");

    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "content": format!("{marker}: run the tests"),
        }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // One hard deadline across the whole turn (per-frame windows would reset on
    // heartbeats and hide a missing terminal reconcile).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut live: Vec<Value> = Vec::new();
    loop {
        let frame = wss_push_until(&mut chat, deadline).await;
        assert_eq!(frame["params"]["kind"], "delta", "push: {frame}");
        let delta = frame["params"]["delta"].clone();
        let terminal = is_terminal_delta(&delta);
        if !terminal {
            for key in ["added", "updated"] {
                for entity in delta[key].as_array().into_iter().flatten() {
                    live.push(entity.clone());
                }
            }
        }
        apply_delta(&mut reconstructed, &delta);
        if terminal {
            break;
        }
    }

    let convo = wss_rpc(
        &mut rpc,
        12,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    Turn {
        live,
        reconstructed,
        durable: convo["messages"]
            .as_array()
            .cloned()
            .expect("conversation messages"),
    }
}

/// The id the durable transcript gave `tool_call_id`'s `tool_result`.
fn durable_result_id(blocks: &[Value], tool_call_id: &str) -> String {
    blocks
        .iter()
        .find(|b| b["type"] == json!("tool_result") && b["tool_use_id"] == json!(tool_call_id))
        .unwrap_or_else(|| panic!("{tool_call_id} has a persisted tool_result: {blocks:#?}"))["id"]
        .as_str()
        .expect("block id")
        .to_string()
}

/// The id the durable transcript gave `tool_call_id`'s `tool_use`.
fn durable_use_id(blocks: &[Value], tool_call_id: &str) -> String {
    blocks
        .iter()
        .find(|b| b["type"] == json!("tool_use") && b["toolCallId"] == json!(tool_call_id))
        .unwrap_or_else(|| panic!("{tool_call_id} has a persisted tool_use: {blocks:#?}"))["id"]
        .as_str()
        .expect("block id")
        .to_string()
}

/// The core invariant: no LIVE block may claim an id the durable transcript
/// owns with a different type. A mispredicted `tool_result` id lands on the
/// interleaved text block (shape a) or the sibling call's `tool_use` (shape b),
/// and every id-keyed client replaces it for the rest of the turn.
fn assert_live_ids_never_retype_durable_blocks(turn: &Turn) {
    let durable: HashMap<String, String> = turn
        .durable_blocks()
        .iter()
        .filter_map(|b| {
            Some((
                b["id"].as_str()?.to_string(),
                b["type"].as_str()?.to_string(),
            ))
        })
        .collect();
    for entity in &turn.live {
        let block = &entity["block"];
        let (Some(id), Some(ty)) = (block["id"].as_str(), block["type"].as_str()) else {
            continue;
        };
        // Ids the turn never persisted are genuine live-only blocks; they
        // self-heal at the terminal reconcile via `removedIds`.
        let Some(durable_ty) = durable.get(id) else {
            continue;
        };
        assert_eq!(
            ty, durable_ty,
            "live block {id} arrived as {ty} but the durable transcript owns it as \
             {durable_ty} — the id-keyed client renders the wrong block until the \
             terminal reconcile (monorepo#2029): {entity}"
        );
    }
}

/// The live delta that delivered `tool_call_id`'s `tool_result`.
fn live_result_block(turn: &Turn, tool_call_id: &str) -> Value {
    turn.live
        .iter()
        .map(|e| e["block"].clone())
        .find(|b| b["type"] == json!("tool_result") && b["tool_use_id"] == json!(tool_call_id))
        .unwrap_or_else(|| {
            panic!("{tool_call_id}'s tool_result reaches the client LIVE, not only at reconcile")
        })
}

/// Shape (a) — text interleaves between a call and its completion. The
/// completing tool's `tool_result` must arrive live under the id the durable
/// transcript assigned it (the index AFTER the flushed interleaved text), and
/// the interleaved text block must never be retyped live.
#[tokio::test]
async fn interleaved_text_tool_result_keeps_its_durable_block_id_over_wss() {
    let Some(script) = gate("WSS chat tool_result block-id E2E (interleaved)") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace(&data_dir).await;
    let behavior = behavior();
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
    let cfg = client_config(
        status["result"]["fingerprint"]
            .as_str()
            .expect("fingerprint"),
    );

    let turn = drive_turn(port, cfg, &ws_id, "Interleaved", INTERLEAVED).await;
    let blocks = turn.durable_blocks();

    // The turn really did reproduce the shape: the interleaved text sits
    // between the call and its result, so `tool_use` index + 1 is a text block.
    let use_id = durable_use_id(&blocks, "call_a");
    let result_id = durable_result_id(&blocks, "call_a");
    let types: Vec<&str> = blocks.iter().filter_map(|b| b["type"].as_str()).collect();
    let use_pos = blocks
        .iter()
        .position(|b| b["id"] == json!(use_id))
        .unwrap();
    assert_eq!(
        blocks.get(use_pos + 1).and_then(|b| b["type"].as_str()),
        Some("text"),
        "the interleaved text owns the index the old mapper predicted: {types:?}"
    );
    assert_ne!(
        result_id,
        blocks[use_pos + 1]["id"].as_str().unwrap(),
        "the real result id is NOT tool_use + 1"
    );

    assert_live_ids_never_retype_durable_blocks(&turn);
    assert_eq!(
        live_result_block(&turn, "call_a")["id"],
        json!(result_id),
        "the live tool_result carries the durable id {result_id}"
    );

    // And the whole stream still reconciles: snapshot + deltas (honoring
    // removedIds) equals a fresh conversation read.
    assert_eq!(
        Value::Array(turn.reconstructed.clone()),
        Value::Array(turn.durable.clone()),
        "snapshot + deltas reconcile to the fresh conversation snapshot"
    );
}

/// Shape (b) — parallel calls: both `tool_use` blocks land before either
/// result, so `tool_use + 1` named the SIBLING call's tool row. Each result
/// must arrive live under its own durable id. Shares the interleaved shape's
/// fixture and mock behaviour — the two differ only by the prompt marker that
/// selects the mock's `rules` entry.
#[tokio::test]
async fn parallel_tool_results_keep_their_durable_block_ids_over_wss() {
    let Some(script) = gate("WSS chat tool_result block-id E2E (parallel)") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace(&data_dir).await;
    let behavior = behavior();
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
    let cfg = client_config(
        status["result"]["fingerprint"]
            .as_str()
            .expect("fingerprint"),
    );

    let turn = drive_turn(port, cfg, &ws_id, "Parallel", PARALLEL).await;
    let blocks = turn.durable_blocks();

    // The shape: t1's `tool_use` is immediately followed by t2's, so the old
    // prediction for t1's result was t2's tool row.
    let t1_use = durable_use_id(&blocks, "call_p1");
    let t2_use = durable_use_id(&blocks, "call_p2");
    let t1_pos = blocks
        .iter()
        .position(|b| b["id"] == json!(t1_use))
        .unwrap();
    assert_eq!(
        blocks.get(t1_pos + 1).map(|b| b["id"].clone()),
        Some(json!(t2_use)),
        "the second call's tool_use owns the index predicted for the first result"
    );

    assert_live_ids_never_retype_durable_blocks(&turn);
    for tool_call_id in ["call_p1", "call_p2"] {
        let want = durable_result_id(&blocks, tool_call_id);
        assert_eq!(
            live_result_block(&turn, tool_call_id)["id"],
            json!(want),
            "{tool_call_id}'s live tool_result carries its own durable id {want}"
        );
    }

    assert_eq!(
        Value::Array(turn.reconstructed.clone()),
        Value::Array(turn.durable.clone()),
        "snapshot + deltas reconcile to the fresh conversation snapshot"
    );
}
