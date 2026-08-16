//! WSS end-to-end for the widened `agent.wakeOrCreate` (C1d-10a).
//!
//! Boots a real `intentd serve` (WSS listener enabled via config), seeds a workspace + task note,
//! and drives the widened composite over a pinned TLS WebSocket. The mock
//! agent is NOT needed for the widened-contract tests — `agent.wakeOrCreate`
//! persists the delivered context message directly (matching the pre-widening
//! path), so those tests are hermetic and skip cleanly when `node` is missing
//! (they don't gate on it). The monorepo#847 migration test DOES drive real
//! turns through the mock ACP fixture and gates on `node` like the other
//! mock-backed e2e suites.
//!
//! Coverage:
//! - Backward compat: pre-widening 3-required-params call still succeeds.
//! - Create branch: rich `create.*` payload lands on the persisted session +
//!   metadata blob; response carries widened `action`/`agentName`/`taskTitle`.
//! - Wake branch after previous create: most-recent-first pick and
//!   `cleanedUpAgentIds` byte-for-byte when an assignment goes stale.
//! - Depth-guard rejection: `delegationDepth == MAX_DELEGATION_DEPTH` yields a
//!   JSON-RPC `-32602` envelope (PROTOCOL §9).
//! - monorepo#847: a quarantined message parked on a poisoned session is
//!   migrated onto the replacement agent (id + content preserved) and the
//!   poisoned session is hard-deleted (`agent:deleted`, `agent.getSession`
//!   → `-32602`).
//! - Occupancy guard: second `agent.delegate` / new-id `task.assignAgent` on
//!   a task with a live assigned agent → `-32602` unless `force: true`.

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
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

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
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-woc-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, listen: &str, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    if listen != "uds" {
        common::enable_ws_api(data_dir);
    }
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_LEGACY_IMPORT_ROOTS", "")
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

/// TLS certificate verifier that pins the daemon's self-signed cert by
/// SHA-256 fingerprint (matches the WSS-1 harness).
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

async fn connect_ws(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

async fn wss_rpc_envelope<S>(
    ws: &mut WebSocketStream<S>,
    id: i64,
    method: &str,
    params: Value,
) -> Value
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
            .expect("wss rpc envelope timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["id"] == json!(id) {
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

async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let env = wss_rpc_envelope(ws, id, method, params.clone()).await;
    assert!(env.get("error").is_none(), "rpc errored: {env}");
    env["result"].clone()
}

fn workspace_seed(id: &intent_core::WorkspaceId) -> intent_core::Workspace {
    use intent_core::{now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus};
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "WSS-WOC".to_string(),
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
    }
}

/// Seed one workspace + one task note (via `mark_as_task`), returning
/// `(workspace_id, task_note_id)`. Runs BEFORE the daemon boots so the daemon
/// picks up the seeded rows on start.
async fn seed_workspace_and_task(data_dir: &Path, title: &str) -> (String, String) {
    use intent_core::{NoteCreate, WorkspaceApi, WorkspaceId};
    use intent_services::Services;
    use intent_store::Store;
    let db_path = data_dir.join("intentd.db");
    let store = Store::open(&db_path).await.expect("open store");
    let ws_root = common::hermetic_workspaces_root();
    let services = Services::new(store.clone()).with_workspaces_root(ws_root.path().to_path_buf());
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace_seed(&ws))
        .await
        .expect("insert ws");
    let note = services
        .create_note(
            ws.clone(),
            NoteCreate {
                title: title.into(),
                content: Some(format!("# {title}\n")),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create note")
        .note;
    services
        .mark_as_task(
            ws.clone(),
            note.id.clone(),
            "not_started".into(),
            vec![],
            None,
            None,
            None,
            None,
        )
        .await
        .expect("markAsTask");
    (ws.0, note.id.0)
}

async fn boot_daemon_with_task(title: &str) -> (Daemon, String, String, u16, String) {
    boot_daemon_with_task_env(title, &[]).await
}

/// Like [`boot_daemon_with_task`] but with extra daemon env vars (e.g. the
/// mock ACP fixture wiring for tests that drive real turns).
async fn boot_daemon_with_task_env(
    title: &str,
    extra_env: &[(&str, &str)],
) -> (Daemon, String, String, u16, String) {
    let data_dir = temp_data_dir();
    let (ws_id, note_id) = seed_workspace_and_task(&data_dir, title).await;
    let mut env: Vec<(&str, &str)> = vec![("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
    env.extend_from_slice(extra_env);
    let child = spawn_serve(&data_dir, "both", &env);
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
    (daemon, ws_id, note_id, port, fingerprint)
}

/// Await the next `events.event` notification frame (any type).
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

/// Gate a mock-ACP-backed test on `node` + the fixture script (mirrors the
/// other mock-backed WSS e2e suites; skip cleanly when unavailable).
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

/// C1d-10a: exercise the four wire-contract slices of the widened
/// `agent.wakeOrCreate` over one pinned WSS connection, all on a single
/// daemon boot. Bundled to keep the (expensive) daemon spawn under one test.
#[tokio::test]
async fn wake_or_create_widened_wire_contract_over_wss() {
    let (_daemon, ws_id, task_note_id, port, fp) = boot_daemon_with_task("WOC Task").await;
    let cfg = client_config(&fp);
    let mut rpc = connect_ws(port, cfg.clone()).await;

    // (1) Backward-compat 3-required-params call: `taskNoteId` + `contextMessage`
    //     (+ optional `model`) is still accepted; the widened response carries
    //     `action: created_new`, `agentName`, and `taskTitle` for zero
    //     round-trip clients.
    let create_res = wss_rpc(
        &mut rpc,
        1,
        "agent.wakeOrCreate",
        json!({
            "workspaceId": ws_id,
            "taskNoteId": task_note_id,
            "contextMessage": "kickoff",
        }),
    )
    .await;
    assert_eq!(create_res["ok"], true);
    assert_eq!(create_res["created"], true);
    assert_eq!(create_res["action"], "created_new");
    assert_eq!(create_res["taskTitle"], "WOC Task");
    assert_eq!(create_res["agentName"], "Task: WOC Task");
    let agent_id = create_res["agentId"].as_str().expect("agentId").to_string();
    assert!(agent_id.starts_with("agent-"));
    assert!(create_res.get("cleanedUpAgentIds").is_none());

    // (2) Wake branch: a subsequent call finds the just-created agent as the
    //     newest resumable assignment. `created: false` and the response
    //     echoes the same agent id / task title so clients don't need a
    //     follow-up `agent.get`.
    //
    //     `action` is `woke_existing` when the assignee's in-flight slot has
    //     been released, or `message_queued_to_active_agent` when the earlier
    //     wake's runtime worker is still holding the slot (DELIV-1: the wake
    //     now routes through the `AgentManager` so a same-tick follow-up
    //     legitimately queues while the drain loop is running). Both action
    //     codes represent successful delivery.
    let wake_res = wss_rpc(
        &mut rpc,
        2,
        "agent.wakeOrCreate",
        json!({
            "workspaceId": ws_id,
            "taskNoteId": task_note_id,
            "contextMessage": "resume",
        }),
    )
    .await;
    assert_eq!(wake_res["ok"], true);
    assert_eq!(wake_res["created"], false);
    let wake_action = wake_res["action"].as_str().unwrap_or_default();
    assert!(
        wake_action == "woke_existing" || wake_action == "message_queued_to_active_agent",
        "wake action must be a live-assignee code (DELIV-1): {wake_res}"
    );
    assert_eq!(wake_res["agentId"], agent_id);
    assert_eq!(wake_res["taskTitle"], "WOC Task");

    // (3) Stale cleanup: delete the assigned agent so its assignment goes
    //     stale, then wake again. The response contains `cleanedUpAgentIds`
    //     with the stale id (byte-for-byte) and `action: created_new` for the
    //     replacement.
    wss_rpc(
        &mut rpc,
        3,
        "agent.delete",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let recreate_res = wss_rpc(
        &mut rpc,
        4,
        "agent.wakeOrCreate",
        json!({
            "workspaceId": ws_id,
            "taskNoteId": task_note_id,
            "contextMessage": "reboot",
            "create": {
                "metadata": { "custom": "field" },
                "skipAutoCommit": true,
            },
            "messageMetadata": { "type": "task_wake", "source": "wake" },
        }),
    )
    .await;
    assert_eq!(recreate_res["ok"], true);
    assert_eq!(recreate_res["created"], true);
    assert_eq!(recreate_res["action"], "created_new");
    assert_eq!(recreate_res["cleanedUpAgentIds"], json!([agent_id]));
    let new_agent_id = recreate_res["agentId"].as_str().unwrap().to_string();
    assert_ne!(new_agent_id, agent_id);

    // (4) Depth-guard rejection: `delegationDepth == MAX_DELEGATION_DEPTH`
    //     surfaces the JSON-RPC `-32602` error envelope (PROTOCOL §9).
    let err_env = wss_rpc_envelope(
        &mut rpc,
        5,
        "agent.wakeOrCreate",
        json!({
            "workspaceId": ws_id,
            "taskNoteId": task_note_id,
            "contextMessage": "too deep",
            "delegationDepth": 2,
        }),
    )
    .await;
    assert_eq!(err_env["jsonrpc"], "2.0");
    assert_eq!(err_env["id"], 5);
    assert!(err_env.get("result").is_none(), "must be error envelope");
    let err = &err_env["error"];
    assert_eq!(err["code"], -32602);
    let msg = err["message"].as_str().unwrap();
    assert!(
        msg.contains("MAX_DELEGATION_DEPTH"),
        "error message must reference the guard constant: {msg}"
    );

    // (5) The metadata payload the (3) create step supplied is persisted on
    //     the new session — read it back via `agent.get` and assert the
    //     folded provenance keys (source / isBackground / skipAutoCommit).
    let got = wss_rpc(
        &mut rpc,
        6,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": new_agent_id }),
    )
    .await;
    assert_eq!(got["agent"]["id"], new_agent_id);

    // (6) monorepo#1217: the `messageMetadata` from step (3) is persisted as
    //     ROW-LEVEL metadata on the delivered wake user message (not just
    //     folded onto the content block) — the FE attribution chip reads the
    //     row's `metadata` via `agent.getConversation` / `chat.subscribe`.
    let convo = wss_rpc(
        &mut rpc,
        7,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": new_agent_id }),
    )
    .await;
    let messages = convo["messages"].as_array().expect("messages array");
    let wake_row = messages
        .iter()
        .find(|m| {
            m["role"] == "user"
                && m["contentBlocks"][0]["text"]
                    .as_str()
                    .is_some_and(|t| t.contains("reboot"))
        })
        .unwrap_or_else(|| panic!("wake user row persisted: {convo}"));
    assert_eq!(
        wake_row["metadata"],
        json!({ "type": "task_wake", "source": "wake" }),
        "wake row carries row-level messageMetadata (monorepo#1217): {wake_row}"
    );
    assert_eq!(
        wake_row["contentBlocks"][0]["messageMetadata"]["type"], "task_wake",
        "in-block fold preserved alongside the row-level copy: {wake_row}"
    );
}

/// monorepo#926: the create branch auto-subscribes the caller. A
/// `agent.wakeOrCreate` with `callerAgentId` on a task with no live assignee
/// (`action: created_new`) must return `subscriptionId` + the notification
/// message line, and `agent.getSubscriptions` for the caller must list the
/// completion watch on the created agent immediately — SUB-1 parity with the
/// wake/queued branches. Hermetic (no ACP provider needed).
#[tokio::test]
async fn wake_or_create_created_new_subscribes_caller_over_wss() {
    let (_daemon, ws_id, task_note_id, port, fp) = boot_daemon_with_task("WOC 926 Task").await;
    let cfg = client_config(&fp);
    let mut rpc = connect_ws(port, cfg.clone()).await;

    // The waking caller (coordinator) lives in the same workspace.
    let created = wss_rpc(
        &mut rpc,
        1,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Coordinator" }),
    )
    .await;
    let caller_id = created["agent"]["id"]
        .as_str()
        .expect("caller id")
        .to_string();

    let res = wss_rpc(
        &mut rpc,
        2,
        "agent.wakeOrCreate",
        json!({
            "workspaceId": ws_id,
            "taskNoteId": task_note_id,
            "contextMessage": "kickoff",
            "callerAgentId": caller_id,
        }),
    )
    .await;
    assert_eq!(res["ok"], true);
    assert_eq!(res["created"], true);
    assert_eq!(res["action"], "created_new");
    let child_id = res["agentId"].as_str().expect("agentId").to_string();
    let sub_id = res["subscriptionId"]
        .as_str()
        .expect("created_new must carry subscriptionId (monorepo#926)")
        .to_string();
    let message = res["message"].as_str().expect("message");
    assert!(
        message.contains("You will be notified when the agent responds."),
        "notification text parity with the wake branches: {message}"
    );

    // The caller's watch is visible immediately via `agent.getSubscriptions`.
    let subs_res = wss_rpc(
        &mut rpc,
        3,
        "agent.getSubscriptions",
        json!({ "workspaceId": ws_id, "agentId": caller_id }),
    )
    .await;
    let subs = subs_res["subscriptions"]
        .as_array()
        .expect("subscriptions array");
    assert_eq!(subs.len(), 1, "one completion watch: {subs:?}");
    assert_eq!(subs[0]["id"], json!(sub_id));
    assert!(
        subs[0].get("oneShot").is_none(),
        "oneShot dropped from wire"
    );
    assert_eq!(subs[0]["actorIds"], json!([child_id]));
    assert_eq!(subs[0]["workspaceId"], json!(ws_id));
}

/// TASK-C2 (follow-up to #104): `agent.delegate` with `taskNoteId` APPENDS
/// the reference `DelegateTaskTool` "Your Task Note" preamble after the
/// delegated child's FIRST message with a `---` separator, so the child sees
/// the body first followed by the note ID/title and the single-task scope
/// contract byte-for-byte. Exercised over the real WSS wire (not just the
/// `intent-services` unit tests) per the repo's e2e requirement. Hermetic —
/// no ACP provider is spawned; the child's persisted `metadata.initialMessage`
/// carries the preamble bytes at delegate time.
#[tokio::test]
async fn delegate_with_task_note_id_appends_preamble_over_wss() {
    const TITLE: &str = "TASK-C preamble task";
    let (_daemon, ws_id, task_note_id, port, fp) = boot_daemon_with_task(TITLE).await;
    let cfg = client_config(&fp);
    let mut rpc = connect_ws(port, cfg.clone()).await;

    let delegated = wss_rpc(
        &mut rpc,
        1,
        "agent.delegate",
        json!({
            "workspaceId": ws_id,
            "taskNoteId": task_note_id,
            "agentInstructions": "do the delegated body",
            "model": "mock:default",
        }),
    )
    .await;
    assert_eq!(delegated["ok"], true, "delegate ok: {delegated}");
    let child_id = delegated["agentId"]
        .as_str()
        .expect("child agent id")
        .to_string();
    assert!(!child_id.is_empty(), "non-empty child agentId");

    // `agent.get` returns `metadata.initialMessage` — the resolved first
    // message the child sees. Byte-exact match against the reference
    // composition `${msg}\n\n---\n${preamble}${commitInstruction}` from
    // `agent-interaction-tools.ts`. `skipAutoCommit` unset => empty tail.
    let got = wss_rpc(
        &mut rpc,
        2,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": child_id }),
    )
    .await;
    let initial = got["agent"]["metadata"]["initialMessage"]
        .as_str()
        .expect("initialMessage string");
    let expected = format!(
        "do the delegated body\n\
\n\
---\n\
**Your Task Note:** \"{TITLE}\" (ID: {task_note_id})\n\
This note is your workspace for this task. Update it with your progress, findings, and deliverables.\n\
\n\
**SCOPE: Complete THIS task only.** When done, mark it complete and end your session. Do not pick up other tasks."
    );
    assert_eq!(
        initial, expected,
        "child first message must be byte-exact: {initial:?}"
    );
}

/// monorepo#1150: `agent.diagnostics` with a `taskNoteId` filter matches the
/// agents actually associated with the task (here the delegated assignee)
/// instead of returning an all-zero snapshot, over the real WSS wire. An
/// unrelated agent stays out of scope, and a nonexistent note id yields an
/// empty (not erroring) snapshot. Hermetic — no ACP provider is spawned.
#[tokio::test]
async fn diagnostics_task_note_filter_matches_delegated_agent_over_wss() {
    let (_daemon, ws_id, task_note_id, port, fp) =
        boot_daemon_with_task("Diagnostics filter task").await;
    let cfg = client_config(&fp);
    let mut rpc = connect_ws(port, cfg.clone()).await;

    let delegated = wss_rpc(
        &mut rpc,
        1,
        "agent.delegate",
        json!({
            "workspaceId": ws_id,
            "taskNoteId": task_note_id,
            "agentInstructions": "do the delegated body",
            "model": "mock:default",
        }),
    )
    .await;
    assert_eq!(delegated["ok"], true, "delegate ok: {delegated}");
    let child_id = delegated["agentId"]
        .as_str()
        .expect("child agent id")
        .to_string();

    // An unrelated agent that the filter must exclude.
    let created = wss_rpc(
        &mut rpc,
        2,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Unrelated" }),
    )
    .await;
    let unrelated_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    let diag = wss_rpc(
        &mut rpc,
        3,
        "agent.diagnostics",
        json!({ "workspaceId": ws_id, "taskNoteId": task_note_id }),
    )
    .await;
    let d = &diag["diagnostics"];
    assert_eq!(d["filters"]["taskNoteId"], json!(task_note_id));
    assert!(
        d["summary"]["agents"].as_u64().expect("agents count") >= 1,
        "task filter must match the delegated agent: {d}"
    );
    let agents = d["agents"].as_array().expect("agents array");
    assert!(
        agents.iter().any(|r| r["id"] == json!(child_id)),
        "delegated agent row present: {d}"
    );
    assert!(
        !agents.iter().any(|r| r["id"] == json!(unrelated_id)),
        "unrelated agent filtered out: {d}"
    );

    // Nonexistent note id: empty snapshot, not an error.
    let empty = wss_rpc(
        &mut rpc,
        4,
        "agent.diagnostics",
        json!({ "workspaceId": ws_id, "taskNoteId": "note-does-not-exist" }),
    )
    .await;
    assert_eq!(
        empty["diagnostics"]["summary"]["agents"],
        json!(0),
        "unknown note id yields an empty snapshot: {empty}"
    );
}

/// Status-neutral commit policy: `agent.delegate` with `taskNoteId` +
/// `skipAutoCommit=true` delivers a first message that ends at the scope
/// directive with NO state-specific auto-commit instruction, byte-for-byte,
/// over the real WSS wire — the opt-out only gates the idle subscriber.
#[tokio::test]
async fn delegate_with_skip_auto_commit_stays_status_neutral_over_wss() {
    const TITLE: &str = "TASK-C skipAutoCommit task";
    let (_daemon, ws_id, task_note_id, port, fp) = boot_daemon_with_task(TITLE).await;
    let cfg = client_config(&fp);
    let mut rpc = connect_ws(port, cfg.clone()).await;

    let delegated = wss_rpc(
        &mut rpc,
        1,
        "agent.delegate",
        json!({
            "workspaceId": ws_id,
            "taskNoteId": task_note_id,
            "agentInstructions": "do the delegated body",
            "skipAutoCommit": true,
            "model": "mock:default",
        }),
    )
    .await;
    assert_eq!(delegated["ok"], true, "delegate ok: {delegated}");
    let child_id = delegated["agentId"]
        .as_str()
        .expect("child agent id")
        .to_string();

    let got = wss_rpc(
        &mut rpc,
        2,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": child_id }),
    )
    .await;
    let initial = got["agent"]["metadata"]["initialMessage"]
        .as_str()
        .expect("initialMessage string");
    let expected = format!(
        "do the delegated body\n\
\n\
---\n\
**Your Task Note:** \"{TITLE}\" (ID: {task_note_id})\n\
This note is your workspace for this task. Update it with your progress, findings, and deliverables.\n\
\n\
**SCOPE: Complete THIS task only.** When done, mark it complete and end your session. Do not pick up other tasks."
    );
    assert_eq!(
        initial, expected,
        "child first message must be byte-exact and status-neutral when skipAutoCommit=true: {initial:?}"
    );
    assert!(
        !initial.contains("Auto-commit is"),
        "no state-specific auto-commit text over the wire: {initial:?}"
    );
}

/// Occupancy guard over the wire: a task note with a live assigned agent
/// rejects a second `agent.delegate` / a new-id `task.assignAgent` with a
/// JSON-RPC `-32602` envelope (PROTOCOL §9) naming the existing agent, and
/// `force: true` deliberately overrides on both methods. Same-id re-assign
/// stays idempotent-ok. Hermetic — no ACP provider is spawned; the guard
/// fires before any turn starts.
#[tokio::test]
async fn occupancy_guard_delegate_and_assign_agent_over_wss() {
    let (_daemon, ws_id, task_note_id, port, fp) = boot_daemon_with_task("Occupied Task").await;
    let cfg = client_config(&fp);
    let mut rpc = connect_ws(port, cfg.clone()).await;

    // (1) First delegate on the unoccupied task succeeds without `force`.
    let first = wss_rpc(
        &mut rpc,
        1,
        "agent.delegate",
        json!({
            "workspaceId": ws_id,
            "taskNoteId": task_note_id,
            "agentInstructions": "start the work",
            "model": "mock:default",
        }),
    )
    .await;
    assert_eq!(first["ok"], true, "first delegate ok: {first}");
    let first_id = first["agentId"].as_str().expect("agentId").to_string();

    // (2) Second delegate without `force` → `-32602` error envelope naming
    //     the occupant and pointing at the override / reach-existing paths.
    let err_env = wss_rpc_envelope(
        &mut rpc,
        2,
        "agent.delegate",
        json!({
            "workspaceId": ws_id,
            "taskNoteId": task_note_id,
            "agentInstructions": "double delegate",
            "model": "mock:default",
        }),
    )
    .await;
    assert_eq!(err_env["jsonrpc"], "2.0");
    assert_eq!(err_env["id"], 2);
    assert!(err_env.get("result").is_none(), "must be error envelope");
    assert_eq!(err_env["error"]["code"], -32602);
    let msg = err_env["error"]["message"].as_str().expect("message");
    assert!(msg.contains(&first_id), "error names the occupant: {msg}");
    assert!(
        msg.contains("already being worked"),
        "error states occupancy: {msg}"
    );
    assert!(
        msg.contains("force: true"),
        "error mentions override: {msg}"
    );

    // (3) Second delegate WITH `force: true` succeeds and adds a second agent.
    let forced = wss_rpc(
        &mut rpc,
        3,
        "agent.delegate",
        json!({
            "workspaceId": ws_id,
            "taskNoteId": task_note_id,
            "agentInstructions": "intentional second agent",
            "model": "mock:default",
            "force": true,
        }),
    )
    .await;
    assert_eq!(forced["ok"], true, "forced delegate ok: {forced}");
    assert_ne!(forced["agentId"].as_str().unwrap(), first_id);

    // (4) `task.assignAgent` of a NEW agent to the occupied task → same
    //     `-32602` guard without `force`.
    let created = wss_rpc(
        &mut rpc,
        4,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Third" }),
    )
    .await;
    let third_id = created["agent"]["id"]
        .as_str()
        .expect("third id")
        .to_string();
    let err_env = wss_rpc_envelope(
        &mut rpc,
        5,
        "task.assignAgent",
        json!({ "workspaceId": ws_id, "noteId": task_note_id, "agentId": third_id }),
    )
    .await;
    assert!(err_env.get("result").is_none(), "must be error envelope");
    assert_eq!(err_env["error"]["code"], -32602);
    let msg = err_env["error"]["message"].as_str().expect("message");
    assert!(
        msg.contains("force: true"),
        "error mentions override: {msg}"
    );

    // (5) Same call with `force: true` succeeds.
    let assigned = wss_rpc(
        &mut rpc,
        6,
        "task.assignAgent",
        json!({
            "workspaceId": ws_id,
            "noteId": task_note_id,
            "agentId": third_id,
            "force": true,
        }),
    )
    .await;
    assert_eq!(assigned["ok"], true, "forced assign ok: {assigned}");

    // (6) Re-assigning an already-assigned id stays idempotent-ok, no force.
    let reassigned = wss_rpc(
        &mut rpc,
        7,
        "task.assignAgent",
        json!({ "workspaceId": ws_id, "noteId": task_note_id, "agentId": third_id }),
    )
    .await;
    assert_eq!(reassigned["ok"], true, "idempotent re-assign: {reassigned}");
}

/// monorepo#847: messages parked on a poisoned session survive an
/// `agent.wakeOrCreate` replacement — migrated onto the fresh agent's queue
/// (id + content preserved, relative order kept) — and the poisoned session
/// is GC'd (hard delete: `agent:deleted` on the wire, `agent.getSession` →
/// `-32602 "Agent not found"` per PROTOCOL §9).
///
/// Drives real turns through the mock ACP fixture: every `session/prompt`
/// fails with the canonical provider safety block, so the first wake's agent
/// poisons itself (Error + session-fatal stopReason, kickoff requeued) and
/// the replacement agent ALSO parks in Error after migration — its queue is
/// deterministically un-drained when we read it back (a session-fatal turn
/// never drains the queue).
#[tokio::test]
async fn parked_messages_survive_wake_or_create_replacement() {
    let Some(script) = gate("WSS wakeOrCreate queue-migration E2E") else {
        return;
    };
    let behavior = json!({
        "promptRpcError": {
            "code": -32603,
            "message": "The model provider blocked this response for safety reasons. \
                        Please start a new session",
        },
    })
    .to_string();
    let (_daemon, ws_id, task_note_id, port, fp) = boot_daemon_with_task_env(
        "WOC Migration Task",
        &[
            ("MOCK_AGENT_SCRIPT_PATH", &script),
            ("MOCK_AGENT_BEHAVIOR", &behavior),
        ],
    )
    .await;
    let cfg = client_config(&fp);

    // SUBSCRIBER conn — subscribe BEFORE any turn so no event is missed.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    let sub_id = sub_resp["subscriptionId"]
        .as_str()
        .expect("subscriptionId")
        .to_string();

    // (1) wakeOrCreate #1 — creates agent A assigned to the task; its wake
    //     turn ("kickoff") fails with the session-fatal block, poisoning A
    //     and requeueing the kickoff at the front of A's queue.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let res1 = wss_rpc(
        &mut rpc,
        10,
        "agent.wakeOrCreate",
        json!({
            "workspaceId": ws_id,
            "taskNoteId": task_note_id,
            "contextMessage": "kickoff",
            "model": "mock:default",
        }),
    )
    .await;
    assert_eq!(res1["ok"], true, "wakeOrCreate #1: {res1}");
    assert_eq!(res1["created"], true);
    assert_eq!(res1["action"], "created_new");
    let poisoned_id = res1["agentId"].as_str().expect("agentId").to_string();

    // Wait for A's terminal failure to persist (status-changed(error) is
    // emitted AFTER the persist), then confirm the session-fatal stopReason.
    let mut saw_status_error = false;
    for _ in 0..200 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() == Some(poisoned_id.as_str())
            && event["type"] == "agent:status-changed"
            && event["data"]["status"] == "error"
        {
            saw_status_error = true;
            break;
        }
    }
    assert!(saw_status_error, "agent A parked in error after the block");
    let session = wss_rpc(
        &mut rpc,
        11,
        "agent.getSession",
        json!({ "workspaceId": ws_id, "agentId": poisoned_id }),
    )
    .await;
    assert_eq!(session["session"]["status"], "error");
    let stop_reason = session["session"]["stopReason"]
        .as_str()
        .expect("stopReason present");
    assert!(
        stop_reason.contains("blocked") && stop_reason.contains("for safety reasons"),
        "session-fatal stopReason, got: {stop_reason}"
    );

    // (2) Quarantined send (monorepo#840 gate): the result carries the FULL
    //     quarantine envelope — `queued: true` AND `quarantined: true` — and
    //     the message parks behind the requeued kickoff.
    let followup = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": poisoned_id, "content": "follow-up" }),
    )
    .await;
    assert_eq!(followup["success"], true, "quarantined send: {followup}");
    assert_eq!(followup["queued"], true, "message parked: {followup}");
    assert_eq!(followup["quarantined"], true, "quarantine flag: {followup}");
    let parked_id = followup["queuedMessage"]["id"]
        .as_str()
        .expect("queuedMessage.id")
        .to_string();
    assert_eq!(followup["queuedMessage"]["content"], "follow-up");

    // Snapshot A's parked queue: requeued kickoff + quarantined follow-up.
    let queue_a = wss_rpc(
        &mut rpc,
        13,
        "agent.getQueue",
        json!({ "workspaceId": ws_id, "agentId": poisoned_id }),
    )
    .await;
    let parked = queue_a["queue"].as_array().expect("queue array");
    assert_eq!(parked.len(), 2, "kickoff requeue + follow-up: {queue_a}");
    assert_eq!(parked[0]["content"], "kickoff");
    assert_eq!(parked[1]["content"], "follow-up");
    let kickoff_id = parked[0]["id"].as_str().expect("kickoff id").to_string();
    assert_eq!(parked[1]["id"], json!(parked_id));

    // (3) wakeOrCreate #2 — A is poisoned, so a fresh agent B is created,
    //     A's parked queue is migrated onto B, and A is GC'd. The response
    //     carries `action: created_new` and `cleanedUpAgentIds: [A]`
    //     byte-for-byte.
    let res2 = wss_rpc(
        &mut rpc,
        14,
        "agent.wakeOrCreate",
        json!({
            "workspaceId": ws_id,
            "taskNoteId": task_note_id,
            "contextMessage": "replacement kickoff",
            "model": "mock:default",
        }),
    )
    .await;
    assert_eq!(res2["ok"], true, "wakeOrCreate #2: {res2}");
    assert_eq!(res2["created"], true);
    assert_eq!(res2["action"], "created_new");
    assert_eq!(res2["cleanedUpAgentIds"], json!([poisoned_id]));
    let new_agent_id = res2["agentId"].as_str().expect("agentId").to_string();
    assert_ne!(new_agent_id, poisoned_id);

    // (4) The parked messages MIGRATED: B's queue holds both entries with id
    //     + content preserved and relative order kept. (B's own wake turn
    //     also fails session-fatal — a fatal turn never drains the queue, so
    //     the migrated entries are deterministically still parked; the queue
    //     may additionally hold B's own requeued "replacement kickoff".)
    let queue_b = wss_rpc(
        &mut rpc,
        15,
        "agent.getQueue",
        json!({ "workspaceId": ws_id, "agentId": new_agent_id }),
    )
    .await;
    let migrated = queue_b["queue"].as_array().expect("queue array");
    let pos_of = |id: &str| migrated.iter().position(|m| m["id"] == json!(id));
    let kickoff_pos = pos_of(&kickoff_id)
        .unwrap_or_else(|| panic!("migrated kickoff {kickoff_id} on B's queue: {queue_b}"));
    let followup_pos = pos_of(&parked_id)
        .unwrap_or_else(|| panic!("migrated follow-up {parked_id} on B's queue: {queue_b}"));
    assert_eq!(migrated[kickoff_pos]["content"], "kickoff");
    assert_eq!(migrated[followup_pos]["content"], "follow-up");
    assert!(
        kickoff_pos < followup_pos,
        "relative order preserved: {queue_b}"
    );

    // (5) The poisoned session is HARD-deleted: `agent.getSession` yields the
    //     `-32602` error envelope (PROTOCOL §9 / router "Agent not found").
    let err_env = wss_rpc_envelope(
        &mut rpc,
        16,
        "agent.getSession",
        json!({ "workspaceId": ws_id, "agentId": poisoned_id }),
    )
    .await;
    assert_eq!(err_env["jsonrpc"], "2.0");
    assert_eq!(err_env["id"], 16);
    assert!(err_env.get("result").is_none(), "must be error envelope");
    assert_eq!(err_env["error"]["code"], -32602);
    assert_eq!(err_env["error"]["message"], "Agent not found");

    // (6) The wire events (§6.3): `agent:queue:updated` for B carrying the
    //     migrated entries, and `agent:deleted` for A. Both were published
    //     inside wakeOrCreate #2, so they are already buffered on the
    //     subscription. Assert the full notification envelope on each.
    let mut saw_migrated_queue = false;
    let mut saw_deleted = false;
    for _ in 0..200 {
        if saw_migrated_queue && saw_deleted {
            break;
        }
        let frame = wss_event(&mut sub, 30).await;
        assert_eq!(frame["jsonrpc"], "2.0");
        assert!(
            frame.get("id").is_none(),
            "events.event is a notification (no id): {frame}"
        );
        assert_eq!(frame["params"]["subscriptionId"], json!(sub_id));
        let event = &frame["params"]["event"];
        for key in ["type", "workspaceId", "id", "timestamp", "actor", "data"] {
            assert!(!event[key].is_null(), "event.{key} present: {frame}");
        }
        assert_eq!(event["workspaceId"], json!(ws_id));
        if event["type"] == "agent:queue:updated"
            && event["data"]["agentId"] == json!(new_agent_id)
            && event["data"]["queue"]
                .as_array()
                .is_some_and(|q| q.iter().any(|m| m["id"] == json!(parked_id)))
        {
            saw_migrated_queue = true;
        }
        if event["type"] == "agent:deleted" && event["data"]["agentId"] == json!(poisoned_id) {
            saw_deleted = true;
        }
    }
    assert!(
        saw_migrated_queue,
        "agent:queue:updated for B carries the migrated follow-up"
    );
    assert!(saw_deleted, "agent:deleted observed for the poisoned agent");
}
