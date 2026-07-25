//! WSS end-to-end change-event emissions for the workspace-lifecycle
//! mutations (FIX 2 parity): drives a real pinned-TLS WebSocket against a
//! live `intentd serve` (WSS listener enabled via config) and asserts that `workspace.update`
//! and `workspace.delete` publish `workspace:updated` / `workspace:deleted`
//! (PROTOCOL.md §6.5) so a subscribed client sees the mutation without a
//! follow-up read. The `git:commit` emission is exercised over UDS in
//! `uds_events.rs` and via unit tests where a git worktree is cheaper to
//! materialise; this file focuses on the pure-daemon lifecycle where no
//! git binary is required.

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
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

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

fn scratch_dir(prefix: &str) -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-events-{prefix}-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir scratch dir");
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

async fn uds_rpc(socket: &Path, id: i64, method: &str, params: Value) -> Value {
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
    let v = wss_rpc_envelope(ws, id, method, params).await;
    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
    v["result"].clone()
}

/// Like [`wss_rpc`] but returns the full response envelope so tests can
/// assert `error.code` / `error.message` for expected-failure paths.
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
    ws.send(Message::Text(frame.to_string()))
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

/// Wait up to `secs` for the next `events.event` notification whose payload
/// `type` matches one of `types`; ignore other frames. Returns the event
/// object (the `params.event` sub-object).
async fn next_event<S>(ws: &mut WebSocketStream<S>, types: &[&str], secs: u64) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!remaining.is_zero(), "timed out waiting for {types:?}");
        let next = timeout(remaining, ws.next())
            .await
            .expect("timeout elapsed");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = match serde_json::from_str(&text) {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                if v["method"] == json!("events.event") {
                    let evt = &v["params"]["event"];
                    let ty = evt["type"].as_str().unwrap_or("");
                    if types.contains(&ty) {
                        return evt.clone();
                    }
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

async fn boot() -> (Daemon, u16, Arc<ClientConfig>) {
    let data_dir = scratch_dir("data");
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
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
    (daemon, port, client_config(&fingerprint))
}

/// End-to-end: `workspace.update` over WSS publishes `workspace:updated` with
/// the applied `WorkspaceUpdate` delta as `changes` (§6.5). A previously
/// subscribed client sees the event without a follow-up `workspace.get`
/// round-trip.
#[tokio::test]
async fn workspace_update_emits_workspace_updated_over_wss() {
    let (daemon, port, cfg) = boot().await;
    let _ = &daemon;

    // Bootstrap a workspace off the UDS to avoid noise on the WSS event stream.
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "Original", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // Subscribe over WSS before mutating so the emission is guaranteed to be
    // observed (subscribers created after publish miss the event).
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:updated"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // Drive workspace.update over a separate WSS RPC connection.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    wss_rpc(
        &mut rpc,
        2,
        "workspace.update",
        json!({ "workspaceId": ws_id, "title": "Renamed", "tags": ["a", "b"] }),
    )
    .await;

    let evt = next_event(&mut sub, &["workspace:updated"], 10).await;
    assert_eq!(evt["workspaceId"], ws_id.as_str());
    assert!(evt["id"].is_string(), "event id: {evt}");
    assert!(evt["timestamp"].is_string(), "timestamp: {evt}");
    assert_eq!(
        evt["actor"],
        json!({ "type": "system", "id": "system", "name": "System" })
    );
    // `changes` is the applied delta only; `skip_serializing_if = "Option::is_none"`
    // keeps un-supplied fields out of the payload (reference-parity emitter).
    assert_eq!(
        evt["data"],
        json!({
            "workspaceId": ws_id,
            "changes": { "title": "Renamed", "tags": ["a", "b"] },
        })
    );
}

/// End-to-end: `workspace.delete` over WSS publishes `workspace:deleted` with
/// the minimal `{ workspaceId }` payload (§6.5). The event fires only after
/// the store row is actually removed.
#[tokio::test]
async fn workspace_delete_emits_workspace_deleted_over_wss() {
    let (daemon, port, cfg) = boot().await;

    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "ToDelete", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:deleted"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    let mut rpc = connect_ws(port, cfg.clone()).await;
    wss_rpc(
        &mut rpc,
        2,
        "workspace.delete",
        json!({ "workspaceId": ws_id }),
    )
    .await;

    let evt = next_event(&mut sub, &["workspace:deleted"], 10).await;
    assert_eq!(evt["workspaceId"], ws_id.as_str());
    assert_eq!(evt["data"], json!({ "workspaceId": ws_id }));
}

/// End-to-end TASKFLOW-1 flow over WSS: authoring a note whose content holds
/// an `@@@task` block auto-converts the fence into a linked child task note
/// (fence-free parent + `note:created` for the child + `note:updated` for the
/// rewritten parent), `note.listTasks` surfaces the linked task, and
/// `task.assignAgent` flips the `not_started` task to `in_progress` with a
/// `note:updated` (assignment write) + `task:status-changed` +
/// `task:ready-tasks-changed` fan-out (PROTOCOL.md §6.5) — author → list →
/// delegate → in-progress, driven end-to-end over WSS with subscribers seeing
/// every emission live and one confirming `task.get` at the end.
#[tokio::test]
async fn task_block_author_list_assign_flow_over_wss() {
    let (daemon, port, cfg) = boot().await;

    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "Taskflow", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // Subscribe before authoring so all conversion emissions are observed.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({
            "eventTypes": [
                "note:created",
                "note:updated",
                "task:status-changed",
                "task:ready-tasks-changed",
            ],
            "workspaceId": ws_id,
        }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // Author: `note.create` with an `@@@task` fence auto-converts on write.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        2,
        "note.create",
        json!({
            "workspaceId": ws_id,
            "title": "Plan",
            "content": "intro\n@@@task\n# Ship It\nbody\n@@@\ntail",
        }),
    )
    .await;
    let parent_id = created["note"]["id"].as_str().expect("note id").to_string();
    let content = created["note"]["content"].as_str().expect("content");
    assert!(!content.contains("@@@task"), "fence not removed: {content}");
    assert!(
        content.contains("- [ ] [Ship It](intent://local/task/"),
        "no task link: {content}"
    );

    // List: the converted block is a linked task row.
    let tasks = wss_rpc(
        &mut rpc,
        3,
        "note.listTasks",
        json!({ "workspaceId": ws_id, "noteId": parent_id }),
    )
    .await;
    let rows = tasks.as_array().expect("bare array");
    assert_eq!(rows.len(), 1, "rows: {tasks}");
    assert_eq!(rows[0]["text"], json!("Ship It"));
    assert_eq!(rows[0]["status"], json!("todo"));
    let task_id = rows[0]["taskNoteId"]
        .as_str()
        .expect("linked task note id")
        .to_string();
    assert_eq!(rows[0]["linkedTaskNoteId"], json!(task_id));

    // Authoring emitted `note:created` for the spawned child, `note:updated`
    // for the rewritten parent, and `note:created` for the parent itself.
    let mut saw_child_created = false;
    let mut saw_parent_updated = false;
    let mut saw_parent_created = false;
    for _ in 0..3 {
        let evt = next_event(&mut sub, &["note:created", "note:updated"], 10).await;
        let ty = evt["type"].as_str().unwrap_or("");
        let nid = evt["data"]["noteId"].as_str().unwrap_or("");
        match (ty, nid) {
            ("note:created", id) if id == task_id => {
                assert_eq!(evt["data"]["title"], json!("Ship It"));
                saw_child_created = true;
            }
            ("note:updated", id) if id == parent_id => saw_parent_updated = true,
            ("note:created", id) if id == parent_id => saw_parent_created = true,
            other => panic!("unexpected event {other:?}: {evt}"),
        }
    }
    assert!(saw_child_created && saw_parent_updated && saw_parent_created);

    // Delegate: assigning an agent to the `not_started` task flips it to
    // `in_progress` and publishes `task:status-changed`.
    let agent = "agent-b0a8044a-5eac-4b52-8456-15d3b784decb";
    let assign = wss_rpc(
        &mut rpc,
        4,
        "task.assignAgent",
        json!({ "workspaceId": ws_id, "noteId": task_id, "agentId": agent }),
    )
    .await;
    assert_eq!(assign["ok"], json!(true), "assign: {assign}");
    assert_eq!(assign["noteId"], json!(task_id));
    assert_eq!(assign["agentId"], json!(agent));

    // The assignment write routes through `updateNote` (TS parity), publishing
    // a `note:updated` for the task note before the status transition fires.
    let evt = next_event(&mut sub, &["note:updated"], 10).await;
    assert_eq!(
        evt["data"]["noteId"],
        json!(task_id),
        "assign updated: {evt}"
    );

    let evt = next_event(&mut sub, &["task:status-changed"], 10).await;
    assert_eq!(evt["data"]["noteId"], json!(task_id));
    assert_eq!(evt["data"]["previousStatus"], json!("not_started"));
    assert_eq!(evt["data"]["newStatus"], json!("in_progress"));
    assert!(evt["data"]["changedAt"].is_string(), "changedAt: {evt}");
    let changed_at = evt["data"]["changedAt"].clone();

    // The transition also recomputes the ready-task set: the now-in-progress
    // task drops out of `readyTaskIds`, and `computedAt` matches the
    // triggering status change's timestamp.
    let evt = next_event(&mut sub, &["task:ready-tasks-changed"], 10).await;
    assert_eq!(evt["data"]["triggeredBy"]["noteId"], json!(task_id));
    assert_eq!(
        evt["data"]["triggeredBy"]["previousStatus"],
        json!("not_started")
    );
    assert_eq!(
        evt["data"]["triggeredBy"]["newStatus"],
        json!("in_progress")
    );
    let ready = evt["data"]["readyTaskIds"]
        .as_array()
        .expect("readyTaskIds array");
    assert!(
        !ready.iter().any(|v| v == &json!(task_id)),
        "in-progress task still ready: {evt}"
    );
    assert_eq!(evt["data"]["computedAt"], changed_at, "computedAt: {evt}");

    // In-progress: the task note reflects the transition and the assignment.
    let got = wss_rpc(
        &mut rpc,
        5,
        "task.get",
        json!({ "workspaceId": ws_id, "taskNoteId": task_id }),
    )
    .await;
    assert_eq!(got["task"]["status"], json!("in_progress"), "task: {got}");
}

/// End-to-end reference-parity self-heal: a workspace whose `spec` note has
/// been deleted gets it reseeded on the next `note.list` (reference:
/// `notes.service.ts getNotes` → `ensureSpecExists`). The reseed emits a
/// single `note:created` for `noteId=spec`, and the WSS `note.list` response
/// includes the freshly-seeded spec in the returned `notes` array.
#[tokio::test]
async fn note_list_reseeds_missing_spec_over_wss() {
    let (daemon, port, cfg) = boot().await;

    // Bootstrap the workspace and delete the initial spec off the UDS so the
    // WSS event stream only carries the reseed emission.
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "SpecHeal", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let del = uds_rpc(
        &socket,
        3,
        "note.delete",
        json!({ "workspaceId": ws_id, "noteId": "spec" }),
    )
    .await;
    assert_eq!(del["result"]["ok"], json!(true), "delete spec: {del}");

    // Subscribe over WSS before invoking `note.list` so the reseed emission is
    // guaranteed to be observed.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["note:created"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // Drive `note.list` over a separate WSS RPC connection.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let list = wss_rpc(&mut rpc, 2, "note.list", json!({ "workspaceId": ws_id })).await;
    let notes = list["notes"].as_array().expect("notes array");
    let spec = notes
        .iter()
        .find(|n| n["id"] == json!("spec"))
        .expect("spec present in response");
    assert_eq!(spec["workspaceId"], json!(ws_id));
    assert_eq!(spec["title"], json!("Spec"));
    assert_eq!(spec["content"], json!(""));
    assert_eq!(spec["isPinned"], json!(true));
    assert_eq!(spec["isDefault"], json!(true));

    let evt = next_event(&mut sub, &["note:created"], 10).await;
    assert_eq!(evt["workspaceId"], ws_id.as_str());
    assert_eq!(evt["data"]["noteId"], json!("spec"));
    assert_eq!(evt["data"]["title"], json!("Spec"));
    assert_eq!(evt["data"]["action"], json!("create"));

    // The reseed publishes exactly one `note:created`; drain the socket for a
    // short window and fail if a second one arrives. Non-matching frames
    // (heartbeats, other event types) are ignored.
    let extra = timeout(Duration::from_millis(500), async {
        loop {
            match sub.next().await {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = match serde_json::from_str(&text) {
                        Ok(x) => x,
                        Err(_) => continue,
                    };
                    if v["method"] == json!("events.event")
                        && v["params"]["event"]["type"] == json!("note:created")
                    {
                        return Some(v);
                    }
                }
                Some(Ok(Message::Ping(p))) => {
                    let _ = sub.send(Message::Pong(p)).await;
                }
                Some(Ok(_)) => continue,
                // Stream close / error during the "should be quiet" window is
                // not the condition this drain is guarding against; surface
                // it instead of spinning silently until the timeout.
                Some(Err(e)) => panic!("subscription socket errored during drain: {e:?}"),
                None => panic!("subscription socket closed during drain"),
            }
        }
    })
    .await;
    assert!(
        extra.is_err(),
        "reseed must publish exactly one note:created, got extra: {extra:?}"
    );
}

/// Drain any additional `events.event` frames matching `event_type` in
/// `window` ms; return the first extra observed, or `None` if the socket
/// stayed quiet. Non-matching frames (heartbeats, unrelated event types) are
/// ignored so the drain is scoped strictly to cardinality of the target
/// emission.
async fn drain_extra<S>(
    ws: &mut WebSocketStream<S>,
    event_type: &str,
    window_ms: u64,
) -> Option<Value>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    timeout(Duration::from_millis(window_ms), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = match serde_json::from_str(&text) {
                        Ok(x) => x,
                        Err(_) => continue,
                    };
                    if v["method"] == json!("events.event")
                        && v["params"]["event"]["type"] == json!(event_type)
                    {
                        return v;
                    }
                }
                Some(Ok(Message::Ping(p))) => {
                    let _ = ws.send(Message::Pong(p)).await;
                }
                Some(Ok(_)) => continue,
                Some(Err(e)) => panic!("subscription socket errored during drain: {e:?}"),
                None => panic!("subscription socket closed during drain"),
            }
        }
    })
    .await
    .ok()
}

/// End-to-end (Audit D C2): `comment.respond` over WSS publishes
/// `comment:added` with `{ noteId, commentId }` for the reply so a subscribed
/// client sees the new thread comment without a re-read (PROTOCOL §6.5,
/// comment channel; reference `comment.respond` dispatches the same domain
/// event as `comment.add`).
#[tokio::test]
async fn comment_respond_emits_comment_added_over_wss() {
    let (daemon, port, cfg) = boot().await;

    // Bootstrap workspace + note + root comment off UDS so the WSS subscriber
    // observes only the respond emission.
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "Comments", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let n = uds_rpc(
        &socket,
        3,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Note", "content": "anchor target text" }),
    )
    .await;
    let note_id = n["result"]["note"]["id"]
        .as_str()
        .expect("note id")
        .to_string();
    let add = uds_rpc(
        &socket,
        4,
        "comment.add",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "searchContext": "anchor target text",
            "commentTarget": "target",
            "comment": "root"
        }),
    )
    .await;
    let root_comment_id = add["result"]["commentId"]
        .as_str()
        .expect("comment id")
        .to_string();

    // Subscribe over WSS before the respond, scoped to comment:added.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["comment:added"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // Drive comment.respond on a separate WSS RPC connection.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let reply = wss_rpc(
        &mut rpc,
        2,
        "comment.respond",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "commentId": root_comment_id,
            "comment": "reply body",
        }),
    )
    .await;
    let reply_id = reply["comment"]["id"]
        .as_str()
        .expect("reply id")
        .to_string();
    assert_ne!(reply_id, root_comment_id, "reply must have its own id");
    // Reply-anchoring contract (monorepo#729): the reply anchors via its
    // threadId/parentId — the wire omits `anchor`/`anchorText` entirely.
    assert!(
        reply["comment"].get("anchor").is_none(),
        "reply must not carry an anchor: {reply}"
    );
    assert!(
        reply["comment"].get("anchorText").is_none(),
        "reply must not carry anchorText: {reply}"
    );
    assert_eq!(reply["comment"]["parentId"], json!(root_comment_id));

    // The root comment keeps its authoritative anchor: getThread returns the
    // root with anchor/anchorText intact and the anchorless reply.
    let thread = wss_rpc(
        &mut rpc,
        3,
        "comment.getThread",
        json!({ "workspaceId": ws_id, "noteId": note_id, "commentId": root_comment_id }),
    )
    .await;
    let root = &thread["rootComment"];
    assert!(root["anchor"].is_object(), "root keeps anchor: {thread}");
    assert_eq!(root["anchorText"], json!("target"));
    let replies = thread["replies"].as_array().expect("replies");
    assert_eq!(replies.len(), 1);
    assert!(
        replies[0].get("anchor").is_none(),
        "reply in thread: {thread}"
    );
    assert!(replies[0].get("anchorText").is_none());

    let evt = next_event(&mut sub, &["comment:added"], 10).await;
    assert_eq!(evt["workspaceId"], ws_id.as_str());
    assert!(evt["id"].is_string(), "event id: {evt}");
    assert!(evt["timestamp"].is_string(), "timestamp: {evt}");
    assert_eq!(
        evt["actor"],
        json!({ "type": "system", "id": "system", "name": "System" })
    );
    assert_eq!(
        evt["data"],
        json!({ "noteId": note_id, "commentId": reply_id })
    );

    // Cardinality exactly 1: no second comment:added lands.
    let extra = drain_extra(&mut sub, "comment:added", 500).await;
    assert!(
        extra.is_none(),
        "comment.respond must publish exactly one comment:added, got extra: {extra:?}"
    );
}

/// End-to-end (Audit A F5): `comment.add` over WSS honours
/// `params.idempotencyKey` — a replay with the same key returns the ORIGINAL
/// result (same `commentId`) without re-executing, so no duplicate comment is
/// persisted and no second `comment:added` event is published.
#[tokio::test]
async fn comment_add_idempotency_key_dedupes_over_wss() {
    let (daemon, port, cfg) = boot().await;

    // Bootstrap workspace + note off UDS so the WSS side drives only the adds.
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "Comments", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let n = uds_rpc(
        &socket,
        3,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Note", "content": "anchor target text" }),
    )
    .await;
    let note_id = n["result"]["note"]["id"]
        .as_str()
        .expect("note id")
        .to_string();

    // Subscribe over WSS before the adds, scoped to comment:added.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["comment:added"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let params = json!({
        "workspaceId": ws_id,
        "noteId": note_id,
        "searchContext": "anchor target text",
        "commentTarget": "target",
        "comment": "root",
        "idempotencyKey": "wss-comment-idem-1",
    });
    let first = wss_rpc(&mut rpc, 2, "comment.add", params.clone()).await;
    let comment_id = first["commentId"].as_str().expect("comment id").to_string();
    let evt = next_event(&mut sub, &["comment:added"], 10).await;
    assert_eq!(
        evt["data"],
        json!({ "noteId": note_id, "commentId": comment_id })
    );

    // Replay with the same idempotencyKey: the stored result comes back (same
    // commentId), nothing re-executes.
    let second = wss_rpc(&mut rpc, 3, "comment.add", params).await;
    assert_eq!(
        second["commentId"].as_str(),
        Some(comment_id.as_str()),
        "replay must return the original commentId: {second}"
    );

    // Exactly one comment persisted, exactly one comment:added published.
    let list = uds_rpc(
        &socket,
        4,
        "comment.list",
        json!({ "workspaceId": ws_id, "noteId": note_id, "includeComments": true }),
    )
    .await;
    assert_eq!(
        list["result"]["totalThreads"],
        json!(1),
        "replay must not duplicate the comment: {list}"
    );
    let extra = drain_extra(&mut sub, "comment:added", 500).await;
    assert!(
        extra.is_none(),
        "idempotent replay must not publish a second comment:added, got extra: {extra:?}"
    );
}

/// End-to-end (monorepo#638): `comment.add` rewrites the note markdown
/// (anchor markers), so over WSS the result must echo the authoritative
/// post-rewrite `noteRev` AND publish exactly one `note:updated` alongside
/// `comment:added` — otherwise subscribed clients hold a stale rev and hit
/// spurious conflicts on their next versioned write. An idempotent replay
/// returns the cached `noteRev` without a second `note:updated`.
#[tokio::test]
async fn comment_add_echoes_note_rev_and_emits_note_updated_over_wss() {
    let (daemon, port, cfg) = boot().await;

    // Bootstrap workspace + note off UDS so the WSS side drives only the add.
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "Comments", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let n = uds_rpc(
        &socket,
        3,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Note", "content": "anchor target text" }),
    )
    .await;
    let note_id = n["result"]["note"]["id"]
        .as_str()
        .expect("note id")
        .to_string();
    let rev_before = n["result"]["note"]["rev"].as_i64().expect("note rev");

    // Subscribe over WSS before the add, scoped to note:updated.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["note:updated"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let params = json!({
        "workspaceId": ws_id,
        "noteId": note_id,
        "searchContext": "anchor target text",
        "commentTarget": "target",
        "comment": "root",
        "idempotencyKey": "wss-comment-rev-1",
    });
    let add = wss_rpc(&mut rpc, 2, "comment.add", params.clone()).await;
    assert_eq!(add["success"], json!(true));
    // The result echoes the post-rewrite rev: exactly one bump over create.
    let note_rev = add["noteRev"].as_i64().expect("noteRev in result");
    assert_eq!(note_rev, rev_before + 1, "result: {add}");

    // The echoed rev is the authoritative stored value.
    let read = wss_rpc(
        &mut rpc,
        3,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    assert_eq!(read["note"]["rev"].as_i64(), Some(note_rev));

    // Exactly one note:updated for the rewrite, with the §6.5 payload shape.
    let evt = next_event(&mut sub, &["note:updated"], 10).await;
    assert_eq!(evt["workspaceId"], ws_id.as_str());
    assert_eq!(
        evt["data"],
        json!({ "noteId": note_id, "title": "Note", "action": "update" })
    );
    let extra = drain_extra(&mut sub, "note:updated", 500).await;
    assert!(
        extra.is_none(),
        "comment.add must publish exactly one note:updated, got extra: {extra:?}"
    );

    // Idempotent replay: cached result carries the SAME noteRev, no rewrite,
    // no second note:updated.
    let replay = wss_rpc(&mut rpc, 4, "comment.add", params).await;
    assert_eq!(
        replay["noteRev"].as_i64(),
        Some(note_rev),
        "replay: {replay}"
    );
    let extra = drain_extra(&mut sub, "note:updated", 500).await;
    assert!(
        extra.is_none(),
        "idempotent replay must not publish a second note:updated, got extra: {extra:?}"
    );
}

/// End-to-end (Audit D C3): `workspace.archive` over WSS publishes
/// `workspace:updated` with the full applied delta
/// `changes: { archived: true, status: "Archived", archivedAt: <ts> }` where
/// `<ts>` equals the persisted `archivedAt`. §6.5 has no `workspace:archived`
/// event; the reference emitter dispatches `workspaceUpdated` with a
/// `changes` delta.
#[tokio::test]
async fn archive_workspace_emits_workspace_updated_over_wss() {
    let (daemon, port, cfg) = boot().await;

    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "ToArchive", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:updated"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let archive_res = wss_rpc(
        &mut rpc,
        2,
        "workspace.archive",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    // §5.1 return shape: `workspace.archive` returns the refreshed record so
    // callers do not need a follow-up `workspace.get`. `lastActivity` is
    // BE-derived and always populated on the wire (§9.1).
    assert_eq!(archive_res["workspace"]["id"], ws_id.as_str());
    assert_eq!(archive_res["workspace"]["archived"], json!(true));
    assert_eq!(archive_res["workspace"]["status"], json!("Archived"));
    assert!(archive_res["workspace"]["archivedAt"].is_string());
    assert!(archive_res["workspace"]["lastActivity"].is_string());
    assert!(archive_res.get("success").is_none());
    let archived_at = archive_res["workspace"]["archivedAt"].clone();

    let evt = next_event(&mut sub, &["workspace:updated"], 10).await;
    assert_eq!(evt["workspaceId"], ws_id.as_str());
    assert_eq!(
        evt["data"],
        json!({
            "workspaceId": ws_id,
            "changes": {
                "archived": true,
                "status": "Archived",
                "archivedAt": archived_at,
            }
        })
    );

    let extra = drain_extra(&mut sub, "workspace:updated", 500).await;
    assert!(
        extra.is_none(),
        "workspace.archive must publish exactly one workspace:updated, got extra: {extra:?}"
    );
}

/// End-to-end (Audit D C3, symmetric): `workspace.unarchive` over WSS
/// publishes `workspace:updated` with the full applied delta
/// `changes: { archived: false, status: "Active", archivedAt: null }` — the
/// explicit JSON `null` tells clients to clear the field.
#[tokio::test]
async fn unarchive_workspace_emits_workspace_updated_over_wss() {
    let (daemon, port, cfg) = boot().await;

    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "ToUnarchive", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    // Archive off UDS so the WSS subscriber observes only the unarchive.
    uds_rpc(
        &socket,
        3,
        "workspace.archive",
        json!({ "workspaceId": ws_id }),
    )
    .await;

    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:updated"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let unarchive_res = wss_rpc(
        &mut rpc,
        2,
        "workspace.unarchive",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    // §5.1 return shape mirror of archive: `archivedAt` cleared (omitted),
    // `archived: false`, `status: "Active"`.
    assert_eq!(unarchive_res["workspace"]["id"], ws_id.as_str());
    assert_eq!(unarchive_res["workspace"]["archived"], json!(false));
    assert_eq!(unarchive_res["workspace"]["status"], json!("Active"));
    assert!(unarchive_res["workspace"].get("archivedAt").is_none());
    assert!(unarchive_res["workspace"]["lastActivity"].is_string());
    assert!(unarchive_res.get("success").is_none());

    let evt = next_event(&mut sub, &["workspace:updated"], 10).await;
    assert_eq!(evt["workspaceId"], ws_id.as_str());
    assert_eq!(
        evt["data"],
        json!({
            "workspaceId": ws_id,
            "changes": {
                "archived": false,
                "status": "Active",
                "archivedAt": null,
            }
        })
    );

    let extra = drain_extra(&mut sub, "workspace:updated", 500).await;
    assert!(
        extra.is_none(),
        "workspace.unarchive must publish exactly one workspace:updated, got extra: {extra:?}"
    );
}

/// End-to-end (Audit D H1+M1): `comment.add` over WSS persists the
/// surrounding `anchorContext` and a subsequent `note.setContent` that wipes
/// the anchor markers must flip the comment to `isOrphaned: true` (reference
/// `updateNote` failed-recovery path).
#[tokio::test]
async fn note_edit_marks_destroyed_anchor_orphaned_over_wss() {
    let (daemon, port, cfg) = boot().await;
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "AnchorResilience", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let n = uds_rpc(
        &socket,
        3,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Note", "content": "prefix target suffix" }),
    )
    .await;
    let note_id = n["result"]["note"]["id"]
        .as_str()
        .expect("note id")
        .to_string();

    // comment.add over WSS — the response nests `anchorContext` per PROTOCOL
    // §5 comment shape, sourced from the anchor_before / anchor_after fields
    // persisted by M1.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let add = wss_rpc(
        &mut rpc,
        1,
        "comment.add",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "searchContext": "prefix target suffix",
            "commentTarget": "target",
            "comment": "root",
        }),
    )
    .await;
    let comment_id = add["commentId"].as_str().expect("comment id").to_string();

    // Read the comment back and assert `anchorContext` was persisted (M1).
    let list = wss_rpc(
        &mut rpc,
        2,
        "comment.list",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "includeComments": true,
        }),
    )
    .await;
    let comment = list["threads"][0]["comments"][0].clone();
    assert_eq!(comment["id"], comment_id.as_str());
    let ctx = &comment["anchorContext"];
    assert!(ctx.is_object(), "anchorContext missing: {comment}");
    assert!(
        ctx["before"]
            .as_str()
            .unwrap_or_default()
            .ends_with("prefix "),
        "unexpected before: {ctx}"
    );
    assert!(
        ctx["after"]
            .as_str()
            .unwrap_or_default()
            .starts_with(" suffix"),
        "unexpected after: {ctx}"
    );
    assert!(
        !comment["isOrphaned"].as_bool().unwrap_or(false),
        "comment should not be orphaned yet: {comment}"
    );

    // Wipe both anchor markers via note.setContent → H1 orphan path.
    wss_rpc(
        &mut rpc,
        3,
        "note.setContent",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "content": "totally different content with no markers",
            "confirmReplacement": true,
        }),
    )
    .await;

    let after = wss_rpc(
        &mut rpc,
        4,
        "comment.list",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "includeComments": true,
        }),
    )
    .await;
    let comment_after = after["threads"][0]["comments"][0].clone();
    assert_eq!(comment_after["id"], comment_id.as_str());
    assert_eq!(
        comment_after["isOrphaned"],
        json!(true),
        "expected orphaned after anchor destruction: {comment_after}"
    );
}

/// End-to-end self-heal for the pre-#110 global-note-identity bug over WSS:
/// a workspace whose spec content lives on a UUID note titled "Spec"
/// (because the buggy agent path called `note.create` for the spec) is
/// adopted onto the reserved `id='spec'` on the next `note.list`. The
/// adoption emits an ordered pair — `note:deleted` for the stray UUID then
/// `note:created` for `spec` — with the adopted title on both, and the
/// WSS `note.list` response carries the adopted content on `id='spec'` so
/// live FE clients replace the stale tree entry without an extra read.
#[tokio::test]
async fn note_list_adopts_stray_spec_note_over_wss() {
    let (daemon, port, cfg) = boot().await;
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "AdoptHeal", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // Reproduce the pre-#110 damaged shape: create a top-level, non-task
    // note titled "Spec" with real content on a random UUID id, then delete
    // the seeded `id='spec'` so `ensure_spec_note` sees exactly one
    // adoption candidate on the next `note.list`.
    let stray = uds_rpc(
        &socket,
        3,
        "note.create",
        json!({
            "workspaceId": ws_id,
            "title": "Spec",
            "content": "# Real spec content\n\nkeep me",
        }),
    )
    .await;
    let stray_id = stray["result"]["note"]["id"]
        .as_str()
        .expect("stray id")
        .to_string();
    assert_ne!(stray_id, "spec", "sanity: stray must have a UUID id");
    let del = uds_rpc(
        &socket,
        4,
        "note.delete",
        json!({ "workspaceId": ws_id, "noteId": "spec" }),
    )
    .await;
    assert_eq!(del["result"]["ok"], json!(true), "delete seed: {del}");

    // Subscribe over WSS *after* setup so the adoption pair is the only
    // note:*/note:deleted traffic we see on this socket.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["note:created", "note:deleted"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // Drive `note.list` over a separate WSS RPC connection; the response
    // must carry the adopted content on `id='spec'` and no longer list the
    // stray UUID.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let list = wss_rpc(&mut rpc, 2, "note.list", json!({ "workspaceId": ws_id })).await;
    let notes = list["notes"].as_array().expect("notes array");
    assert!(
        !notes.iter().any(|n| n["id"] == json!(stray_id)),
        "stray UUID note must be replaced: {list}"
    );
    let spec = notes
        .iter()
        .find(|n| n["id"] == json!("spec"))
        .expect("spec present in response");
    assert_eq!(spec["workspaceId"], json!(ws_id));
    assert_eq!(spec["title"], json!("Spec"));
    assert_eq!(spec["content"], json!("# Real spec content\n\nkeep me"));
    assert_eq!(spec["isPinned"], json!(true));
    assert_eq!(spec["isDefault"], json!(true));

    // Event ordering: `note:deleted` for the stray, then `note:created` for
    // spec. Both carry the adopted title so a subscribed FE tree replaces
    // the stale node in one pass.
    let deleted = next_event(&mut sub, &["note:deleted"], 10).await;
    assert_eq!(deleted["workspaceId"], ws_id.as_str());
    assert_eq!(deleted["data"]["noteId"], json!(stray_id));
    assert_eq!(deleted["data"]["title"], json!("Spec"));
    assert_eq!(deleted["data"]["action"], json!("delete"));
    let created = next_event(&mut sub, &["note:created"], 10).await;
    assert_eq!(created["workspaceId"], ws_id.as_str());
    assert_eq!(created["data"]["noteId"], json!("spec"));
    assert_eq!(created["data"]["title"], json!("Spec"));
    assert_eq!(created["data"]["action"], json!("create"));

    // Adoption is one-shot: no additional note:deleted / note:created on a
    // second `note.list`.
    let _ = wss_rpc(&mut rpc, 3, "note.list", json!({ "workspaceId": ws_id })).await;
    assert!(
        drain_extra(&mut sub, "note:deleted", 400).await.is_none(),
        "adoption must not republish note:deleted on re-list"
    );
    assert!(
        drain_extra(&mut sub, "note:created", 400).await.is_none(),
        "adoption must not republish note:created on re-list"
    );
}

/// End-to-end: `comment.add` whose `searchContext`/`commentTarget` come from
/// the editor's *plain text* (markdown syntax stripped, blocks joined with no
/// separator — the FE's `doc.textBetween` shape) anchors successfully against
/// the formatted markdown source via the plaintext-tolerant fallback.
#[tokio::test]
async fn comment_add_plaintext_context_anchors_over_wss() {
    let (daemon, port, cfg) = boot().await;
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "PlainAnchors", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let n = uds_rpc(
        &socket,
        3,
        "note.create",
        json!({
            "workspaceId": ws_id,
            "title": "Note",
            "content": "## Heading\n\nSome **bold words** in a [link](https://example.com) here.",
        }),
    )
    .await;
    let note_id = n["result"]["note"]["id"]
        .as_str()
        .expect("note id")
        .to_string();

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let add = wss_rpc(
        &mut rpc,
        1,
        "comment.add",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            // Plain text of the doc: heading joined to the paragraph with no
            // separator, bold/link markdown stripped.
            "searchContext": "HeadingSome bold words in a link here.",
            "commentTarget": "bold words",
            "comment": "needs review",
        }),
    )
    .await;
    assert_eq!(add["success"], json!(true));
    assert_eq!(add["anchored"], json!(true));
    assert_eq!(add["location"]["anchoredText"], json!("bold words"));
    let comment_id = add["commentId"].as_str().expect("comment id").to_string();

    // The anchor markers landed around the markdown-source text.
    let read = wss_rpc(
        &mut rpc,
        2,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    let content = read["note"]["content"].as_str().expect("content");
    assert!(
        content.contains(&format!(
            "<!--anchor:{comment_id}:start-->bold words<!--anchor:{comment_id}:end-->"
        )),
        "anchor markers missing: {content}"
    );
}

/// End-to-end: `comment.add` from a *stale* editor doc — the note gained a
/// paragraph on the server after the editor loaded it, so the ±50-char
/// `searchContext` includes text that no longer neighbors the selection. The
/// target-rescue path still anchors the (unique) `commentTarget`.
#[tokio::test]
async fn comment_add_stale_context_target_rescue_over_wss() {
    let (daemon, port, cfg) = boot().await;
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "StaleAnchors", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let n = uds_rpc(
        &socket,
        3,
        "note.create",
        json!({
            "workspaceId": ws_id,
            "title": "Note",
            // Server copy already has the "Status" paragraph inserted between
            // Goal and Diagnosis; the client context below predates it.
            "content": "## Goal\nOld goal paragraph tail.\n\n**Status:** new paragraph the editor never saw.\n\n## Diagnosis\n\n**Symptom:** things broke badly.",
        }),
    )
    .await;
    let note_id = n["result"]["note"]["id"]
        .as_str()
        .expect("note id")
        .to_string();

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let add = wss_rpc(
        &mut rpc,
        1,
        "comment.add",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            // Stale plain-text context: Goal tail joined directly to the
            // Diagnosis heading (the Status paragraph is missing).
            "searchContext": "Old goal paragraph tail.DiagnosisSymptom: things broke badly.",
            "commentTarget": "DiagnosisSymptom: things broke",
            "comment": "cross-block comment",
        }),
    )
    .await;
    assert_eq!(add["success"], json!(true), "response: {add}");
    assert_eq!(add["anchored"], json!(true));
    let comment_id = add["commentId"].as_str().expect("comment id").to_string();

    let read = wss_rpc(
        &mut rpc,
        2,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    let content = read["note"]["content"].as_str().expect("content");
    assert!(
        content.contains(&format!("<!--anchor:{comment_id}:start-->Diagnosis")),
        "start marker missing before the heading text: {content}"
    );
    assert!(
        content.contains(&format!("things broke<!--anchor:{comment_id}:end-->")),
        "end marker missing after the target text: {content}"
    );
}

/// End-to-end: a `comment.add` whose context cannot be found returns the
/// actionable `-32602` error with the descriptive message (not an opaque
/// `-32603 "Internal error"`).
#[tokio::test]
async fn comment_add_context_not_found_returns_invalid_params_over_wss() {
    let (daemon, port, cfg) = boot().await;
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "AnchorErrors", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let n = uds_rpc(
        &socket,
        3,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Note", "content": "some note content" }),
    )
    .await;
    let note_id = n["result"]["note"]["id"]
        .as_str()
        .expect("note id")
        .to_string();

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let v = wss_rpc_envelope(
        &mut rpc,
        1,
        "comment.add",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "searchContext": "totally absent context",
            "commentTarget": "absent",
            "comment": "c",
        }),
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32602), "envelope: {v}");
    assert_eq!(
        v["error"]["message"],
        json!("invalid params: Could not find the search context in the document."),
        "envelope: {v}"
    );
}

/// End-to-end: `comment.add` with `authorType: "user"` persists the author
/// type and round-trips it through `comment.list`; omitting the param keeps
/// the backward-compatible `agent` default.
#[tokio::test]
async fn comment_add_author_type_round_trips_over_wss() {
    let (daemon, port, cfg) = boot().await;
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "AuthorType", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let n = uds_rpc(
        &socket,
        3,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Note", "content": "alpha target-a and target-b omega" }),
    )
    .await;
    let note_id = n["result"]["note"]["id"]
        .as_str()
        .expect("note id")
        .to_string();

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let user_add = wss_rpc(
        &mut rpc,
        1,
        "comment.add",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "searchContext": "alpha target-a and",
            "commentTarget": "target-a",
            "comment": "from the user",
            "authorType": "user",
        }),
    )
    .await;
    let user_comment_id = user_add["commentId"].as_str().expect("id").to_string();

    let agent_add = wss_rpc(
        &mut rpc,
        2,
        "comment.add",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "searchContext": "and target-b omega",
            "commentTarget": "target-b",
            "comment": "from an agent",
        }),
    )
    .await;
    let agent_comment_id = agent_add["commentId"].as_str().expect("id").to_string();

    let list = wss_rpc(
        &mut rpc,
        3,
        "comment.list",
        json!({ "workspaceId": ws_id, "noteId": note_id, "includeComments": true }),
    )
    .await;
    let comments: Vec<Value> = list["threads"]
        .as_array()
        .expect("threads")
        .iter()
        .flat_map(|t| t["comments"].as_array().cloned().unwrap_or_default())
        .collect();
    let by_id = |id: &str| {
        comments
            .iter()
            .find(|c| c["id"] == json!(id))
            .unwrap_or_else(|| panic!("comment {id} missing: {list}"))
    };
    let user_comment = by_id(&user_comment_id);
    assert_eq!(user_comment["authorType"], json!("user"), "{user_comment}");
    assert_eq!(user_comment["author"], json!("User"), "{user_comment}");
    let agent_comment = by_id(&agent_comment_id);
    assert_eq!(
        agent_comment["authorType"],
        json!("agent"),
        "{agent_comment}"
    );

    // Invalid authorType is rejected with -32602.
    let bad = wss_rpc_envelope(
        &mut rpc,
        4,
        "comment.add",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "searchContext": "alpha target-a and",
            "commentTarget": "target-a",
            "comment": "c",
            "authorType": "robot",
        }),
    )
    .await;
    assert_eq!(bad["error"]["code"], json!(-32602), "envelope: {bad}");
}

/// End-to-end: `comment.respond` with `authorType: "user"` persists the
/// author type and round-trips it through `comment.getThread`; omitting the
/// param keeps the backward-compatible `agent` default.
#[tokio::test]
async fn comment_respond_author_type_round_trips_over_wss() {
    let (daemon, port, cfg) = boot().await;
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "RespondAuthorType", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let n = uds_rpc(
        &socket,
        3,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Note", "content": "alpha reply-target omega" }),
    )
    .await;
    let note_id = n["result"]["note"]["id"]
        .as_str()
        .expect("note id")
        .to_string();

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let add = wss_rpc(
        &mut rpc,
        1,
        "comment.add",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "searchContext": "alpha reply-target omega",
            "commentTarget": "reply-target",
            "comment": "root",
        }),
    )
    .await;
    let root_id = add["commentId"].as_str().expect("id").to_string();

    let user_reply = wss_rpc(
        &mut rpc,
        2,
        "comment.respond",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "commentId": root_id,
            "comment": "hi from the user",
            "authorType": "user",
        }),
    )
    .await;
    let user_reply_id = user_reply["comment"]["id"]
        .as_str()
        .expect("reply id")
        .to_string();

    let agent_reply = wss_rpc(
        &mut rpc,
        3,
        "comment.respond",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "commentId": root_id,
            "comment": "hi from an agent",
        }),
    )
    .await;
    let agent_reply_id = agent_reply["comment"]["id"]
        .as_str()
        .expect("reply id")
        .to_string();

    let thread = wss_rpc(
        &mut rpc,
        4,
        "comment.getThread",
        json!({ "workspaceId": ws_id, "noteId": note_id, "commentId": root_id }),
    )
    .await;
    let replies = thread["replies"].as_array().expect("replies");
    let by_id = |id: &str| {
        replies
            .iter()
            .find(|c| c["id"] == json!(id))
            .unwrap_or_else(|| panic!("reply {id} missing: {thread}"))
    };
    let user = by_id(&user_reply_id);
    assert_eq!(user["authorType"], json!("user"), "{user}");
    assert_eq!(user["author"], json!("User"), "{user}");
    let agent = by_id(&agent_reply_id);
    assert_eq!(agent["authorType"], json!("agent"), "{agent}");
    assert_eq!(agent["author"], json!("Agent"), "{agent}");

    // Invalid authorType is rejected with -32602.
    let bad = wss_rpc_envelope(
        &mut rpc,
        5,
        "comment.respond",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "commentId": root_id,
            "comment": "c",
            "authorType": "robot",
        }),
    )
    .await;
    assert_eq!(bad["error"]["code"], json!(-32602), "envelope: {bad}");

    // The pre-existing caller-input validations also reject with -32602, not
    // -32603 (monorepo#632): missing threadId/commentId, empty comment, and a
    // suggestion without the suggestionOriginal/suggestionProposed pair.
    let bad = wss_rpc_envelope(
        &mut rpc,
        6,
        "comment.respond",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "comment": "no target",
        }),
    )
    .await;
    assert_eq!(bad["error"]["code"], json!(-32602), "envelope: {bad}");

    let bad = wss_rpc_envelope(
        &mut rpc,
        7,
        "comment.respond",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "commentId": root_id,
            "comment": "   ",
        }),
    )
    .await;
    assert_eq!(bad["error"]["code"], json!(-32602), "envelope: {bad}");

    let bad = wss_rpc_envelope(
        &mut rpc,
        8,
        "comment.respond",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "commentId": root_id,
            "comment": "try this",
            "type": "suggestion",
            "suggestionOriginal": "only original",
        }),
    )
    .await;
    assert_eq!(bad["error"]["code"], json!(-32602), "envelope: {bad}");

    // The sibling caller-input validations on `comment.getThread` /
    // `comment.resolveThread` (missing threadId AND commentId) and the
    // `comment.list` filter checks also reject with -32602, not -32603
    // (monorepo#649).
    let bad = wss_rpc_envelope(
        &mut rpc,
        9,
        "comment.getThread",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    assert_eq!(bad["error"]["code"], json!(-32602), "envelope: {bad}");

    let bad = wss_rpc_envelope(
        &mut rpc,
        10,
        "comment.resolveThread",
        json!({ "workspaceId": ws_id, "noteId": note_id, "resolved": true }),
    )
    .await;
    assert_eq!(bad["error"]["code"], json!(-32602), "envelope: {bad}");

    let bad = wss_rpc_envelope(
        &mut rpc,
        11,
        "comment.list",
        json!({ "workspaceId": ws_id, "noteId": note_id, "authorType": "robot" }),
    )
    .await;
    assert_eq!(bad["error"]["code"], json!(-32602), "envelope: {bad}");
}
