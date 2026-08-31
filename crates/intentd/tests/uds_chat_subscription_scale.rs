//! Diagnostic scale reproduction for intent-hq/intent#3707: does a daemon
//! holding many workspaces/agents/tasks delay the seq-0 snapshot of two
//! concurrent cold-start `chat.subscribe` opens by multiple seconds?
//!
//! Two scenarios run in one test against fresh in-process daemons:
//! - **small**: 2 workspaces, 2 agents (the subscribers), short transcripts.
//! - **large**: ~100 workspaces, ~300 task notes, ~20 extra background agents
//!   with seeded transcripts — the shape of the field report.
//!
//! Both scenarios open two UDS connections, send `chat.subscribe` for two
//! different agents back-to-back, and measure wall-clock time from request
//! send to (a) the `subscriptionId` response and (b) the seq-0
//! `subscription.push` snapshot frame. Latencies are printed for comparison;
//! the assertion is deliberately lenient (a generous upper bound) because the
//! test is diagnostic — the measured numbers are the deliverable.

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use intent_core::{
    now_iso, AgentId, AgentSession, AgentStatus, ContentType, Note, NoteId, NoteMetadata,
    NoteVisibility, TaskMetadata, TaskStatus, Workspace, WorkspaceActivity, WorkspaceAttention,
    WorkspaceId, WorkspaceStatus,
};
use intent_services::{EventBus, Services};
use intent_store::{ReplaceMessage, Store};
use intent_transport::serve_uds;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::OwnedReadHalf;
use tokio::net::UnixStream;
use tokio::sync::oneshot;
use tokio::time::timeout;
use uuid::Uuid;

struct TempDb {
    path: PathBuf,
}
impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
        }
    }
}

async fn connect_retry(socket: &PathBuf) -> UnixStream {
    for _ in 0..100 {
        if let Ok(s) = UnixStream::connect(socket).await {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("could not connect to {}", socket.display());
}

async fn send(write_half: &mut (impl AsyncWriteExt + Unpin), frame: &str) {
    write_half.write_all(frame.as_bytes()).await.unwrap();
    write_half.write_all(b"\n").await.unwrap();
    write_half.flush().await.unwrap();
}

async fn read_json(reader: &mut BufReader<OwnedReadHalf>) -> Value {
    let mut line = String::new();
    let n = timeout(common::rpc_read_timeout(), reader.read_line(&mut line))
        .await
        .expect("timed out waiting for a frame")
        .expect("read failed");
    assert!(n > 0, "connection closed unexpectedly");
    serde_json::from_str(line.trim_end()).expect("invalid JSON frame")
}

async fn rpc(
    write_half: &mut (impl AsyncWriteExt + Unpin),
    reader: &mut BufReader<OwnedReadHalf>,
    id: i64,
    method: &str,
    params: Value,
) -> Value {
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    send(write_half, &serde_json::to_string(&frame).unwrap()).await;
    let resp = read_json(reader).await;
    assert_eq!(resp["id"], id, "response id mismatch for {method}");
    assert!(resp.get("error").is_none(), "rpc {method} errored: {resp}");
    resp["result"].clone()
}

/// Boot an in-process UDS daemon over a fresh store; returns everything the
/// scenario needs plus the raw [`Store`] handle for direct seeding.
async fn boot() -> (
    PathBuf,
    tokio::task::JoinHandle<()>,
    oneshot::Sender<()>,
    TempDb,
    Store,
    Arc<Services>,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    let tmp = TempDb {
        path: std::env::temp_dir().join(format!("intentd-uds-scale-{}.db", Uuid::new_v4())),
    };
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let sock_dir = common::test_tempdir_in("/tmp", "itd-uds-");
    let socket = sock_dir.path().join("uds.sock");
    let ws_root = common::hermetic_workspaces_root();
    let services = Arc::new(
        Services::new(bus.store().clone())
            .with_workspaces_root(ws_root.path().to_path_buf())
            .with_settings_registry(common::registry_with_default_provider(ws_root.path()))
            .with_event_bus(bus.clone()),
    );
    let api: Arc<dyn intent_core::WorkspaceApi> = services.clone();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server = tokio::spawn({
        let socket = socket.clone();
        async move {
            let _ = serve_uds(api, bus, &socket, None, async {
                let _ = shutdown_rx.await;
            })
            .await;
        }
    });
    (
        socket,
        server,
        shutdown_tx,
        tmp,
        store,
        services,
        ws_root,
        sock_dir,
    )
}

/// Text block of roughly realistic chat-message size (~200 bytes).
fn message_blocks(i: usize) -> Value {
    json!([{
        "type": "text",
        "text": format!("seeded message {i}: {}", "lorem ipsum dolor sit amet ".repeat(7)),
    }])
}

fn seed_workspace(idx: usize) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: WorkspaceId(format!("ws-scale-{idx}-{}", Uuid::new_v4())),
        title: format!("Scale WS {idx}"),
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
        skip_worktree: true,
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
        display_status: None,
        waiting: false,
        token_usage: None,
        cow_supported: None,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
    }
}

fn seed_task_note(ws_id: &WorkspaceId, idx: usize) -> Note {
    let ts = now_iso();
    let status = match idx % 4 {
        0 => TaskStatus::Complete,
        1 => TaskStatus::InProgress,
        2 => TaskStatus::NotStarted,
        _ => TaskStatus::Waiting,
    };
    Note {
        id: NoteId(format!("task-scale-{idx}-{}", Uuid::new_v4())),
        workspace_id: ws_id.clone(),
        title: format!("Scale task {idx}"),
        content: format!("Task body {idx}: {}", "do the thing ".repeat(20)),
        content_type: ContentType::Markdown,
        tags: vec!["task".to_string()],
        is_pinned: false,
        is_archived: false,
        is_default: false,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        metadata: NoteMetadata {
            task: Some(TaskMetadata {
                status,
                ..Default::default()
            }),
        },
        created_at: ts.clone(),
        rev: 0,
        updated_at: ts,
    }
}

fn seed_agent_session(ws_id: &WorkspaceId, idx: usize) -> AgentSession {
    let ts = now_iso();
    AgentSession {
        harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
        harness_features: None,
        id: AgentId(format!("agent-{}", Uuid::new_v4())),
        workspace_id: ws_id.clone(),
        backend_session_id: None,
        acp_session_id: None,
        name: format!("Background {idx}"),
        name_explicitly_set: false,
        model: None,
        reasoning_effort: None,
        effort_levels: None,
        provider: None,
        status: AgentStatus::Completed,
        is_active: false,
        system_prompt: None,
        created_at: ts.clone(),
        updated_at: ts,
        parent_agent_id: None,
        specialist: None,
        task_note_id: None,
        skip_auto_commit: false,
        completion_report: None,
        completion_report_timestamp: None,
        attention_request_kind: None,
        attention_request_reason: None,
        attention_request_timestamp: None,
        delegation_depth: None,
        initial_message: None,
        context_references: None,
        image_blocks: None,
        file_blocks: None,
        is_background: true,
        metadata: None,
        messages: vec![],
        stats: None,
        sandbox_id: None,
        sandbox_path: None,
        sandbox_branch: None,
        stop_reason: None,
        stop_reason_timestamp: None,
        session_corrupted: false,
        pending_delete_at: None,
        retired_at: None,
    }
}

/// Seed the field-report shape directly through the store: `extra_workspaces`
/// additional workspaces, ~3 task notes each, and `background_agents` extra
/// sessions (spread over the seeded workspaces) each with `messages_per_agent`
/// persisted messages.
async fn seed_scale(
    store: &Store,
    extra_workspaces: usize,
    background_agents: usize,
    messages_per_agent: usize,
) {
    let mut ws_ids = Vec::with_capacity(extra_workspaces);
    for w in 0..extra_workspaces {
        let ws = seed_workspace(w);
        store.insert_workspace(&ws).await.expect("seed workspace");
        for t in 0..3 {
            let note = seed_task_note(&ws.id, w * 3 + t);
            store.insert_note(&note).await.expect("seed task note");
        }
        ws_ids.push(ws.id);
    }
    for a in 0..background_agents {
        let ws_id = &ws_ids[a % ws_ids.len()];
        let session = seed_agent_session(ws_id, a);
        let ts = now_iso();
        let contents: Vec<Value> = (0..messages_per_agent).map(message_blocks).collect();
        let messages: Vec<ReplaceMessage<'_>> = contents
            .iter()
            .enumerate()
            .map(|(m, content)| ReplaceMessage {
                role: if m % 2 == 0 { "user" } else { "assistant" },
                content,
                metadata: None,
                created_at: &ts,
            })
            .collect();
        store
            .insert_agent_session_with_messages(&session, &messages)
            .await
            .expect("seed background agent");
    }
}

/// Measured latencies for one cold-start `chat.subscribe`: request send →
/// `subscriptionId` response, and request send → seq-0 snapshot push.
#[derive(Debug, Clone, Copy)]
struct SubscribeLatency {
    ack: Duration,
    snapshot: Duration,
}

/// Send `chat.subscribe {agentId}` on an already-open connection and return
/// the send timestamp. Kept separate from the read loop so both requests can
/// be written before either response is read — `tokio::join!` polls futures
/// in order on one task, so a combined write+read future for connection 1
/// could otherwise complete before connection 2 sends anything, making the
/// "concurrent" measurement serial.
async fn send_subscribe(write: &mut (impl AsyncWriteExt + Unpin), agent_id: &str) -> Instant {
    let frame = json!({
        "jsonrpc": "2.0", "id": 1, "method": "chat.subscribe",
        "params": { "agentId": agent_id },
    });
    let start = Instant::now();
    send(write, &serde_json::to_string(&frame).unwrap()).await;
    start
}

/// Read a connection until both the subscribe ack and the seq-0 snapshot
/// frame have arrived (order-independent), timing each against `start`.
async fn read_subscribe_latency(
    reader: &mut BufReader<OwnedReadHalf>,
    start: Instant,
) -> SubscribeLatency {
    let mut ack = None;
    let mut snapshot = None;
    while ack.is_none() || snapshot.is_none() {
        let msg = read_json(reader).await;
        if msg.get("id") == Some(&json!(1)) {
            assert!(msg.get("error").is_none(), "chat.subscribe errored: {msg}");
            assert!(msg["result"]["subscriptionId"].as_str().is_some());
            ack = Some(start.elapsed());
        } else if msg["method"] == "subscription.push" && msg["params"]["seq"] == json!(0) {
            assert_eq!(msg["params"]["kind"], "snapshot");
            snapshot = Some(start.elapsed());
        }
    }
    SubscribeLatency {
        ack: ack.unwrap(),
        snapshot: snapshot.unwrap(),
    }
}

/// One scenario: boot a daemon, create two subscriber workspaces + agents over
/// RPC with a short seeded transcript each, apply the scale seed, then open
/// two connections and fire both `chat.subscribe` requests back-to-back.
async fn run_scenario(
    label: &str,
    extra_workspaces: usize,
    background_agents: usize,
    messages_per_agent: usize,
) -> (SubscribeLatency, SubscribeLatency) {
    let (socket, server, shutdown_tx, _tmp, store, _services, _ws_root, _sock_dir) = boot().await;
    let (ctl_read, mut ctl_write) = connect_retry(&socket).await.into_split();
    let mut ctl_reader = BufReader::new(ctl_read);

    // The two subscriber agents live in two distinct RPC-created workspaces
    // (the field pattern: concurrent opens for different agents).
    let mut subscribers = Vec::new();
    for i in 0..2i64 {
        let ws = rpc(
            &mut ctl_write,
            &mut ctl_reader,
            10 + i,
            "workspace.create",
            json!({ "title": format!("Subscriber WS {i}") }),
        )
        .await;
        let ws_id = ws["workspace"]["id"].as_str().unwrap().to_string();
        let a = rpc(
            &mut ctl_write,
            &mut ctl_reader,
            20 + i,
            "agent.create",
            json!({ "workspaceId": ws_id, "name": format!("Subscriber {i}") }),
        )
        .await;
        let agent_id = a["agent"]["id"].as_str().unwrap().to_string();
        // A real transcript (60 messages, ~1 KB each) so the snapshot page
        // has realistic content to hydrate and serialize.
        for m in 0..60 {
            store
                .append_agent_message(
                    &AgentId::from(agent_id.as_str()),
                    if m % 2 == 0 { "user" } else { "assistant" },
                    &json!([{
                        "type": "text",
                        "text": format!("subscriber msg {m}: {}", "y".repeat(1024)),
                    }]),
                    &now_iso(),
                )
                .await
                .expect("seed subscriber message");
        }
        subscribers.push(agent_id);
    }

    if extra_workspaces > 0 {
        seed_scale(
            &store,
            extra_workspaces,
            background_agents,
            messages_per_agent,
        )
        .await;
    }

    // Open both subscription connections first, write BOTH subscribe frames
    // before reading either response (so the daemon truly sees the two opens
    // back-to-back), then read both connections concurrently.
    let (r1, mut w1) = connect_retry(&socket).await.into_split();
    let mut reader1 = BufReader::new(r1);
    let (r2, mut w2) = connect_retry(&socket).await.into_split();
    let mut reader2 = BufReader::new(r2);
    let a1 = subscribers[0].clone();
    let a2 = subscribers[1].clone();
    let start1 = send_subscribe(&mut w1, &a1).await;
    let start2 = send_subscribe(&mut w2, &a2).await;
    let (l1, l2) = tokio::join!(
        read_subscribe_latency(&mut reader1, start1),
        read_subscribe_latency(&mut reader2, start2),
    );

    println!(
        "[{label}] conn1: ack {:?}, seq-0 snapshot {:?} | conn2: ack {:?}, seq-0 snapshot {:?}",
        l1.ack, l1.snapshot, l2.ack, l2.snapshot
    );

    let _ = shutdown_tx.send(());
    let _ = server.await;
    (l1, l2)
}

/// The #3707 reproduction: measure two concurrent cold-start `chat.subscribe`
/// opens at small scale and at field-report scale, and print the comparison.
/// The hypothesis predicts multi-second seq-0 latencies at large scale; the
/// hard assertion is a lenient sanity bound so the diagnostic numbers, not a
/// flaky threshold, are the deliverable.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_cold_start_subscribe_latency_at_scale() {
    let (s1, s2) = run_scenario("small: 2 ws, 2 agents", 0, 0, 0).await;
    let (b1, b2) = run_scenario("large: ~100 ws, ~300 tasks, 20 agents", 100, 20, 40).await;

    let small_worst = s1.snapshot.max(s2.snapshot);
    let large_worst = b1.snapshot.max(b2.snapshot);
    println!(
        "worst-case seq-0 snapshot latency: small={small_worst:?} large={large_worst:?} \
         (ratio {:.2}x)",
        large_worst.as_secs_f64() / small_worst.as_secs_f64().max(1e-9)
    );

    // Sanity bound only (generous for oversubscribed CI): the field report
    // describes multi-second stalls; if large-scale seq-0 latency were in
    // that regime even on an idle daemon, this would catch it.
    let bound = common::test_timeout(Duration::from_secs(10));
    assert!(
        large_worst < bound,
        "large-scale seq-0 snapshot took {large_worst:?} (bound {bound:?}) — \
         daemon scale alone reproduces the intent#3707 stall"
    );
}
