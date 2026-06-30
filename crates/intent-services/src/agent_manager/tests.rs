//! Unit tests for the agent process registry (cap + LRU + lifecycle) and the
//! [`AgentManager`] multiplexing/teardown — parity-checked against
//! `agent-process-registry`.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_acp::permission::{PermissionOptionView, RiskLevel};
use intent_acp::{
    Connection, ConnectionHooks, EventSink, IncomingNotification, PermissionOutcome,
    PermissionPolicy, PermissionRequestData,
};
use intent_core::{
    now_iso, AgentId, AgentSession, AgentStatus, Error, Workspace, WorkspaceActivity, WorkspaceApi,
    WorkspaceAttention, WorkspaceId, WorkspaceStatus,
};
use intent_store::Store;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, Mutex as TokioMutex};
use tokio::task::JoinHandle;
use tokio::time::timeout;

use super::{
    compute_process_cap, derive_agent_type, resolve_spawn, text_prompt, user_text_blocks,
    AgentHandle, AgentManager, BusEventSink, KillFn, ProcessRegistry, DEFAULT_AGENT_TYPE,
};
use crate::events::{EventBus, SubscriptionFilter};
use crate::Services;

struct TempDb {
    path: PathBuf,
}

impl TempDb {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("intentd-mgr-{}.db", uuid::Uuid::new_v4()));
        Self { path }
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
        }
    }
}

/// A kill callback that records the agents it was invoked for (the registry
/// itself performs the follow-up `deregister`).
fn recording_kill(id: AgentId, log: Arc<Mutex<Vec<AgentId>>>) -> KillFn {
    Arc::new(move || {
        let log = log.clone();
        let id = id.clone();
        Box::pin(async move {
            log.lock().unwrap().push(id);
        })
    })
}

#[test]
fn compute_process_cap_matches_ts_thresholds() {
    assert_eq!(compute_process_cap(8 * super::GB), 4);
    assert_eq!(compute_process_cap(16 * super::GB), 8);
    assert_eq!(compute_process_cap(32 * super::GB), 20);
    assert_eq!(compute_process_cap(64 * super::GB), 30);
    assert_eq!(compute_process_cap(128 * super::GB), 100);
}

#[tokio::test]
async fn tracks_concurrent_processes_and_deregisters() {
    let reg = ProcessRegistry::new(8);
    let log = Arc::new(Mutex::new(Vec::new()));
    for name in ["a", "b", "c"] {
        let id = AgentId::from(name);
        reg.register(id.clone(), recording_kill(id, log.clone()));
    }
    assert_eq!(reg.size(), 3);
    assert!(reg.is_registered(&AgentId::from("b")));
    assert!(reg.deregister(&AgentId::from("b")));
    assert_eq!(reg.size(), 2);
    assert!(!reg.is_registered(&AgentId::from("b")));
}

#[tokio::test]
async fn acquire_evicts_lru_idle_when_full() {
    let reg = ProcessRegistry::new(2);
    let log = Arc::new(Mutex::new(Vec::new()));
    let (a, b) = (AgentId::from("a"), AgentId::from("b"));
    reg.register(a.clone(), recording_kill(a.clone(), log.clone()));
    reg.register(b.clone(), recording_kill(b.clone(), log.clone()));
    // `a` is the least-recently-used idle process.
    reg.set_last_active(&a, 100);
    reg.set_last_active(&b, 200);

    reg.acquire().await;

    assert_eq!(*log.lock().unwrap(), vec![a.clone()], "evicts LRU idle");
    assert!(!reg.is_registered(&a));
    assert!(reg.is_registered(&b));
    assert_eq!(reg.size(), 1);
}

#[tokio::test]
async fn acquire_queues_until_a_process_goes_idle() {
    let reg = Arc::new(ProcessRegistry::new(1));
    let log = Arc::new(Mutex::new(Vec::new()));
    let a = AgentId::from("a");
    reg.register(a.clone(), recording_kill(a.clone(), log.clone()));
    reg.mark_active(&a);

    let reg2 = reg.clone();
    let acquired = tokio::spawn(async move { reg2.acquire().await });
    // All processes active → the acquire must block.
    assert!(timeout(Duration::from_millis(50), async {}).await.is_ok());
    assert!(!acquired.is_finished(), "acquire blocks while all active");

    // Becoming idle wakes the queued spawn, which evicts `a` and proceeds.
    reg.mark_idle(&a);
    timeout(Duration::from_secs(2), acquired)
        .await
        .expect("acquire resolves once a slot frees")
        .expect("task ok");
    assert_eq!(*log.lock().unwrap(), vec![a]);
    assert_eq!(reg.size(), 0);
}

#[tokio::test]
async fn lifecycle_active_processes_are_not_reaped() {
    let reg = ProcessRegistry::new(8);
    let log = Arc::new(Mutex::new(Vec::new()));
    let (a, b) = (AgentId::from("a"), AgentId::from("b"));
    reg.register(a.clone(), recording_kill(a.clone(), log.clone()));
    reg.register(b.clone(), recording_kill(b.clone(), log.clone()));
    reg.mark_active(&a);
    assert!(reg.is_active(&a));

    let evicted = reg.evict_idle(None).await;
    assert_eq!(evicted, 1);
    assert_eq!(
        *log.lock().unwrap(),
        vec![b.clone()],
        "skips the active one"
    );
    assert!(reg.is_registered(&a));
    assert!(!reg.is_registered(&b));
}

async fn manager() -> (TempDb, AgentManager) {
    let (tmp, mgr, _bus) = manager_with_bus().await;
    (tmp, mgr)
}

/// Like [`manager`] but also returns the [`EventBus`] so a test can subscribe and
/// assert which lifecycle events the manager publishes.
async fn manager_with_bus() -> (TempDb, AgentManager, EventBus) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store).with_event_bus(bus.clone());
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    (tmp, AgentManager::new(services, sink, 8), bus)
}

/// A passive agent handle over an in-memory duplex connection (no child).
fn mock_handle() -> AgentHandle {
    let (client_w, _agent_r) = tokio::io::duplex(1024);
    let (_agent_w, client_r) = tokio::io::duplex(1024);
    let (note_tx, note_rx) = mpsc::unbounded_channel();
    let connection = Arc::new(Connection::new(
        client_w,
        client_r,
        None,
        ConnectionHooks {
            notifications: Some(note_tx),
            ..ConnectionHooks::default()
        },
    ));
    AgentHandle {
        connection,
        notifications: Arc::new(TokioMutex::new(note_rx)),
        serve_task: tokio::spawn(async {}),
        _child: None,
        _mcp_bridge: None,
        _mcp_config: None,
        _rules_config: None,
    }
}

/// Track a mock agent in the manager + registry the way `create_agent` would.
fn track(mgr: &AgentManager, id: &AgentId) {
    mgr.handles
        .lock()
        .unwrap()
        .insert(id.clone(), mock_handle());
    mgr.registry.register(id.clone(), mgr.make_kill(id.clone()));
}

#[tokio::test]
async fn manager_tracks_lookup_stop_and_shuts_down() {
    let (_tmp, mgr) = manager().await;
    let (a, b) = (AgentId::from("a"), AgentId::from("b"));
    track(&mgr, &a);
    track(&mgr, &b);

    assert_eq!(mgr.len(), 2);
    assert_eq!(mgr.registry().size(), 2);
    assert!(mgr.contains(&a));

    assert!(mgr.stop(&a).await);
    assert_eq!(mgr.len(), 1);
    assert!(!mgr.contains(&a));
    assert!(!mgr.registry().is_registered(&a));

    mgr.shutdown().await;
    assert!(mgr.is_empty(), "shutdown tears down every tracked agent");
    assert_eq!(mgr.registry().size(), 0);
}

#[tokio::test]
async fn reap_idle_evicts_handles_and_deregisters() {
    let (_tmp, mgr) = manager().await;
    let (a, b) = (AgentId::from("a"), AgentId::from("b"));
    track(&mgr, &a);
    track(&mgr, &b);

    let reaped = mgr.reap_idle(None).await;
    assert_eq!(reaped, 2);
    assert!(mgr.is_empty(), "reap drops the manager handles");
    assert_eq!(mgr.registry().size(), 0);
}

#[tokio::test]
async fn evict_idle_older_than_evicts_only_stale_idle() {
    let reg = ProcessRegistry::new(8);
    let log = Arc::new(Mutex::new(Vec::new()));
    let (old, fresh, active) = (
        AgentId::from("old"),
        AgentId::from("fresh"),
        AgentId::from("active"),
    );
    for id in [&old, &fresh, &active] {
        reg.register(id.clone(), recording_kill(id.clone(), log.clone()));
    }
    // `old` last streamed at the epoch (well past any TTL); `fresh` just now;
    // `active` is streaming (protected regardless of its timestamp).
    reg.set_last_active(&old, 1);
    reg.set_last_active(&fresh, super::now_ms());
    reg.set_last_active(&active, 1);
    reg.mark_active(&active);

    let evicted = reg
        .evict_idle_older_than(Duration::from_secs(60), |_| true)
        .await;

    assert_eq!(evicted, 1, "only the stale idle process is reaped");
    assert_eq!(*log.lock().unwrap(), vec![old.clone()]);
    assert!(!reg.is_registered(&old));
    assert!(reg.is_registered(&fresh), "within-TTL idle kept");
    assert!(reg.is_registered(&active), "active process kept");
}

#[tokio::test]
async fn reap_idle_older_than_skips_in_flight_agents() {
    let (_tmp, mgr) = manager().await;
    let (busy, idle) = (AgentId::from("busy"), AgentId::from("idle"));
    track(&mgr, &busy);
    track(&mgr, &idle);
    // Both stale past the TTL, but `busy` has an in-flight prompt.
    mgr.registry().set_last_active(&busy, 1);
    mgr.registry().set_last_active(&idle, 1);
    assert!(mgr.try_begin(&busy, &WorkspaceId::new()).await);

    let reaped = mgr.reap_idle_older_than(Duration::from_secs(60)).await;

    assert_eq!(reaped, 1, "only the idle agent is reaped");
    assert!(
        mgr.contains(&busy),
        "agent with an in-flight prompt is kept"
    );
    assert!(!mgr.contains(&idle));
    assert_eq!(mgr.registry().size(), 1);
}

/// Process-tree teardown (§5.6): a provider's whole process group is signalled,
/// so a grandchild spawned by the direct child is terminated too — `kill_on_drop`
/// alone would leave it orphaned.
#[cfg(unix)]
#[tokio::test]
async fn kill_child_tree_terminates_grandchild() {
    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::process::Command;

    // The shell becomes the group leader (`process_group(0)`), backgrounds a
    // grandchild `sleep`, prints its pid, then sleeps so the group stays alive.
    let mut cmd = Command::new("sh");
    cmd.arg("-c").arg("sleep 300 & echo $!; sleep 300");
    cmd.stdout(Stdio::piped());
    cmd.kill_on_drop(true);
    cmd.process_group(0);
    let mut child = cmd.spawn().expect("spawn sleep tree");

    let stdout = child.stdout.take().expect("piped stdout");
    let mut lines = BufReader::new(stdout).lines();
    let line = timeout(Duration::from_secs(5), lines.next_line())
        .await
        .expect("grandchild pid line in time")
        .expect("read ok")
        .expect("a pid line");
    let grandchild: u32 = line.trim().parse().expect("grandchild pid");
    assert!(pid_alive(grandchild), "grandchild alive before teardown");

    super::kill_child_tree(child).await;

    let mut dead = false;
    for _ in 0..100 {
        if !pid_alive(grandchild) {
            dead = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(dead, "grandchild terminated with the process group");
}

/// Signal-0 liveness probe used by the process-group teardown test.
#[cfg(unix)]
fn pid_alive(pid: u32) -> bool {
    use nix::sys::signal::kill;
    use nix::unistd::Pid;
    matches!(
        kill(Pid::from_raw(pid as i32), None),
        Ok(()) | Err(nix::errno::Errno::EPERM)
    )
}

/// A self-cleaning temp git repo with one committed file modified in the workdir.
struct TempRepo {
    dir: PathBuf,
}

impl Drop for TempRepo {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Seed `a.txt`, commit it, then leave an unstaged modification (2 adds / 1 del).
fn seed_repo() -> TempRepo {
    use git2::{Repository, Signature};
    let dir = std::env::temp_dir().join(format!("intentd-ft-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let repo = Repository::init(&dir).unwrap();
    {
        let mut cfg = repo.config().unwrap();
        cfg.set_str("user.name", "Test").unwrap();
        cfg.set_str("user.email", "test@example.com").unwrap();
    }
    std::fs::write(dir.join("a.txt"), "line1\nline2\nline3\n").unwrap();
    {
        let mut index = repo.index().unwrap();
        index.add_path(std::path::Path::new("a.txt")).unwrap();
        index.write().unwrap();
        let tree_oid = index.write_tree().unwrap();
        let tree = repo.find_tree(tree_oid).unwrap();
        let sig = Signature::now("Test", "test@example.com").unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "seed", &tree, &[])
            .unwrap();
    }
    std::fs::write(dir.join("a.txt"), "line1\nCHANGED\nline3\nline4\n").unwrap();
    TempRepo { dir }
}

/// An agent `file:changed` runs the BE-internal review pipeline (§17.1): the
/// sink records both the diff (§17.3) and the attribution row (§17.4) for the
/// edited file, with the agent's stats and lazy blob SHAs.
#[tokio::test]
async fn agent_file_change_records_tracked_change_and_diff() {
    use intent_acp::SinkEvent;
    use intent_core::{
        events::FILE_CHANGED, now_iso, ActorType, EventActor, Workspace, WorkspaceActivity,
        WorkspaceAttention, WorkspaceId, WorkspaceStatus,
    };

    let repo = seed_repo();
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    let ws_id = WorkspaceId::from("ws-ft");
    let ts = now_iso();
    let ws = Workspace {
        id: ws_id.clone(),
        title: "WS".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
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
        worktree_path: Some(repo.dir.display().to_string()),
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        active_pull_request: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
    };
    store.insert_workspace(&ws).await.unwrap();

    let bus = EventBus::new(store.clone());
    let sink = BusEventSink::new(bus);
    sink.publish(SinkEvent {
        workspace_id: ws_id.clone(),
        event_type: FILE_CHANGED.to_string(),
        actor: EventActor {
            actor_type: ActorType::Agent,
            id: Some("agent-1".to_string()),
            name: Some("Agent".to_string()),
            ..Default::default()
        },
        session_id: Some("agent-1".to_string()),
        data: serde_json::json!({
            "path": "a.txt",
            "relativePath": "a.txt",
            "action": "modify",
        }),
    })
    .await;

    let changes = store.list_tracked_changes(&ws_id).await.unwrap();
    assert_eq!(changes.len(), 1, "one attribution row for the edited file");
    let c = &changes[0];
    assert_eq!(c.path, "a.txt");
    assert_eq!(c.stage, "unstaged");
    assert_eq!(c.status, "modified");
    assert_eq!(c.agent_id.as_deref(), Some("agent-1"));
    assert_eq!(c.session_id.as_deref(), Some("agent-1"));
    assert_eq!(c.additions, 2);
    assert_eq!(c.deletions, 1);
    assert!(
        c.new_blob_sha.is_some(),
        "content recoverable lazily via SHA"
    );

    let diffs = store.list_diffs(&ws_id).await.unwrap();
    assert_eq!(diffs.len(), 1, "one diff row for the edited file");
    assert_eq!(diffs[0].file_path, "a.txt");
    assert!(!diffs[0].staged);
    assert!(
        diffs[0].old_content.is_none(),
        "content stays lazy, not inlined"
    );
    assert!(
        diffs[0].hunks_json.contains("CHANGED"),
        "extracted hunks carry the new line"
    );
}

const MGR_ACP_SID: &str = "mgr-acp-new";

/// Configurable mock agent: `initialize` advertises `loadSession` per `load_cap`;
/// `session/new` mints [`MGR_ACP_SID`]; `session/load` succeeds; everything else
/// (e.g. `authenticate`) resolves with `{}`.
fn spawn_cfg_mock_agent<R, W>(read: R, write: W, load_cap: bool) -> JoinHandle<()>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut lines = BufReader::new(read).lines();
        let mut write = write;
        while let Ok(Some(line)) = lines.next_line().await {
            if line.trim().is_empty() {
                continue;
            }
            let value: Value = serde_json::from_str(&line).expect("valid JSON");
            let (Some(id), Some(method)) =
                (value.get("id"), value.get("method").and_then(Value::as_str))
            else {
                continue;
            };
            let result = match method {
                "initialize" => {
                    json!({ "protocolVersion": 1, "agentCapabilities": { "loadSession": load_cap } })
                }
                "session/new" => json!({ "sessionId": MGR_ACP_SID }),
                "session/load" => json!({}),
                _ => json!({}),
            };
            let resp = json!({ "jsonrpc": "2.0", "id": id, "result": result });
            write
                .write_all(format!("{resp}\n").as_bytes())
                .await
                .unwrap();
            write.flush().await.unwrap();
        }
    })
}

/// Track a handle wired to a configurable mock agent (parity with `create_agent`
/// minus a real child), returning the agent task handle.
fn track_mock_agent(mgr: &AgentManager, id: &AgentId, load_cap: bool) -> JoinHandle<()> {
    let (c2a_client, c2a_agent) = tokio::io::duplex(16 * 1024);
    let (a2c_agent, a2c_client) = tokio::io::duplex(16 * 1024);
    let agent = spawn_cfg_mock_agent(c2a_agent, a2c_agent, load_cap);
    let (note_tx, note_rx) = mpsc::unbounded_channel::<IncomingNotification>();
    let connection = Arc::new(Connection::new(
        c2a_client,
        a2c_client,
        None,
        ConnectionHooks {
            notifications: Some(note_tx),
            ..ConnectionHooks::default()
        },
    ));
    mgr.handles.lock().unwrap().insert(
        id.clone(),
        AgentHandle {
            connection,
            notifications: Arc::new(TokioMutex::new(note_rx)),
            serve_task: tokio::spawn(async {}),
            _child: None,
            _mcp_bridge: None,
            _mcp_config: None,
            _rules_config: None,
        },
    );
    mgr.registry.register(id.clone(), mgr.make_kill(id.clone()));
    agent
}

/// A test provider that skips `authenticate` (deterministic handshake).
fn test_provider() -> intent_providers::ProviderConfig {
    intent_providers::ProviderConfig {
        supports_authenticate: false,
        ..*intent_providers::provider_config(intent_providers::default_provider_id())
    }
}

async fn seed_agent(mgr: &AgentManager, ws: &WorkspaceId, id: &AgentId) {
    let ts = now_iso();
    let workspace = Workspace {
        id: ws.clone(),
        title: "WS".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
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
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
    };
    let session = AgentSession {
        id: id.clone(),
        workspace_id: ws.clone(),
        parent_agent_id: None,
        backend_session_id: None,
        acp_session_id: None,
        name: "Builder".to_string(),
        name_explicitly_set: false,
        model: None,
        provider: None,
        system_prompt: None,
        specialist: None,
        status: AgentStatus::Pending,
        is_active: true,
        messages: Vec::new(),
        stats: None,
        task_note_id: None,
        skip_auto_commit: false,
        created_at: ts.clone(),
        updated_at: ts,
    };
    mgr.services
        .store
        .insert_workspace(&workspace)
        .await
        .expect("insert ws");
    mgr.services
        .store
        .insert_agent_session(&session)
        .await
        .expect("insert session");
}

#[tokio::test]
async fn start_session_opens_first_session_without_recreate_flag() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-new"));
    seed_agent(&mgr, &ws, &id).await;
    let _agent = track_mock_agent(&mgr, &id, false);

    let sid = mgr
        .start_session(&id, PathBuf::from("/tmp/ws"), &test_provider())
        .await
        .expect("first session");
    assert_eq!(sid, MGR_ACP_SID);
    assert!(!mgr.take_recreated(&id), "brand-new agent is not flagged");
    let stored = mgr.services.store.get_agent_session(&id).await.unwrap();
    assert_eq!(stored.acp_session_id.as_deref(), Some(MGR_ACP_SID));
}

#[tokio::test]
async fn start_session_resumes_when_load_supported() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-resume"));
    seed_agent(&mgr, &ws, &id).await;
    mgr.services
        .store
        .set_acp_session_id(&id, "existing-id")
        .await
        .unwrap();
    let _agent = track_mock_agent(&mgr, &id, true);

    let sid = mgr
        .start_session(&id, PathBuf::from("/tmp/ws"), &test_provider())
        .await
        .expect("resume");
    assert_eq!(sid, "existing-id", "session/load resumes the stored id");
    assert!(!mgr.take_recreated(&id), "resume needs no history resend");
    let stored = mgr.services.store.get_agent_session(&id).await.unwrap();
    assert_eq!(stored.acp_session_id.as_deref(), Some("existing-id"));
}

#[tokio::test]
async fn start_session_recreates_and_flags_when_load_unsupported() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-recreate"));
    seed_agent(&mgr, &ws, &id).await;
    mgr.services
        .store
        .set_acp_session_id(&id, "stale-id")
        .await
        .unwrap();
    let _agent = track_mock_agent(&mgr, &id, false);

    let sid = mgr
        .start_session(&id, PathBuf::from("/tmp/ws"), &test_provider())
        .await
        .expect("recreate");
    assert_eq!(sid, MGR_ACP_SID, "fresh session replaces the lost id");
    let stored = mgr.services.store.get_agent_session(&id).await.unwrap();
    assert_eq!(stored.acp_session_id.as_deref(), Some(MGR_ACP_SID));
    // The recreate flag is set so the next turn resends history; take() clears it.
    assert!(mgr.take_recreated(&id), "recreate flags a history resend");
    assert!(!mgr.take_recreated(&id), "flag is cleared once taken");
}

#[tokio::test]
async fn build_turn_prompt_prepends_history_once_after_recreate() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-hist"));
    seed_agent(&mgr, &ws, &id).await;
    // Prior transcript + the just-persisted current user message (the last row).
    for (role, text) in [
        ("user", "first question"),
        ("assistant", "first answer"),
        ("user", "current message"),
    ] {
        mgr.services
            .store
            .append_agent_message(
                &id,
                role,
                &json!([{ "type": "text", "text": text }]),
                &now_iso(),
            )
            .await
            .unwrap();
    }
    mgr.recreated.lock().unwrap().insert(id.clone());

    let prompt = mgr.build_turn_prompt(&id, "current message").await;
    let text = serde_json::to_value(&prompt).unwrap()[0]["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(text.contains("<supervisor>"), "history XML is prepended");
    assert!(text.contains("first question"));
    assert!(text.contains("first answer"));
    assert!(
        !text.contains("<text>current message</text>"),
        "current message is excluded from the rendered history"
    );
    assert!(
        text.trim_end().ends_with("current message"),
        "ends with the live prompt"
    );

    // The flag is consumed: a follow-up turn sends only the message text.
    let plain = mgr.build_turn_prompt(&id, "next message").await;
    let plain_text = serde_json::to_value(&plain).unwrap()[0]["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(plain_text, "next message");
}

/// The keep-alive interrupt path emits ONLY the terminal `agent:stream:end` and
/// deliberately NOT `agent:idle`: an interrupted agent is about to resume, so
/// waking parents on idle would be premature (mirrors the TS interrupt
/// suppression in `emitAgentIdleEvent`).
#[tokio::test]
async fn interrupt_emits_terminal_stream_end_but_no_idle() {
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-int"));
    seed_agent(&mgr, &ws, &id).await;
    let _agent = track_mock_agent(&mgr, &id, false);
    // An `acpSessionId` is required for the keep-alive interrupt (otherwise
    // `interrupt` falls back to the hard `stop` kill path).
    mgr.services
        .store
        .set_acp_session_id(&id, "acp-int")
        .await
        .unwrap();
    // Claim the in-flight slot so the interrupt exercises the busy turn path.
    assert!(mgr.try_begin(&id, &ws).await);

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    assert!(mgr.interrupt(&id).await, "interrupt finds the live agent");

    // Drain the published events within a bounded window.
    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let types: Vec<&str> = events.iter().map(|e| e.event_type.as_str()).collect();
    assert!(
        types.contains(&"agent:stream:end"),
        "interrupt emits the terminal stream:end (got {types:?})"
    );
    assert!(
        !types.contains(&"agent:idle"),
        "interrupt suppresses agent:idle (got {types:?})"
    );
}

// --- SP-B: spawn `agent_type` derived from the specialist's `agentType` -------

/// Self-cleaning temp directory for hermetic specialist-file fixtures.
struct TempSpecialistsDir(PathBuf);

impl TempSpecialistsDir {
    fn new() -> Self {
        let dir =
            std::env::temp_dir().join(format!("intentd-spb-specialists-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("create specialists dir");
        Self(dir)
    }

    /// Write `<id>.md` with the given raw markdown-with-frontmatter content.
    fn write(&self, id: &str, content: &str) {
        std::fs::write(self.0.join(format!("{id}.md")), content).expect("write specialist file");
    }
}

impl Drop for TempSpecialistsDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Build a `Services` whose user specialists tier is `dir` (bundled tier left to
/// the env default, which is irrelevant for these ids).
async fn services_with_specialists(dir: &TempSpecialistsDir) -> (TempDb, Services) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let services = Services::new(store).with_specialist_dirs(Some(dir.0.clone()), None);
    (tmp, services)
}

/// An otherwise-empty session carrying just the `specialist` under test.
fn session_with_specialist(specialist: Option<&str>) -> AgentSession {
    AgentSession {
        id: AgentId::from("agent-spb"),
        workspace_id: WorkspaceId::from("ws-spb"),
        parent_agent_id: None,
        backend_session_id: None,
        acp_session_id: None,
        name: "SpB".to_string(),
        name_explicitly_set: false,
        model: None,
        provider: None,
        system_prompt: None,
        specialist: specialist.map(str::to_string),
        status: AgentStatus::Pending,
        is_active: false,
        messages: Vec::new(),
        stats: None,
        task_note_id: None,
        skip_auto_commit: false,
        created_at: now_iso(),
        updated_at: now_iso(),
    }
}

#[tokio::test]
async fn derive_agent_type_uses_specialist_agent_type_and_engages_denylist() {
    use intent_acp::{get_tool_denylist_for_agent_type, SUBAGENT_TOOLS};

    let dir = TempSpecialistsDir::new();
    dir.write(
        "ralph",
        "---\nname: \"Ralph\"\ndescription: \"Loops\"\nagentType: \"ralph-loop\"\n---\n\nYou loop.",
    );
    let (_tmp, services) = services_with_specialists(&dir).await;

    let session = session_with_specialist(Some("ralph"));
    let agent_type = derive_agent_type(&services, &session, None);
    assert_eq!(agent_type, "ralph-loop");

    // The derived type drives the §18.4 denylist: ralph-loop denies the
    // sub-agent orchestration tools (but not the full text-only denylist).
    let denylist = get_tool_denylist_for_agent_type(&agent_type);
    assert!(!denylist.is_empty(), "ralph-loop engages a denylist");
    for tool in SUBAGENT_TOOLS {
        assert!(
            denylist.contains(tool),
            "ralph-loop denylist removes {tool}"
        );
    }
}

#[tokio::test]
async fn derive_agent_type_falls_back_to_default_without_agent_type() {
    use intent_acp::get_tool_denylist_for_agent_type;

    let dir = TempSpecialistsDir::new();
    // A specialist that declares no `agentType` frontmatter.
    dir.write(
        "plain",
        "---\nname: \"Plain\"\ndescription: \"No agentType\"\n---\n\nbody",
    );
    let (_tmp, services) = services_with_specialists(&dir).await;

    let with_specialist = session_with_specialist(Some("plain"));
    assert_eq!(
        derive_agent_type(&services, &with_specialist, None),
        DEFAULT_AGENT_TYPE,
    );

    // A plain agent with no specialist at all keeps the default too.
    let no_specialist = session_with_specialist(None);
    assert_eq!(
        derive_agent_type(&services, &no_specialist, None),
        DEFAULT_AGENT_TYPE,
    );

    // The default (interactive) type is unrestricted — no regression.
    assert!(get_tool_denylist_for_agent_type(DEFAULT_AGENT_TYPE).is_empty());
}

/// Build a normalized prompt for `session_id` keyed by `request_id`.
fn prompt(request_id: &str, session_id: &str) -> PermissionRequestData {
    PermissionRequestData {
        request_id: request_id.to_string(),
        session_id: session_id.to_string(),
        title: "Write file".to_string(),
        description: None,
        options: vec![PermissionOptionView {
            id: "allow_once".to_string(),
            label: "Allow".to_string(),
            description: None,
            destructive: false,
        }],
        agent_name: "auggie".to_string(),
        risk_level: RiskLevel::High,
        timestamp: 0,
    }
}

#[tokio::test]
async fn default_policy_is_auto_by_risk_and_overridable() {
    let (_tmp, mgr) = manager().await;
    // Headless default per §6.7/M3.5.
    assert_eq!(mgr.policy(), PermissionPolicy::AutoByRisk);
    // `with_policy` selects an FE-driven interactive deployment.
    let (_tmp2, mgr2, _bus) = manager_with_bus().await;
    let mgr2 = mgr2.with_policy(PermissionPolicy::Interactive);
    assert_eq!(mgr2.policy(), PermissionPolicy::Interactive);
}

#[tokio::test]
async fn pending_permissions_snapshots_and_respond_unblocks() {
    let (_tmp, mgr) = manager().await;
    // Register two outstanding prompts directly in the registry the way a
    // surfaced (interactive) prompt would.
    let mut rx = mgr.permissions.register(prompt("perm_1", "agent-a"));
    let _rx2 = mgr.permissions.register(prompt("perm_2", "agent-b"));

    let pending = mgr.pending_permissions();
    assert_eq!(pending.len(), 2);
    assert!(pending.iter().any(|p| p.request_id == "perm_1"));
    assert!(pending.iter().any(|p| p.request_id == "perm_2"));

    // Resolving delivers the outcome to the blocked waiter and drops the prompt.
    assert!(mgr.respond_permission(
        "perm_1",
        PermissionOutcome::Selected {
            option_id: "allow_once".to_string()
        }
    ));
    assert_eq!(
        rx.try_recv().expect("waiter receives the resolved outcome"),
        PermissionOutcome::Selected {
            option_id: "allow_once".to_string()
        }
    );
    assert_eq!(mgr.pending_permissions().len(), 1);

    // A second resolve (or an unknown id) finds nothing outstanding.
    assert!(!mgr.respond_permission("perm_1", PermissionOutcome::Cancelled));
    assert!(!mgr.respond_permission("nope", PermissionOutcome::Cancelled));
}

#[tokio::test]
async fn services_pending_and_respond_rpcs_drive_the_registry() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store).with_event_bus(bus.clone());
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let manager = Arc::new(AgentManager::new(services.clone(), sink, 8));
    services.attach_agent_manager(&manager);

    let mut rx = manager.permissions.register(prompt("perm_1", "agent-a"));
    let _rx2 = manager.permissions.register(prompt("perm_2", "agent-b"));

    // Unfiltered snapshot returns both prompts as `{ requests: [...] }`.
    let all = services
        .agent_pending_permissions(None)
        .await
        .expect("pending");
    assert_eq!(all["requests"].as_array().unwrap().len(), 2);

    // Filtering by agentId (= sessionId) keeps only that session's prompt.
    let filtered = services
        .agent_pending_permissions(Some(AgentId::from("agent-a")))
        .await
        .expect("pending filtered");
    let reqs = filtered["requests"].as_array().unwrap();
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0]["requestId"], json!("perm_1"));
    assert_eq!(reqs[0]["sessionId"], json!("agent-a"));

    // Resolving over the RPC unblocks the waiter and reports `{ resolved: true }`.
    let resolved = services
        .agent_respond_permission(
            "perm_1".to_string(),
            json!({ "outcome": "selected", "optionId": "allow_once" }),
        )
        .await
        .expect("respond");
    assert_eq!(resolved, json!({ "resolved": true }));
    assert_eq!(
        rx.try_recv().expect("waiter unblocked"),
        PermissionOutcome::Selected {
            option_id: "allow_once".to_string()
        }
    );

    // An unknown request id is `{ resolved: false }`, not an error.
    let missing = services
        .agent_respond_permission("perm_1".to_string(), json!({ "outcome": "cancelled" }))
        .await
        .expect("respond missing");
    assert_eq!(missing, json!({ "resolved": false }));

    // A malformed `outcome` shape is rejected as invalid params.
    let err = services
        .agent_respond_permission("perm_2".to_string(), json!({ "outcome": "approved" }))
        .await
        .expect_err("malformed outcome rejected");
    assert!(matches!(err, Error::InvalidParams(_)));
}

#[tokio::test]
async fn services_permission_rpcs_are_inert_without_a_manager() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    // No AgentManager attached → no registry to consult.
    let services = Services::new(store);

    let pending = services
        .agent_pending_permissions(None)
        .await
        .expect("pending");
    assert_eq!(pending["requests"].as_array().unwrap().len(), 0);

    let resolved = services
        .agent_respond_permission("perm_1".to_string(), json!({ "outcome": "cancelled" }))
        .await
        .expect("respond");
    assert_eq!(resolved, json!({ "resolved": false }));
}

// --- Lifecycle plumbing -------------------------------------------------------

#[tokio::test]
async fn stop_returns_false_for_unknown_agent() {
    let (_tmp, mgr) = manager().await;
    assert!(!mgr.stop(&AgentId::from("missing")).await);
}

/// `stop` drops any pending `recreated` flag so a stale resend bit cannot
/// survive a teardown into a future spawn (parity with the `recreated` doc on
/// `AgentManager`).
#[tokio::test]
async fn stop_clears_pending_recreate_flag() {
    let (_tmp, mgr) = manager().await;
    let id = AgentId::from("a-stop-recreate");
    track(&mgr, &id);
    mgr.recreated.lock().unwrap().insert(id.clone());

    assert!(mgr.stop(&id).await);
    assert!(
        !mgr.recreated.lock().unwrap().contains(&id),
        "stop wipes the resend flag",
    );
}

#[tokio::test]
async fn is_busy_reflects_try_begin_and_end_turn() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-busy"), AgentId::from("a-busy"));
    assert!(!mgr.is_busy(&id), "fresh agent is not busy");
    assert!(mgr.try_begin(&id, &ws).await);
    assert!(mgr.is_busy(&id), "claim flips busy on");
    // Second `try_begin` is rejected — single-flight per agent (§5.5).
    assert!(
        !mgr.try_begin(&id, &ws).await,
        "single-flight rejects 2nd claim"
    );
    mgr.end_turn(&id).await;
    assert!(!mgr.is_busy(&id), "release flips busy off");
}

/// `try_begin` persists the runtime `Active` transition and publishes the
/// self-sufficient `agent:status-changed` event so a hydrated client reflects
/// the live runtime rather than the stored `Pending` placeholder.
#[tokio::test]
async fn try_begin_persists_active_status_and_emits_event() {
    use intent_core::events::AGENT_STATUS_CHANGED;
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let (ws, id) = (WorkspaceId::from("ws-begin"), AgentId::from("a-begin"));
    seed_agent(&mgr, &ws, &id).await;

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    assert!(mgr.try_begin(&id, &ws).await);

    let stored = mgr.services.store.get_agent_session(&id).await.unwrap();
    assert_eq!(stored.status, AgentStatus::Active);
    assert!(stored.is_active);

    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let status_event = events
        .iter()
        .find(|e| e.event_type == AGENT_STATUS_CHANGED)
        .expect("agent:status-changed published");
    assert_eq!(status_event.data["status"], json!("active"));
    assert_eq!(status_event.data["isActive"], json!(true));
}

/// `end_turn` persists the `RuntimeIdle` transition and publishes
/// `agent:status-changed`. A no-op `end_turn` on an agent that was never busy
/// neither writes nor emits.
#[tokio::test]
async fn end_turn_persists_runtime_idle_and_emits_event() {
    use intent_core::events::AGENT_STATUS_CHANGED;
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let (ws, id) = (WorkspaceId::from("ws-end"), AgentId::from("a-end"));
    seed_agent(&mgr, &ws, &id).await;
    assert!(mgr.try_begin(&id, &ws).await);

    // Subscribe AFTER `try_begin` so we only capture the `end_turn` emission.
    let mut sub = bus.subscribe(SubscriptionFilter::default());
    mgr.end_turn(&id).await;

    let stored = mgr.services.store.get_agent_session(&id).await.unwrap();
    assert_eq!(stored.status, AgentStatus::RuntimeIdle);
    assert!(!stored.is_active);

    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let status_event = events
        .iter()
        .find(|e| e.event_type == AGENT_STATUS_CHANGED)
        .expect("agent:status-changed published on end_turn");
    assert_eq!(status_event.data["status"], json!("idle"));
    assert_eq!(status_event.data["isActive"], json!(false));

    // Calling `end_turn` again on an already-idle agent is a no-op.
    mgr.end_turn(&id).await;
    assert!(!mgr.is_busy(&id));
}

#[tokio::test]
async fn interrupt_returns_false_for_unknown_agent() {
    let (_tmp, mgr) = manager().await;
    assert!(
        !mgr.interrupt(&AgentId::from("nope")).await,
        "no handle → fall through to stop, which reports no removal",
    );
}

/// Without an `acpSessionId` to cancel, `interrupt` falls back to the hard
/// `stop` path (which still tears the handle down).
#[tokio::test]
async fn interrupt_falls_back_to_stop_without_acp_session_id() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-int-fb"), AgentId::from("a-int-fb"));
    seed_agent(&mgr, &ws, &id).await;
    // Track the handle but leave `acp_session_id` unset.
    track(&mgr, &id);

    assert!(
        mgr.interrupt(&id).await,
        "stop fallback reports the removal"
    );
    assert!(!mgr.contains(&id), "fallback tore the handle down");
}

// --- Queue + drain ------------------------------------------------------------

#[tokio::test]
async fn try_drain_queue_no_op_when_already_busy() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-drain"), AgentId::from("a-drain"));
    // Queue a ready message so the only barrier is the busy flag.
    mgr.services
        .enqueue_message(&id, "queued".to_string(), None);
    assert!(mgr.try_begin(&id, &ws).await);

    mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;

    // Queue is left untouched (no dequeue happened) and the slot stays held.
    assert_eq!(mgr.services.queue_snapshot(&id).len(), 1);
    assert!(mgr.is_busy(&id));
}

#[tokio::test]
async fn try_drain_queue_no_op_without_ready_messages() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-empty"), AgentId::from("a-empty"));

    mgr.clone().try_drain_queue(id.clone(), ws.clone()).await;

    assert!(
        !mgr.is_busy(&id),
        "no slot claim without ready-to-send work"
    );
    assert_eq!(mgr.services.queue_snapshot(&id).len(), 0);
}

#[tokio::test]
async fn send_message_queues_when_already_busy() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-q"), AgentId::from("a-q"));
    // Claim the in-flight slot so `send_message` must enqueue.
    assert!(mgr.try_begin(&id, &ws).await);

    let result = mgr
        .send_message(id.clone(), ws.clone(), "queued".to_string(), None)
        .await
        .expect("send_message returns the queued envelope");
    assert_eq!(result["success"], json!(true));
    assert_eq!(result["queued"], json!(true));
    assert_eq!(result["queuedMessage"]["content"], json!("queued"));
    assert_eq!(result["queuedMessage"]["position"], json!(0));
    assert_eq!(mgr.services.queue_snapshot(&id).len(), 1);
}

// --- Recreate flag + history rendering ---------------------------------------

/// When the resend flag is set but the agent has no prior history (the just-
/// persisted current user message is excluded), `build_turn_body` just clears
/// the flag and returns the live content unchanged.
#[tokio::test]
async fn build_turn_body_clears_flag_when_only_current_message_exists() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-bt"), AgentId::from("a-bt"));
    seed_agent(&mgr, &ws, &id).await;
    mgr.services
        .store
        .append_agent_message(
            &id,
            "user",
            &json!([{ "type": "text", "text": "only message" }]),
            &now_iso(),
        )
        .await
        .unwrap();
    mgr.recreated.lock().unwrap().insert(id.clone());

    let body = mgr.build_turn_body(&id, "only message").await;

    assert_eq!(body, "only message", "no prior → live content unchanged");
    assert!(
        !mgr.recreated.lock().unwrap().contains(&id),
        "flag was consumed even though no XML was prepended",
    );
}

// --- resolve_spawn ------------------------------------------------------------

/// A bare session with no `provider`/`model` resolves to the default ACP
/// provider (auggie), no model, and the temp dir as cwd (no workspace path).
#[test]
fn resolve_spawn_defaults_to_default_provider_and_temp_cwd() {
    let session = session_with_specialist(None);
    let resolved = resolve_spawn(&session, None).expect("default resolves");
    assert_eq!(
        resolved.provider.id,
        intent_providers::default_provider_id()
    );
    assert!(resolved.model.is_none(), "no model selected");
    assert!(resolved.extra_env.is_empty());
    assert_eq!(resolved.cwd, std::env::temp_dir());
}

/// A compound `provider:model` id selects both the provider and the bare model
/// id, without needing an explicit `provider` on the session.
#[test]
fn resolve_spawn_parses_compound_model_id() {
    let mut session = session_with_specialist(None);
    session.model = Some("claude-code:sonnet".to_string());
    let resolved = resolve_spawn(&session, None).expect("compound resolves");
    assert_eq!(resolved.provider.id, "claude-code");
    assert_eq!(resolved.model.as_deref(), Some("sonnet"));
}

/// An explicit `session.provider` wins over the prefix encoded in the model id
/// (the session row is authoritative).
#[test]
fn resolve_spawn_session_provider_overrides_model_prefix() {
    let mut session = session_with_specialist(None);
    session.provider = Some("codex".to_string());
    session.model = Some("claude-code:sonnet".to_string());
    let resolved = resolve_spawn(&session, None).expect("explicit provider wins");
    assert_eq!(resolved.provider.id, "codex");
    // The model string is still split off the compound id (the bare half).
    assert_eq!(resolved.model.as_deref(), Some("sonnet"));
}

/// A workspace whose `path` exists on disk becomes the spawn cwd; a missing
/// path silently falls back to the temp dir.
#[test]
fn resolve_spawn_prefers_existing_workspace_path() {
    let session = session_with_specialist(None);
    let ws_dir = std::env::temp_dir().join(format!("intentd-rs-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&ws_dir).unwrap();
    let mut workspace = intent_core::Workspace {
        id: WorkspaceId::from("ws-rs"),
        title: "WS".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: now_iso(),
        updated_at: now_iso(),
        last_activity: None,
        tags: vec![],
        path: Some(ws_dir.display().to_string()),
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
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
    };
    let resolved =
        resolve_spawn(&session, Some(&workspace)).expect("existing workspace path resolves");
    assert_eq!(resolved.cwd, ws_dir);

    // Switch to a non-existent path → fall back to temp.
    workspace.path = Some(
        std::env::temp_dir()
            .join(format!("intentd-missing-{}", uuid::Uuid::new_v4()))
            .display()
            .to_string(),
    );
    let resolved = resolve_spawn(&session, Some(&workspace)).expect("falls back to temp");
    assert_eq!(resolved.cwd, std::env::temp_dir());

    let _ = std::fs::remove_dir_all(&ws_dir);
}

// --- Prompt block shape helpers ----------------------------------------------

/// The persisted/prompt wire shape for a user text message is a single
/// `{ type: "text", text }` block in an array (parity with `agent.sendMessage`).
#[test]
fn user_text_blocks_emits_single_text_block_array() {
    let blocks = user_text_blocks("hello world");
    let arr = blocks.as_array().expect("array");
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["type"], json!("text"));
    assert_eq!(arr[0]["text"], json!("hello world"));
}

#[test]
fn text_prompt_produces_one_acp_text_content_block() {
    let prompt = text_prompt("ping");
    assert_eq!(prompt.len(), 1);
    let rendered = serde_json::to_value(&prompt).unwrap();
    assert_eq!(rendered[0]["type"], json!("text"));
    assert_eq!(rendered[0]["text"], json!("ping"));
}

// --- derive_agent_type workspace path tier -----------------------------------

/// When a specialist sits under the workspace project tier
/// (`<ws>/.augment/specialists/<id>.md`), `derive_agent_type` discovers it via
/// the workspace path and returns its declared `agentType`.
#[tokio::test]
async fn derive_agent_type_uses_workspace_project_specialists_dir() {
    let ws_dir = std::env::temp_dir().join(format!("intentd-dat-{}", uuid::Uuid::new_v4()));
    let specialists_dir = ws_dir.join(".augment/specialists");
    std::fs::create_dir_all(&specialists_dir).unwrap();
    std::fs::write(
        specialists_dir.join("worker.md"),
        "---\nname: \"Worker\"\ndescription: \"d\"\nagentType: \"worker-loop\"\n---\n\nbody",
    )
    .unwrap();

    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    // No global specialists dirs — the only way to find `worker` is via the
    // workspace's project tier.
    let services = Services::new(store);

    let session = session_with_specialist(Some("worker"));
    let workspace = intent_core::Workspace {
        id: WorkspaceId::from("ws-dat"),
        title: "WS".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: now_iso(),
        updated_at: now_iso(),
        last_activity: None,
        tags: vec![],
        path: Some(ws_dir.display().to_string()),
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
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
    };

    assert_eq!(
        derive_agent_type(&services, &session, Some(&workspace)),
        "worker-loop",
    );

    // A session with no specialist set keeps the default regardless of the
    // workspace tier (no lookup happens).
    let plain = session_with_specialist(None);
    assert_eq!(
        derive_agent_type(&services, &plain, Some(&workspace)),
        DEFAULT_AGENT_TYPE,
    );

    let _ = std::fs::remove_dir_all(&ws_dir);
}
