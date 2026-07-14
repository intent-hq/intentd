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
        pull_requests: None,
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

/// Shared capture of `(method, params)` for every request the manager sends to
/// a mock agent; opt-in per [`spawn_cfg_mock_agent`] so tests that need it can
/// assert on the exact request sequence.
type MockCallLog = Arc<Mutex<Vec<(String, Value)>>>;

/// The `availableModes` list a mock agent advertises in its `session/new` /
/// `session/load` response. Defaults to a set that includes `bypassPermissions`
/// so tests exercising the "set_mode was attempted" assertions keep working;
/// tests can substitute a bypass-free set (e.g. `default`+`ask`, matching
/// auggie today) to exercise the skip path.
#[derive(Clone)]
struct MockModes {
    current_mode_id: &'static str,
    available_modes: &'static [&'static str],
}

impl MockModes {
    const fn with_bypass() -> Self {
        Self {
            current_mode_id: "default",
            available_modes: &["default", "bypassPermissions"],
        }
    }

    const fn no_bypass() -> Self {
        // Matches auggie's real advertised set today: no bypass-equivalent, so
        // the manager must skip `session/set_mode` rather than trigger `-32602`.
        Self {
            current_mode_id: "default",
            available_modes: &["default", "ask"],
        }
    }

    fn to_json(&self) -> Value {
        let available: Vec<Value> = self
            .available_modes
            .iter()
            .map(|id| json!({ "id": id, "name": id }))
            .collect();
        json!({
            "currentModeId": self.current_mode_id,
            "availableModes": available,
        })
    }
}

/// Configurable mock agent: `initialize` advertises `loadSession` per `load_cap`;
/// `session/new` mints [`MGR_ACP_SID`] and advertises the caller-chosen
/// `availableModes` (so tests can flip between the bypass-advertised and
/// bypass-absent shapes); `session/load` echoes the same modes; everything else
/// (e.g. `authenticate`) resolves with `{}`. When `log` is `Some`, every request
/// method (and its params) is recorded so tests can assert what the manager sent
/// after handshake / session setup.
fn spawn_cfg_mock_agent_with_modes<R, W>(
    read: R,
    write: W,
    load_cap: bool,
    log: Option<MockCallLog>,
    modes: MockModes,
) -> JoinHandle<()>
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
            if let Some(log) = &log {
                let params = value.get("params").cloned().unwrap_or(Value::Null);
                log.lock().unwrap().push((method.to_string(), params));
            }
            let result = match method {
                "initialize" => {
                    json!({ "protocolVersion": 1, "agentCapabilities": { "loadSession": load_cap } })
                }
                "session/new" => {
                    json!({ "sessionId": MGR_ACP_SID, "modes": modes.to_json() })
                }
                "session/load" => json!({ "modes": modes.to_json() }),
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
    track_mock_agent_inner(mgr, id, load_cap, None, MockModes::with_bypass()).0
}

/// Like [`track_mock_agent`] but also returns a shared log capturing every
/// request the manager sent to the mock (method + params), so tests can assert
/// e.g. that `session/set_mode bypassPermissions` was attempted after session
/// setup.
fn track_mock_agent_with_log(
    mgr: &AgentManager,
    id: &AgentId,
    load_cap: bool,
) -> (JoinHandle<()>, MockCallLog) {
    let log: MockCallLog = Arc::new(Mutex::new(Vec::new()));
    let (handle, _) = track_mock_agent_inner(
        mgr,
        id,
        load_cap,
        Some(log.clone()),
        MockModes::with_bypass(),
    );
    (handle, log)
}

/// Like [`track_mock_agent_with_log`] but with a caller-chosen advertised-modes
/// set (e.g. `MockModes::no_bypass()` to exercise the "provider offers no
/// bypass-equivalent" skip path).
fn track_mock_agent_with_log_modes(
    mgr: &AgentManager,
    id: &AgentId,
    load_cap: bool,
    modes: MockModes,
) -> (JoinHandle<()>, MockCallLog) {
    let log: MockCallLog = Arc::new(Mutex::new(Vec::new()));
    let (handle, _) = track_mock_agent_inner(mgr, id, load_cap, Some(log.clone()), modes);
    (handle, log)
}

fn track_mock_agent_inner(
    mgr: &AgentManager,
    id: &AgentId,
    load_cap: bool,
    log: Option<MockCallLog>,
    modes: MockModes,
) -> (JoinHandle<()>, ()) {
    let (c2a_client, c2a_agent) = tokio::io::duplex(16 * 1024);
    let (a2c_agent, a2c_client) = tokio::io::duplex(16 * 1024);
    let agent = spawn_cfg_mock_agent_with_modes(c2a_agent, a2c_agent, load_cap, log, modes);
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
    (agent, ())
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
        pull_requests: None,
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
        completion_report: None,
        completion_report_timestamp: None,
        delegation_depth: None,
        initial_message: None,
        context_references: None,
        image_blocks: None,
        is_background: false,
        metadata: None,
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
        .set_acp_session_id(&ws, &id, "existing-id")
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
        .set_acp_session_id(&ws, &id, "stale-id")
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

/// Under the shipped `AllowAll` default, `start_session` best-effort asks the
/// provider to run in `bypassPermissions` mode (parity with the TS acp-provider)
/// once a session id is minted. Providers that don't advertise `set_mode` skip
/// the call; providers that do (auggie today) see it after `session/new`,
/// `session/load`, or the recreate path.
#[tokio::test]
async fn start_session_sends_bypass_permissions_under_allow_all() {
    let (_tmp, mgr) = manager().await;
    // `manager()` builds an AllowAll manager (the shipped default), which is
    // what wires `maybe_bypass_permissions` on the three session paths.
    assert_eq!(mgr.policy(), PermissionPolicy::AllowAll);

    // 1) Brand-new session: bypass follows `session/new`.
    let new_id = AgentId::from("a-bypass-new");
    seed_agent(&mgr, &WorkspaceId::from("ws-bypass-new"), &new_id).await;
    let (_agent_new, new_log) = track_mock_agent_with_log(&mgr, &new_id, false);
    let sid = mgr
        .start_session(&new_id, PathBuf::from("/tmp/ws"), &test_provider())
        .await
        .expect("first session");
    assert_eq!(sid, MGR_ACP_SID);
    let new_calls = new_log.lock().unwrap().clone();
    let set_mode = new_calls
        .iter()
        .find(|(m, _)| m == "session/set_mode")
        .expect("session/set_mode called after session/new");
    assert_eq!(set_mode.1["sessionId"], MGR_ACP_SID);
    assert_eq!(set_mode.1["modeId"], "bypassPermissions");
    // Ordering: `session/new` precedes the bypass attempt.
    let new_idx = new_calls
        .iter()
        .position(|(m, _)| m == "session/new")
        .expect("session/new in log");
    let set_idx = new_calls
        .iter()
        .position(|(m, _)| m == "session/set_mode")
        .expect("session/set_mode in log");
    assert!(new_idx < set_idx, "bypass attempted after session/new");

    // 2) Resume path: bypass follows `session/load`.
    let resume_id = AgentId::from("a-bypass-resume");
    let resume_ws = WorkspaceId::from("ws-bypass-resume");
    seed_agent(&mgr, &resume_ws, &resume_id).await;
    mgr.services
        .store
        .set_acp_session_id(&resume_ws, &resume_id, "existing-id")
        .await
        .unwrap();
    let (_agent_r, resume_log) = track_mock_agent_with_log(&mgr, &resume_id, true);
    let sid = mgr
        .start_session(&resume_id, PathBuf::from("/tmp/ws"), &test_provider())
        .await
        .expect("resume");
    assert_eq!(sid, "existing-id");
    let resume_calls = resume_log.lock().unwrap().clone();
    let set_mode = resume_calls
        .iter()
        .find(|(m, _)| m == "session/set_mode")
        .expect("session/set_mode called after session/load");
    assert_eq!(set_mode.1["sessionId"], "existing-id");
    assert_eq!(set_mode.1["modeId"], "bypassPermissions");

    // 3) Recreate path: bypass follows the fallback `session/new`.
    let recreate_id = AgentId::from("a-bypass-recreate");
    let recreate_ws = WorkspaceId::from("ws-bypass-recreate");
    seed_agent(&mgr, &recreate_ws, &recreate_id).await;
    mgr.services
        .store
        .set_acp_session_id(&recreate_ws, &recreate_id, "stale-id")
        .await
        .unwrap();
    let (_agent_rc, recreate_log) = track_mock_agent_with_log(&mgr, &recreate_id, false);
    let sid = mgr
        .start_session(&recreate_id, PathBuf::from("/tmp/ws"), &test_provider())
        .await
        .expect("recreate");
    assert_eq!(sid, MGR_ACP_SID);
    let recreate_calls = recreate_log.lock().unwrap().clone();
    let set_mode = recreate_calls
        .iter()
        .find(|(m, _)| m == "session/set_mode")
        .expect("session/set_mode called on recreate path");
    assert_eq!(set_mode.1["sessionId"], MGR_ACP_SID);
    assert_eq!(set_mode.1["modeId"], "bypassPermissions");
}

/// Every non-`AllowAll` policy leaves the provider alone: `Interactive` drives
/// the FE round-trip, `AutoByRisk` / `DenyAll` apply local decisions, and none
/// of them should ask the provider to disable its own prompts.
#[tokio::test]
async fn start_session_skips_bypass_under_non_allow_all_policies() {
    for policy in [
        PermissionPolicy::Interactive,
        PermissionPolicy::AutoByRisk,
        PermissionPolicy::DenyAll,
    ] {
        let (_tmp, mgr) = manager().await;
        let mgr = mgr.with_policy(policy);
        let (ws, id) = (
            WorkspaceId::from("ws-1"),
            AgentId::from(format!("a-no-bypass-{policy:?}").as_str()),
        );
        seed_agent(&mgr, &ws, &id).await;
        let (_agent, log) = track_mock_agent_with_log(&mgr, &id, false);
        mgr.start_session(&id, PathBuf::from("/tmp/ws"), &test_provider())
            .await
            .expect("session");
        let calls = log.lock().unwrap().clone();
        assert!(
            calls.iter().all(|(m, _)| m != "session/set_mode"),
            "policy {policy:?} must not attempt bypassPermissions; got {calls:?}"
        );
    }
}

/// A provider that doesn't advertise a bypass-equivalent in `availableModes`
/// (auggie today: `default`+`ask`) is left alone under `AllowAll` rather than
/// being hit with `session/set_mode bypassPermissions` and getting `-32602`.
/// The local `AllowAll` auto-approve carries the parity contract by itself.
#[tokio::test]
async fn start_session_skips_bypass_when_provider_doesnt_advertise_bypass_mode() {
    let (_tmp, mgr) = manager().await;
    assert_eq!(mgr.policy(), PermissionPolicy::AllowAll);
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-no-bypass-cap"));
    seed_agent(&mgr, &ws, &id).await;
    // Mock advertises `default`+`ask` only, mirroring what auggie returns today.
    let (_agent, log) = track_mock_agent_with_log_modes(&mgr, &id, false, MockModes::no_bypass());
    mgr.start_session(&id, PathBuf::from("/tmp/ws"), &test_provider())
        .await
        .expect("session");
    let calls = log.lock().unwrap().clone();
    assert!(
        calls.iter().all(|(m, _)| m != "session/set_mode"),
        "no session/set_mode when provider doesn't advertise a bypass-equivalent; got {calls:?}"
    );
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

    let prompt = mgr
        .build_turn_prompt(&id, &ws, "current message", &super::TurnOptions::default())
        .await;
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
    let plain = mgr
        .build_turn_prompt(&id, &ws, "next message", &super::TurnOptions::default())
        .await;
    let plain_text = serde_json::to_value(&plain).unwrap()[0]["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(plain_text, "next message");
}

// --- Attachment blocks (image + file) ----------------------------------------

/// FE-supplied `imageBlocks` become ACP `image` content blocks appended after
/// the text prompt (reference-parity `acp-provider.ts`), preserving `data`
/// and `mimeType` verbatim in the camelCase wire shape.
#[tokio::test]
async fn build_turn_prompt_appends_image_blocks_after_text() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-img"), AgentId::from("a-img"));
    seed_agent(&mgr, &ws, &id).await;

    let options = super::TurnOptions {
        image_blocks: Some(json!([
            {"data": "AAAA", "mimeType": "image/png"},
            {"data": "BBBB", "mimeType": "image/jpeg"},
        ])),
        ..super::TurnOptions::default()
    };
    let prompt = mgr.build_turn_prompt(&id, &ws, "hi", &options).await;
    let wire = serde_json::to_value(&prompt).unwrap();
    let arr = wire.as_array().unwrap();
    assert_eq!(arr.len(), 3, "text + 2 image blocks");
    assert_eq!(arr[0]["type"], json!("text"));
    assert_eq!(arr[1]["type"], json!("image"));
    assert_eq!(arr[1]["data"], json!("AAAA"));
    assert_eq!(arr[1]["mimeType"], json!("image/png"));
    assert_eq!(arr[2]["type"], json!("image"));
    assert_eq!(arr[2]["data"], json!("BBBB"));
    assert_eq!(arr[2]["mimeType"], json!("image/jpeg"));
}

/// FE-supplied `fileBlocks` become ACP `resource` content blocks with a
/// `BlobResourceContents` carrying the file name lifted into the resource
/// `uri` (`file:///<fileName>`), appended after any image blocks.
#[tokio::test]
async fn build_turn_prompt_appends_file_blocks_after_text_and_images() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-file"), AgentId::from("a-file"));
    seed_agent(&mgr, &ws, &id).await;

    let options = super::TurnOptions {
        image_blocks: Some(json!([{"data": "IMG", "mimeType": "image/png"}])),
        file_blocks: Some(json!([
            {"data": "Zm9v", "mimeType": "text/plain", "fileName": "notes.txt"},
            {"data": "YmFy", "mimeType": "application/pdf", "fileName": "spec.pdf"},
        ])),
        ..super::TurnOptions::default()
    };
    let prompt = mgr.build_turn_prompt(&id, &ws, "hi", &options).await;
    let wire = serde_json::to_value(&prompt).unwrap();
    let arr = wire.as_array().unwrap();
    assert_eq!(arr.len(), 4, "text + 1 image + 2 file blocks");
    assert_eq!(arr[0]["type"], json!("text"));
    assert_eq!(arr[1]["type"], json!("image"));
    // Images come before files, files come in caller order.
    assert_eq!(arr[2]["type"], json!("resource"));
    assert_eq!(arr[2]["resource"]["blob"], json!("Zm9v"));
    assert_eq!(arr[2]["resource"]["mimeType"], json!("text/plain"));
    assert_eq!(arr[2]["resource"]["uri"], json!("file:///notes.txt"));
    assert_eq!(arr[3]["type"], json!("resource"));
    assert_eq!(arr[3]["resource"]["blob"], json!("YmFy"));
    assert_eq!(arr[3]["resource"]["mimeType"], json!("application/pdf"));
    assert_eq!(arr[3]["resource"]["uri"], json!("file:///spec.pdf"));
}

/// Malformed attachment entries (missing required fields, wrong types) are
/// silently dropped so a partial array can never poison the whole turn — only
/// the well-formed sibling blocks reach the prompt.
#[tokio::test]
async fn build_turn_prompt_skips_malformed_attachments() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-bad"), AgentId::from("a-bad"));
    seed_agent(&mgr, &ws, &id).await;

    let options = super::TurnOptions {
        image_blocks: Some(json!([
            {"data": "OK"},                        // missing mimeType
            {"data": 42, "mimeType": "image/png"}, // wrong type
            {"data": "GOOD", "mimeType": "image/png"},
        ])),
        file_blocks: Some(json!([
            {"mimeType": "text/plain", "fileName": "x.txt"},   // missing data
            {"data": "d", "fileName": "x.txt"},                 // missing mimeType
            {"data": "d", "mimeType": "text/plain"},            // missing fileName
            {"data": "d", "mimeType": "text/plain", "fileName": "keep.txt"},
        ])),
        ..super::TurnOptions::default()
    };
    let prompt = mgr.build_turn_prompt(&id, &ws, "hi", &options).await;
    let wire = serde_json::to_value(&prompt).unwrap();
    let arr = wire.as_array().unwrap();
    // text + 1 well-formed image + 1 well-formed file.
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[1]["data"], json!("GOOD"));
    assert_eq!(arr[2]["resource"]["uri"], json!("file:///keep.txt"));
}

// --- First-turn workspace-naming instruction ---------------------------------

/// Seed an agent whose workspace already carries `title` (used by naming-instruction
/// tests to distinguish slug-shaped vs custom titles).
async fn seed_agent_with_title(mgr: &AgentManager, ws: &WorkspaceId, id: &AgentId, title: &str) {
    seed_agent(mgr, ws, id).await;
    let mut workspace = mgr.services.store.get_workspace(ws).await.unwrap();
    workspace.title = title.to_string();
    mgr.services
        .store
        .update_workspace(&workspace)
        .await
        .expect("update ws title");
}

/// Slug-shaped workspace title on an agent's first turn → the naming instruction
/// is prepended as a `<system>` block naming the daemon MCP tool
/// (`set_workspace_title_workspace-mcp`), not the FE `workspace_api` surface.
#[tokio::test]
async fn build_turn_prompt_injects_naming_instruction_for_slug_title() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-slug"), AgentId::from("a-slug"));
    seed_agent_with_title(&mgr, &ws, &id, "amber-fox").await;
    // Persist the current user turn so `build_turn_prompt` sees the "first
    // turn" shape (one user message, zero assistant messages).
    mgr.services
        .store
        .append_agent_message(
            &id,
            "user",
            &json!([{ "type": "text", "text": "hello" }]),
            &now_iso(),
        )
        .await
        .unwrap();

    let prompt = mgr
        .build_turn_prompt(&id, &ws, "hello", &super::TurnOptions::default())
        .await;
    let text = serde_json::to_value(&prompt).unwrap()[0]["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        text.starts_with("<system>"),
        "naming instruction prepends the prompt: {text:?}"
    );
    assert!(
        text.contains("`set_workspace_title_workspace-mcp`"),
        "instruction names the daemon MCP tool: {text:?}"
    );
    assert!(
        !text.contains("workspace_api"),
        "instruction must not reference the FE workspace_api surface: {text:?}"
    );
    assert!(text.trim_end().ends_with("hello"));
}

/// Empty workspace title on an agent's first turn → the naming instruction
/// still fires (Untitled parity: `create_workspace` now stores `""` when the
/// caller omits a title, and `needsWorkspaceRename` treats empty/whitespace
/// titles as "needs rename").
#[tokio::test]
async fn build_turn_prompt_injects_naming_instruction_for_empty_title() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-empty"), AgentId::from("a-empty"));
    seed_agent_with_title(&mgr, &ws, &id, "").await;
    mgr.services
        .store
        .append_agent_message(
            &id,
            "user",
            &json!([{ "type": "text", "text": "hello" }]),
            &now_iso(),
        )
        .await
        .unwrap();

    let prompt = mgr
        .build_turn_prompt(&id, &ws, "hello", &super::TurnOptions::default())
        .await;
    let text = serde_json::to_value(&prompt).unwrap()[0]["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        text.starts_with("<system>"),
        "empty title still triggers the naming instruction: {text:?}"
    );
    assert!(text.contains("`set_workspace_title_workspace-mcp`"));
}

/// Custom workspace title on an agent's first turn → no naming instruction is
/// injected (the reference `needsWorkspaceRename` guard skips already-titled
/// workspaces).
#[tokio::test]
async fn build_turn_prompt_skips_naming_instruction_for_custom_title() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-custom"), AgentId::from("a-custom"));
    seed_agent_with_title(&mgr, &ws, &id, "Add dark mode support").await;
    mgr.services
        .store
        .append_agent_message(
            &id,
            "user",
            &json!([{ "type": "text", "text": "hi" }]),
            &now_iso(),
        )
        .await
        .unwrap();

    let prompt = mgr
        .build_turn_prompt(&id, &ws, "hi", &super::TurnOptions::default())
        .await;
    let text = serde_json::to_value(&prompt).unwrap()[0]["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(text, "hi", "no naming block for already-titled workspaces");
}

/// Slug-shaped title but an assistant message already exists → the naming
/// instruction fires only on the FIRST turn and stays absent for every turn
/// after (reference `!messages.some(m => m.role === 'assistant')`).
#[tokio::test]
async fn build_turn_prompt_skips_naming_instruction_after_first_turn() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-second"), AgentId::from("a-second"));
    seed_agent_with_title(&mgr, &ws, &id, "amber-fox").await;
    for (role, text) in [
        ("user", "first question"),
        ("assistant", "first answer"),
        ("user", "follow-up"),
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

    let prompt = mgr
        .build_turn_prompt(&id, &ws, "follow-up", &super::TurnOptions::default())
        .await;
    let text = serde_json::to_value(&prompt).unwrap()[0]["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        !text.contains("<system>"),
        "second-turn prompt carries no naming instruction: {text:?}"
    );
    assert_eq!(text, "follow-up");
}

/// STAB-28: The keep-alive interrupt path emits `agent:stream:end` and NOW
/// ALSO emits `agent:idle` when the agent has no queued ready-to-send messages.
/// This fixes the bug where a parent that re-messages via agent.send after a
/// child settles registers a completion watch that never fires (the aborted
/// worker never reaches run_prompt_turn's idle-emit path). When the agent DOES
/// have queued messages, idle is suppressed (the agent will resume immediately).
#[tokio::test]
async fn interrupt_emits_terminal_stream_end_and_idle_when_no_queue() {
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-int"));
    seed_agent(&mgr, &ws, &id).await;
    let _agent = track_mock_agent(&mgr, &id, false);
    // An `acpSessionId` is required for the keep-alive interrupt (otherwise
    // `interrupt` falls back to the hard `stop` kill path).
    mgr.services
        .store
        .set_acp_session_id(&ws, &id, "acp-int")
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
        types.contains(&"agent:idle"),
        "STAB-28: interrupt NOW emits agent:idle when queue is empty (got {types:?})"
    );
}

/// `priority: "interrupt"` delivery to a BUSY agent preempts the turn
/// keep-alive: the message streams immediately (`queued: false`) instead of
/// queueing behind the turn, the preemption emits the terminal
/// `agent:stream:end`, and the child handle survives — the agent is never
/// killed (contrast `force_message`, which tears the child down).
#[tokio::test]
async fn interrupt_send_message_preempts_busy_turn_without_kill() {
    let (_tmp, mgr, bus) = manager_with_bus().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-int-send"));
    seed_agent(&mgr, &ws, &id).await;
    let _agent = track_mock_agent(&mgr, &id, false);
    // A live `acpSessionId` keeps the preemption on the keep-alive interrupt
    // path (no session → `interrupt` would fall back to the kill path).
    mgr.services
        .store
        .set_acp_session_id(&ws, &id, "acp-int-send")
        .await
        .unwrap();
    // Claim the in-flight slot so the send sees a busy (mid-turn) agent.
    assert!(mgr.try_begin(&id, &ws).await);

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    let result = mgr
        .interrupt_send_message(
            id.clone(),
            ws.clone(),
            "urgent".to_string(),
            None,
            super::TurnOptions::default(),
        )
        .await
        .expect("interrupt send");
    assert_eq!(result["success"], json!(true));
    assert_eq!(
        result["queued"],
        json!(false),
        "interrupt priority streams immediately, never queues: {result}"
    );
    assert!(result["messageId"].is_string());

    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let types: Vec<&str> = events.iter().map(|e| e.event_type.as_str()).collect();
    assert!(
        types.contains(&"agent:stream:end"),
        "preemption emits the terminal stream:end (got {types:?})"
    );
    assert!(
        mgr.handles.lock().unwrap().contains_key(&id),
        "the child handle survives the interrupt (never killed)"
    );
    // The interrupt message was persisted as the next user turn.
    let session = mgr.services.store.get_agent_session(&id).await.unwrap();
    let last = session.messages.last().expect("message persisted");
    assert_eq!(last.role, "user");
    assert!(serde_json::to_string(&last.content)
        .unwrap()
        .contains("urgent"));
}

/// Interrupt-priority delivery to an IDLE agent falls through to the plain
/// `send_message` path unchanged: `{ success, queued: false, messageId }`.
#[tokio::test]
async fn interrupt_send_message_idle_agent_falls_through_to_send() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-int-idle"));
    seed_agent(&mgr, &ws, &id).await;
    let _agent = track_mock_agent(&mgr, &id, false);
    mgr.services
        .store
        .set_acp_session_id(&ws, &id, "acp-int-idle")
        .await
        .unwrap();

    let result = mgr
        .interrupt_send_message(
            id.clone(),
            ws.clone(),
            "hello".to_string(),
            None,
            super::TurnOptions::default(),
        )
        .await
        .expect("idle interrupt send");
    assert_eq!(result["success"], json!(true));
    assert_eq!(result["queued"], json!(false));
    assert!(result["messageId"].is_string());
    assert!(
        mgr.handles.lock().unwrap().contains_key(&id),
        "idle fall-through never touches the handle"
    );
}

/// The SAME interrupt-priority message (same `messageId`) delivered twice in
/// quick succession preempts exactly once: the duplicate is acknowledged
/// idempotently (`deduplicated: true`) without cancelling the interrupt turn
/// it raced and without re-persisting the message; the child handle survives
/// and the agent never reaches a failed status. A DISTINCT `messageId` is a
/// genuinely new interrupt and still preempts.
#[tokio::test]
async fn duplicate_interrupt_send_same_message_id_preempts_once() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-int-dup"));
    seed_agent(&mgr, &ws, &id).await;
    let _agent = track_mock_agent(&mgr, &id, false);
    mgr.services
        .store
        .set_acp_session_id(&ws, &id, "acp-int-dup")
        .await
        .unwrap();
    // Claim the in-flight slot so the first delivery preempts a busy turn.
    assert!(mgr.try_begin(&id, &ws).await);

    let first = mgr
        .interrupt_send_message(
            id.clone(),
            ws.clone(),
            "dup-urgent".to_string(),
            Some("user-msg-dup".to_string()),
            super::TurnOptions::default(),
        )
        .await
        .expect("first interrupt send");
    assert_eq!(first["success"], json!(true));
    assert_eq!(first["queued"], json!(false));
    assert_eq!(first["messageId"], json!("user-msg-dup"));

    // Duplicate delivery of the SAME message id, racing the interrupt turn the
    // first delivery just started: acknowledged, no second preemption/persist.
    let second = mgr
        .interrupt_send_message(
            id.clone(),
            ws.clone(),
            "dup-urgent".to_string(),
            Some("user-msg-dup".to_string()),
            super::TurnOptions::default(),
        )
        .await
        .expect("duplicate interrupt send");
    assert_eq!(second["success"], json!(true));
    assert_eq!(
        second["deduplicated"],
        json!(true),
        "duplicate is acknowledged idempotently: {second}"
    );
    assert_eq!(second["messageId"], json!("user-msg-dup"));

    // Not double-persisted: exactly ONE user message carries the content.
    let session = mgr.services.store.get_agent_session(&id).await.unwrap();
    let dup_count = session
        .messages
        .iter()
        .filter(|m| {
            m.role == "user"
                && serde_json::to_string(&m.content)
                    .unwrap()
                    .contains("dup-urgent")
        })
        .count();
    assert_eq!(dup_count, 1, "duplicate delivery must not double-persist");
    assert!(
        mgr.handles.lock().unwrap().contains_key(&id),
        "the child handle survives the duplicate (never killed)"
    );
    assert_ne!(
        session.status,
        AgentStatus::Error,
        "the agent never transitions to a failed status"
    );

    // A DISTINCT message id is a new interrupt, not a duplicate: it preempts
    // (or claims the idle slot) and persists normally.
    let third = mgr
        .interrupt_send_message(
            id.clone(),
            ws.clone(),
            "next-urgent".to_string(),
            Some("user-msg-next".to_string()),
            super::TurnOptions::default(),
        )
        .await
        .expect("distinct interrupt send");
    assert_eq!(third["success"], json!(true));
    assert!(
        third.get("deduplicated").is_none(),
        "a new messageId is never deduplicated: {third}"
    );
}

/// Interrupt-priority delivery during TURN STARTUP (busy slot claimed but no
/// cancellable turn yet — no `acpSessionId` persisted, the spawn/`session/new`
/// window): preemption is skipped (a keep-alive interrupt is impossible and
/// falling back to `stop` would kill the child) and the message queues behind
/// the starting turn instead. The child handle survives, the starting turn is
/// left intact, and the agent never reaches a failed status.
#[tokio::test]
async fn interrupt_send_during_turn_startup_queues_keep_alive() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (WorkspaceId::from("ws-1"), AgentId::from("a-int-startup"));
    seed_agent(&mgr, &ws, &id).await;
    // Child handle live but NO `acpSessionId` yet — `session/new` in flight.
    let _agent = track_mock_agent(&mgr, &id, false);
    assert!(mgr.try_begin(&id, &ws).await);

    let result = mgr
        .interrupt_send_message(
            id.clone(),
            ws.clone(),
            "early interrupt".to_string(),
            Some("user-msg-early".to_string()),
            super::TurnOptions::default(),
        )
        .await
        .expect("startup-window interrupt send");
    assert_eq!(result["success"], json!(true));
    assert_eq!(
        result["queued"],
        json!(true),
        "startup window queues keep-alive instead of preempting: {result}"
    );
    assert_eq!(result["queuedMessage"]["content"], json!("early interrupt"));
    assert!(
        mgr.handles.lock().unwrap().contains_key(&id),
        "the child handle survives (no stop-kill fallback)"
    );
    assert!(
        mgr.is_busy(&id),
        "the starting turn keeps its in-flight slot"
    );
    let session = mgr.services.store.get_agent_session(&id).await.unwrap();
    assert_ne!(
        session.status,
        AgentStatus::Error,
        "the agent never transitions to a failed status"
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
        completion_report: None,
        completion_report_timestamp: None,
        delegation_depth: None,
        initial_message: None,
        context_references: None,
        image_blocks: None,
        is_background: false,
        metadata: None,
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
async fn default_policy_is_allow_all_and_overridable() {
    let (_tmp, mgr) = manager().await;
    // Shipped default (§6.7/M3.5): reference parity with the TS acp-provider —
    // `start_session` best-effort sets `bypassPermissions` on providers that
    // advertise set-mode, and `AllowAll` auto-approves anything the provider
    // still surfaces.
    assert_eq!(mgr.policy(), PermissionPolicy::AllowAll);
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

/// Insert an additional session row into an existing workspace (companion to
/// [`seed_agent`], which also inserts the workspace and so cannot be called
/// twice with the same `ws`).
async fn insert_extra_session(mgr: &AgentManager, ws: &WorkspaceId, id: &AgentId) {
    let ts = now_iso();
    let session = AgentSession {
        id: id.clone(),
        workspace_id: ws.clone(),
        parent_agent_id: None,
        backend_session_id: None,
        acp_session_id: None,
        name: "Extra".to_string(),
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
        completion_report: None,
        completion_report_timestamp: None,
        delegation_depth: None,
        initial_message: None,
        context_references: None,
        image_blocks: None,
        is_background: false,
        metadata: None,
        created_at: ts.clone(),
        updated_at: ts,
    };
    mgr.services
        .store
        .insert_agent_session(&session)
        .await
        .expect("insert extra session");
}

/// `workspace.delete` walks every session in the workspace through
/// `AgentManager::stop`: the tracked handles, workers, in-flight busy set, and
/// `agent_ws` map all drain, and the workspace insert itself is idempotent —
/// a same-slug recreate observes zero pre-existing agents.
#[tokio::test]
async fn delete_workspace_stops_live_agents_and_leaves_no_ghost_state() {
    // Build the manager inline so we can pin a hermetic `workspaces_root` on
    // Services — the delete path walks it to unlink the daemon-owned
    // workspace dir and must never fall through to the real user home.
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store)
        .with_event_bus(bus.clone())
        .with_workspaces_root(tmp.path.with_extension("workspaces"));
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let mgr = Arc::new(AgentManager::new(services.clone(), sink, 8));
    services.attach_agent_manager(&mgr);

    let ws = WorkspaceId::from("ws-delete");
    let live = AgentId::from("a-live");
    let busy = AgentId::from("a-busy-mid-turn");
    // Seed the workspace once (`seed_agent` re-inserts it on every call).
    seed_agent(&mgr, &ws, &live).await;
    insert_extra_session(&mgr, &ws, &busy).await;
    track(&mgr, &live);
    track(&mgr, &busy);
    // Simulate a mid-turn worker: claim the in-flight slot AND register a
    // JoinHandle in the workers map (the two pieces of state `stop` clears).
    assert!(mgr.try_begin(&busy, &ws).await);
    let worker = tokio::spawn(async {
        // A never-ending worker; `stop` must abort it.
        std::future::pending::<()>().await;
    });
    mgr.workers.lock().unwrap().insert(busy.clone(), worker);
    assert!(mgr.is_busy(&busy));
    assert!(mgr.contains(&live) && mgr.contains(&busy));
    assert_eq!(mgr.registry().size(), 2);

    <Services as WorkspaceApi>::delete_workspace(&services, ws.clone())
        .await
        .expect("delete workspace");

    // Every tracked handle is gone; the process registry is empty; the
    // busy set + agent_ws map + workers map all drained.
    assert!(!mgr.contains(&live), "live handle removed");
    assert!(!mgr.contains(&busy), "busy handle removed");
    assert_eq!(mgr.registry().size(), 0, "registry emptied");
    assert!(!mgr.is_busy(&busy), "busy flag cleared");
    assert!(mgr.workers.lock().unwrap().is_empty(), "worker map cleared");
    assert!(mgr.agent_ws.lock().unwrap().is_empty(), "agent_ws cleared");

    // A same-slug recreate finds zero pre-existing agents.
    let store = Store::open(&tmp.path).await.expect("reopen store");
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
    };
    store
        .insert_workspace(&workspace)
        .await
        .expect("re-insert same-slug workspace");
    let sessions = store
        .list_agent_sessions(&ws)
        .await
        .expect("list on recreated ws");
    assert!(sessions.is_empty(), "recreated workspace shows no ghosts");
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
        .enqueue_message(&id, "queued".to_string(), None, None);
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
        .send_message(
            id.clone(),
            ws.clone(),
            "queued".to_string(),
            None,
            super::TurnOptions::default(),
        )
        .await
        .expect("send_message returns the queued envelope");
    assert_eq!(result["success"], json!(true));
    assert_eq!(result["queued"], json!(true));
    assert_eq!(result["queuedMessage"]["content"], json!("queued"));
    assert_eq!(result["queuedMessage"]["position"], json!(0));
    assert_eq!(mgr.services.queue_snapshot(&id).len(), 1);
}

/// When `send_message` hits the busy auto-queue fallback, the caller's
/// image + file blocks are preserved on the queued entry (the wire snapshot
/// includes both) so the eventual drain turn reaches the agent with the same
/// ACP content blocks.
#[tokio::test]
async fn send_message_auto_queue_preserves_image_and_file_blocks() {
    let (_tmp, mgr) = manager().await;
    let mgr = Arc::new(mgr);
    let (ws, id) = (
        WorkspaceId::from("ws-q-blocks"),
        AgentId::from("a-q-blocks"),
    );
    assert!(mgr.try_begin(&id, &ws).await);

    let options = super::TurnOptions {
        image_blocks: Some(json!([{"data": "IMG", "mimeType": "image/png"}])),
        file_blocks: Some(json!([
            {"data": "FILE", "mimeType": "text/plain", "fileName": "n.txt"}
        ])),
        ..super::TurnOptions::default()
    };
    let result = mgr
        .send_message(id.clone(), ws.clone(), "hi".to_string(), None, options)
        .await
        .expect("queued");
    assert_eq!(result["queued"], json!(true));
    let snap = mgr.services.queue_snapshot(&id);
    assert_eq!(snap.len(), 1);
    assert_eq!(
        snap[0]["imageBlocks"],
        json!([{"data": "IMG", "mimeType": "image/png"}]),
        "image blocks land on the queued entry"
    );
    assert_eq!(
        snap[0]["fileBlocks"],
        json!([{"data": "FILE", "mimeType": "text/plain", "fileName": "n.txt"}]),
        "file blocks land on the queued entry"
    );
}

/// enqueue → dequeue round-trip preserves both attachment arrays so the drain
/// path can pipe them into the next turn's `TurnOptions`.
#[tokio::test]
async fn queue_dequeue_round_trip_preserves_image_and_file_blocks() {
    let (_tmp, mgr) = manager().await;
    let id = AgentId::from("a-rt");
    let images = Some(json!([{"data": "I", "mimeType": "image/png"}]));
    let files = Some(json!([
        {"data": "F", "mimeType": "text/plain", "fileName": "r.txt"}
    ]));
    mgr.services
        .enqueue_message(&id, "msg".to_string(), images.clone(), files.clone());
    let drained = mgr
        .services
        .dequeue_message(&id)
        .expect("dequeue returns the head");
    assert_eq!(drained.content, "msg");
    assert_eq!(drained.image_blocks, images);
    assert_eq!(drained.file_blocks, files);
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
        pull_requests: None,
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
        pull_requests: None,
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

// --- Context references → stdinContext builder (Fidelity B) ---------------

/// Port parity: the builder emits one entry per reference in order, with
/// the reference labels (`Selected text:`, `Task:`, `Code:`, `File <p>:`,
/// `Linear Issue:`, `GitHub Issue:`, `Sentry Issue:`, `Terminal ...`,
/// `Note: <id>`) and joins them with `\n\n`.
#[test]
fn build_stdin_context_from_context_references_ports_reference_shapes() {
    let refs = json!([
        {"type": "selection", "content": "hello"},
        {"type": "task", "taskText": "do the thing"},
        {"type": "code_chunk", "codeChunk": "fn foo() {}"},
        {"type": "file", "path": "src/a.rs", "content": "pub fn a() {}"},
        {"type": "file", "filePath": "src/only-path.rs"},
        {"type": "linear-issue", "content": "XYZ-1 title"},
        {"type": "github-issue", "content": "#42 title"},
        {"type": "sentry-issue", "content": "issue text"},
        {
            "type": "terminal",
            "content": "$ ls",
            "metadata": {"terminalId": "t1", "terminalName": "build"}
        },
        {"type": "note", "noteId": "note-1"},
        {"type": "note", "metadata": {"noteId": "note-2"}},
    ]);
    let out =
        super::build_stdin_context_from_context_references(Some(&refs)).expect("non-empty context");
    let parts: Vec<&str> = out.split("\n\n").collect();
    assert_eq!(parts.len(), 11);
    assert_eq!(parts[0], "Selected text:\nhello");
    assert_eq!(parts[1], "Task:\ndo the thing");
    assert_eq!(parts[2], "Code:\nfn foo() {}");
    assert_eq!(parts[3], "File src/a.rs:\npub fn a() {}");
    assert_eq!(parts[4], "File: src/only-path.rs");
    assert_eq!(parts[5], "Linear Issue:\nXYZ-1 title");
    assert_eq!(parts[6], "GitHub Issue:\n#42 title");
    assert_eq!(parts[7], "Sentry Issue:\nissue text");
    assert_eq!(parts[8], "Terminal \"build\" (terminal_id: t1):\n$ ls");
    assert_eq!(parts[9], "Note: note-1");
    assert_eq!(parts[10], "Note: note-2");
}

/// Empty / absent inputs collapse to `None` so the prompt is left unchanged.
#[test]
fn build_stdin_context_from_context_references_empty_is_none() {
    assert!(super::build_stdin_context_from_context_references(None).is_none());
    assert!(super::build_stdin_context_from_context_references(Some(&json!([]))).is_none());
    // Only-unsupported entries also collapse to None.
    assert!(
        super::build_stdin_context_from_context_references(Some(&json!([
            {"type": "note"}, {"type": "file"}
        ])))
        .is_none()
    );
}

/// End-to-end prompt shape: when `stdin_context` is absent but
/// `context_references` yield content, `build_turn_prompt` prepends a
/// `Context:` block synthesised by the builder; an explicit
/// `stdin_context` still wins over the fallback.
#[tokio::test]
async fn build_turn_prompt_uses_context_references_when_stdin_context_is_absent() {
    let (_tmp, mgr) = manager().await;
    let (ws, id) = (WorkspaceId::from("ws-ctx"), AgentId::from("a-ctx"));
    seed_agent(&mgr, &ws, &id).await;

    // Synthesised path.
    let options = super::TurnOptions {
        context_references: Some(json!([
            {"type": "selection", "content": "selected"},
            {"type": "file", "path": "a.rs", "content": "pub fn a() {}"},
        ])),
        ..super::TurnOptions::default()
    };
    let prompt = mgr.build_turn_prompt(&id, &ws, "do it", &options).await;
    let text = serde_json::to_value(&prompt).unwrap()[0]["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        text.starts_with(
            "Context:\nSelected text:\nselected\n\nFile a.rs:\npub fn a() {}\n\n---\n\n"
        ),
        "unexpected prompt: {text:?}"
    );
    assert!(text.ends_with("do it"));

    // Explicit stdin_context wins over the synthesised fallback.
    let options = super::TurnOptions {
        stdin_context: Some("explicit".to_string()),
        context_references: Some(json!([
            {"type": "selection", "content": "ignored"}
        ])),
        ..super::TurnOptions::default()
    };
    let prompt = mgr.build_turn_prompt(&id, &ws, "do it", &options).await;
    let text = serde_json::to_value(&prompt).unwrap()[0]["text"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(text.starts_with("Context:\nexplicit\n\n---\n\n"));
    assert!(!text.contains("ignored"));
}

/// `noteIds` (PROTOCOL §5.5): the resolver loads workspace-asset
/// images referenced by each note's markdown content, appends them as
/// ACP `image` content blocks, and adds a system text notice so the
/// agent knows the images are inlined (parity with the FE extraction
/// in `agent-backend-handler.service.ts`).
#[tokio::test]
async fn build_turn_prompt_resolves_note_ids_to_image_blocks() {
    use base64::Engine as _;
    use intent_core::{ContentType, Note, NoteId, NoteMetadata, NoteVisibility};

    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let assets_dir =
        std::env::temp_dir().join(format!("intentd-note-img-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&assets_dir).expect("assets tempdir");
    let services = Services::new(store.clone())
        .with_event_bus(bus.clone())
        .with_assets_root(assets_dir.clone());
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let mgr = AgentManager::new(services, sink, 8);

    let ws = WorkspaceId::from("ws-note-img");
    let id = AgentId::from("a-note-img");
    seed_agent(&mgr, &ws, &id).await;

    // Write an on-disk asset the note will reference.
    let asset_id = "asset-abc.png";
    let asset_bytes: &[u8] = b"pretend-png";
    let ws_dir = assets_dir.join(&ws.0);
    std::fs::create_dir_all(&ws_dir).expect("asset dir");
    std::fs::write(ws_dir.join(asset_id), asset_bytes).expect("write asset");

    // Persist a note whose markdown references the asset URL.
    let note_id = NoteId::new();
    let ts = now_iso();
    let note = Note {
        id: note_id.clone(),
        workspace_id: ws.clone(),
        title: "Spec".to_string(),
        content: format!(
            "# Screenshot\n\n![shot](workspace-asset://{ws}/{asset})\n",
            ws = ws.0,
            asset = asset_id,
        ),
        content_type: ContentType::Markdown,
        tags: vec![],
        is_pinned: false,
        is_archived: false,
        is_default: false,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        metadata: NoteMetadata::default(),
        created_at: ts.clone(),
        rev: 0,
        updated_at: ts,
    };
    store.insert_note(&note).await.expect("insert note");

    let options = super::TurnOptions {
        note_ids: Some(json!([note_id.to_string()])),
        ..super::TurnOptions::default()
    };
    let prompt = mgr.build_turn_prompt(&id, &ws, "look", &options).await;
    let wire = serde_json::to_value(&prompt).unwrap();
    let arr = wire.as_array().unwrap();
    // Expect: original text prompt, image block, system notice.
    assert_eq!(arr.len(), 3, "text + image + notice");
    assert_eq!(arr[0]["type"], json!("text"));
    assert!(arr[0]["text"].as_str().unwrap().contains("look"));
    assert_eq!(arr[1]["type"], json!("image"));
    let expected_b64 = base64::engine::general_purpose::STANDARD.encode(asset_bytes);
    assert_eq!(arr[1]["data"], json!(expected_b64));
    assert_eq!(arr[1]["mimeType"], json!("image/png"));
    assert_eq!(arr[2]["type"], json!("text"));
    assert!(arr[2]["text"].as_str().unwrap().contains("1 image(s)"));

    // A cross-workspace URL is silently skipped (no image, no notice).
    let stray_id = NoteId::new();
    let stray = Note {
        id: stray_id.clone(),
        content: format!(
            "![x](workspace-asset://other-ws/{asset})\n",
            asset = asset_id
        ),
        ..note.clone()
    };
    let mut stray = stray;
    stray.id = stray_id.clone();
    store.insert_note(&stray).await.expect("insert stray");
    let options = super::TurnOptions {
        note_ids: Some(json!([stray_id.to_string()])),
        ..super::TurnOptions::default()
    };
    let prompt = mgr.build_turn_prompt(&id, &ws, "look", &options).await;
    let arr_json = serde_json::to_value(&prompt).unwrap();
    let arr = arr_json.as_array().unwrap();
    assert_eq!(arr.len(), 1, "only text; stray URL is skipped");
}

/// A10 — daemon-side merge of user MCP servers into the agent spawn config.
/// Directly exercises [`AgentManager::merge_user_mcp_servers`] against an
/// [`InMemorySecretStore`] and a fresh store so the tests stay hermetic (no
/// keychain / real bridge involved).
mod merge_user_mcp_servers_tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use intent_acp::{NormalizedMcpServer, NormalizedMcpServers};
    use serde_json::json;

    use super::{manager, TempDb};
    use crate::agent_manager::AgentManager;
    use crate::agent_manager::BusEventSink;
    use crate::events::EventBus;
    use crate::settings::{InMemorySecretStore, SecretStore};
    use crate::Services;
    use intent_acp::EventSink;
    use intent_store::Store;

    async fn manager_with_secrets() -> (TempDb, AgentManager, Arc<InMemorySecretStore>) {
        let tmp = super::TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let bus = EventBus::new(store.clone());
        let secrets = Arc::new(InMemorySecretStore::default());
        let services = Services::new(store)
            .with_event_bus(bus.clone())
            .with_secret_store(secrets.clone() as Arc<dyn SecretStore>);
        let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
        (tmp, AgentManager::new(services, sink, 8), secrets)
    }

    fn write_servers(secrets: &InMemorySecretStore, servers: serde_json::Value) {
        secrets
            .store("mcp.servers", &serde_json::to_string(&servers).unwrap())
            .expect("write mcp.servers");
    }

    #[tokio::test]
    async fn skips_when_enable_user_servers_disabled() {
        let (_tmp, mgr, secrets) = manager_with_secrets().await;
        write_servers(
            &secrets,
            json!({ "srv-1": { "id": "srv-1", "name": "u", "transport": "stdio",
                                 "command": "node", "enabled": true } }),
        );
        mgr.services
            .store
            .set_setting("mcp.enableUserServers", "false")
            .await
            .unwrap();
        let mut out = NormalizedMcpServers::new();
        mgr.merge_user_mcp_servers(&mut out).await.unwrap();
        assert!(out.is_empty(), "gate off → nothing merged: {:?}", out);
    }

    #[tokio::test]
    async fn merges_enabled_stdio_server_by_name() {
        let (_tmp, mgr, secrets) = manager_with_secrets().await;
        write_servers(
            &secrets,
            json!({
                "srv-1": {
                    "id": "srv-1", "name": "my-tool", "transport": "stdio",
                    "command": "node", "args": ["srv.js"], "enabled": true,
                    "env": { "A": "1" }
                }
            }),
        );
        let mut out = NormalizedMcpServers::new();
        mgr.merge_user_mcp_servers(&mut out).await.unwrap();
        let entry = out.get("my-tool").expect("keyed by name, not id");
        match entry {
            NormalizedMcpServer::Stdio { command, args, env } => {
                assert_eq!(command, "node");
                assert_eq!(args, &vec!["srv.js".to_string()]);
                assert_eq!(env.get("A").map(String::as_str), Some("1"));
            }
            other => panic!("expected stdio, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn skips_disabled_and_globally_disabled_servers() {
        let (_tmp, mgr, secrets) = manager_with_secrets().await;
        write_servers(
            &secrets,
            json!({
                "srv-off": { "id": "srv-off", "name": "off", "transport": "stdio",
                              "command": "node", "enabled": false },
                "srv-glo": { "id": "srv-glo", "name": "glo", "transport": "stdio",
                              "command": "node", "enabled": true },
                "srv-on":  { "id": "srv-on",  "name": "on",  "transport": "stdio",
                              "command": "node", "enabled": true }
            }),
        );
        mgr.services
            .store
            .set_setting(
                "mcp.disabledServers",
                &serde_json::to_string(&json!(["srv-glo"])).unwrap(),
            )
            .await
            .unwrap();
        let mut out = NormalizedMcpServers::new();
        mgr.merge_user_mcp_servers(&mut out).await.unwrap();
        let names: HashSet<_> = out.keys().cloned().collect();
        assert!(names.contains("on"), "enabled+not-disabled kept: {names:?}");
        assert!(!names.contains("off"), "enabled=false dropped");
        assert!(!names.contains("glo"), "globally-disabled dropped");
    }

    #[tokio::test]
    async fn injects_oauth_authorization_header_for_http() {
        let (_tmp, mgr, secrets) = manager_with_secrets().await;
        write_servers(
            &secrets,
            json!({
                "srv-remote": {
                    "id": "srv-remote", "name": "remote", "transport": "http",
                    "url": "https://example.test/mcp", "enabled": true
                }
            }),
        );
        mgr.services
            .store
            .set_mcp_oauth_token(
                "srv-remote",
                &serde_json::to_string(
                    &json!({ "access_token": "tok-xyz", "token_type": "bearer" }),
                )
                .unwrap(),
                "2026-07-05T00:00:00Z",
            )
            .await
            .unwrap();
        let mut out = NormalizedMcpServers::new();
        mgr.merge_user_mcp_servers(&mut out).await.unwrap();
        match out.get("remote").expect("http server merged") {
            NormalizedMcpServer::Http { url, headers } => {
                assert_eq!(url, "https://example.test/mcp");
                let headers = headers.as_ref().expect("auth header written");
                assert_eq!(
                    headers.get("Authorization").map(String::as_str),
                    Some("Bearer tok-xyz"),
                    "token_type title-cased, access_token appended",
                );
            }
            other => panic!("expected http, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn preserves_existing_authorization_header() {
        let (_tmp, mgr, secrets) = manager_with_secrets().await;
        write_servers(
            &secrets,
            json!({
                "srv-remote": {
                    "id": "srv-remote", "name": "remote", "transport": "sse",
                    "url": "https://example.test/sse", "enabled": true,
                    "headers": { "Authorization": "Basic user:pass" }
                }
            }),
        );
        mgr.services
            .store
            .set_mcp_oauth_token(
                "srv-remote",
                &serde_json::to_string(&json!({ "access_token": "tok-xyz" })).unwrap(),
                "2026-07-05T00:00:00Z",
            )
            .await
            .unwrap();
        let mut out = NormalizedMcpServers::new();
        mgr.merge_user_mcp_servers(&mut out).await.unwrap();
        match out.get("remote").unwrap() {
            NormalizedMcpServer::Sse { headers, .. } => {
                let headers = headers.as_ref().unwrap();
                assert_eq!(
                    headers.get("Authorization").map(String::as_str),
                    Some("Basic user:pass"),
                );
            }
            other => panic!("expected sse, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn does_not_overwrite_reserved_workspace_mcp() {
        let (_tmp, mgr, secrets) = manager_with_secrets().await;
        write_servers(
            &secrets,
            json!({
                "srv-x": { "id": "srv-x", "name": "workspace-mcp", "transport": "stdio",
                             "command": "evil", "enabled": true }
            }),
        );
        let mut out = NormalizedMcpServers::new();
        out.insert(
            "workspace-mcp".to_string(),
            NormalizedMcpServer::Stdio {
                command: "bridge".into(),
                args: vec![],
                env: Default::default(),
            },
        );
        mgr.merge_user_mcp_servers(&mut out).await.unwrap();
        match out.get("workspace-mcp").unwrap() {
            NormalizedMcpServer::Stdio { command, .. } => {
                assert_eq!(command, "bridge", "reserved entry left intact");
            }
            other => panic!("expected stdio, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn empty_secret_is_a_noop() {
        let (_tmp, mgr, _secrets) = manager_with_secrets().await;
        let mut out = NormalizedMcpServers::new();
        mgr.merge_user_mcp_servers(&mut out).await.unwrap();
        assert!(out.is_empty());
    }

    // Prevent dead-code warnings for `manager` when this module compiles alone.
    #[allow(dead_code)]
    async fn _use_manager() {
        let _ = manager().await;
    }
}
