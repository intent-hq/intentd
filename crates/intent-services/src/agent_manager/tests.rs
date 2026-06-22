//! Unit tests for the agent process registry (cap + LRU + lifecycle) and the
//! [`AgentManager`] multiplexing/teardown — parity-checked against
//! `agent-process-registry`.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_acp::{Connection, ConnectionHooks, EventSink};
use intent_core::{AgentId, WorkspaceId};
use intent_store::Store;
use tokio::sync::{mpsc, Mutex as TokioMutex};
use tokio::time::timeout;

use super::{
    compute_process_cap, AgentHandle, AgentManager, BusEventSink, KillFn, ProcessRegistry,
};
use crate::events::EventBus;
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
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store).with_event_bus(bus.clone());
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus));
    (tmp, AgentManager::new(services, sink, 8))
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
