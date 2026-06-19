//! Unit tests for the agent process registry (cap + LRU + lifecycle) and the
//! [`AgentManager`] multiplexing/teardown — parity-checked against
//! `agent-process-registry`.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_acp::{Connection, ConnectionHooks, EventSink};
use intent_core::AgentId;
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
