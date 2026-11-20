//! The startup sweep finishing deletes a previous run left pending
//! ([`Services::finish_pending_deletes`]).
//!
//! `SQLite` keeps no `delete_purge` record, so each test adds the record a
//! stopped Postgres delete would have left through the store's test seam,
//! [`Store::inject_pending_delete`]; the seam lists a record while its
//! workspace or agent row exists, as the Postgres record lives until its
//! delete's final transaction.

use super::*;
use intent_core::events::{AGENT_DELETED, WORKSPACE_DELETED};
use intent_store::PendingDelete;

struct Harness {
    tmp: TempDb,
    services: Services,
    mgr: Arc<AgentManager>,
    bus: EventBus,
}

async fn harness() -> Harness {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new_isolated(store)
        .with_event_bus(bus.clone())
        .with_workspaces_root(tmp.path.with_extension("workspaces"));
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let mgr = Arc::new(AgentManager::new(services.clone(), sink, 8));
    services.attach_agent_manager(&mgr);
    Harness {
        tmp,
        services,
        mgr,
        bus,
    }
}

async fn drain(sub: &mut crate::events::Subscription) -> Vec<intent_core::Event> {
    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(
        intentd_test_support::test_timeout(Duration::from_millis(300)),
        sub.recv(),
    )
    .await
    {
        events.extend(batch);
    }
    events
}

fn has_event(events: &[intent_core::Event], kind: &str, key: &str, id: &str) -> bool {
    events
        .iter()
        .any(|e| e.event_type == kind && e.data.get(key).and_then(Value::as_str) == Some(id))
}

fn counts(finished: usize, failed: usize, covered: usize) -> crate::PendingDeleteSweep {
    crate::PendingDeleteSweep {
        finished,
        failed,
        covered,
        left_for_next_start: 0,
    }
}

fn never_stopping() -> bool {
    false
}

fn queued(id: &str) -> crate::agent_ops::QueuedMessage {
    serde_json::from_value(json!({
        "id": id,
        "content": "queued at the previous shutdown",
        "queuedAt": "2020-01-01T00:00:00Z",
    }))
    .unwrap()
}

/// Persist one queued message for `agent`, as a previous run left it, with
/// nothing loaded in memory.
async fn persist_queue(services: &Services, agent: &AgentId, id: &str) {
    services
        .agent_queues
        .lock()
        .unwrap()
        .insert(agent.clone(), vec![queued(id)]);
    services.persist_queue_snapshot(agent).await;
    services.agent_queues.lock().unwrap().clear();
}

#[intent_test_macros::daemon_test]
async fn sweep_finishes_pending_workspace_delete_with_service_teardown() {
    let h = harness().await;
    let ws = WorkspaceId::from("ws-pending-delete");
    let (a, b) = (AgentId::from("a-one"), AgentId::from("a-two"));
    seed_agent(&h.mgr, &ws, &a).await;
    insert_extra_session(&h.mgr, &ws, &b).await;
    track(&h.mgr, &a);
    track(&h.mgr, &b);
    let store = h.services.store.clone();
    store.inject_pending_delete(PendingDelete::Workspace(ws.clone()));
    // The agent record a stopped workspace delete can sit beside is covered
    // by the workspace delete, not deleted twice.
    store.inject_pending_delete(PendingDelete::Agent {
        workspace_id: ws.clone(),
        agent_id: a.clone(),
    });
    let mut sub = h.bus.subscribe(SubscriptionFilter::default());

    let sweep = h
        .services
        .finish_pending_deletes(never_stopping)
        .await
        .unwrap();

    assert_eq!(
        sweep,
        counts(1, 0, 1),
        "workspace finished, agent record covered"
    );
    assert!(matches!(
        store.get_workspace(&ws).await,
        Err(Error::NotFound(_))
    ));
    assert!(
        !h.mgr.contains(&a) && !h.mgr.contains(&b),
        "agents released"
    );
    assert_eq!(h.mgr.registry().size(), 0, "process slots released");
    let events = drain(&mut sub).await;
    assert!(
        has_event(&events, WORKSPACE_DELETED, "workspaceId", ws.as_str()),
        "workspace:deleted emitted: {events:?}"
    );
    assert!(store.pending_deletes().await.unwrap().is_empty());
    // A second start finds nothing left to do.
    assert_eq!(
        h.services
            .finish_pending_deletes(never_stopping)
            .await
            .unwrap(),
        counts(0, 0, 0)
    );
}

#[intent_test_macros::daemon_test]
async fn sweep_finishes_pending_agent_delete_and_keeps_the_rest() {
    let h = harness().await;
    let ws = WorkspaceId::from("ws-agent-delete");
    let (gone, kept) = (AgentId::from("a-gone"), AgentId::from("a-kept"));
    seed_agent(&h.mgr, &ws, &gone).await;
    insert_extra_session(&h.mgr, &ws, &kept).await;
    track(&h.mgr, &gone);
    track(&h.mgr, &kept);
    let store = h.services.store.clone();
    store.inject_pending_delete(PendingDelete::Agent {
        workspace_id: ws.clone(),
        agent_id: gone.clone(),
    });
    let mut sub = h.bus.subscribe(SubscriptionFilter::default());

    let sweep = h
        .services
        .finish_pending_deletes(never_stopping)
        .await
        .unwrap();

    assert_eq!(sweep, counts(1, 0, 0));
    assert!(matches!(
        store.get_agent_session_with_transcript(&gone).await,
        Err(Error::NotFound(_))
    ));
    assert!(!h.mgr.contains(&gone), "deleted agent released");
    assert!(h.mgr.contains(&kept), "other agent untouched");
    store
        .get_agent_session_with_transcript(&kept)
        .await
        .unwrap();
    store.get_workspace(&ws).await.unwrap();
    let events = drain(&mut sub).await;
    assert!(
        has_event(&events, AGENT_DELETED, "agentId", gone.as_str()),
        "agent:deleted emitted: {events:?}"
    );
    assert!(store.pending_deletes().await.unwrap().is_empty());
}

/// A failing delete is logged, the next record still finishes, and the
/// failed one keeps its record. Its workspace is then live: the startup
/// restores after the sweep load it like any other (its durable queue).
#[intent_test_macros::daemon_test]
async fn a_failed_delete_keeps_its_record_and_is_restored_normally() {
    let h = harness().await;
    let store = h.services.store.clone();
    let failing_ws = WorkspaceId::from("ws-fails");
    let failing_agent = AgentId::from("a-in-failing-ws");
    seed_agent(&h.mgr, &failing_ws, &failing_agent).await;
    persist_queue(&h.services, &failing_agent, "durable-before-crash").await;
    let other_ws = WorkspaceId::from("ws-other");
    let gone = AgentId::from("a-gone-after-failure");
    seed_agent(&h.mgr, &other_ws, &gone).await;
    store.inject_pending_delete(PendingDelete::Workspace(failing_ws.clone()));
    store.inject_pending_delete(PendingDelete::Agent {
        workspace_id: other_ws.clone(),
        agent_id: gone.clone(),
    });
    sqlx::query(
        "CREATE TRIGGER fail_pending_delete BEFORE DELETE ON agent_queue \
         WHEN OLD.agent_id = 'a-in-failing-ws' \
         BEGIN SELECT RAISE(ABORT, 'injected delete failure'); END",
    )
    .execute(store.write_pool())
    .await
    .unwrap();

    let sweep = h.services.finish_pending_deletes(never_stopping).await;
    sqlx::query("DROP TRIGGER fail_pending_delete")
        .execute(store.write_pool())
        .await
        .unwrap();

    assert_eq!(sweep.unwrap(), counts(1, 1, 0));
    assert!(matches!(
        store.get_agent_session_with_transcript(&gone).await,
        Err(Error::NotFound(_))
    ));
    assert_eq!(
        store.pending_deletes().await.unwrap(),
        vec![PendingDelete::Workspace(failing_ws.clone())],
        "the failed record stays for the next start"
    );
    // The startup restores, after the sweep, load the failed workspace.
    store.get_workspace(&failing_ws).await.unwrap();
    assert_eq!(h.services.rehydrate_agent_queues().await.unwrap(), 1);
    assert_eq!(h.services.queue_snapshot(&failing_agent).len(), 1);
}

/// A record whose subject is already gone counts as finished.
#[intent_test_macros::daemon_test]
async fn a_record_whose_subject_is_gone_counts_as_finished() {
    let h = harness().await;
    let agent = AgentId::from("a-elsewhere");
    seed_agent(&h.mgr, &WorkspaceId::from("ws-real"), &agent).await;
    // `agent.delete` reports the agent as not found in the named workspace.
    h.services
        .store
        .inject_pending_delete(PendingDelete::Agent {
            workspace_id: WorkspaceId::from("ws-recorded"),
            agent_id: agent,
        });
    assert_eq!(
        h.services
            .finish_pending_deletes(never_stopping)
            .await
            .unwrap(),
        counts(1, 0, 0)
    );
}

#[intent_test_macros::daemon_test]
async fn sweep_before_rehydration_restores_nothing_of_a_deleted_workspace() {
    let h = harness().await;
    let ws = WorkspaceId::from("ws-queued");
    let agent = AgentId::from("a-queued");
    seed_agent(&h.mgr, &ws, &agent).await;
    persist_queue(&h.services, &agent, "queued-before-crash").await;
    let store = h.services.store.clone();
    assert_eq!(store.load_all_agent_queues().await.unwrap().len(), 1);
    store.inject_pending_delete(PendingDelete::Workspace(ws.clone()));

    // A restart, in the daemon's order: the sweep, then queue rehydration.
    let restarted = Services::new_isolated(store.clone())
        .with_workspaces_root(h.tmp.path.with_extension("workspaces"));
    let sweep = restarted
        .finish_pending_deletes(never_stopping)
        .await
        .unwrap();
    assert_eq!(sweep, counts(1, 0, 0));
    assert_eq!(restarted.rehydrate_agent_queues().await.unwrap(), 0);
    assert!(restarted.queue_snapshot(&agent).is_empty());
    assert!(matches!(
        store.get_workspace(&ws).await,
        Err(Error::NotFound(_))
    ));
}

/// A surviving parent (Chief may watch any agent) watching a child in the
/// deleted workspace: the startup restores after the sweep load the watch,
/// find the child gone, and the startup reconciliation settles it.
#[intent_test_macros::daemon_test]
async fn a_surviving_parents_watch_on_a_deleted_child_settles_after_startup() {
    let h = harness().await;
    let store = h.services.store.clone();
    let chief = WorkspaceId::chief();
    let parent = AgentId::from("a-chief-parent");
    let ws = WorkspaceId::from("ws-child-deleted");
    let child = AgentId::from("a-child-deleted");
    seed_agent(&h.mgr, &chief, &parent).await;
    seed_agent(&h.mgr, &ws, &child).await;
    store
        .upsert_completion_watch(&intent_store::PersistedCompletionWatch {
            id: "watch-on-deleted-child".into(),
            parent_workspace_id: chief.clone(),
            child_workspace_id: ws.clone(),
            parent_agent_id: parent.clone(),
            parent_agent_name: "Chief".into(),
            child_agent_id: child.clone(),
            group_id: None,
            report_delivered: false,
            wake_on_attention: false,
            completion_only: false,
            created_at: now_iso(),
        })
        .await
        .unwrap();
    store.inject_pending_delete(PendingDelete::Workspace(ws.clone()));

    assert_eq!(
        h.services
            .finish_pending_deletes(never_stopping)
            .await
            .unwrap(),
        counts(1, 0, 0)
    );
    // The daemon's restore, then its owned reconciliation.
    let recovery = h.services.prepare_startup_completion_recovery().await;
    let (_stop, stopping) = tokio::sync::watch::channel(false);
    h.services
        .reconcile_startup_completions(recovery, &stopping)
        .await;
    let settled = timeout(
        intentd_test_support::test_timeout(Duration::from_secs(10)),
        async {
            while !h.services.find_watches_for_child(&child).is_empty() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        },
    )
    .await;
    assert!(
        settled.is_ok(),
        "the watch on the deleted child is still waiting"
    );
    // The parent got the child's deletion: a completion message naming it.
    let messages = store
        .get_agent_session_with_transcript(&parent)
        .await
        .unwrap()
        .messages;
    assert!(
        serde_json::to_string(&messages)
            .unwrap()
            .contains(child.as_str()),
        "no completion wake for the deleted child reached the parent: {messages:?}"
    );
}

/// A daemon stop ends the sweep between records: the delete in progress
/// finishes, and the next record is untouched and kept for the next start.
#[intent_test_macros::daemon_test]
async fn a_daemon_stop_leaves_the_remaining_records_for_the_next_start() {
    let h = harness().await;
    let store = h.services.store.clone();
    let (first, second) = (
        WorkspaceId::from("ws-first"),
        WorkspaceId::from("ws-second"),
    );
    let (a, b) = (AgentId::from("a-first"), AgentId::from("a-second"));
    seed_agent(&h.mgr, &first, &a).await;
    seed_agent(&h.mgr, &second, &b).await;
    track(&h.mgr, &b);
    store.inject_pending_delete(PendingDelete::Workspace(first.clone()));
    store.inject_pending_delete(PendingDelete::Workspace(second.clone()));
    // The stop arrives while the first record is handled.
    let checks = std::sync::atomic::AtomicUsize::new(0);
    let stopping = || checks.fetch_add(1, std::sync::atomic::Ordering::SeqCst) >= 1;

    let sweep = h.services.finish_pending_deletes(stopping).await.unwrap();

    assert_eq!(
        sweep,
        crate::PendingDeleteSweep {
            finished: 1,
            failed: 0,
            covered: 0,
            left_for_next_start: 1,
        }
    );
    assert!(matches!(
        store.get_workspace(&first).await,
        Err(Error::NotFound(_))
    ));
    store
        .get_workspace(&second)
        .await
        .expect("second untouched");
    assert!(h.mgr.contains(&b), "second workspace's agent untouched");
    assert_eq!(
        store.pending_deletes().await.unwrap(),
        vec![PendingDelete::Workspace(second)],
        "kept for the next start"
    );
}
