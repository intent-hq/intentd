//! Service-level incremental deletion regressions (intent-hq/intent#5337).

use super::{test_registry_with_default_provider, workspace, TempDb, WorkspacesRoot};
use crate::{EventBus, Services};
use intent_core::{AgentId, Error, WorkspaceApi, WorkspaceId};
use intent_store::Store;
use serde_json::json;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

const ROWS: i64 = 1_201;

#[derive(Clone, Default)]
pub(crate) struct DeleteGate(Arc<Mutex<Option<DeletePause>>>);

struct DeletePause {
    workspace_id: WorkspaceId,
    reached: Arc<Notify>,
    resume: Arc<Notify>,
}

impl DeleteGate {
    pub(crate) fn arm(&self, ws: WorkspaceId) -> (Arc<Notify>, Arc<Notify>) {
        let reached = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        *self.0.lock().unwrap() = Some(DeletePause {
            workspace_id: ws,
            reached: reached.clone(),
            resume: resume.clone(),
        });
        (reached, resume)
    }

    pub(crate) async fn pause(&self, ws: &WorkspaceId) {
        let gate = {
            let mut gate = self.0.lock().unwrap();
            if gate.as_ref().is_some_and(|g| &g.workspace_id == ws) {
                gate.take()
            } else {
                None
            }
        };
        if let Some(gate) = gate {
            gate.reached.notify_one();
            gate.resume.notified().await;
        }
    }
}

struct Harness {
    svc: Services,
    store: Store,
    _tmp: TempDb,
    _root: WorkspacesRoot,
}

impl Harness {
    async fn new() -> Self {
        let tmp = TempDb::new();
        let root = WorkspacesRoot::new();
        let store = Store::open(&tmp.path).await.unwrap();
        let bus = EventBus::new(store.clone());
        let svc = Services::new(store.clone())
            .with_settings_registry(test_registry_with_default_provider(&tmp))
            .with_event_bus(bus)
            .with_workspaces_root(root.path().to_path_buf());
        Self {
            svc,
            store,
            _tmp: tmp,
            _root: root,
        }
    }

    async fn workspace(&self, name: &str, agents: usize) -> (WorkspaceId, Vec<AgentId>) {
        let ws = WorkspaceId::from(name);
        self.store.insert_workspace(&workspace(&ws)).await.unwrap();
        let mut ids = Vec::new();
        for i in 0..agents {
            let agent = self.create(&ws, &format!("agent-{i}")).await.unwrap();
            let mut tx = self.store.write_pool().begin().await.unwrap();
            sqlx::query(
                "WITH RECURSIVE rows(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM rows WHERE n < ?) \
                 INSERT INTO agent_message (id,agent_id,seq,role,content,created_at) \
                 SELECT ? || '-' || n, ?, n, 'assistant', \
                 '[{\"type\":\"text\",\"text\":\"loaded history\"}]', 't0' FROM rows",
            )
            .bind(ROWS)
            .bind(&agent.0)
            .bind(&agent.0)
            .execute(&mut *tx)
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO agent_message_payload (message_id,agent_id,block_ordinal,kind,encoding,body) \
                 SELECT id,agent_id,0,'tool_result_output','none',zeroblob(8192) \
                 FROM agent_message WHERE agent_id = ?",
            ).bind(&agent.0).execute(&mut *tx).await.unwrap();
            tx.commit().await.unwrap();
            ids.push(agent);
        }
        (ws, ids)
    }

    async fn create(&self, ws: &WorkspaceId, name: &str) -> intent_core::Result<AgentId> {
        let value = self
            .svc
            .agent_create(
                ws.clone(),
                Some(name.into()),
                None,
                None,
                None,
                None,
                intent_core::AgentCreateExtra::default(),
            )
            .await?;
        Ok(AgentId::from(value["agent"]["id"].as_str().unwrap()))
    }

    async fn remaining(&self, ws: &WorkspaceId) -> i64 {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM agent_message_payload p JOIN agent_session s ON s.id=p.agent_id WHERE s.workspace_id=?",
        ).bind(&ws.0).fetch_one(self.store.read_pool()).await.unwrap()
    }

    async fn partial(&self, ws: &WorkspaceId, initial: i64) {
        loop {
            let n = self.remaining(ws).await;
            if n > 0 && n < initial {
                return;
            }
            tokio::task::yield_now().await;
        }
    }
}

/// Pause after the runtime snapshot/sweeps, with no database connection held.
/// A late session here would escape `stop_many` even if the store removes it.
#[intent_test_macros::daemon_test]
async fn incremental_delete_refuses_new_agents_after_session_snapshot() {
    let h = Harness::new().await;
    let (ws, _) = h.workspace("deleting", 3).await;
    let (reached, _) = h.svc.workspace_delete_test_gate.arm(ws.clone());
    let mut deleting = h.svc.delete_workspace(ws.clone());
    tokio::time::timeout(Duration::from_secs(30), async {
        tokio::select! {
            result = &mut deleting => panic!("delete finished before partial observation: {result:?}"),
            () = reached.notified() => {}
        }
    }).await.unwrap();
    let created = h.create(&ws, "late agent").await;
    assert!(
        created.is_err(),
        "new agent escaped the session snapshot: {created:?}"
    );
    drop(deleting);
    h.create(&ws, "after cancellation")
        .await
        .expect("cancellation releases admission");
    h.svc.delete_workspace(ws.clone()).await.expect("retry");
    assert!(matches!(
        h.store.get_workspace(&ws).await,
        Err(Error::NotFound(_))
    ));
}

#[intent_test_macros::daemon_test]
async fn incremental_delete_refuses_messages_after_runtime_sweep() {
    let h = Harness::new().await;
    let (ws, agents) = h.workspace("deleting", 3).await;
    let (reached, resume) = h.svc.workspace_delete_test_gate.arm(ws.clone());
    let mut deleting = h.svc.delete_workspace(ws.clone());
    tokio::time::timeout(Duration::from_secs(30), async {
        tokio::select! {
            result = &mut deleting => panic!("delete finished before partial observation: {result:?}"),
            () = reached.notified() => {}
        }
    }).await.unwrap();
    for agent in agents {
        let sent = h
            .svc
            .agent_send_message_op(agent, "late message".into(), None, None, None, None)
            .await;
        assert!(sent.is_err(), "message accepted after teardown: {sent:?}");
    }
    resume.notify_one();
    deleting.await.unwrap();
}

#[intent_test_macros::daemon_test]
async fn incremental_delete_refuses_new_producers_after_cleanup_sweeps() {
    let h = Harness::new().await;
    let (ws, agents) = h.workspace("deleting", 3).await;
    let (reached, resume) = h.svc.workspace_delete_test_gate.arm(ws.clone());
    let mut deleting = h.svc.delete_workspace(ws.clone());
    tokio::time::timeout(Duration::from_secs(30), async {
        tokio::select! {
            result = &mut deleting => panic!("delete finished before partial observation: {result:?}"),
            () = reached.notified() => {}
        }
    }).await.unwrap();
    let note = h
        .svc
        .create_note(
            ws.clone(),
            intent_core::NoteCreate {
                title: "late note".into(),
                ..Default::default()
            },
            None,
            None,
        )
        .await;
    let hook = h
        .svc
        .hook_schedule_op(
            &ws,
            &agents[2],
            &json!({"name":"late hook", "delayMs":60000, "code":"return {dispatch:false}"}),
        )
        .await;
    let subscription = h
        .svc
        .agent_subscribe(
            ws.clone(),
            Some(agents[2].clone()),
            vec!["note:*".into()],
            None,
            None,
        )
        .await;
    let watch = h.svc.register_completion_watch(
        &ws,
        &ws,
        agents[1].clone(),
        "parent".into(),
        agents[2].clone(),
        None,
    );
    assert!(note.is_err(), "late history producer admitted: {note:?}");
    assert!(hook.is_err(), "hook escaped scheduler sweep: {hook:?}");
    assert!(
        subscription.is_err(),
        "subscription escaped sweep: {subscription:?}"
    );
    assert!(watch.is_err(), "completion watch escaped sweep: {watch:?}");
    let waiting = h
        .svc
        .app_agents_wait_op(
            ws.clone(),
            agents[1].clone(),
            vec![agents[2].0.clone()],
            Some("after_all".into()),
        )
        .await;
    assert!(waiting.is_err(), "group watch escaped sweep: {waiting:?}");
    assert!(
        h.svc
            .agent_subscriptions
            .lock()
            .unwrap()
            .delegation_groups
            .is_empty(),
        "rejected wait must not leave a late delegation group"
    );
    let monitor = h
        .svc
        .pr_monitor_try_register(&ws, &agents[2], "owner", "repo", 1)
        .await;
    let Err(error) = monitor else {
        panic!("monitor escaped cleanup");
    };
    assert!(error.to_string().contains("being deleted"));
    let tab: intent_core::BrowserTabInput = serde_json::from_value(json!({
        "tabId":"late-tab", "workspaceId":ws.0, "url":"http://localhost/"
    }))
    .unwrap();
    let host = intent_core::ClientId::from("test-host");
    let upsert = h.svc.browser_tab_upsert(host.clone(), tab.clone()).await;
    let sync = h.svc.browser_tabs_sync(host, vec![tab]).await;
    assert!(upsert.unwrap_err().to_string().contains("being deleted"));
    assert!(sync.unwrap_err().to_string().contains("being deleted"));
    // note.list is a read, but normally recreates Spec. It must not refill
    // a workspace that the incremental note sweep just emptied.
    h.store
        .delete_note(&ws, &intent_core::NoteId::from("spec"))
        .await
        .ok();
    let notes = h.svc.list_notes(&ws).await.unwrap();
    assert!(!notes.iter().any(|n| n.id.0 == "spec"));
    resume.notify_one();
    deleting.await.unwrap();
}

#[intent_test_macros::daemon_test]
async fn loaded_deletions_allow_unrelated_writes_and_persisted_events_before_completion() {
    let h = Harness::new().await;
    let (a, _) = h.workspace("delete-a", 3).await;
    let (b, _) = h.workspace("delete-b", 3).await;
    let (keeper, _) = h.workspace("keeper", 1).await;
    sqlx::query("CREATE TABLE deletion_probes(kind TEXT, remaining INTEGER)")
        .execute(h.store.write_pool())
        .await
        .unwrap();
    for (table, condition, kind) in [
        ("agent_session", "NEW.workspace_id='keeper'", "write"),
        (
            "event",
            "NEW.workspace_id='keeper' AND NEW.event_type='agent:created'",
            "event",
        ),
    ] {
        sqlx::query(&format!("CREATE TRIGGER probe_{kind} AFTER INSERT ON {table} WHEN {condition} BEGIN \
            INSERT INTO deletion_probes SELECT '{kind}', COALESCE(MAX(n),0) FROM \
            (SELECT COUNT(*) n FROM agent_message_payload p JOIN agent_session s ON s.id=p.agent_id \
            WHERE s.workspace_id IN ('delete-a','delete-b') GROUP BY s.workspace_id \
            HAVING COUNT(*) > 0 AND COUNT(*) < {}); END", 3 * ROWS))
            .execute(h.store.write_pool()).await.unwrap();
    }
    let done = AtomicBool::new(false);
    let deletes = async {
        let result = tokio::join!(
            h.svc.delete_workspace(a.clone()),
            h.svc.delete_workspace(b.clone())
        );
        done.store(true, Ordering::SeqCst);
        result
    };
    let probes = async {
        let mut ids = Vec::new();
        while !done.load(Ordering::SeqCst) {
            ids.push(h.create(&keeper, "unrelated write").await.unwrap());
        }
        ids
    };
    let ((one, two), probes) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(deletes, probes)
    })
    .await
    .unwrap();
    one.unwrap();
    two.unwrap();
    for kind in ["write", "event"] {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM deletion_probes WHERE kind=? AND remaining > 0 AND remaining < ?",
        )
        .bind(kind)
        .bind(3 * ROWS)
        .fetch_one(h.store.read_pool())
        .await
        .unwrap();
        assert!(count > 0, "{kind} must commit during partial cleanup");
    }
    let events = h.store.events_by_workspace(&keeper, 10_000).await.unwrap();
    for probe in probes {
        assert!(events
            .iter()
            .any(|e| e.event_type == "agent:created" && e.data["agentId"] == probe.0));
    }
    assert_eq!(h.remaining(&keeper).await, ROWS);
    for ws in [&a, &b] {
        let events = h.store.events_by_workspace(ws, 100).await.unwrap();
        let deleted = events
            .iter()
            .find(|e| e.event_type == "workspace:deleted")
            .unwrap();
        let agents: Vec<_> = events
            .iter()
            .filter(|e| e.event_type == "agent:deleted")
            .collect();
        assert_eq!(agents.len(), 3);
        assert!(agents.iter().all(|e| e.timestamp <= deleted.timestamp));
    }
}

#[intent_test_macros::daemon_test]
async fn partial_failure_does_not_publish_success_or_block_independent_delete_and_retry() {
    let h = Harness::new().await;
    let (failed, _) = h.workspace("failed", 3).await;
    let (other, _) = h.workspace("other", 3).await;
    sqlx::query("CREATE TRIGGER fail_partial_delete BEFORE DELETE ON agent_message \
        WHEN OLD.seq > 500 AND OLD.agent_id IN (SELECT id FROM agent_session WHERE workspace_id='failed') \
        BEGIN SELECT RAISE(ABORT, 'injected partial cleanup failure'); END")
        .execute(h.store.write_pool()).await.unwrap();
    let (failed_result, other_result) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(
            h.svc.delete_workspace(failed.clone()),
            h.svc.delete_workspace(other.clone())
        )
    })
    .await
    .unwrap();
    let err = failed_result.unwrap_err();
    assert!(
        err.to_string().contains("injected partial cleanup failure"),
        "{err}"
    );
    other_result.unwrap();
    h.store.get_workspace(&failed).await.unwrap();
    assert!(
        h.remaining(&failed).await < 3 * ROWS,
        "committed partial cleanup remains"
    );
    assert!(matches!(
        h.store.get_workspace(&other).await,
        Err(Error::NotFound(_))
    ));
    assert!(!h
        .store
        .events_by_workspace(&failed, 100)
        .await
        .unwrap()
        .iter()
        .any(|e| e.event_type == "workspace:deleted"));
    h.create(&failed, "after failure")
        .await
        .expect("failure releases admission");
    sqlx::query("DROP TRIGGER fail_partial_delete")
        .execute(h.store.write_pool())
        .await
        .unwrap();
    // Duplicate requests must both succeed, but only one root removal and
    // terminal event may occur. A later retry remains idempotent too.
    let (one, two) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(
            h.svc.delete_workspace(failed.clone()),
            h.svc.delete_workspace(failed.clone())
        )
    })
    .await
    .unwrap();
    one.unwrap();
    two.unwrap();
    h.svc.delete_workspace(failed.clone()).await.unwrap();
    assert!(matches!(
        h.store.get_workspace(&failed).await,
        Err(Error::NotFound(_))
    ));
    assert_eq!(
        h.store
            .events_by_workspace(&failed, 100)
            .await
            .unwrap()
            .iter()
            .filter(|e| e.event_type == "workspace:deleted")
            .count(),
        1
    );
}

#[intent_test_macros::daemon_test]
async fn cancelled_partial_delete_releases_admission_and_retries() {
    let h = Harness::new().await;
    let (ws, _) = h.workspace("cancelled", 3).await;
    let svc = h.svc.clone();
    let id = ws.clone();
    let deleting = intent_core::spawn_daemon(async move { svc.delete_workspace(id).await });
    tokio::time::timeout(Duration::from_secs(30), h.partial(&ws, 3 * ROWS))
        .await
        .unwrap();
    deleting.abort();
    assert!(deleting.await.unwrap_err().is_cancelled());
    h.store
        .get_workspace(&ws)
        .await
        .expect("partial deletion retains root");
    let remaining = h.remaining(&ws).await;
    assert!(remaining > 0 && remaining < 3 * ROWS);
    assert!(!h
        .store
        .events_by_workspace(&ws, 100)
        .await
        .unwrap()
        .iter()
        .any(|e| e.event_type == "workspace:deleted"));
    h.create(&ws, "retry after cancellation").await.unwrap();
    h.svc.delete_workspace(ws.clone()).await.unwrap();
    assert!(matches!(
        h.store.get_workspace(&ws).await,
        Err(Error::NotFound(_))
    ));
}

#[intent_test_macros::daemon_test]
async fn completion_watch_persistence_keeps_admission_until_its_async_write_finishes() {
    let h = Harness::new().await;
    let (ws, _) = h.workspace("watch", 0).await;
    let parent = h.create(&ws, "parent").await.unwrap();
    let child = h.create(&ws, "child").await.unwrap();
    // Delay the existing background persist on the scratch database's writer.
    // Registration returns before that write; its admission must travel with it.
    let writer = h.store.write_pool().begin().await.unwrap();
    let watch_id = h
        .svc
        .register_completion_watch(&ws, &ws, parent, "parent".into(), child, None)
        .unwrap();
    let mut deletion = Box::pin(h.svc.workspace_mutations.delete(&ws));
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(deletion.as_mut().poll(cx).is_pending()))
            .await,
        "deletion cannot sweep ahead of a delayed watch write"
    );
    writer.rollback().await.unwrap();
    let permit = tokio::time::timeout(Duration::from_secs(30), deletion)
        .await
        .unwrap();
    drop(permit);
    sqlx::query(
        "CREATE TRIGGER fail_watch_sweep BEFORE DELETE ON completion_watch \
        BEGIN SELECT RAISE(ABORT, 'injected watch cleanup failure'); END",
    )
    .execute(h.store.write_pool())
    .await
    .unwrap();
    let error = h.svc.delete_workspace(ws.clone()).await.unwrap_err();
    assert!(error.to_string().contains("injected watch cleanup failure"));
    h.store.get_workspace(&ws).await.unwrap();
    assert!(h.svc.workspace_mutations.enter(&ws).is_ok());
    assert!(
        h.svc
            .agent_subscriptions
            .lock()
            .unwrap()
            .subscriptions
            .iter()
            .any(|w| w.id == watch_id),
        "failed persistence cleanup keeps the watch available to retry"
    );
    sqlx::query("DROP TRIGGER fail_watch_sweep")
        .execute(h.store.write_pool())
        .await
        .unwrap();
    h.svc.delete_workspace(ws).await.unwrap();
    let watches: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM completion_watch")
        .fetch_one(h.store.read_pool())
        .await
        .unwrap();
    assert_eq!(watches, 0, "no late watch survives workspace cleanup");
}

#[intent_test_macros::daemon_test]
async fn admitted_chief_wait_finishes_registration_before_queued_target_deletion() {
    let h = Harness::new().await;
    for name in ["child-a-ws", "child-b-ws"] {
        h.workspace(name, 0).await;
    }
    for (id, ws) in [
        ("parent", "__chief__"),
        ("child-a", "child-a-ws"),
        ("child-b", "child-b-ws"),
    ] {
        let session = serde_json::from_value(json!({
            "id": id, "workspaceId": ws, "name": id, "status": "active",
            "createdAt": "t0", "updatedAt": "t0"
        }))
        .unwrap();
        h.store.insert_agent_session(&session).await.unwrap();
    }
    let chief = WorkspaceId::from("__chief__");
    let parent = AgentId::from("parent");
    let writer = h.store.write_pool().acquire().await.unwrap();
    let mut waiting = h.svc.app_agents_wait(
        chief.clone(),
        parent.clone(),
        vec!["child-a".into(), "child-b".into()],
        Some("after_all".into()),
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            assert!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(waiting.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            if h.svc
                .agent_subscriptions
                .lock()
                .unwrap()
                .subscriptions
                .len()
                == 1
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut deleting = h.svc.delete_workspace(WorkspaceId::from("child-b-ws"));
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(deleting.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    drop(writer);
    let result = tokio::time::timeout(Duration::from_secs(10), waiting)
        .await
        .unwrap();
    deleting.await.unwrap();
    h.svc
        .delete_workspace(WorkspaceId::from("child-a-ws"))
        .await
        .unwrap();
    let subscriptions = h
        .svc
        .agent_get_subscriptions(chief, parent.clone())
        .await
        .unwrap();
    for group in subscriptions["delegationGroups"].as_array().unwrap() {
        for child in ["child-a", "child-b"] {
            assert!(
                group["deletedAgentIds"]
                    .as_array()
                    .unwrap()
                    .contains(&json!(child)),
                "unwatched deleted child strands Chief: {subscriptions}; wait={result:?}"
            );
        }
    }
    result.expect("already admitted operation must finish all registrations");
    assert!(subscriptions["subscriptions"]
        .as_array()
        .unwrap()
        .is_empty());
    let group = h.svc.seal_group_for_parent(&parent).await.unwrap();
    assert!(
        h.svc.take_group_if_ready(&group).is_some(),
        "both deletions must let the Chief group settle"
    );
}

#[intent_test_macros::daemon_test]
async fn task_materialization_cannot_recreate_history_after_version_sweep() {
    let h = Harness::new().await;
    let (ws, _) = h.workspace("task-delete", 0).await;
    let (keeper, _) = h.workspace("task-keeper", 0).await;
    sqlx::query("WITH RECURSIVE rows(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM rows WHERE n<5001) INSERT INTO note(id,workspace_id,title,content,created_at,updated_at) SELECT 'padding-'||n,'task-delete','Padding','','t0','t0' FROM rows")
        .execute(h.store.write_pool()).await.unwrap();
    let mut ids = Vec::new();
    for title in ["Task", "Unmarked"] {
        ids.push(
            h.svc
                .create_note(
                    ws.clone(),
                    intent_core::NoteCreate {
                        title: title.into(),
                        ..Default::default()
                    },
                    None,
                    None,
                )
                .await
                .unwrap()
                .note
                .id,
        );
    }
    h.svc
        .mark_as_task(
            ws.clone(),
            ids[0].clone(),
            "not_started".into(),
            vec![],
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    h.svc.create_note(ws.clone(), intent_core::NoteCreate {
        title: "Parent".into(), content: Some(format!("- [ ] [Task](intent://local/task/{})\n- [ ] [Unmarked](intent://local/task/{})", ids[0], ids[1])), ..Default::default()
    }, None, None).await.unwrap();
    sqlx::query("INSERT OR REPLACE INTO note_line_attribution(note_id,workspace_id,computed_at,attributions_json) SELECT id,workspace_id,'t0','[]' FROM note WHERE workspace_id='task-delete'").execute(h.store.write_pool()).await.unwrap();
    sqlx::query("CREATE TABLE late_versions(n INTEGER); CREATE TRIGGER capture_late_version AFTER INSERT ON note_version WHEN NEW.workspace_id='task-delete' BEGIN INSERT INTO late_versions VALUES(1); END").execute(h.store.write_pool()).await.unwrap();
    sqlx::query("CREATE TABLE keeper_progress(root_present INTEGER); CREATE TRIGGER capture_keeper_progress AFTER INSERT ON note WHEN NEW.workspace_id='task-keeper' BEGIN INSERT INTO keeper_progress SELECT COUNT(*) FROM workspace WHERE id='task-delete'; END").execute(h.store.write_pool()).await.unwrap();
    let reached = Arc::new(Notify::new());
    let (release, blocked) = std::sync::mpsc::sync_channel(1);
    {
        let mut conn = h.store.write_pool().acquire().await.unwrap();
        let mut handle = conn.lock_handle().await.unwrap();
        let reached = reached.clone();
        let mut blocked = Some(blocked);
        handle.set_update_hook(move |update| {
            if update.table == "note_line_attribution"
                && update.operation == sqlx::sqlite::SqliteOperation::Delete
            {
                if let Some(blocked) = blocked.take() {
                    reached.notify_one();
                    blocked.recv_timeout(Duration::from_secs(10)).unwrap();
                }
            }
        });
    }
    let mut deleting = h.svc.delete_workspace(ws.clone());
    tokio::select! {
        () = reached.notified() => {},
        result = &mut deleting => panic!("delete passed sweep barrier: {result:?}"),
    }
    let versions: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM note_version WHERE workspace_id='task-delete'")
            .fetch_one(h.store.read_pool())
            .await
            .unwrap();
    assert_eq!(versions, 0, "version sweep has completed");
    let mut changes = Box::pin(async {
        tokio::join!(
            h.svc.task_update_note_status(
                ws.clone(),
                ids[0].clone(),
                "complete".into(),
                None,
                None
            ),
            h.svc.mark_as_task(
                ws.clone(),
                ids[1].clone(),
                "complete".into(),
                vec![],
                None,
                None,
                None,
                None
            )
        )
    });
    let early = std::future::poll_fn(|cx| std::task::Poll::Ready(changes.as_mut().poll(cx))).await;
    let mut unrelated = h.svc.create_note(
        keeper.clone(),
        intent_core::NoteCreate {
            title: "Still writable".into(),
            ..Default::default()
        },
        None,
        None,
    );
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(unrelated.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    release.send(()).unwrap();
    let mutations = async {
        match early {
            std::task::Poll::Ready(r) => r,
            std::task::Poll::Pending => changes.await,
        }
    };
    let (deleted, (status, marked), unrelated) = tokio::join!(deleting, mutations, unrelated);
    {
        let mut conn = h.store.write_pool().acquire().await.unwrap();
        conn.lock_handle().await.unwrap().remove_update_hook();
    }
    deleted.unwrap();
    unrelated.unwrap();
    assert!(
        status.is_err(),
        "late task status bypassed admission: {status:?}"
    );
    assert!(
        marked.is_err(),
        "late task marking bypassed admission: {marked:?}"
    );
    let recreated: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM late_versions")
        .fetch_one(h.store.read_pool())
        .await
        .unwrap();
    assert_eq!(
        recreated, 0,
        "task operations must not refill swept note history"
    );
    let present: i64 = sqlx::query_scalar("SELECT root_present FROM keeper_progress LIMIT 1")
        .fetch_one(h.store.read_pool())
        .await
        .unwrap();
    assert_eq!(
        present, 1,
        "unrelated write completed before deleting workspace disappeared"
    );
}

#[intent_test_macros::daemon_test]
async fn admitted_delegation_finishes_create_assign_watch_and_send_before_delete() {
    let h = Harness::new().await;
    let chief = WorkspaceId::from("__chief__");
    let parent = h.create(&chief, "chief").await.unwrap();
    let (ws, _) = h.workspace("delegation-target", 0).await;
    let task = h
        .svc
        .create_note(
            ws.clone(),
            intent_core::NoteCreate {
                title: "Child task".into(),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap()
        .note;
    h.svc
        .mark_as_task(
            ws.clone(),
            task.id.clone(),
            "not_started".into(),
            vec![],
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let reached = Arc::new(Notify::new());
    let (release, blocked) = std::sync::mpsc::sync_channel(1);
    {
        let mut conn = h.store.write_pool().acquire().await.unwrap();
        let mut handle = conn.lock_handle().await.unwrap();
        let reached = reached.clone();
        let mut blocked = Some(blocked);
        handle.set_update_hook(move |update| {
            if update.table == "agent_session"
                && update.operation == sqlx::sqlite::SqliteOperation::Insert
            {
                if let Some(blocked) = blocked.take() {
                    reached.notify_one();
                    blocked.recv_timeout(Duration::from_secs(10)).unwrap();
                }
            }
        });
    }
    let input = serde_json::from_value(
        json!({"taskNoteId":task.id,"agentInstructions":"work", "waitMode":"after_all"}),
    )
    .unwrap();
    let mut delegate = Box::pin(
        h.svc
            .agent_delegate_op(ws.clone(), input, Some(parent.clone())),
    );
    tokio::select! {
        () = reached.notified() => {},
        result = &mut delegate => panic!("delegate finished before create barrier: {result:?}"),
    }
    let mut deleting = h.svc.delete_workspace(ws.clone());
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(deleting.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    release.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(10), delegate)
        .await
        .unwrap()
        .unwrap();
    let child = AgentId::from(result["agentId"].as_str().unwrap());
    assert!(h
        .store
        .get_note(&ws, &task.id)
        .await
        .unwrap()
        .metadata
        .task
        .unwrap()
        .assigned_agent_ids
        .contains(&child));
    let before = h
        .svc
        .agent_get_subscriptions(chief.clone(), parent.clone())
        .await
        .unwrap();
    assert_eq!(
        before["subscriptions"].as_array().unwrap().len(),
        1,
        "delegated child must have a watch: {result}"
    );
    deleting.await.unwrap();
    {
        let mut conn = h.store.write_pool().acquire().await.unwrap();
        conn.lock_handle().await.unwrap().remove_update_hook();
    }
    let after = h
        .svc
        .agent_get_subscriptions(chief, parent.clone())
        .await
        .unwrap();
    assert!(after["subscriptions"].as_array().unwrap().is_empty());
    let group = h.svc.seal_group_for_parent(&parent).await.unwrap();
    assert!(
        h.svc.take_group_if_ready(&group).is_some(),
        "deleted delegated child must settle: {after}"
    );
}

#[intent_test_macros::daemon_test]
async fn admitted_task_update_keeps_ownership_through_linked_status_materialization() {
    let h = Harness::new().await;
    let (ws, _) = h.workspace("task-update-target", 0).await;
    let task = h
        .svc
        .create_note(
            ws.clone(),
            intent_core::NoteCreate {
                title: "Task".into(),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap()
        .note;
    h.svc
        .mark_as_task(
            ws.clone(),
            task.id.clone(),
            "not_started".into(),
            vec![],
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let parent = h
        .svc
        .create_note(
            ws.clone(),
            intent_core::NoteCreate {
                title: "Parent".into(),
                content: Some(format!("- [ ] [Task](intent://local/task/{})", task.id)),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap()
        .note;
    let reached = Arc::new(Notify::new());
    let (release, blocked) = std::sync::mpsc::sync_channel(1);
    {
        let mut conn = h.store.write_pool().acquire().await.unwrap();
        let mut handle = conn.lock_handle().await.unwrap();
        let reached = reached.clone();
        let mut blocked = Some(blocked);
        handle.set_update_hook(move |update| {
            if update.table == "note" && update.operation == sqlx::sqlite::SqliteOperation::Update {
                if let Some(blocked) = blocked.take() {
                    reached.notify_one();
                    blocked.recv_timeout(Duration::from_secs(10)).unwrap();
                }
            }
        });
    }
    let mut changing = h.svc.task_update(
        ws.clone(),
        parent.id.clone(),
        1,
        None,
        Some("done".into()),
        None,
        None,
    );
    tokio::select! {
        () = reached.notified() => {},
        result = &mut changing => panic!("task update missed parent-write barrier: {result:?}"),
    }
    let mut deleting = h.svc.delete_workspace(ws.clone());
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(deleting.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    release.send(()).unwrap();
    changing
        .await
        .expect("nested status helper reuses the admitted parent write");
    assert_eq!(
        h.store
            .get_note(&ws, &task.id)
            .await
            .unwrap()
            .metadata
            .task
            .unwrap()
            .status,
        intent_core::TaskStatus::Complete
    );
    assert!(h
        .store
        .get_note(&ws, &parent.id)
        .await
        .unwrap()
        .content
        .starts_with("- [x]"));
    deleting.await.unwrap();
    let mut conn = h.store.write_pool().acquire().await.unwrap();
    conn.lock_handle().await.unwrap().remove_update_hook();
}
