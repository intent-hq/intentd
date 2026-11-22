//! Persisted delete grace deadlines (batch G, step 3): a delete in its
//! grace period survives a restart or a takeover and commits once.

use intent_store::{DeleteGrace, Store};

use crate::harness::{sqlite_store, test_db, TestDb};

/// A store of `tenant` opened as if by another process named `process`.
async fn open_as_process(db: &TestDb, tenant: &str, process: &str) -> Store {
    Store::open_postgres_as_process(
        db.app.clone(),
        intent_store::postgres::TenantId::new(tenant).unwrap(),
        intent_store::postgres::PostgresOptions::default(),
        process,
        intent_store::OrchestratorOnly::for_tests(),
    )
    .await
    .expect("open tenant store as another process")
}

fn grace(kind: &str, id: &str, ws: &str, delete_at: &str) -> DeleteGrace {
    DeleteGrace {
        subject_kind: kind.into(),
        subject_id: id.into(),
        workspace_id: ws.into(),
        delete_at: delete_at.into(),
    }
}

/// Process A schedules a workspace and an agent delete, then stops (a crash,
/// or it lost the tenant). Process B finds both deadlines and commits them;
/// A, still running late, finds them gone and does not commit again.
/// `SQLite` keeps the desktop behaviour: nothing is stored.
#[tokio::test]
async fn a_delete_in_grace_survives_takeover_and_commits_once() {
    let Some(db) = test_db(false).await else {
        let (store, _dir) = sqlite_store().await;
        let ws = grace("workspace", "ws-1", "ws-1", "2026-11-22T08:00:00.000Z");
        assert_eq!(store.record_delete_grace(&ws).await.unwrap(), ws.delete_at);
        assert!(store.list_delete_graces().await.unwrap().is_empty());
        assert!(store.take_delete_grace("workspace", "ws-1").await.unwrap());
        return;
    };
    let tenant = "graces";
    let a = open_as_process(&db, tenant, "process-a").await;
    let ws = grace("workspace", "ws-1", "ws-1", "2026-11-22T08:00:00.000Z");
    let agent = grace("agent", "agent-1", "ws-2", "2026-11-22T08:00:30.000Z");
    assert_eq!(a.record_delete_grace(&ws).await.unwrap(), ws.delete_at);
    assert_eq!(
        a.record_delete_grace(&agent).await.unwrap(),
        agent.delete_at
    );
    // A re-schedule never moves the stored deadline.
    let later = grace("workspace", "ws-1", "ws-1", "2026-11-22T09:00:00.000Z");
    assert_eq!(a.record_delete_grace(&later).await.unwrap(), ws.delete_at);

    // B (a restart, or the process that took the tenant over) finds both.
    let b = open_as_process(&db, tenant, "process-b").await;
    let mut found = b.list_delete_graces().await.unwrap();
    found.sort_by(|x, y| x.subject_id.cmp(&y.subject_id));
    assert_eq!(found, [agent.clone(), ws.clone()], "the deadlines survive");

    // Each commits once: whoever takes the row first.
    assert!(b.take_delete_grace("workspace", "ws-1").await.unwrap());
    assert!(!a.take_delete_grace("workspace", "ws-1").await.unwrap());
    assert!(a.take_delete_grace("agent", "agent-1").await.unwrap());
    assert!(!b.take_delete_grace("agent", "agent-1").await.unwrap());
    assert!(b.list_delete_graces().await.unwrap().is_empty());

    // A cancelled delete (row taken) leaves nothing to commit.
    b.record_delete_grace(&ws).await.unwrap();
    assert!(b.take_delete_grace("workspace", "ws-1").await.unwrap());
    assert!(!a.take_delete_grace("workspace", "ws-1").await.unwrap());
}
