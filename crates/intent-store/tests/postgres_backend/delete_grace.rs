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
/// A, still running late, cannot claim them. A commit that crashes after its
/// claim but before the delete finished leaves the row for the next process,
/// which re-runs it. A cancel removes only an unclaimed deadline. `SQLite`
/// keeps the desktop behaviour: nothing is stored.
#[tokio::test]
async fn a_delete_in_grace_survives_takeover_and_commits_once() {
    let Some(db) = test_db(false).await else {
        let (store, _dir) = sqlite_store().await;
        let ws = grace("workspace", "ws-1", "ws-1", "2026-11-22T08:00:00.000Z");
        assert_eq!(store.record_delete_grace(&ws).await.unwrap(), ws.delete_at);
        assert!(store.list_delete_graces().await.unwrap().is_empty());
        assert!(store.claim_delete_grace("workspace", "ws-1").await.unwrap());
        assert!(store
            .cancel_delete_grace("workspace", "ws-1")
            .await
            .unwrap());
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

    // Each commits once: B claims and finishes the workspace delete while A,
    // late, cannot claim it; A commits the agent delete and B cannot.
    assert!(b.claim_delete_grace("workspace", "ws-1").await.unwrap());
    assert!(!a.claim_delete_grace("workspace", "ws-1").await.unwrap());
    assert!(
        !a.cancel_delete_grace("workspace", "ws-1").await.unwrap(),
        "too late to cancel"
    );
    b.finish_delete_grace("workspace", "ws-1").await.unwrap();
    assert!(!a.claim_delete_grace("workspace", "ws-1").await.unwrap());
    assert!(a.claim_delete_grace("agent", "agent-1").await.unwrap());
    assert!(!b.claim_delete_grace("agent", "agent-1").await.unwrap());

    // A crashes after claiming the agent delete, before it finished: the row
    // stays, and a restarted process re-runs the delete.
    let restarted = open_as_process(&db, tenant, "process-restarted").await;
    assert_eq!(
        restarted.list_delete_graces().await.unwrap(),
        std::slice::from_ref(&agent)
    );
    assert!(restarted
        .claim_delete_grace("agent", "agent-1")
        .await
        .unwrap());
    restarted
        .finish_delete_grace("agent", "agent-1")
        .await
        .unwrap();
    assert!(restarted.list_delete_graces().await.unwrap().is_empty());

    // A delete that failed before it was durably under way releases its
    // claim and keeps the row, so another attempt (or the next start)
    // retries it instead of losing it.
    restarted.record_delete_grace(&agent).await.unwrap();
    assert!(restarted
        .claim_delete_grace("agent", "agent-1")
        .await
        .unwrap());
    restarted
        .release_delete_grace("agent", "agent-1")
        .await
        .unwrap();
    assert!(
        b.claim_delete_grace("agent", "agent-1").await.unwrap(),
        "released, so claimable"
    );
    b.finish_delete_grace("agent", "agent-1").await.unwrap();

    // A cancel removes an unclaimed deadline, so nothing is left to commit.
    restarted.record_delete_grace(&ws).await.unwrap();
    assert!(restarted
        .cancel_delete_grace("workspace", "ws-1")
        .await
        .unwrap());
    assert!(!restarted
        .claim_delete_grace("workspace", "ws-1")
        .await
        .unwrap());
    // An immediate delete removes the deadline whatever its claim.
    restarted.record_delete_grace(&ws).await.unwrap();
    assert!(restarted
        .claim_delete_grace("workspace", "ws-1")
        .await
        .unwrap());
    assert!(restarted
        .take_delete_grace("workspace", "ws-1")
        .await
        .unwrap());
    assert!(restarted.list_delete_graces().await.unwrap().is_empty());
}
