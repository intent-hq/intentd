//! Lifecycle leases on Postgres: two processes of one tenant serialize a
//! retirement or an archive of the same target, and a workspace deletion in
//! one process closes creates and sends of the other. See
//! `lifecycle_leases.rs` for the rules.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use intent_core::{AgentId, AgentSession, AgentStatus, Error, WorkspaceId};
use intent_store::{Store, AGENT_RETIREMENT_SCOPE, ARCHIVE_FENCE_SCOPE, WORKSPACE_DELETION_SCOPE};
use intentd_test_support::test_timeout;
use serde_json::json;
use tokio::sync::Barrier;

use crate::harness::TestDb;

const WS: &str = "__chief__";
const SHORT_TTL: Duration = Duration::from_millis(400);

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

fn session(id: &str) -> AgentSession {
    AgentSession {
        id: AgentId(id.into()),
        workspace_id: WorkspaceId(WS.into()),
        name: id.into(),
        model: Some("opus".into()),
        provider: Some("claude".into()),
        status: AgentStatus::Idle,
        created_at: "2026-01-01T00:00:00Z".into(),
        updated_at: "2026-01-01T00:00:00Z".into(),
        ..AgentSession::default()
    }
}

async fn send(store: &Store, agent: &str) -> intent_core::Result<()> {
    store
        .append_agent_message(
            &AgentId(agent.into()),
            "user",
            &json!([{ "type": "text", "text": "hi" }]),
            "2026-01-01T00:00:01Z",
        )
        .await
        .map(|_| ())
}

/// Waits until waiters in this process found (`scope`, `key`) held at
/// least `n` times.
async fn wait_contended(scope: &str, key: &str, n: u64) {
    tokio::time::timeout(test_timeout(Duration::from_secs(30)), async {
        while intent_store::contended_lease_attempts_for_tests(scope, key) < n {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the waiter tries the held key");
}

fn is_being_deleted<T: std::fmt::Debug>(result: &intent_core::Result<T>) -> bool {
    matches!(result, Err(Error::NotFound(m)) if m.contains("being deleted"))
}

/// Holders of one lifecycle key in two processes never overlap: every
/// contender is spawned first and released together from a barrier on a
/// multi-thread runtime, each waits for the key, and at most one is inside
/// at a time. Each later holder has a higher token.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn retirement_and_archive_in_one_process_block_the_same_in_another() {
    let Some(db) = crate::harness::test_db(false).await else {
        return;
    };
    let tenant = "lifecycle-holders";
    let a = open_as_process(&db, tenant, "process-a").await;
    let b = open_as_process(&db, tenant, "process-b").await;
    for scope in [AGENT_RETIREMENT_SCOPE, ARCHIVE_FENCE_SCOPE] {
        let contenders = 8;
        let start = Arc::new(Barrier::new(contenders));
        let inside = Arc::new(AtomicUsize::new(0));
        let tokens = Arc::new(std::sync::Mutex::new(Vec::new()));
        let handles: Vec<_> = (0..contenders)
            .map(|i| {
                let store = if i % 2 == 0 { a.clone() } else { b.clone() };
                let (start, inside, tokens) =
                    (Arc::clone(&start), Arc::clone(&inside), Arc::clone(&tokens));
                tokio::spawn(async move {
                    start.wait().await;
                    let hold = store.hold_lifecycle_lease(scope, "target").await.unwrap();
                    assert!(hold.is_shared(), "Postgres holds a store lease");
                    assert_eq!(inside.fetch_add(1, Ordering::SeqCst), 0, "two holders");
                    tokens.lock().unwrap().push(hold.token().unwrap());
                    // Let the other contenders try while this one holds.
                    tokio::task::yield_now().await;
                    inside.fetch_sub(1, Ordering::SeqCst);
                    hold.release().await;
                })
            })
            .collect();
        for handle in handles {
            tokio::time::timeout(test_timeout(Duration::from_secs(60)), handle)
                .await
                .expect("every contender gets the key in turn")
                .unwrap();
        }
        let tokens = tokens.lock().unwrap().clone();
        assert_eq!(tokens.len(), contenders);
        assert!(
            tokens.windows(2).all(|w| w[0] < w[1]),
            "each holder has a higher token: {tokens:?}"
        );
    }

    // A holder in A blocks B until A releases; another key is other work.
    let held = a
        .hold_lifecycle_lease(AGENT_RETIREMENT_SCOPE, "agent-x")
        .await
        .unwrap();
    let waiter = {
        let b = b.clone();
        tokio::spawn(async move {
            b.hold_lifecycle_lease(AGENT_RETIREMENT_SCOPE, "agent-x")
                .await
                .unwrap()
        })
    };
    let other = b
        .hold_lifecycle_lease(AGENT_RETIREMENT_SCOPE, "agent-y")
        .await
        .unwrap();
    // B cannot legally get agent-x while A holds and renews it: once B has
    // found the key held twice, it is still waiting.
    wait_contended(AGENT_RETIREMENT_SCOPE, "agent-x", 2).await;
    assert!(!waiter.is_finished(), "B waits while A holds the key");
    let a_token = held.token().unwrap();
    held.release().await;
    let taken = tokio::time::timeout(test_timeout(Duration::from_secs(30)), waiter)
        .await
        .expect("B gets the key once A released it")
        .unwrap();
    assert!(taken.token().unwrap() > a_token);
    taken.release().await;
    other.release().await;
}

/// A holder that crashed (stopped renewing without releasing) blocks the key
/// only until its TTL passes by the database clock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crashed_holder_blocks_the_key_until_its_ttl() {
    let Some(db) = crate::harness::test_db(false).await else {
        return;
    };
    let tenant = "lifecycle-crash";
    let a = open_as_process(&db, tenant, "process-a").await;
    let b = open_as_process(&db, tenant, "process-b").await;
    // The crashed holder: acquired, never renewed or released.
    let crashed = a
        .acquire_lease(ARCHIVE_FENCE_SCOPE, "ws-1", SHORT_TTL)
        .await
        .unwrap()
        .expect("free key");
    let hold = tokio::time::timeout(
        test_timeout(Duration::from_secs(30)),
        b.hold_lifecycle_lease(ARCHIVE_FENCE_SCOPE, "ws-1"),
    )
    .await
    .expect("the crashed holder's lease lapses")
    .unwrap();
    assert!(hold.token().unwrap() > crashed.token());
    assert!(!a.lease_is_held(&crashed).await.unwrap());
    hold.release().await;
}

/// While process A deletes the workspace (holds its deletion lease), B's
/// creates and sends in it fail at once; A's own writes stay under A's
/// local gate and are not refused; once A releases (a failed delete) or its
/// lease lapses (a crashed deleter), B writes again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deletion_in_one_process_fails_creates_and_sends_of_another_fast() {
    let Some(db) = crate::harness::test_db(false).await else {
        return;
    };
    let tenant = "lifecycle-delete";
    let a = open_as_process(&db, tenant, "process-a").await;
    let b = open_as_process(&db, tenant, "process-b").await;
    b.insert_agent_session(&session("agent-b")).await.unwrap();
    send(&b, "agent-b").await.unwrap();

    let deletion = a
        .hold_lifecycle_lease(WORKSPACE_DELETION_SCOPE, WS)
        .await
        .unwrap();
    let refused = tokio::time::timeout(
        test_timeout(Duration::from_secs(10)),
        b.insert_agent_session(&session("agent-late")),
    )
    .await
    .expect("a create fails fast, it does not wait for the deletion");
    assert!(is_being_deleted(&refused), "{refused:?}");
    let refused = tokio::time::timeout(test_timeout(Duration::from_secs(10)), send(&b, "agent-b"))
        .await
        .expect("a send fails fast");
    assert!(is_being_deleted(&refused), "{refused:?}");
    // Agent output is not a send: a running agent's reply still lands.
    b.append_agent_message(
        &AgentId("agent-b".into()),
        "assistant",
        &json!([{ "type": "text", "text": "reply" }]),
        "2026-01-01T00:00:02Z",
    )
    .await
    .unwrap();
    // Other workspaces of the tenant stay writable.
    let mut elsewhere = session("agent-elsewhere");
    elsewhere.workspace_id = WorkspaceId("other-ws".into());
    let elsewhere_result = b.insert_agent_session(&elsewhere).await;
    assert!(
        !is_being_deleted(&elsewhere_result),
        "only the deleting workspace refuses: {elsewhere_result:?}"
    );
    // The deleting process's own writes pass (its local gate admits them).
    a.insert_agent_session(&session("agent-a")).await.unwrap();
    send(&a, "agent-a").await.unwrap();
    assert!(b
        .get_agent_session_summary(&AgentId("agent-late".into()))
        .await
        .is_err());

    // A failed delete releases the lease: B writes again.
    deletion.release().await;
    b.insert_agent_session(&session("agent-after"))
        .await
        .unwrap();
    send(&b, "agent-after").await.unwrap();

    // A crashed deleter: its lease lapses by the database clock.
    let crashed = a
        .acquire_lease(WORKSPACE_DELETION_SCOPE, WS, SHORT_TTL)
        .await
        .unwrap()
        .expect("free key");
    assert!(is_being_deleted(
        &b.insert_agent_session(&session("agent-crash")).await
    ));
    // B's own deletion waits for the crashed lease to lapse, then frees it.
    let lapsed = tokio::time::timeout(
        test_timeout(Duration::from_secs(30)),
        b.hold_lifecycle_lease(WORKSPACE_DELETION_SCOPE, WS),
    )
    .await
    .expect("the crashed deleter's lease lapses")
    .unwrap();
    assert!(!b.lease_is_held(&crashed).await.unwrap());
    lapsed.release().await;
    b.insert_agent_session(&session("agent-crash"))
        .await
        .unwrap();
}

/// Creates and sends of B race A taking the deletion lease. Every write of
/// B that was admitted (committed) is in A's session snapshot taken after
/// the lease, and every write that came later failed as "being deleted":
/// the lease acquire and each producer write are ordered by the tenant
/// writer lock, so no write lands behind the snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn admitted_creates_finish_before_the_deletion_snapshot_and_later_ones_fail() {
    let Some(db) = crate::harness::test_db(false).await else {
        return;
    };
    let tenant = "lifecycle-delete-race";
    let a = open_as_process(&db, tenant, "process-a").await;
    let b = open_as_process(&db, tenant, "process-b").await;
    b.insert_agent_session(&session("agent-seed"))
        .await
        .unwrap();
    let producers = 12;
    let start = Arc::new(Barrier::new(producers + 1));
    let handles: Vec<_> = (0..producers)
        .map(|i| {
            let (b, start) = (b.clone(), Arc::clone(&start));
            tokio::spawn(async move {
                start.wait().await;
                if i % 2 == 0 {
                    let id = format!("agent-race-{i}");
                    (
                        Some(id.clone()),
                        b.insert_agent_session(&session(&id)).await,
                    )
                } else {
                    (None, send(&b, "agent-seed").await)
                }
            })
        })
        .collect();
    let deleter = {
        let (a, start) = (a.clone(), Arc::clone(&start));
        tokio::spawn(async move {
            start.wait().await;
            let hold = a
                .hold_lifecycle_lease(WORKSPACE_DELETION_SCOPE, WS)
                .await
                .unwrap();
            let snapshot: Vec<String> = a
                .list_agent_session_summaries(&WorkspaceId(WS.into()))
                .await
                .unwrap()
                .into_iter()
                .map(|s| s.id.0.clone())
                .collect();
            (hold, snapshot)
        })
    };
    let (hold, snapshot) = deleter.await.unwrap();
    for handle in handles {
        let (created, result) = handle.await.unwrap();
        match (created, result) {
            (Some(id), Ok(())) => assert!(
                snapshot.contains(&id),
                "admitted create {id} is in the deletion snapshot {snapshot:?}"
            ),
            (_, Ok(())) => {}
            (_, refused) => assert!(is_being_deleted(&refused), "{refused:?}"),
        }
    }
    // After the snapshot nothing of B lands.
    assert!(is_being_deleted(
        &b.insert_agent_session(&session("agent-after-snapshot"))
            .await
    ));
    let now: Vec<String> = a
        .list_agent_session_summaries(&WorkspaceId(WS.into()))
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.id.0.clone())
        .collect();
    assert_eq!(now, snapshot, "no create landed behind the snapshot");
    hold.release().await;
}
