//! Process-local gates are per tenant, not per `open_postgres` call: two
//! opens of one tenant share the note write lanes, task-event baselines, the
//! displayed-tab overlay, the desktop teardown gate and the writer slot.
//! Other tenants, other databases and `SQLite` stores do not, and the shared
//! state goes away with the last store that holds it.
use std::time::Duration;

use intentd_test_support::test_timeout;

use intent_core::{ContentType, Note, NoteId, NoteMetadata, NoteVisibility, WorkspaceId};
use intent_store::postgres::{PgPool, PostgresOptions, TenantId, WriterLimits};
use intent_store::Store;

use crate::harness::{hold_writer_lock, open, sqlite_store, test_db, writer_lock_waiters};

/// How long a second writer gets to show it is not blocked, or long enough
/// that it would have finished had it not been blocked. No production timer
/// is involved, so a longer window only makes the check stricter.
fn wait() -> Duration {
    test_timeout(Duration::from_millis(300))
}

fn ws() -> WorkspaceId {
    WorkspaceId("__chief__".into())
}

fn nid() -> NoteId {
    NoteId("shared".into())
}

fn note(content: &str) -> Note {
    Note {
        id: nid(),
        workspace_id: ws(),
        title: "Shared".into(),
        content: content.into(),
        content_type: ContentType::Markdown,
        tags: Vec::new(),
        is_pinned: false,
        is_archived: false,
        is_default: false,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        metadata: NoteMetadata::default(),
        created_at: "2026-11-01T00:00:00Z".into(),
        rev: 0,
        updated_at: "2026-11-01T00:00:00Z".into(),
    }
}

fn all_shared(a: &Store, b: &Store) -> bool {
    a.postgres_shared_gates(b)
        .expect("both are Postgres stores")
        .iter()
        .all(|(_, shared)| *shared)
}

fn none_shared(a: &Store, b: &Store) -> bool {
    a.postgres_shared_gates(b)
        .expect("both are Postgres stores")
        .iter()
        .all(|(_, shared)| !*shared)
}

/// Two opens of one tenant take the same note lane, so read → persist
/// windows run one after another and neither edit is lost. With a lane per
/// open, B reads while A holds its lock and A's edit is overwritten.
#[tokio::test]
async fn two_opens_of_one_tenant_serialize_note_writes_on_one_lane() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let a = open(&db, "tenant").await;
    let b = open(&db, "tenant").await;
    a.insert_note(&note("base")).await.unwrap();

    let lock_a = a.lock_note_content(&ws(), &nid()).await;
    assert_eq!(b.note_content_lock_users(&ws(), &nid()), 1);
    let read_a = a.get_note(&ws(), &nid()).await.unwrap();

    let mut writer_b = tokio::spawn({
        let b = b.clone();
        async move {
            let lock_b = b.lock_note_content(&ws(), &nid()).await;
            let mut read_b = b.get_note(&ws(), &nid()).await.unwrap();
            read_b.content.push_str("+B");
            b.update_note(&lock_b, &read_b).await.unwrap();
        }
    });
    assert!(
        tokio::time::timeout(wait(), &mut writer_b).await.is_err(),
        "B wrote while A held the note lane"
    );
    assert_eq!(a.note_content_lock_users(&ws(), &nid()), 2);

    let mut next = read_a;
    next.content.push_str("+A");
    a.update_note(&lock_a, &next).await.unwrap();
    drop(lock_a);
    tokio::time::timeout(test_timeout(Duration::from_secs(10)), writer_b)
        .await
        .expect("B never got the lane")
        .unwrap();

    assert_eq!(a.get_note(&ws(), &nid()).await.unwrap().content, "base+A+B");
    assert!(a.note_content_locks_idle() && b.note_content_locks_idle());
}

#[tokio::test]
async fn opens_of_one_tenant_share_every_gate_and_others_share_none() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let a = open(&db, "tenant").await;
    let b = open(&db, "tenant").await;
    assert!(all_shared(&a, &b), "{:?}", a.postgres_shared_gates(&b));
    assert!(std::sync::Arc::ptr_eq(
        &a.task_event_baselines(),
        &b.task_event_baselines()
    ));

    let other_tenant = open(&db, "other").await;
    assert!(none_shared(&a, &other_tenant));

    let Some(other_db) = test_db(false).await else {
        return;
    };
    let same_tenant_elsewhere = open(&other_db, "tenant").await;
    assert!(none_shared(&a, &same_tenant_elsewhere));

    // Neither a lock nor a waiter crosses tenants or databases.
    let _held = a.lock_note_content(&ws(), &nid()).await;
    for store in [&other_tenant, &same_tenant_elsewhere] {
        assert_eq!(store.note_content_lock_users(&ws(), &nid()), 0);
        let lock = tokio::time::timeout(wait(), store.lock_note_content(&ws(), &nid()))
            .await
            .expect("another tenant's lock waited on this one");
        drop(lock);
    }

    let (sqlite, _dir) = sqlite_store().await;
    assert!(a.postgres_shared_gates(&sqlite).is_none());
    assert!(sqlite.postgres_tenant_state_holders().is_none());
}

/// The registry holds no strong reference: the state lives exactly as long
/// as the stores for that tenant, and the next open starts fresh.
#[tokio::test]
async fn tenant_state_is_released_when_every_store_drops() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let a = open(&db, "tenant").await;
    assert_eq!(a.postgres_tenant_state_holders(), Some(1));
    let b = open(&db, "tenant").await;
    let clone = b.clone();
    assert_eq!(a.postgres_tenant_state_holders(), Some(3));
    drop((b, clone));
    assert_eq!(a.postgres_tenant_state_holders(), Some(1));

    drop(a);
    let fresh = open(&db, "tenant").await;
    assert_eq!(fresh.postgres_tenant_state_holders(), Some(1));
}

/// Batch A's writer slot is per tenant too: writers through two opens of one
/// tenant queue on one slot, so together they park one connection in the
/// writer lock, not one per open.
#[tokio::test]
async fn writers_of_two_opens_of_one_tenant_share_one_writer_slot() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let patient = PostgresOptions {
        writer: WriterLimits {
            wait_budget: test_timeout(Duration::from_secs(60)),
            ..WriterLimits::default()
        },
        ..PostgresOptions::default()
    };
    let tenant = TenantId::new("tenant").unwrap();
    let a = Store::open_postgres(db.app.clone(), tenant.clone(), patient)
        .await
        .unwrap();
    let b = Store::open_postgres(db.app.clone(), tenant, patient)
        .await
        .unwrap();

    // Another daemon holds the tenant's writer lock.
    let held = hold_writer_lock(&db.admin, "tenant").await;
    let writers: Vec<_> = [&a, &a, &b, &b]
        .into_iter()
        .enumerate()
        .map(|(i, store)| {
            let store = store.clone();
            tokio::spawn(async move { store.set_setting(&format!("k{i}"), "\"v\"").await })
        })
        .collect();
    wait_until("all four writers queue", || {
        a.postgres_writers_waiting() + b.postgres_writers_waiting() >= 4
    })
    .await;
    assert_eq!(
        (a.postgres_writers_waiting(), b.postgres_writers_waiting()),
        (4, 4),
        "each open counts the other's writers: one slot"
    );
    let admin = db.admin.clone();
    wait_until_async("one writer waits in the lock", || {
        let admin = admin.clone();
        async move { lock_waiters(&admin).await >= 1 }
    })
    .await;
    assert_eq!(
        lock_waiters(&db.admin).await,
        1,
        "the tenant parks one connection in the lock, not one per open"
    );

    held.rollback().await.unwrap();
    for writer in writers {
        tokio::time::timeout(test_timeout(Duration::from_secs(30)), writer)
            .await
            .expect("writers finish once the lock is free")
            .unwrap()
            .unwrap();
    }
    assert_eq!(a.postgres_writers_waiting(), 0);
}

/// Sessions of this test's database waiting for the writer lock.
async fn lock_waiters(admin: &PgPool) -> usize {
    writer_lock_waiters(admin).await.len()
}

async fn wait_until(what: &str, mut check: impl FnMut() -> bool) {
    wait_until_async(what, || std::future::ready(check())).await;
}

/// Poll `check` until it holds; Postgres emits no event when a lock wait
/// starts.
async fn wait_until_async<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    tokio::time::timeout(test_timeout(Duration::from_secs(15)), async {
        while !check().await {
            // timing-guard: poll interval; Postgres emits no event for lock waits.
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting until {what}"));
}
