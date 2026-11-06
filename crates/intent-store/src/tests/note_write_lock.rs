//! `Store::lock_note_content`: the per-note lock every text-writing `Store`
//! method requires (intent-hq/intent#7184).
use std::time::Duration;

use intent_core::{Error, NoteId, WorkspaceId};

use super::{sample_comment, sample_workspace, stray_note, version_author as author, TempDb};
use crate::Store;

async fn store_with_notes(tmp: &TempDb) -> (Store, WorkspaceId) {
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::from("lock-ws");
    store
        .insert_workspace(&sample_workspace(&ws, "Lock", false))
        .await
        .unwrap();
    for id in ["a", "b"] {
        let mut note = stray_note(&ws, id, id);
        note.content = format!("{id} original");
        store.insert_note(&note).await.unwrap();
    }
    (store, ws)
}

fn assert_refused<T: std::fmt::Debug>(result: crate::Result<T>, what: &str) {
    match result {
        Err(Error::Internal(msg)) => assert!(msg.contains("note write lock"), "{what}: {msg}"),
        other => panic!("{what}: expected the lock to be refused, got {other:?}"),
    }
}

/// Wait until `count` callers hold or await `id`'s lock.
async fn wait_for_users(store: &Store, ws: &WorkspaceId, id: &NoteId, count: usize) {
    tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(10)),
        async {
            while store.note_content_lock_users(ws, id) < count {
                tokio::task::yield_now().await;
            }
        },
    )
    .await
    .expect("lock users never reached the expected count");
}

/// A lock for note A cannot write note B through any text-writing method,
/// and nothing about B changes.
#[tokio::test]
async fn lock_for_another_note_is_refused_and_writes_nothing() {
    let tmp = TempDb::new();
    let (store, ws) = store_with_notes(&tmp).await;
    let a = NoteId::from("a");
    let b = NoteId::from("b");
    let lock_a = store.lock_note_content(&ws, &a).await;
    let mut note_b = store.get_note(&ws, &b).await.unwrap();
    let before = note_b.clone();
    note_b.content = "overwritten".into();

    assert_refused(store.update_note(&lock_a, &note_b).await, "update_note");
    assert_refused(
        store.update_note_versioned(&lock_a, &note_b, None).await,
        "update_note_versioned",
    );
    assert_refused(
        store
            .update_note_with_version(&lock_a, &note_b, None, &author(), "2026-01-02T00:00:00Z")
            .await,
        "update_note_with_version",
    );
    assert_refused(
        store
            .update_note_with_version_and_children(
                &lock_a,
                &note_b,
                None,
                &[],
                &author(),
                "2026-01-02T00:00:00Z",
            )
            .await,
        "update_note_with_version_and_children",
    );
    assert_refused(
        store
            .update_note_with_comment(
                &lock_a,
                &note_b,
                None,
                &sample_comment(&b, "c1", "c1"),
                &author(),
            )
            .await,
        "update_note_with_comment",
    );

    let after = store.get_note(&ws, &b).await.unwrap();
    assert_eq!(after.content, before.content);
    assert_eq!(after.rev, before.rev);
    assert!(store.list_note_versions(&ws, &b).await.unwrap().is_empty());
}

/// A lock taken from a different store (another registry) is refused even
/// for the right note id.
#[tokio::test]
async fn lock_from_another_store_is_refused() {
    let tmp = TempDb::new();
    let (store, ws) = store_with_notes(&tmp).await;
    let other_tmp = TempDb::new();
    let other = Store::open(&other_tmp.path).await.unwrap();
    let a = NoteId::from("a");
    let foreign = other.lock_note_content(&ws, &a).await;
    let mut note = store.get_note(&ws, &a).await.unwrap();
    note.content = "overwritten".into();
    assert_refused(
        store
            .update_note_with_version(&foreign, &note, None, &author(), "2026-01-02T00:00:00Z")
            .await,
        "foreign lock",
    );
    assert_eq!(store.get_note(&ws, &a).await.unwrap().content, "a original");
}

/// The right lock writes, and every clone of the store shares its registry.
#[tokio::test]
async fn lock_from_a_store_clone_writes() {
    let tmp = TempDb::new();
    let (store, ws) = store_with_notes(&tmp).await;
    let a = NoteId::from("a");
    let lock = store.clone().lock_note_content(&ws, &a).await;
    let mut note = store.get_note(&ws, &a).await.unwrap();
    note.content = "written".into();
    store
        .update_note_with_version(
            &lock,
            &note,
            Some(note.rev),
            &author(),
            "2026-01-02T00:00:00Z",
        )
        .await
        .expect("write under the note's own lock");
    assert_eq!(store.get_note(&ws, &a).await.unwrap().content, "written");
}

/// Two callers on one note serialize; the second gets the lock only after
/// the first drops it, and the registry empties once both are done.
#[tokio::test]
async fn lock_serializes_callers_on_one_note() {
    let tmp = TempDb::new();
    let (store, ws) = store_with_notes(&tmp).await;
    let a = NoteId::from("a");
    let first = store.lock_note_content(&ws, &a).await;
    assert_eq!(store.note_content_lock_users(&ws, &a), 1);

    let second = {
        let (store, ws, a) = (store.clone(), ws.clone(), a.clone());
        intent_core::spawn_daemon(async move {
            let lock = store.lock_note_content(&ws, &a).await;
            drop(lock);
        })
    };
    wait_for_users(&store, &ws, &a, 2).await;
    assert!(!second.is_finished(), "second caller got a held lock");

    drop(first);
    tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(10)),
        second,
    )
    .await
    .expect("second caller never got the released lock")
    .expect("second caller task");
    assert!(store.note_content_locks_idle());
}

/// Callers on different notes never wait on each other.
#[tokio::test]
async fn locks_on_different_notes_are_independent() {
    let tmp = TempDb::new();
    let (store, ws) = store_with_notes(&tmp).await;
    let _a = store.lock_note_content(&ws, &NoteId::from("a")).await;
    let b = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(10)),
        store.lock_note_content(&ws, &NoteId::from("b")),
    )
    .await
    .expect("lock on another note waited");
    assert_eq!(b.note_id(), &NoteId::from("b"));
    assert_eq!(b.workspace_id(), &ws);
}

/// A caller that gives up while waiting deregisters, so the entry is
/// dropped once the holder releases.
#[tokio::test]
async fn cancelled_waiter_leaves_no_entry() {
    let tmp = TempDb::new();
    let (store, ws) = store_with_notes(&tmp).await;
    let a = NoteId::from("a");
    let held = store.lock_note_content(&ws, &a).await;
    let waited = tokio::time::timeout(Duration::ZERO, store.lock_note_content(&ws, &a)).await;
    assert!(waited.is_err(), "waiter got a held lock");
    assert_eq!(store.note_content_lock_users(&ws, &a), 1);
    drop(held);
    assert!(store.note_content_locks_idle());
}
