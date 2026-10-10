//! Immediate parent deletion must publish surviving children's comment indexes.
use super::setup;
use crate::{
    note_annotation_repo::{CommentFilter, SourceRange},
    tests::{sample_comment, sample_workspace},
    Store,
};
use intent_core::{Error, Note, NoteId, WorkspaceId};

async fn child(store: &Store, parent: &Note) -> Note {
    let mut note = parent.clone();
    note.id = NoteId::from("child");
    note.parent_id = Some(parent.id.clone());
    note.content = "😀<!--anchor:healthy:start-->ab<!--anchor:healthy:end-->".into();
    store.insert_note(&note).await.unwrap();
    let healthy = sample_comment(&note.id, "healthy-thread", "healthy");
    store
        .insert_comment(&note.workspace_id, &healthy)
        .await
        .unwrap();
    let mut orphan = sample_comment(&note.id, "orphan-thread", "orphan");
    orphan.is_orphaned = Some(true);
    store
        .insert_comment(&note.workspace_id, &orphan)
        .await
        .unwrap();
    readable(store, &note).await;
    note
}

async fn readable(store: &Store, note: &Note) {
    let epochs = store
        .note_annotation_epochs(&note.workspace_id, &note.id)
        .await
        .unwrap();
    let anchored = store
        .read_comment_threads(
            &note.workspace_id,
            &note.id,
            &epochs,
            &[SourceRange {
                start: 0,
                end: i64::try_from(note.content.encode_utf16().count()).unwrap(),
            }],
            CommentFilter::Anchored,
            None,
            8,
        )
        .await
        .unwrap();
    assert_eq!((anchored.total_threads, anchored.total_comments), (1, 1));
    assert_eq!(
        anchored.page.items[0].root_comment_id.as_deref(),
        Some("healthy")
    );
    let orphaned = store
        .read_comment_threads(
            &note.workspace_id,
            &note.id,
            &epochs,
            &[],
            CommentFilter::Orphaned,
            None,
            8,
        )
        .await
        .unwrap();
    assert_eq!((orphaned.total_threads, orphaned.total_comments), (1, 1));
    assert_eq!(
        orphaned.page.items[0].root_comment_id.as_deref(),
        Some("orphan")
    );
    let geometry: Vec<(i64, i64)> = sqlx::query_as("SELECT a.start,a.end FROM note_comment_anchor a JOIN note_annotation_head h ON h.id=a.head_id WHERE h.workspace_id=? AND h.note_id=? ORDER BY a.occurrence_id")
        .bind(note.workspace_id.as_str()).bind(note.id.as_str()).fetch_all(store.read_pool()).await.unwrap();
    assert_eq!(geometry, vec![(29, 31)]);
    assert_eq!(
        store
            .get_note(&note.workspace_id, &note.id)
            .await
            .unwrap()
            .content,
        note.content
    );
}

#[tokio::test]
async fn immediate_delete_keeps_child_anchored_and_orphan_reads_ready() {
    for versioned in [false, true] {
        let (store, _tmp, parent) = setup("parent").await;
        let child = child(&store, &parent).await;
        let before = store
            .note_annotation_epochs(&child.workspace_id, &child.id)
            .await
            .unwrap();
        store
            .delete_note_versioned(
                &parent.workspace_id,
                &parent.id,
                versioned.then_some(parent.rev),
            )
            .await
            .unwrap();
        assert!(matches!(
            store.get_note(&parent.workspace_id, &parent.id).await,
            Err(Error::NotFound(_))
        ));
        let after = store
            .get_note(&child.workspace_id, &child.id)
            .await
            .unwrap();
        assert_eq!(after.parent_id, None);
        assert_eq!(after.rev, child.rev);
        readable(&store, &child).await;
        let epochs = store
            .note_annotation_epochs(&child.workspace_id, &child.id)
            .await
            .unwrap();
        assert_ne!(epochs.comment_revision, before.comment_revision);
        assert!(epochs.anchors_ready);
    }
}

#[tokio::test]
async fn immediate_delete_conflict_missing_and_workspace_guards_preserve_children() {
    let (store, _tmp, parent) = setup("parent").await;
    let child = child(&store, &parent).await;
    let before = store
        .note_annotation_epochs(&child.workspace_id, &child.id)
        .await
        .unwrap();
    let ws = WorkspaceId::from("other");
    store
        .insert_workspace(&sample_workspace(&ws, "Other", false))
        .await
        .unwrap();
    assert!(matches!(
        store
            .delete_note_versioned(&parent.workspace_id, &parent.id, Some(parent.rev + 1))
            .await,
        Err(Error::Conflict { .. })
    ));
    assert!(matches!(
        store.delete_note_versioned(&ws, &parent.id, None).await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        store
            .delete_note_versioned(&parent.workspace_id, &NoteId::from("missing"), None)
            .await,
        Err(Error::NotFound(_))
    ));
    assert_eq!(
        store
            .get_note(&child.workspace_id, &child.id)
            .await
            .unwrap(),
        child
    );
    assert_eq!(
        store
            .note_annotation_epochs(&child.workspace_id, &child.id)
            .await
            .unwrap(),
        before
    );
    let mut other_parent = parent.clone();
    other_parent.workspace_id = ws.clone();
    store.insert_note(&other_parent).await.unwrap();
    let mut other_child = child.clone();
    other_child.workspace_id = ws;
    store.insert_note(&other_child).await.unwrap();
    let other_before = store
        .note_annotation_epochs(&other_child.workspace_id, &other_child.id)
        .await
        .unwrap();
    store
        .delete_note_versioned(&parent.workspace_id, &parent.id, None)
        .await
        .unwrap();
    assert_eq!(
        store
            .get_note(&other_child.workspace_id, &other_child.id)
            .await
            .unwrap(),
        other_child
    );
    assert_eq!(
        store
            .note_annotation_epochs(&other_child.workspace_id, &other_child.id)
            .await
            .unwrap(),
        other_before
    );
    readable(&store, &child).await;
}

#[tokio::test]
async fn immediate_delete_anchor_failure_rolls_back_parent_and_child() {
    let (store, _tmp, parent) = setup("parent").await;
    let child = child(&store, &parent).await;
    let before = store
        .note_annotation_epochs(&child.workspace_id, &child.id)
        .await
        .unwrap();
    let page_before: (String, i64, i64) = sqlx::query_as("SELECT generation,current_rev,indexed_rev FROM note_page_head WHERE workspace_id=? AND note_id=?")
        .bind(child.workspace_id.as_str()).bind(child.id.as_str()).fetch_one(store.read_pool()).await.unwrap();
    sqlx::query("CREATE TRIGGER reject_delete_anchor BEFORE INSERT ON note_comment_anchor WHEN NEW.comment_id='healthy' BEGIN SELECT RAISE(ABORT,'child anchor rejected'); END")
        .execute(store.write_pool()).await.unwrap();
    assert!(
        matches!(store.delete_note_versioned(&parent.workspace_id, &parent.id, Some(parent.rev)).await, Err(Error::Internal(message)) if message.contains("child anchor rejected"))
    );
    assert_eq!(
        store
            .get_note(&parent.workspace_id, &parent.id)
            .await
            .unwrap(),
        parent
    );
    assert_eq!(
        store
            .get_note(&child.workspace_id, &child.id)
            .await
            .unwrap(),
        child
    );
    assert_eq!(
        store
            .note_annotation_epochs(&child.workspace_id, &child.id)
            .await
            .unwrap(),
        before
    );
    let page_after: (String, i64, i64) = sqlx::query_as("SELECT generation,current_rev,indexed_rev FROM note_page_head WHERE workspace_id=? AND note_id=?")
        .bind(child.workspace_id.as_str()).bind(child.id.as_str()).fetch_one(store.read_pool()).await.unwrap();
    assert_eq!(page_after, page_before);
    readable(&store, &child).await;
    sqlx::query("DROP TRIGGER reject_delete_anchor")
        .execute(store.write_pool())
        .await
        .unwrap();
    store
        .delete_note_versioned(&parent.workspace_id, &parent.id, Some(parent.rev))
        .await
        .unwrap();
    readable(&store, &child).await;
}

#[tokio::test]
async fn immediate_delete_cancel_during_child_finalization_rolls_back() {
    use crate::note_annotation_repo::{FinalizerPause, FINALIZER_PAUSE};
    use std::{sync::Arc, time::Duration};
    let (store, _tmp, parent) = setup("parent").await;
    let child = child(&store, &parent).await;
    let before = store
        .note_annotation_epochs(&child.workspace_id, &child.id)
        .await
        .unwrap();
    let pause = Arc::new(FinalizerPause::default());
    let mut delete = Box::pin(FINALIZER_PAUSE.scope(
        Arc::clone(&pause),
        store.delete_note_versioned(&parent.workspace_id, &parent.id, Some(parent.rev)),
    ));
    tokio::select! {
        result = &mut delete => panic!("delete finished before finalizer: {result:?}"),
        () = pause.entered.notified() => {},
        () = tokio::time::sleep(Duration::from_secs(5)) => panic!("delete never reached finalizer"),
    }
    assert!(
        !pause
            .observed
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .anchors_ready
    );
    drop(delete);
    tokio::time::timeout(
        Duration::from_secs(5),
        sqlx::query("BEGIN IMMEDIATE; ROLLBACK").execute(store.write_pool()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        store
            .get_note(&parent.workspace_id, &parent.id)
            .await
            .unwrap(),
        parent
    );
    assert_eq!(
        store
            .get_note(&child.workspace_id, &child.id)
            .await
            .unwrap(),
        child
    );
    assert_eq!(
        store
            .note_annotation_epochs(&child.workspace_id, &child.id)
            .await
            .unwrap(),
        before
    );
    readable(&store, &child).await;
    store
        .delete_note_versioned(&parent.workspace_id, &parent.id, None)
        .await
        .unwrap();
    readable(&store, &child).await;
}

#[tokio::test]
async fn immediate_delete_preserves_legacy_more_than_256_children() {
    let (store, _tmp, parent) = setup("parent").await;
    let mut child = parent.clone();
    child.parent_id = Some(parent.id.clone());
    for index in 0..257 {
        child.id = NoteId::from(format!("child-{index}"));
        store.insert_note(&child).await.unwrap();
    }
    store
        .delete_note_versioned(&parent.workspace_id, &parent.id, None)
        .await
        .unwrap();
    let detached: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM note WHERE workspace_id=? AND parent_id IS NULL AND id LIKE 'child-%'")
        .bind(parent.workspace_id.as_str()).fetch_one(store.read_pool()).await.unwrap();
    assert_eq!(detached, 257);
    let pending: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM note_annotation_head WHERE workspace_id=? AND anchors_rev!=source_rev")
        .bind(parent.workspace_id.as_str()).fetch_one(store.read_pool()).await.unwrap();
    assert_eq!(pending, 0);
}

#[tokio::test]
async fn immediate_delete_self_parent_does_not_finalize_deleted_row() {
    let (store, _tmp, template) = setup("template").await;
    let mut parent = template.clone();
    parent.id = NoteId::from("self-parent");
    parent.parent_id = Some(parent.id.clone());
    store.insert_note(&parent).await.unwrap();
    store
        .delete_note_versioned(&parent.workspace_id, &parent.id, Some(parent.rev))
        .await
        .unwrap();
    assert!(matches!(
        store.get_note(&parent.workspace_id, &parent.id).await,
        Err(Error::NotFound(_))
    ));
}
