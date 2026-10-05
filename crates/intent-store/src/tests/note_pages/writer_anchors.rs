//! Anchor readiness follows the last mutation in each registered note writer.
use super::setup;
use crate::{tests::sample_comment, Store};
use intent_core::{Error, Note, NoteId, NoteVersionAuthor};

fn author() -> NoteVersionAuthor {
    NoteVersionAuthor {
        id: "alice".into(),
        name: "Alice".into(),
        author_type: "user".into(),
    }
}
async fn ready(store: &Store, note: &Note) {
    let epochs = store
        .note_annotation_epochs(&note.workspace_id, &note.id)
        .await
        .unwrap();
    assert!(epochs.anchors_ready);
    let actual = store.get_note(&note.workspace_id, &note.id).await.unwrap();
    assert_eq!(epochs.source_revision, actual.rev);
}
async fn anchors(store: &Store, note: &Note) -> Vec<(String, i64, i64)> {
    sqlx::query_as("SELECT a.comment_id,a.start,a.end FROM note_comment_anchor a JOIN note_annotation_head h ON h.id=a.head_id WHERE h.workspace_id=? AND h.note_id=? ORDER BY a.occurrence_id")
        .bind(note.workspace_id.as_str()).bind(note.id.as_str()).fetch_all(store.read_pool()).await.unwrap()
}

#[tokio::test]
async fn versioned_writers_finalize_after_root_insert_and_ignore_metadata_body_placeholder() {
    let (store, _tmp, template) = setup("template").await;
    let mut note = template.clone();
    note.id = NoteId("created".into());
    note.content = "😀<!--anchor:x:start-->ab<!--anchor:x:end-->".into();
    store
        .insert_note_with_version(&note, &author(), &note.updated_at)
        .await
        .unwrap();
    ready(&store, &note).await;
    assert!(anchors(&store, &note).await.is_empty());
    let comment = sample_comment(&note.id, "x", "x");
    note.rev = store
        .update_note_with_comment(&note, Some(note.rev), &comment, &author())
        .await
        .unwrap();
    ready(&store, &note).await;
    assert_eq!(anchors(&store, &note).await, vec![("x".into(), 23, 25)]);
    note.content.insert(0, 'Q');
    note.rev = store
        .update_note_with_version(&note, Some(note.rev), &author(), &note.updated_at)
        .await
        .unwrap()
        .0;
    ready(&store, &note).await;
    assert_eq!(anchors(&store, &note).await, vec![("x".into(), 24, 26)]);
    let persisted_source = note.content.clone();
    note.content.clear();
    note.title = "metadata only".into();
    store
        .update_note_metadata_versioned(&note, Some(note.rev))
        .await
        .unwrap();
    ready(&store, &note).await;
    assert_eq!(
        store
            .get_note(&note.workspace_id, &note.id)
            .await
            .unwrap()
            .content,
        persisted_source
    );
    assert_eq!(anchors(&store, &note).await, vec![("x".into(), 24, 26)]);
}

#[tokio::test]
async fn versioned_anchor_publication_failure_rolls_back_source_version_and_epochs() {
    let (store, _tmp, mut note) = setup("<!--anchor:x:start-->a<!--anchor:x:end-->").await;
    let comment = sample_comment(&note.id, "x", "x");
    note.rev = store
        .update_note_with_comment(&note, Some(note.rev), &comment, &author())
        .await
        .unwrap();
    let before = store.get_note(&note.workspace_id, &note.id).await.unwrap();
    let epochs = store
        .note_annotation_epochs(&note.workspace_id, &note.id)
        .await
        .unwrap();
    let rows = anchors(&store, &note).await;
    let versions = store
        .list_note_versions(&note.workspace_id, &note.id)
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER reject_final_anchor BEFORE INSERT ON note_comment_anchor BEGIN SELECT RAISE(ABORT,'final anchor rejected'); END").execute(store.write_pool()).await.unwrap();
    note.content.insert(0, '😀');
    let result = store
        .update_note_with_version(&note, Some(note.rev), &author(), &note.updated_at)
        .await;
    assert!(
        matches!(result,Err(Error::Internal(message)) if message.contains("final anchor rejected"))
    );
    assert_eq!(
        store.get_note(&note.workspace_id, &note.id).await.unwrap(),
        before
    );
    assert_eq!(
        store
            .note_annotation_epochs(&note.workspace_id, &note.id)
            .await
            .unwrap(),
        epochs
    );
    assert_eq!(anchors(&store, &note).await, rows);
    assert_eq!(
        store
            .list_note_versions(&note.workspace_id, &note.id)
            .await
            .unwrap()
            .len(),
        versions.len()
    );
    sqlx::query("DROP TRIGGER reject_final_anchor")
        .execute(store.write_pool())
        .await
        .unwrap();
    store
        .update_note_with_version(&note, Some(note.rev), &author(), &note.updated_at)
        .await
        .unwrap();
    ready(&store, &note).await;
}

#[tokio::test]
async fn parent_and_children_publish_readiness_in_the_same_versioned_write() {
    let (store, _tmp, mut parent) = setup("before").await;
    let mut child = parent.clone();
    child.id = NoteId("child".into());
    child.parent_id = Some(parent.id.clone());
    child.content = "child source".into();
    parent.content = "after".into();
    store
        .update_note_with_version_and_children(
            &parent,
            Some(parent.rev),
            &[child.clone()],
            &author(),
            &parent.updated_at,
        )
        .await
        .unwrap();
    ready(&store, &parent).await;
    ready(&store, &child).await;
    assert_eq!(
        store
            .list_note_versions(&child.workspace_id, &child.id)
            .await
            .unwrap()
            .len(),
        1
    );
}
