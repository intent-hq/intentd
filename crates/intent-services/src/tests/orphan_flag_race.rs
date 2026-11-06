//! intentd#2592 review: flagging a comment orphaned after a note write must
//! not undo a `comment.resolveThread` that ran between the reanchor read and
//! the orphan write (resolving does not bump the note rev, so the note CAS
//! cannot catch it).

use super::setup;
use intent_core::{
    AuthorType, Comment, CommentAnchor, CommentAnchorType, CommentStatus, CommentType, NoteId,
};

fn comment(note: &NoteId, id: &str, parent: Option<&str>) -> Comment {
    let ts = "2026-10-22T00:00:00Z".to_string();
    Comment {
        id: id.into(),
        thread_id: "thread".into(),
        note_id: Some(note.clone()),
        kind: CommentType::Comment,
        content: format!("{id} body"),
        author: "alice".into(),
        author_type: AuthorType::User,
        author_principal_id: None,
        author_identity: None,
        status: CommentStatus::Open,
        parent_id: parent.map(str::to_string),
        anchor: parent.is_none().then(|| CommentAnchor {
            kind: CommentAnchorType::Range,
            start_id: Some("a1".into()),
            end_id: Some("a2".into()),
            point_id: None,
        }),
        anchor_text: None,
        anchor_before: None,
        anchor_after: None,
        suggestion_original: None,
        suggestion_proposed: None,
        agent_id: None,
        is_orphaned: None,
        created_at: ts.clone(),
        updated_at: ts,
    }
}

#[tokio::test]
async fn orphan_flag_write_keeps_a_thread_resolved_between_read_and_write() {
    // The note has no markers for the root, so the reanchor pass orphans it.
    let (_tmp, svc, ws, note) = setup("plain content with no anchors").await;
    let store = &svc.store;
    let root = comment(&note, "11111111-1111-4111-8111-111111111111", None);
    let reply = comment(&note, "reply", Some(&root.id));
    store.insert_comment(&ws, &root).await.unwrap();
    store.insert_comment(&ws, &reply).await.unwrap();

    // Read: the plan captures what to orphan from the current comment rows.
    let plan =
        crate::reanchor_note_comments(store, &ws, &note, "plain content with no anchors".into())
            .await
            .unwrap();

    // Deterministic interleaving: the thread is resolved after the read and
    // before the orphan write.
    let resolved_at = "2026-10-22T01:00:00Z";
    assert_eq!(
        store
            .set_thread_status(&ws, "thread", CommentStatus::Resolved, resolved_at)
            .await
            .unwrap(),
        2
    );
    let reply_before = store.get_comment("reply").await.unwrap();

    // Write.
    plan.apply_orphaned(store, &ws).await.unwrap();

    let root_after = store.get_comment(&root.id).await.unwrap();
    assert_eq!(
        root_after.status,
        CommentStatus::Resolved,
        "resolution kept"
    );
    assert_eq!(root_after.is_orphaned, Some(true), "orphan flag applied");
    assert_eq!(root_after.content, root.content);
    assert_eq!(root_after.anchor, root.anchor);
    assert_ne!(
        root_after.updated_at, resolved_at,
        "orphan flip stamps the row"
    );
    let reply_after = store.get_comment("reply").await.unwrap();
    assert_eq!(reply_after, reply_before, "replies are untouched");
}

#[tokio::test]
async fn orphan_flag_write_ignores_a_comment_deleted_in_between() {
    let (_tmp, svc, ws, note) = setup("plain content with no anchors").await;
    let store = &svc.store;
    let root = comment(&note, "22222222-2222-4222-8222-222222222222", None);
    store.insert_comment(&ws, &root).await.unwrap();
    let plan =
        crate::reanchor_note_comments(store, &ws, &note, "plain content with no anchors".into())
            .await
            .unwrap();
    store.delete_comment(&ws, &root.id).await.unwrap();
    plan.apply_orphaned(store, &ws).await.unwrap();
    assert!(store.get_comment(&root.id).await.is_err());
}
