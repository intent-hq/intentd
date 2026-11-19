//! Note write lanes under concurrent editors (spec §6.5.3).
//!
//! Invariants:
//! - On one handle, the note lane serializes read → edit → persist: N
//!   editors that each append a marker through `lock_note_content` and the
//!   unversioned `update_note` all land, and the revision counts every
//!   write.
//! - Across handles (separate lanes, as across daemons) a versioned write is
//!   a compare-and-swap on the revision it read: an editor that lost the
//!   race gets `Conflict`, re-reads and retries, so again every marker lands
//!   exactly once. Unversioned writes across handles are not safe today
//!   (audit finding 3); they are not tested here.

use std::fmt::Write as _;

use intent_core::{ContentType, Error, Note, NoteId, NoteMetadata, NoteVisibility, WorkspaceId};

use crate::concurrency_support::{handles, race, WRITERS};
use crate::harness::parity_test;

const AT: &str = "2026-02-01T00:00:00Z";

fn ws() -> WorkspaceId {
    WorkspaceId("__chief__".into())
}

fn note(id: &str, tenant: &str) -> Note {
    Note {
        id: NoteId(id.into()),
        workspace_id: ws(),
        title: format!("{id} of {tenant}"),
        content: format!("{tenant}:"),
        content_type: ContentType::Markdown,
        tags: vec![],
        is_pinned: false,
        is_archived: false,
        is_default: false,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        metadata: NoteMetadata::default(),
        created_at: AT.into(),
        rev: 0,
        updated_at: AT.into(),
    }
}

/// Every marker `[0]..[WRITERS)` appears exactly once after the prefix.
#[track_caller]
fn assert_every_marker_once(content: &str, tenant: &str) {
    let body = content
        .strip_prefix(&format!("{tenant}:"))
        .unwrap_or_else(|| panic!("the note keeps its prefix: {content:?}"));
    for writer in 0..WRITERS {
        assert_eq!(
            body.matches(&format!("[{writer}]")).count(),
            1,
            "writer {writer}'s edit is present once: {content:?}"
        );
    }
    assert_eq!(
        body.matches('[').count(),
        WRITERS,
        "no other edit: {content:?}"
    );
}

parity_test!(
    one_handle_lane_serializes_unversioned_note_edits,
    |store, tenant| {
        store.insert_note(&note("lane", tenant)).await.unwrap();
        let id = NoteId("lane".into());
        let lane_id = id.clone();
        race(std::slice::from_ref(&store), WRITERS, |writer, store| {
            let id = lane_id.clone();
            async move {
                let lock = store.lock_note_content(&ws(), &id).await;
                let mut current = store.get_note(&ws(), &id).await.unwrap();
                tokio::task::yield_now().await;
                write!(current.content, "[{writer}]").expect("write to String");
                store.update_note(&lock, &current).await.unwrap();
            }
        })
        .await;
        let after = store.get_note(&ws(), &id).await.unwrap();
        assert_every_marker_once(&after.content, tenant);
        assert_eq!(after.rev, i64::try_from(WRITERS).unwrap());
    }
);

parity_test!(
    versioned_note_edits_across_handles_retry_on_conflict,
    |store, tenant, db| {
        store.insert_note(&note("cas", tenant)).await.unwrap();
        let all = handles(&store, db, tenant).await;
        let id = NoteId("cas".into());
        let cas_id = id.clone();
        let conflicts = race(&all, WRITERS, |writer, store| {
            let id = cas_id.clone();
            async move {
                let mut conflicts = 0_usize;
                loop {
                    let lock = store.lock_note_content(&ws(), &id).await;
                    let mut current = store.get_note(&ws(), &id).await.unwrap();
                    let read = current.rev;
                    tokio::task::yield_now().await;
                    write!(current.content, "[{writer}]").expect("write to String");
                    match store
                        .update_note_versioned(&lock, &current, Some(read))
                        .await
                    {
                        Ok(rev) => {
                            assert_eq!(rev, read + 1, "a won write bumps the revision it read");
                            return conflicts;
                        }
                        Err(Error::Conflict { .. }) => conflicts += 1,
                        Err(e) => panic!("writer {writer}: {e}"),
                    }
                }
            }
        })
        .await;
        let after = store.get_note(&ws(), &id).await.unwrap();
        assert_every_marker_once(&after.content, tenant);
        assert_eq!(after.rev, i64::try_from(WRITERS).unwrap());
        if db.is_none() {
            assert_eq!(
                conflicts.iter().sum::<usize>(),
                0,
                "one handle's lane leaves no versioned write to conflict"
            );
        }
    }
);
