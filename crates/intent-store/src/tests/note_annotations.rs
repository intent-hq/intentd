use crate::note_annotation_repo::*;
use crate::tests::{sample_comment, sample_workspace, stray_note, TempDb};
use crate::Store;
use intent_core::{LineAttributionData, LineAttributionInfo, NoteId, WorkspaceId};

async fn fixture() -> (TempDb, Store, WorkspaceId, NoteId) {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.unwrap();
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .unwrap();
    let mut note = stray_note(&ws, "spec", "Note");
    note.content = "one\n😀 two\nthree".into();
    store.insert_note(&note).await.unwrap();
    (db, store, ws, note.id)
}

fn attribution(ws: &WorkspaceId, note: &NoteId, timestamp: i64) -> LineAttributionData {
    LineAttributionData {
        workspace_id: ws.clone(),
        note_id: note.clone(),
        computed_at: "2026-10-05T00:00:00Z".into(),
        attributions: (1..=3)
            .map(|line| {
                (
                    line.to_string(),
                    LineAttributionInfo {
                        timestamp,
                        author: None,
                    },
                )
            })
            .collect(),
    }
}

#[tokio::test]
async fn attribution_rejects_delayed_generation_and_pages_utf16_line_extents() {
    let (_db, store, ws, note) = fixture().await;
    let initial = store.note_annotation_epochs(&ws, &note).await.unwrap();
    let old = store
        .begin_note_attribution(&ws, &note, initial.source_revision)
        .await
        .unwrap();
    let current = store
        .begin_note_attribution(&ws, &note, initial.source_revision)
        .await
        .unwrap();
    assert!(store
        .publish_note_attribution(&old, "one\n😀 two\nthree", &attribution(&ws, &note, 1))
        .await
        .is_err());
    store
        .publish_note_attribution(&current, "one\n😀 two\nthree", &attribution(&ws, &note, 2))
        .await
        .unwrap();
    let ranges = [SourceRange { start: 5, end: 6 }];
    let page = store
        .read_attribution_rows(&ws, &note, &current.epochs, &ranges, None, 1)
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].line, 2);
    assert_eq!(
        page.items[0].source_range,
        SourceRange { start: 4, end: 11 }
    );
    assert_eq!(page.items[0].timestamp, 2);
    assert!(!page.has_more);
    assert!(store
        .read_attribution_rows(&ws, &note, &current.epochs, &[], None, 1)
        .await
        .unwrap()
        .items
        .is_empty());
    assert_eq!(
        store
            .get_note_line_attribution(&ws, &note)
            .await
            .unwrap()
            .unwrap()
            .attributions["2"]
            .timestamp,
        2
    );
    assert_eq!(page.epochs.comment_revision, initial.comment_revision);
}

#[tokio::test]
async fn source_changes_reject_inflight_attribution_without_replacing_legacy_snapshot() {
    let (_db, store, ws, note) = fixture().await;
    let epochs = store.note_annotation_epochs(&ws, &note).await.unwrap();
    let job = store
        .begin_note_attribution(&ws, &note, epochs.source_revision)
        .await
        .unwrap();
    sqlx::query("UPDATE note SET content = 'new', rev = rev + 1 WHERE workspace_id = ? AND id = ?")
        .bind(ws.as_str())
        .bind(note.as_str())
        .execute(store.write_pool())
        .await
        .unwrap();
    assert!(store
        .publish_note_attribution(&job, "one\n😀 two\nthree", &attribution(&ws, &note, 3))
        .await
        .is_err());
    assert!(store
        .get_note_line_attribution(&ws, &note)
        .await
        .unwrap()
        .is_none());
    let changed = store.note_annotation_epochs(&ws, &note).await.unwrap();
    assert!(!changed.attribution_ready);
    assert!(!changed.anchors_ready);
    assert_ne!(changed.comment_revision, epochs.comment_revision);
}

#[tokio::test]
async fn comment_epochs_and_keysets_cover_large_replies_without_bodies() {
    let (_db, store, ws, note) = fixture().await;
    let initial = store.note_annotation_epochs(&ws, &note).await.unwrap();
    let mut root = sample_comment(&note, "thread", "root");
    root.content = "😀".repeat(100_000);
    root.created_at = "same".into();
    store.insert_comment(&ws, &root).await.unwrap();
    for id in ["a", "b", "c"] {
        let mut reply = root.clone();
        reply.id = id.into();
        reply.parent_id = Some("root".into());
        reply.anchor = None;
        store.insert_comment(&ws, &reply).await.unwrap();
    }
    let epoch = store.note_annotation_epochs(&ws, &note).await.unwrap();
    assert_eq!(epoch.source_revision, initial.source_revision);
    assert_eq!(epoch.attribution_generation, initial.attribution_generation);
    assert_ne!(epoch.comment_revision, initial.comment_revision);
    let page = store
        .read_comment_rows(&ws, &note, &epoch, "thread", None, 2)
        .await
        .unwrap();
    assert_eq!(page.total_comments, 4);
    assert_eq!(page.root_comment_id.as_deref(), Some("root"));
    assert_eq!(
        page.page
            .items
            .iter()
            .map(|r| r.id.as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
    assert!(page.page.has_more);
    assert!(page
        .page
        .items
        .iter()
        .all(|r| r.preview.len() <= 1024 && r.truncated));
    let next = store
        .read_comment_rows(&ws, &note, &epoch, "thread", Some(("same", "b")), 2)
        .await
        .unwrap();
    assert_eq!(
        next.page
            .items
            .iter()
            .map(|r| r.id.as_str())
            .collect::<Vec<_>>(),
        ["c", "root"]
    );
    assert!(!next.page.has_more);
    store.delete_comment(&ws, "a").await.unwrap();
    assert!(store
        .read_comment_rows(&ws, &note, &epoch, "thread", None, 2)
        .await
        .is_err());
}

#[tokio::test]
async fn comment_overlap_includes_outside_start_points_and_deduplicates_occurrences() {
    let (_db, store, ws, note) = fixture().await;
    for id in ["span", "point", "orphan"] {
        store
            .insert_comment(&ws, &sample_comment(&note, id, id))
            .await
            .unwrap();
    }
    let epoch = store.note_annotation_epochs(&ws, &note).await.unwrap();
    let anchors = [
        AnchorOccurrence {
            comment_id: "span".into(),
            occurrence_id: "first".into(),
            source_range: SourceRange { start: 0, end: 15 },
        },
        AnchorOccurrence {
            comment_id: "span".into(),
            occurrence_id: "repeat".into(),
            source_range: SourceRange { start: 4, end: 12 },
        },
        AnchorOccurrence {
            comment_id: "point".into(),
            occurrence_id: "point".into(),
            source_range: SourceRange { start: 5, end: 5 },
        },
    ];
    store
        .publish_comment_anchors(&ws, &note, &epoch, &anchors)
        .await
        .unwrap();
    let ranges = [
        SourceRange { start: 5, end: 6 },
        SourceRange { start: 10, end: 11 },
    ];
    let page = store
        .read_comment_threads(
            &ws,
            &note,
            &epoch,
            &ranges,
            CommentFilter::Anchored,
            None,
            1,
        )
        .await
        .unwrap();
    assert_eq!((page.total_threads, page.total_comments), (2, 2));
    assert!(page.page.has_more);
    assert_eq!(page.page.items[0].thread_id, "point");
    let next = store
        .read_comment_threads(
            &ws,
            &note,
            &epoch,
            &ranges,
            CommentFilter::Anchored,
            Some((5, "point")),
            1,
        )
        .await
        .unwrap();
    assert_eq!(next.page.items[0].thread_id, "span");
    assert!(!next.page.has_more);
    let endpoint = store
        .read_comment_threads(
            &ws,
            &note,
            &epoch,
            &[SourceRange { start: 4, end: 5 }],
            CommentFilter::Anchored,
            None,
            10,
        )
        .await
        .unwrap();
    assert_eq!(endpoint.total_threads, 1);
    let empty = store
        .read_comment_threads(&ws, &note, &epoch, &[], CommentFilter::Anchored, None, 10)
        .await
        .unwrap();
    assert_eq!(empty.total_threads, 0);
    let orphan = store
        .read_comment_threads(&ws, &note, &epoch, &[], CommentFilter::Orphaned, None, 10)
        .await
        .unwrap();
    assert_eq!(orphan.page.items[0].thread_id, "orphan");
    let all = store
        .read_comment_threads(&ws, &note, &epoch, &[], CommentFilter::All, None, 10)
        .await
        .unwrap();
    assert_eq!(all.total_threads, 3);
    assert!(store
        .publish_comment_anchors(&ws, &note, &epoch, &anchors)
        .await
        .is_err());
}

#[tokio::test]
async fn subscription_generation_is_exact_above_signed_max_and_exhaustion_rolls_back() {
    let (_db, store, ws, note) = fixture().await;
    for value in ["999999999", "9223372036854775807", "18446744073709551614"] {
        sqlx::query("UPDATE note_annotation_state SET state_generation=? WHERE workspace_id=? AND note_id=?")
            .bind(value).bind(ws.as_str()).bind(note.as_str()).execute(store.write_pool()).await.unwrap();
        let epoch = store.note_annotation_epochs(&ws, &note).await.unwrap();
        store
            .begin_note_attribution(&ws, &note, epoch.source_revision)
            .await
            .unwrap();
        let state = store.read_note_page_state(&ws, &note, None).await.unwrap();
        assert_eq!(
            state["stateGeneration"],
            (value.parse::<u64>().unwrap() + 1).to_string()
        );
    }
    let before = store.read_note_page_state(&ws, &note, None).await.unwrap();
    assert!(store
        .insert_comment(&ws, &sample_comment(&note, "thread", "root"))
        .await
        .is_err());
    assert!(store.get_comment("root").await.is_err());
    assert_eq!(
        store.read_note_page_state(&ws, &note, None).await.unwrap(),
        before
    );
}

#[tokio::test]
async fn deletion_tombstone_preserves_last_epochs_and_recreated_note_has_new_scope() {
    let (_db, store, ws, note) = fixture().await;
    store
        .insert_comment(&ws, &sample_comment(&note, "thread", "root"))
        .await
        .unwrap();
    let before = store.read_note_page_state(&ws, &note, None).await.unwrap();
    let instance = before["scope"]["noteInstanceId"].as_str().unwrap();
    sqlx::query("DELETE FROM note WHERE workspace_id=? AND id=?")
        .bind(ws.as_str())
        .bind(note.as_str())
        .execute(store.write_pool())
        .await
        .unwrap();
    let deleted = store
        .read_note_page_state(&ws, &note, Some(instance))
        .await
        .unwrap();
    assert_eq!(deleted["deleted"], true);
    for field in [
        "sourceRevision",
        "attributionGeneration",
        "attributionState",
        "commentRevision",
    ] {
        assert_eq!(deleted[field], before[field]);
    }
    assert_eq!(
        deleted["stateGeneration"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap(),
        before["stateGeneration"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            + 1
    );
    store
        .insert_note(&stray_note(&ws, note.as_str(), "recreated"))
        .await
        .unwrap();
    let new = store.read_note_page_state(&ws, &note, None).await.unwrap();
    assert_ne!(
        new["scope"]["noteInstanceId"],
        before["scope"]["noteInstanceId"]
    );
    assert_eq!(new["deleted"], false);
    assert_eq!(
        store
            .read_note_page_state(&ws, &note, Some(instance))
            .await
            .unwrap(),
        deleted
    );
}

#[tokio::test]
async fn legacy_attribution_invalidates_index_and_state_survives_restart() {
    let (db, store, ws, note) = fixture().await;
    let epoch = store.note_annotation_epochs(&ws, &note).await.unwrap();
    let job = store
        .begin_note_attribution(&ws, &note, epoch.source_revision)
        .await
        .unwrap();
    store
        .publish_note_attribution(&job, "one\n😀 two\nthree", &attribution(&ws, &note, 1))
        .await
        .unwrap();
    store
        .upsert_note_line_attribution(&attribution(&ws, &note, 2))
        .await
        .unwrap();
    let updated = store.note_annotation_epochs(&ws, &note).await.unwrap();
    assert!(!updated.attribution_ready);
    assert_ne!(
        updated.attribution_generation,
        job.epochs.attribution_generation
    );
    assert!(store
        .read_attribution_rows(
            &ws,
            &note,
            &job.epochs,
            &[SourceRange { start: 0, end: 1 }],
            None,
            1
        )
        .await
        .is_err());
    let state = store.read_note_page_state(&ws, &note, None).await.unwrap();
    drop(store);
    let reopened = Store::open(&db.path).await.unwrap();
    assert_eq!(
        reopened
            .read_note_page_state(&ws, &note, None)
            .await
            .unwrap(),
        state
    );
    assert_eq!(
        reopened
            .get_note_line_attribution(&ws, &note)
            .await
            .unwrap()
            .unwrap()
            .attributions["1"]
            .timestamp,
        2
    );
}

#[tokio::test]
async fn annotation_queries_do_not_cross_workspaces_and_ranges_deduplicate_lines() {
    let (_db, store, ws, note) = fixture().await;
    let other = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&other, "Other", false))
        .await
        .unwrap();
    store
        .insert_note(&stray_note(&other, note.as_str(), "Same ID"))
        .await
        .unwrap();
    let epoch = store.note_annotation_epochs(&ws, &note).await.unwrap();
    let job = store
        .begin_note_attribution(&ws, &note, epoch.source_revision)
        .await
        .unwrap();
    store
        .publish_note_attribution(&job, "one\n😀 two\nthree", &attribution(&ws, &note, 1))
        .await
        .unwrap();
    let page = store
        .read_attribution_rows(
            &ws,
            &note,
            &job.epochs,
            &[
                SourceRange { start: 4, end: 5 },
                SourceRange { start: 8, end: 9 },
            ],
            None,
            2,
        )
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].line, 2);
    assert!(store
        .read_attribution_rows(
            &other,
            &note,
            &job.epochs,
            &[SourceRange { start: 0, end: 1 }],
            None,
            1
        )
        .await
        .is_err());
    store
        .insert_comment(&ws, &sample_comment(&note, "private-thread", "root"))
        .await
        .unwrap();
    let foreign = store.note_annotation_epochs(&other, &note).await.unwrap();
    assert!(store
        .read_comment_rows(&other, &note, &foreign, "private-thread", None, 10)
        .await
        .is_err());
}

#[tokio::test]
async fn anchor_index_participates_in_source_transaction_rollback() {
    let (_db, store, ws, note) = fixture().await;
    store
        .insert_comment(&ws, &sample_comment(&note, "thread", "root"))
        .await
        .unwrap();
    let before = store.read_note_page_state(&ws, &note, None).await.unwrap();
    let mut tx = store
        .write_pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap();
    sqlx::query("UPDATE note SET content='replacement',rev=rev+1 WHERE workspace_id=? AND id=?")
        .bind(ws.as_str())
        .bind(note.as_str())
        .execute(&mut *tx)
        .await
        .unwrap();
    // Simulate the source index maintained by the owning mutation transaction.
    sqlx::query("UPDATE note_page_head SET indexed_rev=current_rev,source_length=11 WHERE workspace_id=? AND note_id=?")
        .bind(ws.as_str()).bind(note.as_str()).execute(&mut *tx).await.unwrap();
    let (_, epoch) = crate::note_annotation_repo::head(&mut tx, &ws, &note)
        .await
        .unwrap();
    crate::note_annotation_repo::publish_anchors_in_transaction(
        &mut tx,
        &ws,
        &note,
        &epoch,
        &[AnchorOccurrence {
            comment_id: "root".into(),
            occurrence_id: "kept".into(),
            source_range: SourceRange { start: 1, end: 5 },
        }],
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(
        store.read_note_page_state(&ws, &note, None).await.unwrap(),
        before
    );
    assert_eq!(
        store.get_note(&ws, &note).await.unwrap().content,
        "one\n😀 two\nthree"
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM note_comment_anchor")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    let original = store.note_annotation_epochs(&ws, &note).await.unwrap();
    // A reply cannot acquire an independent anchor. The successful first insert
    // in the failed batch is rolled back too, leaving no partially visible index.
    let mut reply = sample_comment(&note, "thread", "reply");
    reply.parent_id = Some("root".into());
    reply.anchor = None;
    store.insert_comment(&ws, &reply).await.unwrap();
    let changed = store.note_annotation_epochs(&ws, &note).await.unwrap();
    assert!(store
        .publish_comment_anchors(&ws, &note, &original, &[])
        .await
        .is_err());
    assert!(store
        .publish_comment_anchors(
            &ws,
            &note,
            &changed,
            &[
                AnchorOccurrence {
                    comment_id: "root".into(),
                    occurrence_id: "good".into(),
                    source_range: SourceRange { start: 1, end: 2 }
                },
                AnchorOccurrence {
                    comment_id: "reply".into(),
                    occurrence_id: "bad".into(),
                    source_range: SourceRange { start: 1, end: 2 }
                },
            ]
        )
        .await
        .is_err());
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM note_comment_anchor")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn huge_comment_fields_reconstruct_from_bounded_scalar_safe_fragments() {
    let (_db, store, ws, note) = fixture().await;
    let mut root = sample_comment(&note, "thread", "root");
    root.content = format!("{}{}\0tail", "a".repeat(1023), "😀界\\\"".repeat(2000));
    root.author = "huge author".repeat(1000);
    root.anchor_text = None;
    store.insert_comment(&ws, &root).await.unwrap();
    let epoch = store.note_annotation_epochs(&ws, &note).await.unwrap();
    let mut rebuilt = String::new();
    let mut offset = 0;
    let mut utf16 = 0;
    loop {
        let fragment = store
            .read_annotation_fragment(
                &ws,
                &note,
                &epoch,
                AnnotationDetail::Comment {
                    comment_id: "root",
                    field: CommentDetailField::Body,
                },
                offset,
                1024,
            )
            .await
            .unwrap();
        assert!(fragment.text.len() <= 1024);
        assert!(fragment.byte_end > offset);
        rebuilt.push_str(&fragment.text);
        utf16 += fragment.utf16_length;
        offset = fragment.byte_end;
        if offset == fragment.total_bytes {
            break;
        }
    }
    assert_eq!(rebuilt, root.content);
    assert_eq!(utf16, root.content.encode_utf16().count());
    let absent = store
        .read_annotation_fragment(
            &ws,
            &note,
            &epoch,
            AnnotationDetail::Comment {
                comment_id: "root",
                field: CommentDetailField::AnchorText,
            },
            0,
            4,
        )
        .await
        .unwrap();
    assert!(absent.is_null);
    assert!(absent.text.is_empty());
    assert!(store
        .read_annotation_fragment(
            &ws,
            &note,
            &epoch,
            AnnotationDetail::Comment {
                comment_id: "root",
                field: CommentDetailField::Body
            },
            1024,
            4
        )
        .await
        .is_err());
    root.content = "changed".into();
    store.update_comment(&ws, &root).await.unwrap();
    assert!(store
        .read_annotation_fragment(
            &ws,
            &note,
            &epoch,
            AnnotationDetail::Comment {
                comment_id: "root",
                field: CommentDetailField::Body
            },
            0,
            1024
        )
        .await
        .is_err());
}
