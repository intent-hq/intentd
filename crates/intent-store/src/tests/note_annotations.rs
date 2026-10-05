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

async fn annotation_scope(
    store: &Store,
    ws: &WorkspaceId,
    note: &NoteId,
) -> (intent_core::note_page::NoteScope, String) {
    let state = store.read_note_page_state(ws, note, None).await.unwrap();
    (
        serde_json::from_value(state["scope"].clone()).unwrap(),
        state["sourceRevision"].as_str().unwrap().into(),
    )
}

#[tokio::test]
async fn annotation_page_cursors_bind_query_principal_epochs_budget_and_expiry() {
    use serde_json::json;
    let (_db, store, ws, note) = fixture().await;
    let epochs = store.note_annotation_epochs(&ws, &note).await.unwrap();
    let job = store
        .begin_note_attribution(&ws, &note, epochs.source_revision)
        .await
        .unwrap();
    store
        .publish_note_attribution(&job, "one\n😀 two\nthree", &attribution(&ws, &note, 2))
        .await
        .unwrap();
    let (scope, source) = annotation_scope(&store, &ws, &note).await;
    let mut request:AnnotationPageRequest=serde_json::from_value(json!({"kind":"attribution","ranges":[{"start":0,"end":17}],"maxItems":1,"maxWireBytes":4096})).unwrap();
    let first = store
        .read_note_annotation_page("alice", &scope, &source, None, None, &request, &json!(3))
        .await
        .unwrap();
    assert_eq!(first["items"][0]["startLine"], 1);
    let generation = first["attributionGeneration"].as_str().unwrap();
    request.cursor = Some(first["nextCursor"].as_str().unwrap().into());
    store
        .insert_comment(&ws, &sample_comment(&note, "thread", "root"))
        .await
        .unwrap();
    let second = store
        .read_note_annotation_page(
            "alice",
            &scope,
            &source,
            Some(generation),
            None,
            &request,
            &json!(4),
        )
        .await
        .unwrap();
    assert_eq!(second["items"][0]["startLine"], 2);
    assert!(store
        .read_note_annotation_page(
            "bob",
            &scope,
            &source,
            Some(generation),
            None,
            &request,
            &json!(4)
        )
        .await
        .is_err());
    request.max_items = Some(2);
    assert!(store
        .read_note_annotation_page(
            "alice",
            &scope,
            &source,
            Some(generation),
            None,
            &request,
            &json!(4)
        )
        .await
        .is_err());
    request.max_items = Some(1);
    request.ranges = Some(vec![AnnotationRange { start: 0, end: 4 }]);
    assert!(store
        .read_note_annotation_page(
            "alice",
            &scope,
            &source,
            Some(generation),
            None,
            &request,
            &json!(4)
        )
        .await
        .is_err());
    request.ranges = Some(vec![AnnotationRange { start: 0, end: 17 }]);
    sqlx::query("UPDATE note_annotation_snapshot SET expires_ms=0")
        .execute(store.write_pool())
        .await
        .unwrap();
    assert!(store
        .read_note_annotation_page(
            "alice",
            &scope,
            &source,
            Some(generation),
            None,
            &request,
            &json!(4)
        )
        .await
        .is_err());
    request.cursor = None;
    let fresh = store
        .read_note_annotation_page("alice", &scope, &source, None, None, &request, &json!(4))
        .await
        .unwrap();
    request.cursor = Some(fresh["nextCursor"].as_str().unwrap().into());
    store
        .begin_note_attribution(&ws, &note, epochs.source_revision)
        .await
        .unwrap();
    assert!(store
        .read_note_annotation_page(
            "alice",
            &scope,
            &source,
            Some(generation),
            None,
            &request,
            &json!(4)
        )
        .await
        .is_err());
    request.cursor = None;
    let pending = store
        .read_note_annotation_page("alice", &scope, &source, None, None, &request, &json!(4))
        .await
        .unwrap();
    assert_eq!(pending["state"], "pending");
    assert_eq!(pending["items"], json!([]));
    assert!(pending["nextCursor"].is_null());
}

#[tokio::test]
async fn annotation_reply_pages_and_context_reconstruct_escaped_bodies_with_exact_frame_bounds() {
    use serde_json::json;
    let (_db, store, ws, note) = fixture().await;
    let text = "\"\\\n\u{0000}😀".repeat(5000);
    for index in 0..20 {
        let mut c = sample_comment(&note, "huge", &format!("comment{index:02}"));
        c.content = text.clone();
        if index > 0 {
            c.parent_id = Some("comment00".into());
        }
        store.insert_comment(&ws, &c).await.unwrap();
    }
    let (scope, source) = annotation_scope(&store, &ws, &note).await;
    let mut request: AnnotationPageRequest =
        serde_json::from_value(json!({"kind":"replies","maxItems":64,"maxWireBytes":4096}))
            .unwrap();
    let first = store
        .read_note_annotation_page(
            "alice",
            &scope,
            &source,
            None,
            Some("huge"),
            &request,
            &json!("request"),
        )
        .await
        .unwrap();
    assert_eq!(first["totalComments"], 20);
    assert!(first["items"].as_array().unwrap().len() < 20);
    let epoch = first["commentRevision"].as_str().unwrap();
    let mut ids = Vec::new();
    let mut current = first.clone();
    loop {
        assert!(
            json!({"jsonrpc":"2.0","id":"request","result":current})
                .to_string()
                .len()
                <= 4096
        );
        for item in current["items"].as_array().unwrap() {
            assert!(item["preview"].as_str().unwrap().len() <= 512);
            ids.push(item["commentId"].as_str().unwrap().to_owned());
        }
        let Some(cursor) = current["nextCursor"].as_str() else {
            break;
        };
        request.cursor = Some(cursor.to_owned());
        current = store
            .read_note_annotation_page(
                "alice",
                &scope,
                &source,
                Some(epoch),
                Some("huge"),
                &request,
                &json!("request"),
            )
            .await
            .unwrap();
    }
    ids.sort();
    assert_eq!(
        ids,
        (0..20)
            .map(|i| format!("comment{i:02}"))
            .collect::<Vec<_>>()
    );
    let mut detail = AnnotationContextRequest {
        kind: "context".into(),
        context_ref: first["items"][0]["bodyRef"].as_str().unwrap().into(),
        cursor: None,
        max_items: Some(1),
        max_wire_bytes: Some(4096),
    };
    let mut reconstructed = String::new();
    loop {
        let part = store
            .read_note_annotation_context(
                "alice",
                &scope,
                &source,
                epoch,
                &detail,
                &json!("request"),
            )
            .await
            .unwrap();
        assert!(
            json!({"jsonrpc":"2.0","id":"request","result":part})
                .to_string()
                .len()
                <= 4096
        );
        let fragment = &part["items"][0];
        assert_eq!(
            usize::try_from(fragment["offset"].as_u64().unwrap()).unwrap(),
            reconstructed.encode_utf16().count()
        );
        reconstructed.push_str(fragment["text"].as_str().unwrap());
        let Some(cursor) = part["nextCursor"].as_str() else {
            break;
        };
        detail.cursor = Some(cursor.into());
    }
    assert_eq!(reconstructed, text);
    assert!(store
        .read_note_annotation_context("bob", &scope, &source, epoch, &detail, &json!("request"))
        .await
        .is_err());
    sqlx::query("UPDATE comment SET status='resolved' WHERE id='comment00'")
        .execute(store.write_pool())
        .await
        .unwrap();
    assert!(store
        .read_note_annotation_context("alice", &scope, &source, epoch, &detail, &json!("request"))
        .await
        .is_err());
}

#[tokio::test]
async fn annotation_identity_directory_preserves_absent_empty_and_oversized_fields() {
    use serde_json::json;
    let (_db, store, ws, note) = fixture().await;
    store
        .insert_comment(&ws, &sample_comment(&note, "thread", "root"))
        .await
        .unwrap();
    let host = "é😀".repeat(1200);
    sqlx::query("UPDATE comment SET extra_json=? WHERE id='root'").bind(json!({"authorPrincipalId":"","authorIdentity":{"provider":"github","host":host,"externalUserId":""}}).to_string()).execute(store.write_pool()).await.unwrap();
    let (scope, source) = annotation_scope(&store, &ws, &note).await;
    let request: AnnotationPageRequest =
        serde_json::from_value(json!({"kind":"replies","maxWireBytes":4096})).unwrap();
    let reply = store
        .read_note_annotation_page(
            "alice",
            &scope,
            &source,
            None,
            Some("thread"),
            &request,
            &json!(1),
        )
        .await
        .unwrap();
    let item = &reply["items"][0];
    assert!(item.get("authorPrincipalId").is_none());
    assert!(item.get("authorIdentity").is_none());
    let epoch = reply["commentRevision"].as_str().unwrap();
    let mut detail = AnnotationContextRequest {
        kind: "context".into(),
        context_ref: item["authorPrincipalIdRef"].as_str().unwrap().into(),
        cursor: None,
        max_items: Some(1),
        max_wire_bytes: Some(4096),
    };
    let principal = store
        .read_note_annotation_context("alice", &scope, &source, epoch, &detail, &json!(1))
        .await
        .unwrap();
    assert_eq!(principal["items"][0]["text"], "");
    assert!(principal["items"][0].get("isNull").is_none());
    detail.context_ref = item["authorIdentityRef"].as_str().unwrap().into();
    let mut fields = Vec::new();
    let mut host_ref = None;
    loop {
        let page = store
            .read_note_annotation_context("alice", &scope, &source, epoch, &detail, &json!(1))
            .await
            .unwrap();
        let field = &page["items"][0];
        fields.push(field["field"].as_str().unwrap().to_owned());
        if field["field"] == "host" {
            host_ref = Some((
                field["text"].as_str().unwrap().to_owned(),
                field["nextRef"].as_str().unwrap().to_owned(),
            ));
        }
        let Some(cursor) = page["nextCursor"].as_str() else {
            break;
        };
        detail.cursor = Some(cursor.into());
    }
    assert_eq!(fields, ["provider", "host", "externalUserId"]);
    let (mut recovered, next) = host_ref.unwrap();
    detail.context_ref = next;
    detail.cursor = None;
    loop {
        let page = store
            .read_note_annotation_context("alice", &scope, &source, epoch, &detail, &json!(1))
            .await
            .unwrap();
        recovered.push_str(page["items"][0]["text"].as_str().unwrap());
        let Some(next) = page["nextCursor"].as_str() else {
            break;
        };
        detail.cursor = Some(next.into());
    }
    assert_eq!(recovered, host);
}

#[tokio::test]
async fn annotation_anchor_context_pages_canonical_occurrences_and_explicit_orphans() {
    use serde_json::json;
    let (_db, store, ws, note) = fixture().await;
    for id in ["root", "orphan"] {
        store
            .insert_comment(&ws, &sample_comment(&note, id, id))
            .await
            .unwrap();
    }
    let epochs = store.note_annotation_epochs(&ws, &note).await.unwrap();
    store
        .publish_comment_anchors(
            &ws,
            &note,
            &epochs,
            &[AnchorOccurrence {
                comment_id: "root".into(),
                occurrence_id: "derived".into(),
                source_range: SourceRange { start: 1, end: 3 },
            }],
        )
        .await
        .unwrap();
    let (scope, source) = annotation_scope(&store, &ws, &note).await;
    let request: AnnotationPageRequest = serde_json::from_value(
        json!({"kind":"comments","ranges":[],"anchorState":"all","maxWireBytes":4096}),
    )
    .unwrap();
    let page = store
        .read_note_annotation_page("alice", &scope, &source, None, None, &request, &json!(1))
        .await
        .unwrap();
    assert_eq!(page["totalThreads"], 2);
    let epoch = page["commentRevision"].as_str().unwrap();
    for item in page["items"].as_array().unwrap() {
        let mut detail = AnnotationContextRequest {
            kind: "context".into(),
            context_ref: item["anchorRef"].as_str().unwrap().into(),
            cursor: None,
            max_items: Some(1),
            max_wire_bytes: Some(4096),
        };
        let mut anchor_json = String::new();
        let mut occurrences = Vec::new();
        let mut orphan = false;
        loop {
            let context = store
                .read_note_annotation_context("alice", &scope, &source, epoch, &detail, &json!(1))
                .await
                .unwrap();
            for fragment in context["items"].as_array().unwrap() {
                if fragment["field"] == "anchor" {
                    anchor_json.push_str(fragment["text"].as_str().unwrap());
                }
                if fragment["kind"] == "span" {
                    occurrences.push(fragment.clone());
                }
            }
            orphan |= context["orphaned"] == true;
            let Some(cursor) = context["nextCursor"].as_str() else {
                break;
            };
            detail.cursor = Some(cursor.into());
        }
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&anchor_json).unwrap()["startId"],
            "a1"
        );
        if item["threadId"] == "root" {
            assert_eq!(occurrences.len(), 1);
            assert_eq!(occurrences[0]["canonicalId"], "a1");
            assert_ne!(occurrences[0]["occurrenceId"], "a1");
            assert!(!orphan);
        } else {
            assert!(occurrences.is_empty());
            assert!(orphan);
        }
    }
}

#[tokio::test]
async fn annotation_root_identity_survives_root_deletion_and_exhausted_reply_pages() {
    let (_db, store, ws, note) = fixture().await;
    let root = sample_comment(&note, "thread-not-root-id", "canonical-root");
    store.insert_comment(&ws, &root).await.unwrap();
    let mut reply = sample_comment(&note, "thread-not-root-id", "survivor");
    reply.parent_id = Some(root.id.clone());
    store.insert_comment(&ws, &reply).await.unwrap();
    sqlx::query("DELETE FROM comment WHERE id='canonical-root'")
        .execute(store.write_pool())
        .await
        .unwrap();
    let epochs = store.note_annotation_epochs(&ws, &note).await.unwrap();
    for after in [None, Some(("zzzz", "zzzz"))] {
        let result = store
            .read_comment_rows(&ws, &note, &epochs, "thread-not-root-id", after, 1)
            .await
            .unwrap();
        assert_eq!(result.root_comment_id.as_deref(), Some("canonical-root"));
        assert!(!result.root_present);
        assert_eq!(result.total_comments, 1);
        assert_eq!(result.page.items.len(), usize::from(after.is_none()));
    }
    sqlx::query("DELETE FROM comment WHERE id='survivor'")
        .execute(store.write_pool())
        .await
        .unwrap();
    let epochs = store.note_annotation_epochs(&ws, &note).await.unwrap();
    assert!(store
        .read_comment_rows(&ws, &note, &epochs, "thread-not-root-id", None, 1)
        .await
        .is_err());
}

#[tokio::test]
async fn annotation_adopt_stray_spec_retires_old_scope_atomically_and_rolls_back_at_exhaustion() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.unwrap();
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .unwrap();
    let note = stray_note(&ws, "old-id", "Spec");
    store.insert_note(&note).await.unwrap();
    let before = store
        .read_note_page_state(&ws, &note.id, None)
        .await
        .unwrap();
    let instance = before["scope"]["noteInstanceId"].as_str().unwrap();
    sqlx::query("UPDATE note_annotation_state SET state_generation='18446744073709551615' WHERE note_id='old-id'").execute(store.write_pool()).await.unwrap();
    assert!(store.adopt_stray_spec_note(&ws).await.is_err());
    let preserved = store
        .read_note_page_state(&ws, &note.id, Some(instance))
        .await
        .unwrap();
    assert_eq!(preserved["deleted"], false);
    sqlx::query("UPDATE note_annotation_state SET state_generation='10' WHERE note_id='old-id'")
        .execute(store.write_pool())
        .await
        .unwrap();
    assert!(store.adopt_stray_spec_note(&ws).await.unwrap().is_some());
    let retired = store
        .read_note_page_state(&ws, &note.id, Some(instance))
        .await
        .unwrap();
    assert_eq!(retired["deleted"], true);
    assert_eq!(retired["stateGeneration"], "11");
    for field in ["sourceRevision", "attributionGeneration", "commentRevision"] {
        assert_eq!(retired[field], before[field]);
    }
    let current = store
        .read_note_page_state(&ws, &NoteId::from("spec"), None)
        .await
        .unwrap();
    assert_eq!(current["deleted"], false);
    assert_eq!(current["scope"]["noteId"], "spec");
}

#[tokio::test]
async fn annotation_maintained_all_orphan_counts_follow_reply_updates_anchor_rebuilds_and_deletion()
{
    let (_db, store, ws, note) = fixture().await;
    for id in ["root", "orphan"] {
        store
            .insert_comment(&ws, &sample_comment(&note, id, id))
            .await
            .unwrap();
    }
    let mut reply = sample_comment(&note, "root", "reply");
    reply.parent_id = Some("root".into());
    store.insert_comment(&ws, &reply).await.unwrap();
    for round in 0..2 {
        let epochs = store.note_annotation_epochs(&ws, &note).await.unwrap();
        store
            .publish_comment_anchors(
                &ws,
                &note,
                &epochs,
                &[
                    AnchorOccurrence {
                        comment_id: "root".into(),
                        occurrence_id: "first".into(),
                        source_range: SourceRange { start: 1, end: 3 },
                    },
                    AnchorOccurrence {
                        comment_id: "root".into(),
                        occurrence_id: "second".into(),
                        source_range: SourceRange { start: 3, end: 4 },
                    },
                ],
            )
            .await
            .unwrap();
        let epoch = store.note_annotation_epochs(&ws, &note).await.unwrap();
        let all = store
            .read_comment_threads(&ws, &note, &epoch, &[], CommentFilter::All, None, 1)
            .await
            .unwrap();
        assert_eq!((all.total_threads, all.total_comments), (2, 3));
        let orphan = store
            .read_comment_threads(&ws, &note, &epoch, &[], CommentFilter::Orphaned, None, 1)
            .await
            .unwrap();
        assert_eq!((orphan.total_threads, orphan.total_comments), (1, 1));
        assert_eq!(orphan.page.items[0].thread_id, "orphan");
        if round == 0 {
            sqlx::query("UPDATE comment SET status='resolved' WHERE id='root'")
                .execute(store.write_pool())
                .await
                .unwrap();
        }
    }
    sqlx::query("DELETE FROM comment WHERE id='root'")
        .execute(store.write_pool())
        .await
        .unwrap();
    let epoch = store.note_annotation_epochs(&ws, &note).await.unwrap();
    store
        .publish_comment_anchors(&ws, &note, &epoch, &[])
        .await
        .unwrap();
    let epoch = store.note_annotation_epochs(&ws, &note).await.unwrap();
    let orphan = store
        .read_comment_threads(&ws, &note, &epoch, &[], CommentFilter::Orphaned, None, 10)
        .await
        .unwrap();
    assert_eq!((orphan.total_threads, orphan.total_comments), (2, 2));
    let root = orphan
        .page
        .items
        .iter()
        .find(|r| r.thread_id == "root")
        .unwrap();
    assert_eq!(root.root_comment_id.as_deref(), Some("root"));
    assert!(!root.root_present);
}

#[tokio::test]
async fn annotation_dense_range_preparation_is_once_per_lease_bounded_and_invalidated() {
    use serde_json::json;
    use sqlx::Row;
    let _serial = crate::note_annotation_repo::ANNOTATION_PREPARATION_TEST
        .lock()
        .await;
    let (_db, store, ws, note) = fixture().await;
    let mut tx = store.write_pool().begin().await.unwrap();
    sqlx::query("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<1000) INSERT INTO comment(id,workspace_id,note_id,thread_id,kind,content,author,author_type,status,anchor_json,created_at,updated_at) SELECT printf('dense%04d',x),?,?,printf('dense%04d',x),'comment','body','author','user','open','null','date','date' FROM n")
        .bind(ws.as_str()).bind(note.as_str()).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    let epoch = store.note_annotation_epochs(&ws, &note).await.unwrap();
    let occurrences = (1..=1000)
        .map(|x| AnchorOccurrence {
            comment_id: format!("dense{x:04}"),
            occurrence_id: "occurrence".into(),
            source_range: SourceRange { start: 0, end: 10 },
        })
        .collect::<Vec<_>>();
    store
        .publish_comment_anchors(&ws, &note, &epoch, &occurrences)
        .await
        .unwrap();
    let (scope, source) = annotation_scope(&store, &ws, &note).await;
    let mut request: AnnotationPageRequest = serde_json::from_value(
        json!({"kind":"comments","ranges":[{"start":1,"end":2}],"maxItems":2,"maxWireBytes":4096}),
    )
    .unwrap();
    let first = store
        .read_note_annotation_page("alice", &scope, &source, None, None, &request, &json!(1))
        .await
        .unwrap();
    assert_eq!(first["totalThreads"], 1000);
    assert_eq!(first["items"].as_array().unwrap().len(), 2);
    let sid = first["snapshotId"].as_str().unwrap();
    let before = sqlx::query(
        "SELECT total_threads,prepare_steps FROM note_annotation_match_head WHERE snapshot_id=?",
    )
    .bind(sid)
    .fetch_one(store.read_pool())
    .await
    .unwrap();
    assert!(before.get::<i64, _>("prepare_steps") > 1000);
    request.cursor = Some(first["nextCursor"].as_str().unwrap().into());
    let epoch = first["commentRevision"].as_str().unwrap();
    let next = store
        .read_note_annotation_page(
            "alice",
            &scope,
            &source,
            Some(epoch),
            None,
            &request,
            &json!(1),
        )
        .await
        .unwrap();
    assert_eq!(next["items"][0]["threadId"], "dense0003");
    let after: i64 = sqlx::query_scalar(
        "SELECT prepare_steps FROM note_annotation_match_head WHERE snapshot_id=?",
    )
    .bind(sid)
    .fetch_one(store.read_pool())
    .await
    .unwrap();
    assert_eq!(before.get::<i64, _>("prepare_steps"), after);
    request.cursor = None;
    for _ in 0..4 {
        store
            .read_note_annotation_page("alice", &scope, &source, None, None, &request, &json!(1))
            .await
            .unwrap();
    }
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM note_annotation_match_head")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(count, 5);
    sqlx::query("DELETE FROM note_annotation_snapshot WHERE id=?")
        .bind(sid)
        .execute(store.write_pool())
        .await
        .unwrap();
    request.cursor = Some(first["nextCursor"].as_str().unwrap().into());
    assert!(store
        .read_note_annotation_page(
            "alice",
            &scope,
            &source,
            Some(epoch),
            None,
            &request,
            &json!(1)
        )
        .await
        .is_err());
    sqlx::query("UPDATE comment SET status='resolved' WHERE id='dense0001'")
        .execute(store.write_pool())
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM note_annotation_match_head")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
}
