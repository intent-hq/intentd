//! Lifecycle publication through public Store entry points, with legacy raw
//! comment rows as input. No private anchor publisher is used by these tests.
use super::{request, sample_workspace, stray_note, TempDb};
use crate::{
    note_annotation_repo::{CommentFilter, SourceRange},
    Store,
};
use intent_core::{Error, Note, NoteId, WorkspaceId};
use serde_json::{json, Value};

const SOURCE: &str = "😀pre<!--anchor:healthy:start-->中e\u{301}<!--anchor:healthy:end--><!--anchor:orphan:point--><!--anchor:gone:point-->";

async fn raw_comment(
    store: &Store,
    ws: &WorkspaceId,
    note: &NoteId,
    id: &str,
    thread: &str,
    parent: Option<&str>,
    orphan: bool,
) {
    sqlx::query("INSERT INTO comment(id,thread_id,note_id,workspace_id,kind,content,author,author_type,parent_id,anchor_json,extra_json,created_at,updated_at) VALUES(?,?,?,?,'comment','body','Author','user',?,'{}',?,'t0','t0')")
        .bind(id).bind(thread).bind(note.as_str()).bind(ws.as_str()).bind(parent)
        .bind(json!({"isOrphaned":orphan}).to_string()).execute(store.write_pool()).await.unwrap();
}

async fn legacy_source() -> (TempDb, Store, Note) {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.unwrap();
    let ws = WorkspaceId("lifecycle-import".into());
    store
        .insert_workspace(&sample_workspace(&ws, "Source", false))
        .await
        .unwrap();
    let mut note = stray_note(&ws, "n", "Note");
    note.content = SOURCE.into();
    store.insert_note(&note).await.unwrap();
    raw_comment(
        &store,
        &ws,
        &note.id,
        "healthy",
        "healthy-thread",
        None,
        false,
    )
    .await;
    raw_comment(&store, &ws, &note.id, "orphan", "orphan-thread", None, true).await;
    // Canonical retained reply names a deleted root; no root row exists to
    // authorize the lookalike marker still present in the source.
    raw_comment(
        &store,
        &ws,
        &note.id,
        "reply",
        "gone-thread",
        Some("gone"),
        false,
    )
    .await;
    (db, store, note)
}

async fn geometry(store: &Store, ws: &WorkspaceId, note: &NoteId) -> Vec<(String, i64, i64)> {
    sqlx::query_as("SELECT a.comment_id,a.start,a.end FROM note_comment_anchor a JOIN note_annotation_head h ON h.id=a.head_id WHERE h.workspace_id=? AND h.note_id=? ORDER BY a.comment_id,a.occurrence_id")
        .bind(ws.as_str()).bind(note.as_str()).fetch_all(store.read_pool()).await.unwrap()
}

async fn source_page(store: &Store, note: &Note) -> Value {
    store
        .read_note_page(
            note.workspace_id.as_str(),
            note.id.as_str(),
            "lifecycle-reader",
            request(json!({"kind":"source","maxWireBytes":4096})),
            &json!(1),
        )
        .await
        .unwrap()
}

async fn imported_ready(store: &Store, note: &Note) {
    let epochs = store
        .note_annotation_epochs(&note.workspace_id, &note.id)
        .await
        .unwrap();
    assert!(epochs.anchors_ready);
    assert_eq!(epochs.source_revision, note.rev);
    assert_eq!(
        geometry(store, &note.workspace_id, &note.id).await,
        vec![("healthy".into(), 32, 35)]
    );
    assert_eq!(
        store
            .get_note(&note.workspace_id, &note.id)
            .await
            .unwrap()
            .content,
        SOURCE
    );
    let anchored = store
        .read_comment_threads(
            &note.workspace_id,
            &note.id,
            &epochs,
            &[SourceRange {
                start: 0,
                end: i64::try_from(SOURCE.encode_utf16().count()).unwrap(),
            }],
            CommentFilter::Anchored,
            None,
            8,
        )
        .await
        .unwrap();
    assert_eq!(anchored.total_threads, 1);
    assert_eq!(anchored.total_comments, 1);
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
    assert_eq!(orphaned.total_threads, 2);
    assert_eq!(orphaned.total_comments, 2);
    let gone = orphaned
        .page
        .items
        .iter()
        .find(|row| row.thread_id == "gone-thread")
        .unwrap();
    assert_eq!(gone.root_comment_id.as_deref(), Some("gone"));
    assert!(!gone.root_present);
    assert_eq!(gone.latest_comment_id, "reply");
}

#[tokio::test]
async fn transfer_finalizes_after_comments_and_assigns_fresh_incarnation() {
    let (_source_db, source, note) = legacy_source().await;
    let original = source_page(&source, &note).await;
    let rows = source
        .transfer_export_rows(&note.workspace_id)
        .await
        .unwrap();
    assert!(rows
        .iter()
        .all(|(table, _)| !table.starts_with("note_annotation_")
            && !table.starts_with("note_comment_")));
    let target_db = TempDb::new();
    let target = Store::open(&target_db.path).await.unwrap();
    target.transfer_import_rows(&rows).await.unwrap();
    imported_ready(&target, &note).await;
    let imported = source_page(&target, &note).await;
    assert_eq!(imported["text"], original["text"]);
    assert_ne!(
        imported["scope"]["noteInstanceId"],
        original["scope"]["noteInstanceId"]
    );
    let snapshots: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM note_annotation_snapshot")
        .fetch_one(target.read_pool())
        .await
        .unwrap();
    assert_eq!(snapshots, 0);
}

#[tokio::test]
async fn final_anchor_failure_rolls_back_import_then_same_rows_retry() {
    let (_source_db, source, note) = legacy_source().await;
    let rows = source
        .transfer_export_rows(&note.workspace_id)
        .await
        .unwrap();
    let target_db = TempDb::new();
    let target = Store::open(&target_db.path).await.unwrap();
    sqlx::query("CREATE TRIGGER reject_import_anchor BEFORE INSERT ON note_comment_anchor WHEN new.comment_id='healthy' BEGIN SELECT RAISE(ABORT,'import final anchor rejected'); END")
        .execute(target.write_pool()).await.unwrap();
    let result = target.transfer_import_rows(&rows).await;
    assert!(
        matches!(result, Err(Error::Internal(message)) if message.contains("import final anchor rejected"))
    );
    assert!(target
        .transfer_table_stats(&note.workspace_id)
        .await
        .unwrap()
        .iter()
        .all(|table| table.row_count == 0));
    for table in [
        "note_annotation_head",
        "note_comment_projection",
        "note_comment_anchor",
        "note_comment_anchor_cover",
    ] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(target.read_pool())
            .await
            .unwrap();
        assert_eq!(count, 0, "{table}");
    }
    sqlx::query("DROP TRIGGER reject_import_anchor")
        .execute(target.write_pool())
        .await
        .unwrap();
    target.transfer_import_rows(&rows).await.unwrap();
    imported_ready(&target, &note).await;
}

#[tokio::test]
async fn reopening_repairs_legacy_pending_anchors_without_reviving_retirement() {
    let (db, store, note) = legacy_source().await;
    let retired_ws = WorkspaceId("retired".into());
    store
        .insert_workspace(&sample_workspace(&retired_ws, "Retired", false))
        .await
        .unwrap();
    let mut retired = stray_note(&retired_ws, "n", "Retired note");
    retired.content = "<!--anchor:retired-root:point-->".into();
    store.insert_note(&retired).await.unwrap();
    raw_comment(
        &store,
        &retired_ws,
        &retired.id,
        "retired-root",
        "retired-thread",
        None,
        false,
    )
    .await;
    sqlx::query("INSERT INTO note_annotation_workspace_retirement VALUES('retired')")
        .execute(store.write_pool())
        .await
        .unwrap();
    assert!(
        !store
            .note_annotation_epochs(&note.workspace_id, &note.id)
            .await
            .unwrap()
            .anchors_ready
    );
    let original = source_page(&store, &note).await;
    store.close().await;
    drop(store);
    let reopened = Store::open(&db.path).await.unwrap();
    imported_ready(&reopened, &note).await;
    assert_eq!(
        source_page(&reopened, &note).await["scope"],
        original["scope"]
    );
    assert!(matches!(
        reopened
            .note_annotation_epochs(&retired_ws, &retired.id)
            .await,
        Err(Error::NotFound(_))
    ));
    let state: (i64, i64) = sqlx::query_as("SELECT h.anchors_rev,(SELECT COUNT(*) FROM note_comment_anchor a WHERE a.head_id=h.id) FROM note_annotation_head h WHERE h.workspace_id='retired'")
        .fetch_one(reopened.read_pool()).await.unwrap();
    assert_eq!(state, (-1, 0));
    assert_eq!(
        reopened
            .get_note(&retired_ws, &retired.id)
            .await
            .unwrap()
            .content,
        retired.content
    );
    reopened.close().await;
    drop(reopened);
    let again = Store::open(&db.path).await.unwrap();
    imported_ready(&again, &note).await;
}

#[tokio::test]
async fn adopting_spec_finalizes_new_scope_and_reparented_child() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.unwrap();
    let ws = WorkspaceId("adoption".into());
    store
        .insert_workspace(&sample_workspace(&ws, "Adopt", false))
        .await
        .unwrap();
    let mut note = stray_note(&ws, "old-spec", " Spec ");
    note.content = "😀<!--anchor:healthy:start-->abc<!--anchor:healthy:end-->".into();
    store.insert_note(&note).await.unwrap();
    raw_comment(
        &store,
        &ws,
        &note.id,
        "healthy",
        "healthy-thread",
        None,
        false,
    )
    .await;
    let mut child = stray_note(&ws, "child", "Child");
    child.parent_id = Some(note.id.clone());
    child.content = "😀<!--anchor:child-root:point-->".into();
    store.insert_note(&child).await.unwrap();
    raw_comment(
        &store,
        &ws,
        &child.id,
        "child-root",
        "child-thread",
        None,
        false,
    )
    .await;
    let adopted = store
        .adopt_stray_spec_note(&ws)
        .await
        .unwrap()
        .expect("one candidate");
    assert_eq!(adopted.0, note.id);
    let spec = NoteId("spec".into());
    assert!(matches!(
        store.get_note(&ws, &note.id).await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        store.note_annotation_epochs(&ws, &note.id).await,
        Err(Error::NotFound(_))
    ));
    assert_eq!(
        store.get_note(&ws, &spec).await.unwrap().content,
        note.content
    );
    assert!(
        store
            .note_annotation_epochs(&ws, &spec)
            .await
            .unwrap()
            .anchors_ready
    );
    assert_eq!(
        geometry(&store, &ws, &spec).await,
        vec![("healthy".into(), 29, 32)]
    );
    assert!(
        store
            .note_annotation_epochs(&ws, &child.id)
            .await
            .unwrap()
            .anchors_ready
    );
    assert_eq!(
        geometry(&store, &ws, &child.id).await,
        vec![("child-root".into(), 2, 2)]
    );
    assert_eq!(
        store.get_note(&ws, &child.id).await.unwrap().parent_id,
        Some(spec)
    );
    assert!(store.adopt_stray_spec_note(&ws).await.unwrap().is_none());
    let violations = sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(store.read_pool())
        .await
        .unwrap();
    assert!(violations.is_empty());
}
