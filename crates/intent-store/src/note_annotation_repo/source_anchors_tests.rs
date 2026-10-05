use super::*;
use crate::Store;

fn tuples(items: Vec<AnchorOccurrence>) -> Vec<(String, String, i64, i64)> {
    items
        .into_iter()
        .map(|v| {
            (
                v.comment_id,
                v.occurrence_id,
                v.source_range.start,
                v.source_range.end,
            )
        })
        .collect()
}

// Independent legacy literal oracle: per-root nonoverlapping str matches and
// first suffix find, with offsets computed from independently sliced prefixes.
fn legacy(source: &str, roots: &[&str]) -> Vec<(String, String, i64, i64)> {
    let mut out = Vec::new();
    for id in roots {
        let start = format!("<!--anchor:{id}:start-->");
        let end = format!("<!--anchor:{id}:end-->");
        for (at, _) in source.match_indices(&start) {
            let from = at + start.len();
            if let Some(relative) = source[from..].find(&end) {
                out.push((
                    (*id).into(),
                    format!("range:{at}"),
                    i64::try_from(source[..from].encode_utf16().count()).unwrap(),
                    i64::try_from(source[..from + relative].encode_utf16().count()).unwrap(),
                ));
            }
        }
        for (at, _) in source.match_indices(&format!("<!--anchor:{id}:point-->")) {
            let position = i64::try_from(source[..at].encode_utf16().count()).unwrap();
            out.push(((*id).into(), format!("point:{at}"), position, position));
        }
    }
    out
}

#[test]
fn unicode_coordinates_and_byte_identity_are_distinct() {
    let source = "😀<!--anchor:x:start-->ab<!--anchor:x:end--><!--anchor:x:point-->";
    assert_eq!(
        tuples(anchor_occurrences(source, &["x"]).unwrap()),
        vec![
            ("x".into(), "range:4".into(), 23, 25),
            ("x".into(), "point:46".into(), 44, 44),
        ]
    );
    assert!(anchor_occurrences(source, &[]).unwrap().is_empty());
}

#[test]
fn nested_starts_share_first_subsequent_end_without_stack_pairing() {
    let source =
        "<!--anchor:x:start--><!--anchor:x:start-->Q<!--anchor:x:end--><!--anchor:x:end-->";
    let rows = tuples(anchor_occurrences(source, &["x"]).unwrap());
    assert_eq!(
        rows,
        vec![
            ("x".into(), "range:0".into(), 21, 43),
            ("x".into(), "range:21".into(), 42, 43),
        ]
    );
    assert!(
        anchor_occurrences("<!--anchor:x:start--><!--anchor:y:end-->", &["x"])
            .unwrap()
            .is_empty()
    );
}

#[test]
fn literal_parity_includes_opaque_ids_repetition_partial_and_adjacent_markers() {
    let roots = ["x", "x:y", "", "😀", "x:start--><!--anchor:y", "a-->"];
    for id in roots {
        let fragments = [
            "😀e\u{301}\r\n".into(),
            format!("<!--anchor:{id}:start-->"),
            format!("<!--anchor:{id}:end-->"),
            format!("<!--anchor:{id}:point-->"),
            "<!--anchor:foreign:end-->".into(),
        ];
        for a in &fragments {
            for b in &fragments {
                for c in &fragments {
                    let source = format!("{a}{b}{c}{a}");
                    assert_eq!(
                        tuples(anchor_occurrences(&source, &roots).unwrap()),
                        legacy(&source, &roots),
                        "{source:?}"
                    );
                }
            }
        }
    }
}

async fn fixture(source: &str) -> (tempfile::TempDir, Store, WorkspaceId, NoteId) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    for workspace in ["owner", "keeper"] {
        sqlx::query("INSERT INTO workspace(id,title,branch,created_at,updated_at) VALUES(?,?,'main','t0','t0')")
            .bind(workspace).bind(workspace).execute(store.write_pool()).await.unwrap();
        sqlx::query("INSERT INTO note(id,workspace_id,title,content,created_at,updated_at) VALUES('n',?,'Note',?,'t0','t0')")
            .bind(workspace).bind(source).execute(store.write_pool()).await.unwrap();
    }
    let mut tx = store
        .write_pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap();
    crate::note_page_index::rebuild_pending(&mut tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    (dir, store, WorkspaceId("owner".into()), NoteId("n".into()))
}

async fn root(
    conn: &mut SqliteConnection,
    workspace: &str,
    id: &str,
    parent: Option<&str>,
    extra: Option<&str>,
) {
    sqlx::query("INSERT INTO comment(id,thread_id,note_id,workspace_id,kind,content,author,author_type,parent_id,anchor_json,extra_json,created_at,updated_at) VALUES(?,?,'n',?,'comment','body','author','user',?,'{}',?,'t0','t0')")
        .bind(id).bind(id).bind(workspace).bind(parent).bind(extra).execute(conn).await.unwrap();
}

async fn rows(conn: &mut SqliteConnection) -> Vec<(String, String, i64, i64)> {
    sqlx::query_as("SELECT comment_id,occurrence_id,start,end FROM note_comment_anchor ORDER BY comment_id,occurrence_id")
        .fetch_all(conn).await.unwrap()
}

#[tokio::test]
async fn current_writer_selects_only_scoped_non_orphan_roots_and_preserves_ready_index() {
    let source = ["x", "orphan", "legacy", "reply", "foreign"]
        .map(|id| format!("<!--anchor:{id}:start-->😀<!--anchor:{id}:end-->"))
        .join("");
    let (_dir, store, ws, note) = fixture(&source).await;
    let mut tx = store
        .write_pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap();
    root(&mut tx, "owner", "x", None, None).await;
    root(
        &mut tx,
        "owner",
        "orphan",
        None,
        Some(r#"{"isOrphaned":true}"#),
    )
    .await;
    root(
        &mut tx,
        "owner",
        "legacy",
        None,
        Some(r#"{"isOrphaned":"true"}"#),
    )
    .await;
    root(&mut tx, "owner", "reply", Some("x"), None).await;
    root(&mut tx, "keeper", "foreign", None, None).await;
    rebuild_note_anchors(&mut tx, &ws, &note, None)
        .await
        .unwrap();
    let expected = rows(&mut tx).await;
    assert_eq!(expected, legacy(&source, &["legacy", "x"]));
    let covers: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM note_comment_anchor_cover")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert!(covers > 0);
    let before = head(&mut tx, &ws, &note).await.unwrap().1;
    assert!(before.anchors_ready);
    // Ready no-op must not delete/reinsert existing anchor rows.
    sqlx::query("CREATE TEMP TRIGGER reject_anchor_delete BEFORE DELETE ON note_comment_anchor BEGIN SELECT RAISE(ABORT,'unexpected rebuild'); END")
        .execute(&mut *tx).await.unwrap();
    rebuild_note_anchors(&mut tx, &ws, &note, None)
        .await
        .unwrap();
    assert_eq!(head(&mut tx, &ws, &note).await.unwrap().1, before);
    assert_eq!(rows(&mut tx).await, expected);
    assert!(
        !head(&mut tx, &WorkspaceId("keeper".into()), &note)
            .await
            .unwrap()
            .1
            .anchors_ready
    );
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn empty_roots_require_current_source_index_and_retirement_blocks_ready_noop() {
    let (_dir, store, ws, note) = fixture("large source with no roots").await;
    let mut tx = store
        .write_pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap();
    // No roots: supplied content need not be visited at all.
    rebuild_note_anchors(&mut tx, &ws, &note, Some(""))
        .await
        .unwrap();
    assert!(head(&mut tx, &ws, &note).await.unwrap().1.anchors_ready);
    sqlx::query("UPDATE note_page_head SET indexed_rev=-1 WHERE workspace_id='owner'")
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(matches!(
        rebuild_note_anchors(&mut tx, &ws, &note, None).await,
        Err(Error::NotePage(_))
    ));
    tx.rollback().await.unwrap();
    let mut tx = store
        .write_pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap();
    rebuild_note_anchors(&mut tx, &ws, &note, None)
        .await
        .unwrap();
    sqlx::query("INSERT INTO note_annotation_workspace_retirement VALUES('owner')")
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(matches!(
        rebuild_note_anchors(&mut tx, &ws, &note, None).await,
        Err(Error::NotFound(_))
    ));
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn source_and_index_publication_roll_back_together_then_retry() {
    let source = "<!--anchor:x:start-->old<!--anchor:x:end-->";
    let (_dir, store, ws, note) = fixture(source).await;
    let mut tx = store
        .write_pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap();
    root(&mut tx, "owner", "x", None, None).await;
    rebuild_note_anchors(&mut tx, &ws, &note, Some(source))
        .await
        .unwrap();
    let before = head(&mut tx, &ws, &note).await.unwrap().1;
    let old_rows = rows(&mut tx).await;
    tx.commit().await.unwrap();
    let changed = "😀<!--anchor:x:start-->new text<!--anchor:x:end-->";
    let mut invalid_tx = store
        .write_pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap();
    sqlx::query("UPDATE note SET content=?,rev=rev+1 WHERE workspace_id='owner' AND id='n'")
        .bind(changed)
        .execute(&mut *invalid_tx)
        .await
        .unwrap();
    crate::note_page_index::rebuild_pending(&mut invalid_tx)
        .await
        .unwrap();
    assert!(
        rebuild_note_anchors(&mut invalid_tx, &ws, &note, Some("metadata placeholder"))
            .await
            .is_err()
    );
    invalid_tx.rollback().await.unwrap();
    for commit in [false, true] {
        let mut tx = store
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .unwrap();
        sqlx::query("UPDATE note SET content=?,rev=rev+1 WHERE workspace_id='owner' AND id='n'")
            .bind(changed)
            .execute(&mut *tx)
            .await
            .unwrap();
        crate::note_page_index::rebuild_pending(&mut tx)
            .await
            .unwrap();
        rebuild_note_anchors(&mut tx, &ws, &note, Some(changed))
            .await
            .unwrap();
        assert_eq!(rows(&mut tx).await, legacy(changed, &["x"]));
        if commit {
            tx.commit().await.unwrap();
        } else {
            tx.rollback().await.unwrap();
            let mut conn = store.read_pool().acquire().await.unwrap();
            assert_eq!(head(&mut conn, &ws, &note).await.unwrap().1, before);
            assert_eq!(rows(&mut conn).await, old_rows);
            let actual: String = sqlx::query_scalar(
                "SELECT content FROM note WHERE workspace_id='owner' AND id='n'",
            )
            .fetch_one(&mut *conn)
            .await
            .unwrap();
            assert_eq!(actual, source);
        }
    }
}

#[tokio::test]
async fn late_publication_error_requires_outer_rollback_and_keeps_prior_index() {
    let source = "<!--anchor:x:point--><!--anchor:y:point-->";
    let (_dir, store, ws, note) = fixture(source).await;
    let mut tx = store
        .write_pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap();
    root(&mut tx, "owner", "x", None, None).await;
    rebuild_note_anchors(&mut tx, &ws, &note, None)
        .await
        .unwrap();
    let before = head(&mut tx, &ws, &note).await.unwrap().1;
    let previous = rows(&mut tx).await;
    tx.commit().await.unwrap();
    let mut tx = store
        .write_pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap();
    root(&mut tx, "owner", "y", None, None).await;
    sqlx::query("CREATE TEMP TRIGGER fail_second_anchor BEFORE INSERT ON note_comment_anchor WHEN new.comment_id='y' BEGIN SELECT RAISE(ABORT,'late anchor failure'); END")
        .execute(&mut *tx).await.unwrap();
    assert!(rebuild_note_anchors(&mut tx, &ws, &note, None)
        .await
        .is_err());
    tx.rollback().await.unwrap();
    let mut conn = store.read_pool().acquire().await.unwrap();
    assert_eq!(head(&mut conn, &ws, &note).await.unwrap().1, before);
    assert_eq!(rows(&mut conn).await, previous);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM comment WHERE id='y'")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(count, 0);
}
