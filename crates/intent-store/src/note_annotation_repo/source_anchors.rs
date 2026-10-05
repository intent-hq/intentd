//! Write-time projection of existing literal comment markers. This module does
//! not repair source, recover orphaned comments, or establish marker authority.
use super::{
    db_error, head, invalid, publish_anchors_in_transaction, stale, AnchorOccurrence, SourceRange,
};
use intent_core::{Error, NoteId, Result, WorkspaceId};
use sqlx::SqliteConnection;

#[cfg(test)]
#[derive(Default)]
pub(crate) struct FinalizerPause {
    pub entered: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
    pub observed: std::sync::Mutex<Option<super::AnnotationEpochs>>,
}

#[cfg(test)]
tokio::task_local! {
    pub(crate) static FINALIZER_PAUSE: std::sync::Arc<FinalizerPause>;
}

/// Project the legacy selection geometry for unique, scoped, non-orphan roots.
/// Each literal start independently uses its first subsequent same-ID end;
/// nested starts may share an end. IDs are opaque literal strings, not parsed
/// or normalized. Occurrence identity uses byte offsets, coordinates UTF16.
///
/// This preserves the legacy per-root scans and occurrence-sized output. It is
/// write-sized work, not a bounded paged read or a linear-time guarantee.
///
/// # Errors
/// Returns an error if a source offset cannot be represented.
pub fn anchor_occurrences(
    source: &str,
    eligible_root_ids: &[&str],
) -> Result<Vec<AnchorOccurrence>> {
    let mut occurrences = Vec::new();
    for &id in eligible_root_ids {
        let open = format!("<!--anchor:{id}:start-->");
        let close = format!("<!--anchor:{id}:end-->");
        for (at, _) in source.match_indices(&open) {
            let start = at + open.len();
            if let Some(relative) = source[start..].find(&close) {
                occurrences.push(AnchorOccurrence {
                    comment_id: id.into(),
                    occurrence_id: format!("range:{at}"),
                    source_range: SourceRange {
                        start: utf16_offset(source, start)?,
                        end: utf16_offset(source, start + relative)?,
                    },
                });
            }
        }
        let point = format!("<!--anchor:{id}:point-->");
        for (at, _) in source.match_indices(&point) {
            let position = utf16_offset(source, at)?;
            occurrences.push(AnchorOccurrence {
                comment_id: id.into(),
                occurrence_id: format!("point:{at}"),
                source_range: SourceRange {
                    start: position,
                    end: position,
                },
            });
        }
    }
    Ok(occurrences)
}

fn utf16_offset(source: &str, byte: usize) -> Result<i64> {
    i64::try_from(source[..byte].encode_utf16().count())
        .map_err(|_| Error::Internal("Source offset overflow".into()))
}

/// Finalize one note after the caller's LAST source/root mutation, in its active
/// serialized writer transaction. The caller owns canonical repair, all-error
/// rollback and commit. Never call this from a read or retirement cleanup.
///
/// `final_source`, when supplied, MUST be the exact persisted final source in
/// this transaction, never metadata-only Note.content. None loads only this
/// note's current content. A source-length check is not a byte-identity proof.
/// Empty root sets need no source hydration. Already-ready current indexes are
/// left unchanged, but retirement and source-index readiness are still checked.
pub(crate) async fn rebuild_note_anchors(
    conn: &mut SqliteConnection,
    workspace: &WorkspaceId,
    note: &NoteId,
    final_source: Option<&str>,
) -> Result<()> {
    let (_, epochs) = head(conn, workspace, note).await?;
    #[cfg(test)]
    if let Ok(pause) = FINALIZER_PAUSE.try_with(std::sync::Arc::clone) {
        *pause.observed.lock().expect("finalizer observation") = Some(epochs.clone());
        pause.entered.notify_one();
        pause.release.notified().await;
    }
    let length: i64 = sqlx::query_scalar(
        "SELECT source_length FROM note_page_head WHERE workspace_id=? AND note_id=? \
         AND indexed_rev=current_rev AND current_rev=?",
    )
    .bind(workspace.as_str())
    .bind(note.as_str())
    .bind(epochs.source_revision)
    .fetch_optional(&mut *conn)
    .await
    .map_err(db_error)?
    .ok_or_else(stale)?;
    if epochs.anchors_ready {
        return Ok(());
    }
    // The comment PK guarantees uniqueness. Only JSON boolean true excludes a
    // root, matching the existing lenient isOrphaned policy; strings/numbers do
    // not. Malformed JSON fails the transaction rather than promoting an index.
    let roots: Vec<String> = sqlx::query_scalar(
        "SELECT id FROM comment WHERE workspace_id=? AND note_id=? AND parent_id IS NULL \
         AND COALESCE(json_type(extra_json,'$.isOrphaned'),'null')!='true' ORDER BY id",
    )
    .bind(workspace.as_str())
    .bind(note.as_str())
    .fetch_all(&mut *conn)
    .await
    .map_err(db_error)?;
    let occurrences = if roots.is_empty() {
        Vec::new()
    } else {
        let stored;
        let source = if let Some(source) = final_source {
            source
        } else {
            stored = sqlx::query_scalar::<_, String>(
                "SELECT content FROM note WHERE workspace_id=? AND id=? AND rev=?",
            )
            .bind(workspace.as_str())
            .bind(note.as_str())
            .bind(epochs.source_revision)
            .fetch_optional(&mut *conn)
            .await
            .map_err(db_error)?
            .ok_or_else(stale)?;
            &stored
        };
        if utf16_offset(source, source.len())? != length {
            return Err(invalid());
        }
        let ids: Vec<&str> = roots.iter().map(String::as_str).collect();
        anchor_occurrences(source, &ids)?
    };
    publish_anchors_in_transaction(conn, workspace, note, &epochs, &occurrences).await
}

/// Startup-only repair of derived readiness. The keyset retains one note's
/// identity and body at a time; retirement is durable and must not be resumed
/// by publishing partial annotations. This remains write-sized backfill work.
pub(crate) async fn rebuild_pending_source_anchors(conn: &mut SqliteConnection) -> Result<()> {
    let mut after = 0_i64;
    loop {
        let next: Option<(i64, String, String)> = sqlx::query_as(
            "SELECT h.id,h.workspace_id,h.note_id FROM note_annotation_head h WHERE h.id>? AND h.anchors_rev!=h.source_rev AND NOT EXISTS(SELECT 1 FROM note_annotation_workspace_retirement r WHERE r.workspace_id=h.workspace_id) ORDER BY h.id LIMIT 1"
        ).bind(after).fetch_optional(&mut *conn).await.map_err(db_error)?;
        let Some((id, workspace, note)) = next else {
            return Ok(());
        };
        rebuild_note_anchors(conn, &WorkspaceId(workspace), &NoteId(note), None).await?;
        after = id;
    }
}

#[cfg(test)]
#[path = "source_anchors_tests.rs"]
mod tests;
