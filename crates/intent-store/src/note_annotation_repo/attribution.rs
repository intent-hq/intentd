use super::{
    db_error, head, invalid, stale, validate_limit, validate_ranges, AnnotationEpochs,
    AnnotationPage, SourceRange, MAX_OFFSET,
};
use crate::Store;
use intent_core::LineAttributionData;
use intent_core::{Error, NoteId, Result, WorkspaceId};
use sqlx::QueryBuilder;
use sqlx::Row;

/// A computation ticket is superseded when another computation starts, even
/// when source revision stays the same. Only its owner may publish that result.
#[derive(Clone, Debug)]
pub struct AttributionJob {
    pub epochs: AnnotationEpochs,
    workspace_id: WorkspaceId,
    note_id: NoteId,
}

/// Bounded row projection. Author details are addressed by `(generation,line)`;
/// potentially large author labels are never decoded on the range path.
#[derive(Clone, Debug)]
pub struct AttributionRow {
    pub line: i64,
    pub source_range: SourceRange,
    pub timestamp: i64,
    pub has_author: bool,
}

pub(super) fn range_query(
    id: i64,
    ranges: &[SourceRange],
    after_line: Option<i64>,
    take: i64,
) -> QueryBuilder<'static, sqlx::Sqlite> {
    let mut sql = QueryBuilder::new("SELECT line,start,end,timestamp,has_author FROM (");
    for (index, range) in ranges.iter().enumerate() {
        if index > 0 {
            sql.push(" UNION ");
        }
        sql.push("SELECT * FROM (SELECT line,start,end,timestamp,has_author FROM note_attribution_line WHERE head_id=").push_bind(id)
            .push(" AND line > ").push_bind(after_line.unwrap_or(0))
            .push(" AND line >= (SELECT line FROM note_attribution_line WHERE head_id=").push_bind(id)
            .push(" AND end > ").push_bind(range.start).push(" ORDER BY end,start,line LIMIT 1)")
            .push(" AND line <= (SELECT line FROM note_attribution_line WHERE head_id=").push_bind(id)
            .push(" AND start < ").push_bind(range.end).push(" ORDER BY start DESC,line DESC LIMIT 1)")
            .push(" ORDER BY line LIMIT ").push_bind(take).push(")");
    }
    sql.push(") ORDER BY line LIMIT ").push_bind(take);
    sql
}

impl Store {
    /// Reserve a generation before computing attribution from this revision.
    ///
    /// # Errors
    /// Returns a stale-page error on revision mismatch, or a database error.
    pub async fn begin_note_attribution(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
        source_revision: i64,
    ) -> Result<AttributionJob> {
        let mut tx = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db_error)?;
        let (id, epochs) = head(&mut tx, workspace_id, note_id).await?;
        if epochs.source_revision != source_revision {
            return Err(stale());
        }
        sqlx::query("UPDATE note_annotation_head SET attribution_generation=lower(hex(randomblob(16))),attribution_rev=-1 WHERE id=?")
            .bind(id).execute(&mut *tx).await.map_err(db_error)?;
        let epochs = head(&mut tx, workspace_id, note_id).await?.1;
        tx.commit().await.map_err(db_error)?;
        Ok(AttributionJob {
            epochs,
            workspace_id: workspace_id.clone(),
            note_id: note_id.clone(),
        })
    }

    /// Publish normalized lines and the compatible legacy snapshot atomically.
    /// Work proportional to the complete computation belongs here, never in a
    /// range read. Checks source content as well as the generation ticket.
    ///
    /// # Errors
    /// Returns stale on a superseded ticket, invalid params on inconsistent
    /// computation input, or an encoding/database error. Failure rolls back.
    pub async fn publish_note_attribution(
        &self,
        job: &AttributionJob,
        source: &str,
        data: &LineAttributionData,
    ) -> Result<()> {
        if data.workspace_id != job.workspace_id || data.note_id != job.note_id {
            return Err(invalid());
        }
        let legacy = serde_json::to_string(&data.attributions)
            .map_err(|e| Error::Internal(format!("encode attribution: {e}")))?;
        let mut tx = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db_error)?;
        let (id, epochs) = head(&mut tx, &job.workspace_id, &job.note_id).await?;
        if epochs.source_revision != job.epochs.source_revision
            || epochs.attribution_generation != job.epochs.attribution_generation
            || epochs.attribution_ready
        {
            return Err(stale());
        }
        let matches: bool =
            sqlx::query_scalar("SELECT content = ? FROM note WHERE workspace_id=? AND id=?")
                .bind(source)
                .bind(job.workspace_id.as_str())
                .bind(job.note_id.as_str())
                .fetch_one(&mut *tx)
                .await
                .map_err(db_error)?;
        if !matches {
            return Err(invalid());
        }
        sqlx::query("DELETE FROM note_attribution_line WHERE head_id=?")
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
        let mut offset = 0_i64;
        let mut inserted = 0;
        let mut lines = source.split('\n').enumerate().peekable();
        while let Some((index, text)) = lines.next() {
            let length = i64::try_from(text.encode_utf16().count()).map_err(|_| invalid())?;
            let end = offset
                .checked_add(length)
                .and_then(|n| n.checked_add(i64::from(lines.peek().is_some())))
                .ok_or_else(invalid)?;
            if end > MAX_OFFSET {
                return Err(invalid());
            }
            let line = i64::try_from(index + 1).map_err(|_| invalid())?;
            if let Some(info) = data.attributions.get(&line.to_string()) {
                let author = info
                    .author
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()
                    .map_err(|e| Error::Internal(format!("encode attribution author: {e}")))?;
                sqlx::query("INSERT INTO note_attribution_line(head_id,line,start,end,timestamp,has_author) VALUES(?,?,?,?,?,?)")
                    .bind(id).bind(line).bind(offset).bind(end).bind(info.timestamp).bind(author.is_some())
                    .execute(&mut *tx).await.map_err(db_error)?;
                if let Some(author) = author {
                    sqlx::query("INSERT INTO note_attribution_author(head_id,line,author_json) VALUES(?,?,?)")
                        .bind(id).bind(line).bind(author).execute(&mut *tx).await.map_err(db_error)?;
                }
                inserted += 1;
            }
            offset = end;
        }
        if inserted != data.attributions.len() {
            return Err(invalid());
        }
        sqlx::query("INSERT INTO note_line_attribution(note_id,workspace_id,computed_at,attributions_json) VALUES(?,?,?,?) ON CONFLICT(workspace_id,note_id) DO UPDATE SET computed_at=excluded.computed_at,attributions_json=excluded.attributions_json")
            .bind(job.note_id.as_str()).bind(job.workspace_id.as_str()).bind(&data.computed_at).bind(legacy)
            .execute(&mut *tx).await.map_err(db_error)?;
        sqlx::query("UPDATE note_annotation_head SET attribution_rev=source_rev,attribution_generation=? WHERE id=?")
            .bind(&job.epochs.attribution_generation).bind(id)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
        tx.commit().await.map_err(db_error)
    }

    /// Read disjoint visible intervals using indexed line endpoints. The SQL
    /// UNION deduplicates lines touched by more than one viewport interval.
    /// `after_line` is a storage keyset; transport must bind it in its cursor.
    ///
    /// # Errors
    /// Returns invalid params for bad ranges/limits, stale for mismatched
    /// epochs, or a database error. Pending attribution has an empty page.
    pub async fn read_attribution_rows(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
        expected: &AnnotationEpochs,
        ranges: &[SourceRange],
        after_line: Option<i64>,
        limit: usize,
    ) -> Result<AnnotationPage<AttributionRow>> {
        validate_ranges(ranges)?;
        let take = validate_limit(limit)?;
        if after_line.is_some_and(|line| line < 0) {
            return Err(invalid());
        }
        let mut tx = self.read_pool().begin().await.map_err(db_error)?;
        let (id, epochs) = head(&mut tx, workspace_id, note_id).await?;
        if epochs.source_revision != expected.source_revision
            || epochs.attribution_generation != expected.attribution_generation
        {
            return Err(stale());
        }
        let mut items = Vec::new();
        if epochs.attribution_ready && !ranges.is_empty() {
            let mut sql = range_query(id, ranges, after_line, take);
            let rows = sql.build().fetch_all(&mut *tx).await.map_err(db_error)?;
            items = rows
                .iter()
                .map(|row| AttributionRow {
                    line: row.get("line"),
                    source_range: SourceRange {
                        start: row.get("start"),
                        end: row.get("end"),
                    },
                    timestamp: row.get("timestamp"),
                    has_author: row.get("has_author"),
                })
                .collect();
        }
        let has_more = items.len() > limit;
        items.truncate(limit);
        tx.commit().await.map_err(db_error)?;
        Ok(AnnotationPage {
            epochs,
            items,
            has_more,
        })
    }
}
