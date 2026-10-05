//! External search-selection normalization, not mutation/selection upload rewriting.
//!
//! The caller holds the SAME writer transaction as verified manifest/frozen-view
//! preparation and must roll back all errors. Original scope/header/root, current
//! authorization and original expiry remain the enclosing seal's responsibility.
//! Uploaded JSON and its hashes are never updated. Work/storage are O(range count)
//! rows and indexed insertion/seeks; resident memory is one <=64KiB record plus
//! bounded source pieces and one pending interval, with no total-result cap.
use super::{seal::PreparedView, view_read};
use intent_core::{
    note_mutation::NoteMutationError,
    note_stage::{
        NoteStageHeader, NoteStageOutput, NoteStageRecord, NoteStageSearchMode, NoteStageSelection,
    },
    Error, Result,
};
use sqlx::SqliteConnection;

const SAFE: u64 = 9_007_199_254_740_991;
const UPLOADED: &str = "SELECT chunk_sequence,ordinal,CASE WHEN length(CAST(value AS BLOB))<=65536 THEN value ELSE NULL END FROM note_stage_record WHERE operation_key=? AND stream='selection' AND (chunk_sequence,ordinal)>(?,?) ORDER BY chunk_sequence,ordinal LIMIT 1";
const SORTED: &str = "SELECT start,end,ordinal FROM note_stage_search_input WHERE operation_key=? AND generation=? AND (start,end,ordinal)>(?,?,?) ORDER BY start,end,ordinal LIMIT 1";
const FIRST: &str = "SELECT start,end FROM note_stage_search_range WHERE operation_key=? AND generation=? ORDER BY start LIMIT 1";
const NEXT: &str = "SELECT start,end FROM note_stage_search_range WHERE operation_key=? AND generation=? AND start>? ORDER BY start LIMIT 1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SearchInterval {
    pub(super) start: u64,
    pub(super) end: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SearchRangeSummary {
    pub(super) generation: u64,
    pub(super) length: u64,
    pub(super) intervals: u64,
    pub(super) selected_units: u64,
}
fn invalid() -> Error {
    Error::NoteMutation(NoteMutationError::Invalid)
}
fn db(error: impl std::fmt::Display) -> Error {
    Error::Internal(format!("staged search ranges: {error}"))
}
fn integer(value: u64) -> Result<i64> {
    if value > SAFE {
        return Err(invalid());
    }
    i64::try_from(value).map_err(db)
}
fn number(value: i64) -> Result<u64> {
    u64::try_from(value)
        .ok()
        .filter(|n| *n <= SAFE)
        .ok_or_else(invalid)
}

async fn binding(
    conn: &mut SqliteConnection,
    operation: &str,
    generation: u64,
    length: u64,
) -> Result<()> {
    integer(generation)?;
    integer(length)?;
    // Normalization belongs to the final prepared dirty generation, not an older
    // same-length generation. The enclosing operation prevents further writes.
    let stored: Option<(i64,i64)>=sqlx::query_as("SELECT generation,length FROM note_stage_view WHERE operation_key=? ORDER BY generation DESC LIMIT 1")
        .bind(operation).fetch_optional(&mut *conn).await.map_err(db)?;
    if stored != Some((integer(generation)?, integer(length)?)) {
        return Err(invalid());
    }
    // Even an empty view must have this operation's retained root binding.
    view_read::read_piece(conn, operation, generation, length, length, 4).await?;
    Ok(())
}

/// Validate every endpoint against the frozen source, then externally sort and
/// union only Source-search selections. Touching intervals are ONE matcher span;
/// a consumer resets its matcher only between returned (strictly separated) rows.
/// This helper neither seals nor authorizes an operation. Both tables must be
/// initially empty for the operation; interrupted attempts roll back externally.
pub(super) async fn normalize(
    conn: &mut SqliteConnection,
    operation: &str,
    header: &NoteStageHeader,
    view: &PreparedView,
) -> Result<SearchRangeSummary> {
    header.validate().map_err(Error::NoteMutation)?;
    if header.output != NoteStageOutput::Search
        || header
            .query
            .as_ref()
            .is_none_or(|q| q.mode != NoteStageSearchMode::Source)
    {
        return Err(invalid());
    }
    binding(conn, operation, view.generation, view.length).await?;
    for table in ["note_stage_search_input", "note_stage_search_range"] {
        let exists: Option<i64> = sqlx::query_scalar(&format!(
            "SELECT 1 FROM {table} WHERE operation_key=? LIMIT 1"
        ))
        .bind(operation)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db)?;
        if exists.is_some() {
            return Err(invalid());
        }
    }
    let mut summary = SearchRangeSummary {
        generation: view.generation,
        length: view.length,
        intervals: 0,
        selected_units: 0,
    };
    let mut after = (-1, -1);
    let mut ordinal = 0u64;
    while let Some((chunk, slot, encoded)) =
        sqlx::query_as::<_, (i64, i64, Option<String>)>(UPLOADED)
            .bind(operation)
            .bind(after.0)
            .bind(after.1)
            .fetch_optional(&mut *conn)
            .await
            .map_err(db)?
    {
        number(chunk)?;
        number(slot)?;
        if header.selection != NoteStageSelection::Ranges {
            return Err(invalid());
        }
        let record: NoteStageRecord =
            serde_json::from_str(&encoded.ok_or_else(invalid)?).map_err(db)?;
        let NoteStageRecord::Range {
            ordinal: got,
            start,
            end,
            ..
        } = record
        else {
            return Err(invalid());
        };
        if got != ordinal || start > end || end > view.length {
            return Err(invalid());
        }
        integer(start)?;
        integer(end)?;
        // Point selections are validated too; a point inside a surrogate pair is
        // not silently discarded into an otherwise accepted operation.
        view_read::read_piece(conn, operation, view.generation, view.length, start, 4).await?;
        view_read::read_piece(conn, operation, view.generation, view.length, end, 4).await?;
        if start < end {
            sqlx::query("INSERT INTO note_stage_search_input(operation_key,generation,start,end,ordinal) VALUES(?,?,?,?,?)")
                .bind(operation).bind(integer(view.generation)?).bind(integer(start)?).bind(integer(end)?).bind(integer(ordinal)?)
                .execute(&mut *conn).await.map_err(db)?;
        }
        ordinal = ordinal
            .checked_add(1)
            .filter(|n| *n <= SAFE)
            .ok_or_else(invalid)?;
        after = (chunk, slot);
    }
    if header.selection == NoteStageSelection::All {
        if view.length > 0 {
            emit(
                conn,
                operation,
                SearchInterval {
                    start: 0,
                    end: view.length,
                },
                &mut summary,
            )
            .await?;
        }
        return Ok(summary);
    }
    let mut sorted_after = (-1, -1, -1);
    let mut pending: Option<SearchInterval> = None;
    while let Some((start, end, ordinal)) = sqlx::query_as::<_, (i64, i64, i64)>(SORTED)
        .bind(operation)
        .bind(integer(view.generation)?)
        .bind(sorted_after.0)
        .bind(sorted_after.1)
        .bind(sorted_after.2)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db)?
    {
        let next = SearchInterval {
            start: number(start)?,
            end: number(end)?,
        };
        match pending {
            Some(mut previous) if next.start <= previous.end => {
                previous.end = previous.end.max(next.end);
                pending = Some(previous);
            }
            Some(previous) => {
                emit(conn, operation, previous, &mut summary).await?;
                pending = Some(next);
            }
            None => pending = Some(next),
        }
        sorted_after = (start, end, ordinal);
    }
    if let Some(last) = pending {
        emit(conn, operation, last, &mut summary).await?;
    }
    Ok(summary)
}
async fn emit(
    conn: &mut SqliteConnection,
    operation: &str,
    interval: SearchInterval,
    summary: &mut SearchRangeSummary,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO note_stage_search_range(operation_key,generation,start,end) VALUES(?,?,?,?)",
    )
    .bind(operation)
    .bind(integer(summary.generation)?)
    .bind(integer(interval.start)?)
    .bind(integer(interval.end)?)
    .execute(conn)
    .await
    .map_err(db)?;
    summary.intervals = summary
        .intervals
        .checked_add(1)
        .filter(|n| *n <= SAFE)
        .ok_or_else(invalid)?;
    summary.selected_units = summary
        .selected_units
        .checked_add(interval.end - interval.start)
        .filter(|n| *n <= summary.length)
        .ok_or_else(invalid)?;
    Ok(())
}

/// One indexed normalized interval. Caller binds cursor/operation/generation and
/// lifetime; this function alone is not public sealed-state admission. Returned
/// starts increase strictly and intervals never touch, so no match crosses a gap.
pub(super) async fn next_interval(
    conn: &mut SqliteConnection,
    operation: &str,
    generation: u64,
    length: u64,
    after_start: Option<u64>,
) -> Result<Option<SearchInterval>> {
    binding(conn, operation, generation, length).await?;
    let query = if after_start.is_some() { NEXT } else { FIRST };
    let mut query = sqlx::query_as::<_, (i64, i64)>(query)
        .bind(operation)
        .bind(integer(generation)?);
    if let Some(after) = after_start {
        if after > length {
            return Err(invalid());
        }
        query = query.bind(integer(after)?);
    }
    let row = query.fetch_optional(conn).await.map_err(db)?;
    row.map(|(start, end)| {
        let interval = SearchInterval {
            start: number(start)?,
            end: number(end)?,
        };
        if interval.start >= interval.end || interval.end > length {
            return Err(invalid());
        }
        Ok(interval)
    })
    .transpose()
}

#[cfg(test)]
#[path = "search_ranges_tests.rs"]
mod tests;
