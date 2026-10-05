//! Stream inverse coordinates from captured edits, never from a text diff.
//!
//! Caller owns one immutable writer transaction, exact sealed operation/root,
//! verified records, authorization and original expiry. Writer retains source
//! and provenance, assigns receipt state tokens, and publishes atomically.
//! Canonical phases compose with the newest user group via `MappingCursor` and
//! that group's `NoteSourceHistory` mapping; earlier groups remain separate.
use intent_core::{
    note_mutation::{NoteMutationError, NoteSpliceMapping},
    note_stage::NoteStageRecord,
    Error, Result,
};
use sqlx::{Row, SqliteConnection};

const SAFE: u64 = 9_007_199_254_740_991;
const PREVIOUS_RECORD: &str = "SELECT chunk_sequence,ordinal,CASE WHEN length(CAST(value AS BLOB))<=65536 THEN value ELSE NULL END AS value FROM note_stage_record WHERE operation_key=? AND stream='dirty' AND (chunk_sequence,ordinal)<(?,?) ORDER BY chunk_sequence DESC,ordinal DESC LIMIT 1";
const NEXT_RECORD: &str = "SELECT chunk_sequence,ordinal,CASE WHEN length(CAST(value AS BLOB))<=65536 THEN value ELSE NULL END AS value FROM note_stage_record WHERE operation_key=? AND stream='dirty' AND (chunk_sequence,ordinal)>=(?,?) AND (chunk_sequence,ordinal)<=(?,?) ORDER BY chunk_sequence,ordinal LIMIT 1";
fn invalid() -> Error {
    Error::NoteMutation(NoteMutationError::Invalid)
}
fn db(error: impl std::fmt::Display) -> Error {
    Error::Internal(format!("staged inverse: {error}"))
}
fn number(value: i64) -> Result<u64> {
    u64::try_from(value)
        .ok()
        .filter(|n| *n <= SAFE)
        .ok_or_else(invalid)
}
fn add(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b).filter(|n| *n <= SAFE).ok_or_else(invalid)
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct RecordKey(i64, i64);
#[derive(Debug)]
struct Record {
    key: RecordKey,
    local_sequence: u64,
    ordinal: u64,
    mapping: NoteSpliceMapping,
}
fn record(row: &sqlx::sqlite::SqliteRow) -> Result<Record> {
    let key = RecordKey(
        row.try_get("chunk_sequence").map_err(db)?,
        row.try_get("ordinal").map_err(db)?,
    );
    number(key.0)?;
    number(key.1)?;
    let value: Option<String> = row.try_get("value").map_err(db)?;
    let NoteStageRecord::Splice {
        local_sequence: Some(local_sequence),
        ordinal,
        start,
        end,
        replacement,
    } = serde_json::from_str(&value.ok_or_else(invalid)?).map_err(db)?
    else {
        return Err(invalid());
    };
    if local_sequence > SAFE
        || ordinal > SAFE
        || start > end
        || end > SAFE
        || replacement.length > SAFE
    {
        return Err(invalid());
    }
    Ok(Record {
        key,
        local_sequence,
        ordinal,
        mapping: NoteSpliceMapping {
            start,
            end,
            inserted_length: replacement.length,
        },
    })
}
/// One retained dirty group, with no text or whole-group record collection.
#[derive(Debug)]
pub(super) struct CapturedGroup {
    pub generation: u64,
    pub input_generation: u64,
    pub history_group: String,
    pub input_length: u64,
    pub output_length: u64,
    first: RecordKey,
    last: RecordKey,
}
/// Reverse chronological view/record walker. One reverse boundary pass and one
/// forward emission pass per group: O(records + groups) work, a constant number
/// of <=64KiB envelopes in memory. No repeated scan of the earlier prefix.
pub(super) struct GroupWalk {
    generation: u64,
    before: RecordKey,
}
impl GroupWalk {
    pub fn new(latest_generation: u64) -> Result<Self> {
        if latest_generation > SAFE {
            return Err(invalid());
        }
        Ok(Self {
            generation: latest_generation,
            before: RecordKey(i64::MAX, i64::MAX),
        })
    }
}
async fn previous_record(
    conn: &mut SqliteConnection,
    operation: &str,
    before: RecordKey,
) -> Result<Option<Record>> {
    sqlx::query(PREVIOUS_RECORD)
        .bind(operation)
        .bind(before.0)
        .bind(before.1)
        .fetch_optional(conn)
        .await
        .map_err(db)?
        .as_ref()
        .map(record)
        .transpose()
}
/// Start with the exact sealed latest generation and use the same operation
/// throughout. Cursor advancement happens only after successful discovery.
pub(super) async fn previous_group(
    conn: &mut SqliteConnection,
    operation: &str,
    walk: &mut GroupWalk,
) -> Result<Option<CapturedGroup>> {
    let newest = previous_record(conn, operation, walk.before).await?;
    if walk.generation == 0 {
        if newest.is_some() {
            return Err(invalid());
        }
        return Ok(None);
    }
    let row = sqlx::query("SELECT v.input_generation,v.history_group,v.length,p.length AS input_length FROM note_stage_view v JOIN note_stage_view p ON p.operation_key=v.operation_key AND p.generation=v.input_generation WHERE v.operation_key=? AND v.generation=?")
        .bind(operation).bind(i64::try_from(walk.generation).map_err(db)?).fetch_optional(&mut *conn).await.map_err(db)?.ok_or_else(invalid)?;
    let input_generation = number(row.try_get("input_generation").map_err(db)?)?;
    let history_group: String = row.try_get("history_group").map_err(db)?;
    let input_length = number(row.try_get("input_length").map_err(db)?)?;
    let output_length = number(row.try_get("length").map_err(db)?)?;
    if add(input_generation, 1)? != walk.generation
        || history_group.is_empty()
        || history_group.len() > 256
    {
        return Err(invalid());
    }
    let newest = newest.ok_or_else(invalid)?;
    // Seal's exact localSequence encoding, not a numeric interpretation of
    // public receipt historyGroup identities such as "0" versus "00".
    if newest.local_sequence.to_string() != history_group {
        return Err(invalid());
    }
    let sequence = newest.local_sequence;
    let last = newest.key;
    let mut first = last;
    let mut ordinal = newest.ordinal;
    while let Some(prior) = previous_record(conn, operation, first).await? {
        if prior.local_sequence != sequence {
            if prior.local_sequence >= sequence {
                return Err(invalid());
            }
            break;
        }
        if add(prior.ordinal, 1)? != ordinal {
            return Err(invalid());
        }
        ordinal = prior.ordinal;
        first = prior.key;
    }
    if ordinal != 0 {
        return Err(invalid());
    }
    let group = CapturedGroup {
        generation: walk.generation,
        input_generation,
        history_group,
        input_length,
        output_length,
        first,
        last,
    };
    walk.generation = input_generation;
    walk.before = first;
    Ok(Some(group))
}
/// UTF16 inverse coordinates. Replacement bytes come from the original input
/// generation's `[replacement_start,replacement_end)`, including scalar boundaries
/// already verified at seal; this helper does not supply marker authority.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct InverseRange {
    pub ordinal: u64,
    pub start: u64,
    pub end: u64,
    pub replacement_start: u64,
    pub replacement_end: u64,
}
/// Feed one group's exact provenance mappings in ascending input order. A
/// newest mapping may include canonical edits outside the user's splice range.
/// Explicit same-byte replacements keep identity; no text comparison is used.
#[derive(Clone, Debug)]
pub(super) struct MappingCursor {
    input_length: u64,
    consumed: u64,
    output: u64,
    ordinal: u64,
}
impl MappingCursor {
    pub fn new(input_length: u64) -> Result<Self> {
        if input_length > SAFE {
            return Err(invalid());
        }
        Ok(Self {
            input_length,
            consumed: 0,
            output: 0,
            ordinal: 0,
        })
    }
    pub fn push(&mut self, item: &NoteSpliceMapping) -> Result<InverseRange> {
        if item.start < self.consumed || item.start > item.end || item.end > self.input_length {
            return Err(invalid());
        }
        let start = add(self.output, item.start - self.consumed)?;
        let end = add(start, item.inserted_length)?;
        let next_ordinal = add(self.ordinal, 1)?;
        let inverse = InverseRange {
            ordinal: self.ordinal,
            start,
            end,
            replacement_start: item.start,
            replacement_end: item.end,
        };
        self.consumed = item.end;
        self.output = end;
        self.ordinal = next_ordinal;
        Ok(inverse)
    }
    pub fn finish(&self, output_length: u64) -> Result<()> {
        if add(self.output, self.input_length - self.consumed)? != output_length {
            return Err(invalid());
        }
        Ok(())
    }
}
pub(super) struct InverseCursor {
    next: RecordKey,
    mapping: MappingCursor,
    done: bool,
}
impl CapturedGroup {
    pub fn cursor(&self) -> Result<InverseCursor> {
        Ok(InverseCursor {
            next: self.first,
            mapping: MappingCursor::new(self.input_length)?,
            done: false,
        })
    }
}
/// Drain through None before publication and roll back the outer transaction on
/// any error. Mapping cursor is not advanced if a record/length check fails.
pub(super) async fn next_inverse(
    conn: &mut SqliteConnection,
    operation: &str,
    group: &CapturedGroup,
    cursor: &mut InverseCursor,
) -> Result<Option<InverseRange>> {
    if cursor.done {
        cursor.mapping.finish(group.output_length)?;
        return Ok(None);
    }
    let row = sqlx::query(NEXT_RECORD)
        .bind(operation)
        .bind(cursor.next.0)
        .bind(cursor.next.1)
        .bind(group.last.0)
        .bind(group.last.1)
        .fetch_optional(conn)
        .await
        .map_err(db)?
        .ok_or_else(invalid)?;
    let item = record(&row)?;
    if item.local_sequence.to_string() != group.history_group
        || item.ordinal != cursor.mapping.ordinal
    {
        return Err(invalid());
    }
    let mut mapping = cursor.mapping.clone();
    let inverse = mapping.push(&item.mapping)?;
    let done = item.key == group.last;
    if done {
        mapping.finish(group.output_length)?;
    }
    let next = RecordKey(item.key.0, item.key.1.checked_add(1).ok_or_else(invalid)?);
    cursor.mapping = mapping;
    cursor.next = next;
    cursor.done = done;
    Ok(Some(inverse))
}
#[cfg(test)]
#[path = "staged_inverse_tests.rs"]
mod tests;
